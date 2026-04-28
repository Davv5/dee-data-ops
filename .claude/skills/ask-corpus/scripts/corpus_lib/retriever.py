"""nlm CLI subprocess wrappers.

Two retrieval primitives:
- `query_one(notebook_id, search_query)` -> list[RawHit] via `nlm notebook query --json`
- `list_sources(notebook_id)` -> dict[source_id, title] via `nlm source list --json`,
  cached per process.

The actual `nlm notebook query --json` response shape (verified at U3 time):

    {
      "value": {
        "answer": "<markdown answer with [1, 2] inline citations>",
        "conversation_id": "...",
        "sources_used": ["uuid1", "uuid2", ...],
        "citations": {"1": "uuid1", "2": "uuid2", ...}
      }
    }

There is no per-source `cited_text` field — we extract snippets from the
answer text by finding the sentences that contain each citation marker.
That makes corpus retrieval different in kind from last30days (which has
explicit per-item bodies); we get LLM-curated explanation text per citation.

A RawHit is the canonical shape passed to normalize.py:
    {source_id, citation_numbers, snippet, scope, raw_answer}
"""

from __future__ import annotations

import json
import re
import subprocess
import time
from dataclasses import dataclass
from typing import Any

from .log import trace

# Default timeout for nlm subprocess calls. nlm itself has a 120s default for
# `query`, but we wrap with a slightly higher subprocess deadline so the
# inner timeout fires first with a clean error message.
DEFAULT_QUERY_TIMEOUT_SEC = 130
DEFAULT_LIST_TIMEOUT_SEC = 30

# Process-wide source-list cache. Keyed by notebook_id. Entries are
# dict[source_id, title]. Invalidated on process restart only — for v2.0
# the staleness window is bounded to one engine invocation.
_SOURCE_LIST_CACHE: dict[str, dict[str, str]] = {}


@dataclass
class RawHit:
    """One pre-normalize source citation extracted from an nlm answer.

    citation_numbers preserves which inline `[N]` markers the source was
    referenced under (one source can cover multiple citation numbers).
    """

    source_id: str
    scope: str
    snippet: str
    citation_numbers: list[int]
    raw_answer: str  # full answer text — useful for downstream context

    def to_dict(self) -> dict[str, Any]:
        return {
            "source_id": self.source_id,
            "scope": self.scope,
            "snippet": self.snippet,
            "citation_numbers": self.citation_numbers,
            "raw_answer": self.raw_answer,
        }


class RetrievalError(RuntimeError):
    """Raised when the nlm subprocess fails after retry."""


def query_one(
    notebook_id: str,
    search_query: str,
    *,
    scope: str = "",
    timeout: int = DEFAULT_QUERY_TIMEOUT_SEC,
) -> list[RawHit]:
    """Query one notebook and extract per-source RawHits from the answer.

    Args:
        notebook_id: NotebookLM UUID.
        search_query: keyword-style query string passed to nlm.
        scope: scope label (e.g. "methodology.data_ops") to tag hits with.
            Optional; passed through to RawHit.scope for downstream fan-in.
        timeout: subprocess deadline in seconds.

    Returns:
        List of RawHit, one per unique source_id referenced in the answer.

    Raises:
        RetrievalError: if the subprocess fails after one retry.
    """
    started = time.time()
    cmd = [
        "nlm",
        "notebook",
        "query",
        "--json",
        notebook_id,
        search_query,
    ]
    payload = _run_with_retry(cmd, timeout=timeout, scope=scope, op="query")
    hits = _extract_hits_from_answer(payload, scope=scope)
    elapsed_ms = int((time.time() - started) * 1000)
    trace(
        "Retriever",
        f"scope={scope or notebook_id} hits={len(hits)} took={elapsed_ms}ms",
    )
    return hits


def list_sources(
    notebook_id: str,
    *,
    timeout: int = DEFAULT_LIST_TIMEOUT_SEC,
) -> dict[str, str]:
    """List notebook sources, returning {source_id: title}. Cached per process.

    Lazy: should be called only when titles are actually needed (post-rerank
    for synthesis). The hot retrieval path stays single-API-call per scope.

    Args:
        notebook_id: NotebookLM UUID.
        timeout: subprocess deadline in seconds.

    Returns:
        dict mapping source_id -> source title. Untitled sources fall back
        to the source_id string.

    Raises:
        RetrievalError: if the subprocess fails after one retry.
    """
    cached = _SOURCE_LIST_CACHE.get(notebook_id)
    if cached is not None:
        trace("SourceList", f"notebook={notebook_id} n={len(cached)} cached=true")
        return cached

    started = time.time()
    cmd = [
        "nlm",
        "source",
        "list",
        "--json",
        notebook_id,
    ]
    payload = _run_with_retry(cmd, timeout=timeout, scope=notebook_id, op="source_list")
    if not isinstance(payload, list):
        raise RetrievalError(
            f"nlm source list returned non-list payload for {notebook_id}: "
            f"{type(payload).__name__}"
        )
    titles = {
        str(entry.get("id", "")): str(entry.get("title", "") or entry.get("id", ""))
        for entry in payload
        if isinstance(entry, dict) and entry.get("id")
    }
    _SOURCE_LIST_CACHE[notebook_id] = titles
    elapsed_ms = int((time.time() - started) * 1000)
    trace(
        "SourceList",
        f"notebook={notebook_id} n={len(titles)} took={elapsed_ms}ms cached=false",
    )
    return titles


def clear_source_list_cache() -> None:
    """Test helper — clears the process-wide cache."""
    _SOURCE_LIST_CACHE.clear()


# ---------------------------------------------------------------------------
# internals


def _run_with_retry(
    cmd: list[str],
    *,
    timeout: int,
    scope: str,
    op: str,
) -> Any:
    """Run nlm and parse JSON stdout. Retry once on transient failure.

    Treats subprocess.TimeoutExpired and nonzero exit codes as transient on
    first attempt; the second failure surfaces as RetrievalError.
    """
    last_exc: Exception | None = None
    for attempt in (1, 2):
        try:
            result = subprocess.run(  # noqa: S603 - args are fixed-shape internal
                cmd,
                capture_output=True,
                text=True,
                timeout=timeout,
                check=False,
            )
        except subprocess.TimeoutExpired as exc:
            last_exc = exc
            trace("Retriever", f"op={op} scope={scope} attempt={attempt} TIMEOUT")
            if attempt == 1:
                time.sleep(1.0)
                continue
            raise RetrievalError(
                f"nlm {op} for scope={scope} timed out after retry"
            ) from exc

        if result.returncode != 0:
            last_exc = RuntimeError(
                f"nlm exit={result.returncode} stderr={result.stderr[:200]!r}"
            )
            trace(
                "Retriever",
                f"op={op} scope={scope} attempt={attempt} exit={result.returncode}",
            )
            if attempt == 1:
                time.sleep(1.0)
                continue
            raise RetrievalError(
                f"nlm {op} for scope={scope} failed after retry: {last_exc}"
            )

        if not result.stdout.strip():
            return [] if op == "source_list" else {}

        try:
            return json.loads(result.stdout)
        except json.JSONDecodeError as exc:
            raise RetrievalError(
                f"nlm {op} for scope={scope} returned non-JSON stdout: "
                f"{result.stdout[:200]!r}"
            ) from exc

    # Unreachable — both attempts have raised by this point.
    raise RetrievalError(f"nlm {op} for scope={scope} failed: {last_exc}")


# Sentence splitter that handles the common case in nlm answers: ". " followed
# by uppercase letter or `*` (markdown bullet) or newline. Not exhaustive —
# good enough for v2.0 snippet extraction.
_SENTENCE_BOUNDARY = re.compile(r"(?<=[.!?])\s+(?=[A-Z*\n])")


def _extract_hits_from_answer(payload: Any, *, scope: str) -> list[RawHit]:
    """Parse nlm notebook query JSON into RawHits, one per unique source_id.

    Strategy:
      1. citations dict gives citation_number -> source_id mapping.
      2. For each citation marker `[N]` in the answer, find the sentence
         containing it.
      3. Group by source_id: snippet is `\\n\\n`.join(sentences) where the
         sentences are deduped while preserving order.
    """
    if not isinstance(payload, dict):
        return []
    value = payload.get("value")
    if not isinstance(value, dict):
        return []
    answer = value.get("answer", "") or ""
    citations = value.get("citations") or {}
    if not isinstance(citations, dict) or not answer:
        return []

    # Normalize citation_number keys to ints. nlm returns string keys.
    citation_to_source: dict[int, str] = {}
    for key, source_id in citations.items():
        try:
            citation_to_source[int(key)] = str(source_id)
        except (ValueError, TypeError):
            continue

    if not citation_to_source:
        return []

    # Split answer into sentences once.
    sentences = [s.strip() for s in _SENTENCE_BOUNDARY.split(answer) if s.strip()]

    # For each sentence, extract the set of citation numbers it mentions.
    # nlm uses three citation forms: single [1], list [1, 2], range [3-5],
    # and mixed [5, 15-17]. Parse all three.
    citation_marker_re = re.compile(r"\[\s*([\d,\s\-]+?)\s*\]")
    sentence_citations: list[set[int]] = []
    for sentence in sentences:
        matched: set[int] = set()
        for marker in citation_marker_re.finditer(sentence):
            for token in marker.group(1).split(","):
                token = token.strip()
                if not token:
                    continue
                if "-" in token:
                    parts = token.split("-", 1)
                    if len(parts) == 2 and parts[0].isdigit() and parts[1].isdigit():
                        start, end = int(parts[0]), int(parts[1])
                        if 0 < end - start < 100:  # sanity bound
                            matched.update(range(start, end + 1))
                elif token.isdigit():
                    matched.add(int(token))
        sentence_citations.append(matched)

    # Group sentences per source_id.
    per_source_sentences: dict[str, list[str]] = {}
    per_source_citations: dict[str, set[int]] = {}
    for sentence, marker_set in zip(sentences, sentence_citations, strict=False):
        for marker in marker_set:
            source_id = citation_to_source.get(marker)
            if not source_id:
                continue
            sentences_list = per_source_sentences.setdefault(source_id, [])
            if sentence not in sentences_list:
                sentences_list.append(sentence)
            per_source_citations.setdefault(source_id, set()).add(marker)

    hits: list[RawHit] = []
    for source_id, snippet_sentences in per_source_sentences.items():
        snippet = " ".join(snippet_sentences)
        if len(snippet) > 1200:
            snippet = snippet[:1197] + "..."
        hits.append(
            RawHit(
                source_id=source_id,
                scope=scope,
                snippet=snippet,
                citation_numbers=sorted(per_source_citations.get(source_id, set())),
                raw_answer=answer,
            )
        )
    return hits
