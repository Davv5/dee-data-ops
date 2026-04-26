"""Per-stream signal annotation.

Computes ``local_relevance`` (Jaccard token overlap between snippet and
ranking_query) and applies an optional prune floor. Per the plan's Key
Technical Decisions, this is a deliberately-simple scorer pinned in module
constants — it is a load-bearing tuning surface for U7's diversity guard
threshold and gets revisited before declaring U7 done.

Constants tuned at v2.0:

- ``RELEVANCE_PRUNE_FLOOR``: items with ``local_relevance`` strictly below
  this are dropped in ``prune_low_relevance``. Default 0.0 means
  "annotate, don't filter" — fusion handles thinning. Lift only if a
  measured question shows zero-overlap noise reaching the candidate pool.
- ``_MIN_TOKEN_LEN``: tokens shorter than this are ignored. 3 keeps "dbt",
  "sql", "ghl" as signal while dropping noise like "or", "is".
- ``_STOPWORDS``: small English function-word set; intentionally tight so
  domain terms ("model", "table") survive.
"""

from __future__ import annotations

import re

from .schema import SourceItem

RELEVANCE_PRUNE_FLOOR = 0.0
_MIN_TOKEN_LEN = 3

_STOPWORDS = frozenset(
    {
        "a", "an", "the", "is", "are", "was", "were", "be", "been", "being",
        "do", "does", "did", "have", "has", "had", "of", "in", "on", "at",
        "to", "for", "with", "by", "from", "as", "and", "or", "but", "if",
        "what", "how", "why", "which", "this", "that", "these", "those",
        "it", "its", "their", "they", "them", "we", "us", "our", "you",
        "your", "i", "my", "should", "would", "could", "can", "will", "may",
        "might", "shall", "about", "into", "through", "during", "before",
        "after", "above", "below", "between", "than", "then", "more", "most",
        "some", "any", "all", "each", "every", "no", "not", "yes", "also",
        "just", "very", "much", "many", "few", "such", "so", "out", "up",
        "down", "off", "over", "under", "here", "there", "when", "where",
        "who", "whom", "whose", "while", "because", "due",
    }
)

_TOKEN_RE = re.compile(r"[A-Za-z0-9_]+")


def _tokens(text: str) -> set[str]:
    return {
        tok
        for tok in (m.group(0).lower() for m in _TOKEN_RE.finditer(text))
        if len(tok) >= _MIN_TOKEN_LEN and tok not in _STOPWORDS
    }


def local_relevance(snippet: str, ranking_query: str) -> float:
    """Jaccard overlap on tokens, case-folded and stopwords-stripped.

    Returns 0.0 if either side has no qualifying tokens. Symmetric — does
    not penalize long snippets explicitly. The penalty for irrelevant long
    snippets falls out naturally: more snippet tokens means a larger union
    means smaller overlap fraction.
    """
    snippet_tokens = _tokens(snippet)
    query_tokens = _tokens(ranking_query)
    if not snippet_tokens or not query_tokens:
        return 0.0
    intersect = snippet_tokens & query_tokens
    union = snippet_tokens | query_tokens
    return len(intersect) / len(union)


def annotate_stream(
    items: list[SourceItem],
    *,
    ranking_query: str,
) -> list[SourceItem]:
    """Mutates each item's ``local_relevance`` in place; returns the list."""
    for item in items:
        item.local_relevance = local_relevance(item.snippet, ranking_query)
    return items


def prune_low_relevance(
    items: list[SourceItem],
    *,
    floor: float = RELEVANCE_PRUNE_FLOOR,
) -> list[SourceItem]:
    """Drop items with ``local_relevance`` strictly below ``floor``.

    Default floor is 0.0 (annotate-don't-filter at v2.0). Callers can lift
    the floor for testing or once empirical questions justify a tighter cut.
    """
    if floor <= 0.0:
        return items
    return [item for item in items if item.local_relevance >= floor]
