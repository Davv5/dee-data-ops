"""RawHit -> SourceItem canonicalization.

Per U5: collapses the retriever's RawHit into the canonical SourceItem shape
the rest of the pipeline operates on. ``source_title`` stays None at this
stage — title lookup is lazy and fires only after rerank for the surviving
candidates (per Key Technical Decisions in the plan).
"""

from __future__ import annotations

from .retriever import RawHit
from .schema import SourceItem


def normalize_hits(
    hits: list[RawHit],
    *,
    scope: str,
    source_quality: float = 1.0,
) -> list[SourceItem]:
    """Convert a list of RawHits into SourceItems.

    Args:
        hits: per-source RawHits returned by ``retriever.query_one``.
        scope: scope label to stamp on each item (already on each RawHit, but
            we re-apply here so the caller stays explicit about the stream
            identity).
        source_quality: per-scope quality weight from corpus.yaml (typically
            ``ScopeRef.weight``). Default 1.0.

    Returns:
        SourceItems with ``citation_number`` set to the first marker for each
        source (if any). All citation markers are preserved in
        ``metadata["all_citation_numbers"]`` for downstream Phase-2 use.
    """
    items: list[SourceItem] = []
    for hit in hits:
        first_citation = hit.citation_numbers[0] if hit.citation_numbers else None
        items.append(
            SourceItem(
                source_id=hit.source_id,
                scope=scope,
                snippet=hit.snippet,
                citation_number=first_citation,
                source_title=None,
                local_relevance=0.0,
                source_quality=source_quality,
                metadata={
                    "all_citation_numbers": list(hit.citation_numbers),
                },
            )
        )
    return items
