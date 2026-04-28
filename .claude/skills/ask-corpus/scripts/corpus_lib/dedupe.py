"""Within-stream dedup by (source_id, snippet[:120]) hash.

Per U5: prevents the same source surfacing twice within one (subquery, scope)
stream. Cross-stream dedup happens later in fusion (per-source-id cap, U7).
"""

from __future__ import annotations

from .schema import SourceItem

_SNIPPET_HASH_LEN = 120


def dedupe_within_stream(items: list[SourceItem]) -> list[SourceItem]:
    """Remove duplicates by ``(source_id, snippet[:120])`` key.

    Order is preserved; first occurrence wins. The truncated-snippet portion
    of the key catches near-duplicate citations that share a source but vary
    by trailing whitespace or sentence-boundary noise; a stricter exact-text
    match would let those pass.
    """
    seen: set[tuple[str, str]] = set()
    out: list[SourceItem] = []
    for item in items:
        key = (item.source_id, item.snippet[:_SNIPPET_HASH_LEN])
        if key in seen:
            continue
        seen.add(key)
        out.append(item)
    return out
