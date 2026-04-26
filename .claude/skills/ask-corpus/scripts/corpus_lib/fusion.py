"""Weighted reciprocal rank fusion + quality-aware diversity guard.

Adapted from ``last30days/scripts/lib/fusion.py``. Differences from upstream:

- Candidate key is ``source_id`` (notebook citation), not URL. One Candidate
  per source_id; multiple SourceItems from different (subquery, scope) streams
  collapse into one Candidate with summed rrf_score and merged provenance.
- No URL normalization (corpus citations carry source_id, not URLs).
- No per-author cap (notebook citations don't carry an author the way social-
  media posts do). The diversity unit is the **scope**.
- The per-scope diversity guard is **quality-aware** (the load-bearing
  adversarial-review fix): a scope reserves slots only when its top item
  meets the relevance floor AND its top relevance is at least
  ``QUALITY_PARITY_FLOOR`` fraction of the dominant scope's top relevance.
  Stops the guard from injecting mediocre citations when one scope is
  nominally above threshold but materially weaker than the dominant scope
  for *this* question.

Both the diversity-relevance threshold and the quality-parity floor are
**provisional ship-defaults** per the plan's Key Technical Decisions —
revisit before declaring U7 production-ready by running 3 known-correct-
scope acceptance questions.
"""

from __future__ import annotations

from .log import trace
from .schema import Candidate, QueryPlan, SourceItem

# Standard RRF smoothing constant (Cormack et al. 2009).
RRF_K = 60

# Minimum local_relevance for a scope's top item to qualify for reserved
# slots in the diversity guard. Provisional — see module docstring.
DIVERSITY_RELEVANCE_THRESHOLD = 0.30

# A scope reserves slots only when its top item's local_relevance is at
# least this fraction of the dominant scope's top item's local_relevance.
# Below this floor, the scope competes on RRF merit alone. Provisional.
QUALITY_PARITY_FLOOR = 0.6

# How many slots each qualifying scope reserves in the diversified pool.
DEFAULT_MIN_PER_SCOPE = 2

DEFAULT_POOL_LIMIT = 24


def weighted_rrf(
    streams: dict[tuple[str, str], list[SourceItem]],
    plan: QueryPlan,
    *,
    pool_limit: int = DEFAULT_POOL_LIMIT,
) -> list[Candidate]:
    """Fuse per-(subquery, scope) streams into a ranked candidate pool.

    For each item at native rank ``r`` in stream ``(label, scope)``:

        rrf_score += (subquery.weight * scope_weight) / (RRF_K + r)

    Scores are summed across all streams the candidate appears in. Sort
    order then enforces the diversity guard.

    Args:
        streams: dict keyed by ``(subquery_label, scope_label)``; values
            are SourceItem lists in native rank order (rank 1 = best).
        plan: ``QueryPlan`` with ``subqueries`` and ``scope_weights``.
        pool_limit: maximum candidates returned after diversification.

    Returns:
        Sorted candidate pool (length ≤ pool_limit).
    """
    subqueries = {s.label: s for s in plan.subqueries}
    candidates: dict[str, Candidate] = {}

    for (label, scope), items in streams.items():
        sub = subqueries.get(label)
        if sub is None:
            trace(
                "Fusion",
                f"stream label {label!r} not in plan; skipping",
            )
            continue
        scope_weight = plan.scope_weights.get(scope, 1.0)
        weight = sub.weight * scope_weight
        for native_rank, item in enumerate(items, start=1):
            rrf_increment = weight / (RRF_K + native_rank)
            existing = candidates.get(item.source_id)
            if existing is None:
                candidates[item.source_id] = _new_candidate(
                    item=item,
                    label=label,
                    scope=scope,
                    native_rank=native_rank,
                    rrf_score=rrf_increment,
                )
                continue
            _merge_into_candidate(
                existing,
                item=item,
                label=label,
                scope=scope,
                native_rank=native_rank,
                rrf_score=rrf_increment,
            )

    fused = sorted(candidates.values(), key=_candidate_sort_key)
    diversified = _diversify_pool(fused, pool_limit=pool_limit)
    trace(
        "Fusion",
        f"streams={len(streams)} unique_candidates={len(candidates)} "
        f"pool_size={len(diversified)} pool_limit={pool_limit}",
    )
    return diversified


def _candidate_sort_key(c: Candidate) -> tuple:
    """Stable sort: highest rrf_score, then highest local_relevance, then
    scope label and source_id for determinism.
    """
    return (-c.rrf_score, -c.local_relevance, c.scope, c.source_id)


def _new_candidate(
    *,
    item: SourceItem,
    label: str,
    scope: str,
    native_rank: int,
    rrf_score: float,
) -> Candidate:
    return Candidate(
        candidate_id=item.source_id,
        source_id=item.source_id,
        scope=scope,
        snippet=item.snippet,
        rrf_score=rrf_score,
        local_relevance=item.local_relevance,
        source_quality=item.source_quality,
        source_title=None,
        citation_number=item.citation_number,
        sources=[scope],
        subquery_labels=[label],
        native_ranks={f"{label}:{scope}": native_rank},
        source_items=[item],
        metadata={
            "provenance": [
                {
                    "scope": scope,
                    "subquery_label": label,
                    "native_rank": native_rank,
                    "citation_number": item.citation_number,
                }
            ]
        },
    )


def _merge_into_candidate(
    candidate: Candidate,
    *,
    item: SourceItem,
    label: str,
    scope: str,
    native_rank: int,
    rrf_score: float,
) -> None:
    candidate.rrf_score += rrf_score
    candidate.local_relevance = max(candidate.local_relevance, item.local_relevance)
    candidate.source_quality = max(candidate.source_quality, item.source_quality)
    candidate.native_ranks[f"{label}:{scope}"] = native_rank
    if label not in candidate.subquery_labels:
        candidate.subquery_labels.append(label)
    if scope not in candidate.sources:
        candidate.sources.append(scope)
    candidate.source_items.append(item)
    candidate.metadata.setdefault("provenance", []).append(
        {
            "scope": scope,
            "subquery_label": label,
            "native_rank": native_rank,
            "citation_number": item.citation_number,
        }
    )
    # Prefer the longer snippet (more context for synthesis); ties keep the
    # original. Snippet content is informational only — fusion's outputs
    # don't depend on which snippet the candidate carries.
    if len(item.snippet) > len(candidate.snippet):
        candidate.snippet = item.snippet


def _diversify_pool(
    fused: list[Candidate],
    *,
    pool_limit: int,
    min_per_scope: int = DEFAULT_MIN_PER_SCOPE,
) -> list[Candidate]:
    """Quality-aware per-scope diversity guard.

    A scope reserves up to ``min_per_scope`` slots in the pool only if **both**
    of these hold:

    1. Its top candidate's ``local_relevance >= DIVERSITY_RELEVANCE_THRESHOLD``.
    2. Its top candidate's ``local_relevance / dominant_scope_top >=
       QUALITY_PARITY_FLOOR``.

    Below either floor, the scope competes on RRF merit alone — no slot
    reservation. The dominant scope is whichever scope has the highest
    top-item local_relevance across the entire fused list.
    """
    if not fused:
        return []

    scope_top_relevance: dict[str, float] = {}
    for c in fused:
        current = scope_top_relevance.get(c.scope, 0.0)
        if c.local_relevance > current:
            scope_top_relevance[c.scope] = c.local_relevance

    dominant_top = max(scope_top_relevance.values(), default=0.0)

    qualifying_scopes: set[str] = set()
    if dominant_top > 0:
        for scope, top in scope_top_relevance.items():
            meets_floor = top >= DIVERSITY_RELEVANCE_THRESHOLD
            meets_parity = (top / dominant_top) >= QUALITY_PARITY_FLOOR
            if meets_floor and meets_parity:
                qualifying_scopes.add(scope)

    reserved: dict[str, list[Candidate]] = {}
    remainder: list[Candidate] = []
    for c in fused:
        bucket = reserved.setdefault(c.scope, [])
        if c.scope in qualifying_scopes and len(bucket) < min_per_scope:
            bucket.append(c)
        else:
            remainder.append(c)

    pool: list[Candidate] = [c for items in reserved.values() for c in items]
    seen_ids: set[str] = {c.candidate_id for c in pool}
    for c in remainder:
        if len(pool) >= pool_limit:
            break
        if c.candidate_id in seen_ids:
            continue
        pool.append(c)
        seen_ids.add(c.candidate_id)

    pool.sort(key=_candidate_sort_key)
    return pool[:pool_limit]
