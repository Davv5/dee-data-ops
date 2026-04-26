"""Orchestrator — Phase-1 fan-out + per-stream normalize/signals/dedupe.

U5 skeleton: drives ``planner.plan_query`` then fans out (subquery × scope)
calls to ``retriever.query_one`` through a small ThreadPoolExecutor and
normalizes the resulting hits into a ``RetrievalBundle``. No fusion, no
rerank, no clustering — those layers land in U7/U8/U11.

The public entry point ``run`` returns a ``PipelineResult`` carrying the
sanitized ``QueryPlan``, the per-stream bundle, and bookkeeping metadata.
The phase-aware CLI (U6) wraps this and writes JSON to disk for the host
LLM handshake.

Concurrency is capped at 3 workers per the plan's risk register: a default
``methodology`` query already issues 3 subqueries × 3 notebooks = 9 calls
before any supplemental work; higher concurrency risks NotebookLM
throttling without measured headroom.
"""

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field

from . import dedupe, normalize, planner, retriever, signals
from .env import ScopeRef
from .log import trace
from .schema import Candidate, Cluster, QueryPlan, RetrievalBundle, SubQuery

DEFAULT_MAX_WORKERS = 3


@dataclass
class PipelineResult:
    """Intermediate handoff shape between U5 and U7+.

    At U5 this is the engine's full output. U7 (fusion) and U8 (rerank)
    consume ``bundle`` to produce ``Candidate`` lists; U11 wraps the
    ``Report``. Plan source is captured so the CLI can emit the
    ``plan-fallback`` warning later.
    """

    plan: QueryPlan
    bundle: RetrievalBundle
    plan_source: str  # "host-llm" | "deterministic"
    streams_run: int = 0
    streams_errored: int = 0
    items_kept: int = 0
    streams_attempted: list[tuple[str, str]] = field(default_factory=list)


def run(
    *,
    question: str,
    scope_refs: list[ScopeRef],
    supplied_plan: dict | None = None,
    depth: str = "default",  # noqa: ARG001 — accepted for U6 forward-compat
    max_workers: int = DEFAULT_MAX_WORKERS,
) -> PipelineResult:
    """Plan + fan-out + normalize. No fusion or rerank yet.

    Args:
        question: user's natural-language question.
        scope_refs: env-resolved ScopeRef list (provides notebook_id + weight
            per scope_label).
        supplied_plan: parsed JSON from the host LLM, or None for fallback.
        depth: ``"quick" | "default" | "deep"``. Currently a no-op pass-through
            — phase-2 supplemental search (U9-deferred) and phase-2b retry
            (U10-deferred) honor it.
        max_workers: ThreadPoolExecutor cap. Default 3.

    Returns:
        PipelineResult with plan, RetrievalBundle, and per-stream counters.
    """
    available_scopes = [ref.scope_label for ref in scope_refs]
    plan = planner.plan_query(
        question=question,
        available_scopes=available_scopes,
        supplied_plan=supplied_plan,
    )
    plan_source = (
        "deterministic" if "fallback-plan" in plan.notes else "host-llm"
    )
    trace(
        "Planner",
        f"source={plan_source} intent={plan.intent} "
        f"n_subqueries={len(plan.subqueries)}",
    )
    for sub in plan.subqueries:
        trace(
            "Planner",
            f"subquery {sub.label!r} weight={sub.weight:.2f} "
            f"scopes={sub.scopes}",
        )

    scope_index = {ref.scope_label: ref for ref in scope_refs}
    streams = _enumerate_streams(plan.subqueries, scope_index)
    bundle = RetrievalBundle()
    streams_run = 0
    streams_errored = 0
    items_kept = 0

    if not streams:
        trace(
            "Pipeline",
            "no streams to run — plan produced no (subquery, scope) pairs",
        )
        return PipelineResult(
            plan=plan,
            bundle=bundle,
            plan_source=plan_source,
            streams_attempted=[],
        )

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {
            executor.submit(
                retriever.query_one,
                ref.notebook_id,
                search_query,
                scope=scope_label,
            ): (label, scope_label, ranking_query, ref)
            for (label, scope_label, search_query, ranking_query, ref) in streams
        }

        for future in as_completed(futures):
            label, scope_label, ranking_query, ref = futures[future]
            try:
                hits = future.result()
            except retriever.RetrievalError as exc:
                bundle.errors_by_source[scope_label] = str(exc)
                streams_errored += 1
                trace(
                    "Pipeline",
                    f"stream {label!r} scope={scope_label} ERRORED: {exc}",
                )
                continue

            items = normalize.normalize_hits(
                hits,
                scope=scope_label,
                source_quality=ref.weight,
            )
            items = signals.annotate_stream(items, ranking_query=ranking_query)
            items = signals.prune_low_relevance(items)
            items = dedupe.dedupe_within_stream(items)
            bundle.add_items(label, scope_label, items)
            streams_run += 1
            items_kept += len(items)

    trace(
        "Pipeline",
        f"streams_run={streams_run} errored={streams_errored} "
        f"items_kept={items_kept}",
    )
    return PipelineResult(
        plan=plan,
        bundle=bundle,
        plan_source=plan_source,
        streams_run=streams_run,
        streams_errored=streams_errored,
        items_kept=items_kept,
        streams_attempted=[(label, scope) for (label, scope, *_rest) in streams],
    )


# ---------------------------------------------------------------------------
# Finalization (U11) — provisional cluster grouping + structured warnings


def group_by_subquery(
    sorted_cands: list[Candidate],
    plan: QueryPlan,
) -> list[Cluster]:
    """Group ranked candidates into one Cluster per surviving subquery.

    Cluster ordering follows ``plan.subqueries`` order (stable). A candidate
    that matched multiple subqueries lands in the cluster whose stream gave
    it the best (lowest) ``native_rank`` — the proxy for "highest-scoring
    subquery" without re-running the per-stream RRF math. Candidates with
    no matching plan subquery (label produced under a label not in the plan
    — defensive case) get a fallback cluster keyed on the orphan label.
    """
    label_to_query = {s.label: s.ranking_query for s in plan.subqueries}
    plan_labels = list(label_to_query.keys())

    by_label: dict[str, list[Candidate]] = {}
    for c in sorted_cands:
        primary = _best_subquery_label(c, plan_labels)
        by_label.setdefault(primary, []).append(c)

    clusters: list[Cluster] = []
    for sub in plan.subqueries:
        if sub.label in by_label:
            clusters.append(
                Cluster(
                    theme=sub.ranking_query,
                    subquery_label=sub.label,
                    candidates=by_label[sub.label],
                )
            )
    for label, cands in by_label.items():
        if label not in label_to_query:
            clusters.append(
                Cluster(theme=label, subquery_label=label, candidates=cands)
            )
    return clusters


def warnings_for(
    *,
    sorted_cands: list[Candidate],
    plan_source: str,
    streams_errored: int,
    seed_warnings: list[str] | None = None,
) -> list[str]:
    """Compose the structured warnings list per plan U11.

    Categories:

    - ``thin-evidence``: fewer than 5 ranked candidates
    - ``scope-concentration``: top 5 candidates all from a single scope
    - ``scope-errors``: any retrieval stream errored
    - ``no-usable-items``: ranked list is empty
    - ``plan-fallback``: plan came from deterministic fallback path
    - ``rerank-fallback``: caller already added this when rerank fell back

    ``seed_warnings`` lets callers carry warnings forward (e.g.,
    ``"rerank-fallback"`` added by ``rerank.apply_scores``). Duplicates are
    suppressed.
    """
    out: list[str] = list(seed_warnings or [])

    def _add(name: str) -> None:
        if name not in out:
            out.append(name)

    if streams_errored > 0:
        _add("scope-errors")
    if plan_source == "deterministic":
        _add("plan-fallback")
    if not sorted_cands:
        _add("no-usable-items")
        return out  # downstream checks need at least one candidate
    if len(sorted_cands) < 5:
        _add("thin-evidence")
    top_5 = sorted_cands[:5]
    if len({c.scope for c in top_5}) == 1:
        _add("scope-concentration")
    return out


def _best_subquery_label(c: Candidate, plan_labels: list[str]) -> str:
    """Pick the subquery_label whose stream gave this candidate the best
    native_rank. ``native_ranks`` keys are formatted as ``"{label}:{scope}"``;
    we read the label prefix and pick the smallest rank seen.
    """
    best_label: str | None = None
    best_rank = float("inf")
    for key, rank in c.native_ranks.items():
        label = key.split(":", 1)[0]
        if label in plan_labels and rank < best_rank:
            best_label = label
            best_rank = rank
    if best_label is not None:
        return best_label
    if c.subquery_labels:
        return c.subquery_labels[0]
    return "primary"


def _enumerate_streams(
    subqueries: list[SubQuery],
    scope_index: dict[str, ScopeRef],
) -> list[tuple[str, str, str, str, ScopeRef]]:
    """Expand subqueries × scopes into the flat stream list.

    Skips subquery-scope pairs whose scope isn't in the resolved scope_index
    (defensive — sanitization should already have filtered, but a host-supplied
    plan with a typo gets a quiet skip rather than a crash).
    """
    out: list[tuple[str, str, str, str, ScopeRef]] = []
    for sub in subqueries:
        for scope_label in sub.scopes:
            ref = scope_index.get(scope_label)
            if ref is None:
                trace(
                    "Pipeline",
                    f"skipping stream {sub.label!r} scope={scope_label!r} "
                    f"— scope not in resolved scope_refs",
                )
                continue
            out.append((sub.label, scope_label, sub.search_query, sub.ranking_query, ref))
    return out
