"""Plan validation + deterministic fallback for corpus retrieval.

Adapted from last30days/scripts/lib/planner.py. Key adaptations:

- "source" (reddit/x/youtube/...) -> "scope" (methodology.data_ops/...).
- 5 corpus-domain intents: convention, ops, howto, history, design (subset of
  last30days's 8 intents).
- No internal LLM provider plumbing. The host LLM IS the planner. supplied_plan
  is either the host's JSON output (`--plan plan.json`) or absent (fallback).
- No depth/budget/source-cascade logic. One scope-list per subquery.
- Planner prompt itself lives in SKILL.md (U12), not here. This module only
  consumes JSON the host produced.
"""

from __future__ import annotations

import re

from .log import trace
from .schema import ALLOWED_INTENTS, QueryPlan, SubQuery

# Capability sets per scope; drives default-scope selection in
# `_default_scopes_for_intent`. Order is significant in INTENT_DEFAULT_SCOPES
# below — earliest scope is the primary fallback target.
SCOPE_CAPABILITIES: dict[str, frozenset[str]] = {
    "methodology.data_ops": frozenset(
        {"craft", "dbt", "modeling", "ci_cd", "mds_theory"}
    ),
    "methodology.metabase": frozenset(
        {"metabase_ops", "metabase_integration", "licensing", "bigquery_cost"}
    ),
    "methodology.metabase_learn": frozenset(
        {
            "metabase_authoring",
            "dashboards",
            "sql_howto",
            "viz_choice",
            "bi_transition",
        }
    ),
    "engagement": frozenset(
        {"client_decisions", "oracle_metrics", "scope_history"}
    ),
}

INTENT_DEFAULT_SCOPES: dict[str, list[str]] = {
    "convention": [
        "methodology.data_ops",
        "methodology.metabase",
        "methodology.metabase_learn",
    ],
    "ops": ["methodology.metabase", "methodology.data_ops"],
    "howto": ["methodology.metabase_learn", "methodology.data_ops"],
    "history": ["engagement"],
    "design": [
        "methodology.data_ops",
        "methodology.metabase",
        "methodology.metabase_learn",
    ],
}

# The "no plan supplied" stderr nudge. Adapted from last30days/planner.py:134-141
# (LAW 7 nudge). Targets the hosting reasoning model: YOU are the planner, the
# fallback below is the headless/cron path only.
_FALLBACK_NUDGE = (
    "No --plan passed. If you are the reasoning model hosting this skill "
    "(Claude Code, Codex, Hermes, Gemini, or any agent runtime), YOU ARE the "
    "planner: generate a JSON query plan yourself and pass it via --plan. "
    "You do not need an API key or credentials; you ARE the LLM. The "
    "deterministic fallback below is the headless/cron path only. "
    "See SKILL.md PLAN GENERATION RULES for the JSON schema."
)


def plan_query(
    *,
    question: str,
    available_scopes: list[str],
    supplied_plan: dict | None,
) -> QueryPlan:
    """Build a QueryPlan from the host's supplied plan dict, or fall back.

    Args:
        question: the user's natural-language question.
        available_scopes: scope strings resolved from corpus.yaml (e.g.,
            ``["methodology.data_ops", "methodology.metabase", "engagement"]``).
            Empty list is permitted; fallback then uses INTENT_DEFAULT_SCOPES
            for the inferred intent without availability filtering.
        supplied_plan: parsed JSON from the host LLM, or None when the host
            invoked the engine bare.

    Returns:
        QueryPlan with at least one SubQuery. The deterministic fallback path
        always produces a single primary subquery covering all available
        scopes.
    """
    if supplied_plan is None:
        trace("Planner", _FALLBACK_NUDGE)
        return _fallback_plan(question, available_scopes)

    plan = _sanitize_plan(supplied_plan, question, available_scopes)
    if not plan.subqueries:
        trace(
            "Planner",
            "supplied plan produced no valid subqueries after sanitization; "
            "falling back to deterministic single-subquery plan",
        )
        return _fallback_plan(question, available_scopes)
    return plan


def _sanitize_plan(
    raw: dict,
    question: str,
    available_scopes: list[str],
) -> QueryPlan:
    """Validate and normalize a host-supplied plan dict.

    Mirrors ``last30days/planner.py:_sanitize_plan``. Behavior:

    - intent: kept if in ``ALLOWED_INTENTS``, else reset to
      ``_infer_intent(question)`` with a `[Planner]` warning.
    - scope_weights: filtered to ``available_scopes``; missing scopes default
      to ``1.0``; weights normalized to sum to ``1.0``.
    - subqueries: each must carry non-empty ``search_query`` AND
      ``ranking_query``; missing-either subquery is dropped with a warning.
      Subquery scopes are filtered to available; if filtering empties them,
      replaced with ``INTENT_DEFAULT_SCOPES[intent]`` intersected with
      available (or the full available list if that intersection is empty).
    """
    intent = _validate_intent(raw.get("intent"), question)
    available_set = set(available_scopes)

    scope_weights_raw = raw.get("scope_weights") or {}
    scope_weights = {
        scope: float(weight)
        for scope, weight in scope_weights_raw.items()
        if scope in available_set and isinstance(weight, (int, float))
    }
    for scope in available_scopes:
        scope_weights.setdefault(scope, 1.0)
    scope_weights = _normalize_weights(scope_weights)

    intent_defaults = [
        scope for scope in INTENT_DEFAULT_SCOPES.get(intent, [])
        if scope in available_set
    ] or list(available_scopes)

    subqueries: list[SubQuery] = []
    for index, sub_raw in enumerate(raw.get("subqueries") or [], start=1):
        if not isinstance(sub_raw, dict):
            trace("Planner", f"subquery #{index} is not a dict; dropping")
            continue
        label = str(sub_raw.get("label") or f"q{index}").strip() or f"q{index}"
        search_query = str(sub_raw.get("search_query") or "").strip()
        ranking_query = str(sub_raw.get("ranking_query") or "").strip()
        if not search_query or not ranking_query:
            trace(
                "Planner",
                f"subquery {label!r} missing search_query or ranking_query; "
                f"dropping",
            )
            continue
        # Accept either "scopes" (preferred) or "sources" (last30days legacy
        # key — host LLM may emit either depending on prompt familiarity).
        sub_scopes_raw = sub_raw.get("scopes") or sub_raw.get("sources") or []
        sub_scopes = [s for s in sub_scopes_raw if s in available_set]
        if not sub_scopes:
            sub_scopes = list(intent_defaults)
            if sub_scopes_raw:
                trace(
                    "Planner",
                    f"subquery {label!r} scopes filtered to empty; "
                    f"using intent-default scopes for {intent!r}",
                )
        try:
            weight = float(sub_raw.get("weight") or 1.0)
        except (TypeError, ValueError):
            weight = 1.0
        subqueries.append(
            SubQuery(
                label=label,
                search_query=search_query,
                ranking_query=ranking_query,
                scopes=sub_scopes,
                weight=max(0.05, weight),
            )
        )

    subqueries = _normalize_subquery_weights(subqueries)
    notes = [str(n).strip() for n in (raw.get("notes") or []) if str(n).strip()]
    return QueryPlan(
        intent=intent,
        subqueries=subqueries,
        scope_weights=scope_weights,
        notes=notes,
    )


def _fallback_plan(question: str, available_scopes: list[str]) -> QueryPlan:
    """Single-subquery deterministic plan.

    ``search_query`` == ``ranking_query`` == raw question; one stream per
    available scope. Used when no plan supplied or when sanitization yields
    no usable subqueries.
    """
    intent = _infer_intent(question)
    if available_scopes:
        scopes = list(available_scopes)
    else:
        scopes = list(INTENT_DEFAULT_SCOPES.get(intent, []))
    scope_weights = (
        _normalize_weights({scope: 1.0 for scope in scopes}) if scopes else {}
    )
    body = question.strip() or "(empty question)"
    return QueryPlan(
        intent=intent,
        subqueries=[
            SubQuery(
                label="primary",
                search_query=body,
                ranking_query=body,
                scopes=scopes,
                weight=1.0,
            )
        ],
        scope_weights=scope_weights,
        notes=["fallback-plan"],
    )


def _validate_intent(raw_intent: object, question: str) -> str:
    if raw_intent is None:
        return _infer_intent(question)
    candidate = str(raw_intent).strip()
    if candidate in ALLOWED_INTENTS:
        return candidate
    inferred = _infer_intent(question)
    trace(
        "Planner",
        f"intent {candidate!r} not in allowed set; inferred {inferred!r} "
        f"from question",
    )
    return inferred


def _infer_intent(question: str) -> str:
    """Heuristic intent inference for the deterministic fallback path.

    Five-intent mapping for corpus retrieval. Defaults to ``"convention"`` —
    the safest middle ground for cross-craft cross-querying when no signal
    matches. Order is significant: history/howto/ops/design/convention checks
    are arranged so the most specific signals win.
    """
    text = question.lower().strip()
    if re.search(
        r"\b(what did we|what was decided|previously decided|history of|"
        r"back when|earlier we|in the past|which version did we|"
        r"which one did we|prior decision|when did we)\b",
        text,
    ):
        return "history"
    if re.search(
        r"\b(how to|how do i|how does|how should i|step by step|tutorial|"
        r"walkthrough|guide me)\b",
        text,
    ):
        return "howto"
    if re.search(
        r"\b(deploy|deployment|backup|upgrade|install|operational|provision|"
        r"service account|role|permission|cron|scheduling|cloud sql|"
        r"bigquery cost|restart|monitor|outage|rotate|teardown)\b",
        text,
    ):
        return "ops"
    if re.search(
        r"\b(architecture|design|model|schema|star schema|scd|medallion|"
        r"layer|should we use|build vs buy|choose between|pick between|"
        r"trade-?off|tradeoffs)\b",
        text,
    ):
        return "design"
    if re.search(
        r"\b(naming|convention|what do we call|should we name|name this|"
        r"name for|prefix|suffix|style guide|standard for)\b",
        text,
    ):
        return "convention"
    return "convention"


def _normalize_subquery_weights(subqueries: list[SubQuery]) -> list[SubQuery]:
    if not subqueries:
        return subqueries
    total = sum(s.weight for s in subqueries) or 1.0
    return [
        SubQuery(
            label=s.label,
            search_query=s.search_query,
            ranking_query=s.ranking_query,
            scopes=s.scopes,
            weight=s.weight / total,
        )
        for s in subqueries
    ]


def _normalize_weights(weights: dict[str, float]) -> dict[str, float]:
    total = sum(max(w, 0.0) for w in weights.values()) or 1.0
    return {scope: max(w, 0.0) / total for scope, w in weights.items()}
