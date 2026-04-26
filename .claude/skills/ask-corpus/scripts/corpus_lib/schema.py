"""Dataclasses for the corpus research engine.

Mirrors last30days/scripts/lib/schema.py discipline. Field renames:
- last30days "source" (reddit/x/youtube/...) -> our "scope" (methodology.data_ops/...)
- per-author cap -> per-source-id cap (notebook citations carry source_id, not author)
- no `engagement` engagement-score field; `engagement` here would only mean the notebook scope
- no `freshness` int (corpus content is static at v2.0)

The Candidate.source_title field is filled lazily after rerank via cached
`nlm source list --json`. During retrieval and fusion, only source_id is set.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass
class SubQuery:
    """One leg of a query plan. Carries both query forms — keyword for nlm
    retrieval, natural-language for reranker. The two forms are deliberately
    different artifacts; conflating them is the most common failure mode.
    """

    label: str
    search_query: str
    ranking_query: str
    scopes: list[str]
    weight: float = 1.0


@dataclass
class QueryPlan:
    intent: str
    subqueries: list[SubQuery] = field(default_factory=list)
    scope_weights: dict[str, float] = field(default_factory=dict)
    freshness_mode: str = "evergreen_ok"
    cluster_mode: str = "none"
    notes: list[str] = field(default_factory=list)


@dataclass
class SourceItem:
    """One citation hit returned by `nlm notebook query --json`.

    The raw response shape is {source_id, citation_number, cited_text}; we
    canonicalize cited_text -> snippet and add scope/scoring fields.

    source_title stays None during retrieval; filled after rerank.
    """

    source_id: str
    scope: str
    snippet: str
    citation_number: int | None = None
    source_title: str | None = None
    local_relevance: float = 0.0
    source_quality: float = 0.6
    metadata: dict[str, Any] = field(default_factory=dict)

    @property
    def item_id(self) -> str:
        """Stable identity within a stream. source_id + citation_number disambiguates
        when the same source produces multiple citations for one query.
        """
        if self.citation_number is not None:
            return f"{self.source_id}#{self.citation_number}"
        return self.source_id


@dataclass
class Candidate:
    """A merged candidate after RRF fusion.

    candidate_id is keyed on source_id (per-source-id diversity unit).
    Multiple SourceItems can collapse into one Candidate when the same
    source_id appears in multiple (subquery, scope) streams.
    """

    candidate_id: str
    source_id: str
    scope: str
    snippet: str
    rrf_score: float = 0.0
    local_relevance: float = 0.0
    source_quality: float = 0.6
    final_score: float = 0.0
    rerank_reason: str | None = None
    source_title: str | None = None
    citation_number: int | None = None
    sources: list[str] = field(default_factory=list)
    subquery_labels: list[str] = field(default_factory=list)
    native_ranks: dict[str, int] = field(default_factory=dict)
    source_items: list[SourceItem] = field(default_factory=list)
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass
class Cluster:
    """Subquery-grouped candidates. v2.0 groups by subquery label (not semantic
    clustering). theme is the originating subquery's ranking_query.
    """

    theme: str
    subquery_label: str
    candidates: list[Candidate] = field(default_factory=list)


@dataclass
class TraceSummary:
    plan_source: str  # "host-llm" | "deterministic"
    n_subqueries: int
    n_streams_run: int
    n_streams_errored: int


@dataclass
class Report:
    """The synthesis-input payload the host LLM consumes."""

    question: str
    intent: str
    plan: QueryPlan
    ranked_candidates: list[Candidate]
    clusters: list[Cluster]
    warnings: list[str]
    trace_summary: TraceSummary
    range_from: str | None = None
    range_to: str | None = None


@dataclass
class RetrievalBundle:
    """In-flight state held by the pipeline between fan-out and fusion."""

    items_by_source: dict[str, list[SourceItem]] = field(default_factory=dict)
    items_by_source_and_query: dict[tuple[str, str], list[SourceItem]] = field(
        default_factory=dict
    )
    errors_by_source: dict[str, str] = field(default_factory=dict)
    artifacts: dict[str, Any] = field(default_factory=dict)

    def add_items(self, label: str, scope: str, items: list[SourceItem]) -> None:
        """Append items to both per-scope and per-(label, scope) views."""
        self.items_by_source.setdefault(scope, []).extend(items)
        self.items_by_source_and_query.setdefault((label, scope), []).extend(items)


# Module-level constants used by both planner.py and rerank.py
ALLOWED_INTENTS: frozenset[str] = frozenset(
    {"convention", "ops", "howto", "history", "design"}
)

ALLOWED_FRESHNESS_MODES: frozenset[str] = frozenset(
    {"strict_recent", "balanced_recent", "evergreen_ok"}
)

ALLOWED_CLUSTER_MODES: frozenset[str] = frozenset(
    {"none", "story", "workflow", "market", "debate"}
)

ALLOWED_WARNINGS: frozenset[str] = frozenset(
    {
        "thin-evidence",
        "scope-concentration",
        "scope-errors",
        "no-usable-items",
        "plan-fallback",
        "rerank-fallback",
    }
)
