"""Tests for pipeline.group_by_subquery + pipeline.warnings_for (U11)."""

from __future__ import annotations

import sys
import unittest
from pathlib import Path

SCRIPTS_DIR = Path(__file__).resolve().parent.parent / "scripts"
sys.path.insert(0, str(SCRIPTS_DIR))

from corpus_lib.pipeline import group_by_subquery, warnings_for  # noqa: E402
from corpus_lib.schema import Candidate, QueryPlan, SubQuery  # noqa: E402


def _candidate(
    candidate_id: str,
    *,
    scope: str = "methodology.data_ops",
    subquery_labels: list[str] | None = None,
    native_ranks: dict[str, int] | None = None,
    rrf_score: float = 0.04,
    final_score: float = 50.0,
) -> Candidate:
    return Candidate(
        candidate_id=candidate_id,
        source_id=candidate_id,
        scope=scope,
        snippet="snip",
        rrf_score=rrf_score,
        final_score=final_score,
        subquery_labels=list(subquery_labels or ["primary"]),
        native_ranks=dict(native_ranks or {}),
    )


def _plan(*labels: str) -> QueryPlan:
    return QueryPlan(
        intent="design",
        subqueries=[
            SubQuery(
                label=label,
                search_query=label,
                ranking_query=f"What about {label}?",
                scopes=["methodology.data_ops"],
            )
            for label in labels
        ],
    )


class TestGroupBySubquery(unittest.TestCase):
    def test_three_subqueries_three_clusters(self):
        cands = [
            _candidate("a", subquery_labels=["primary"], native_ranks={"primary:methodology.data_ops": 1}),
            _candidate("b", subquery_labels=["primary"], native_ranks={"primary:methodology.data_ops": 2}),
            _candidate("c", subquery_labels=["secondary"], native_ranks={"secondary:methodology.data_ops": 1}),
            _candidate("d", subquery_labels=["tertiary"], native_ranks={"tertiary:methodology.data_ops": 1}),
        ]
        plan = _plan("primary", "secondary", "tertiary")
        clusters = group_by_subquery(cands, plan)
        self.assertEqual(len(clusters), 3)
        self.assertEqual([c.subquery_label for c in clusters], ["primary", "secondary", "tertiary"])

    def test_multi_label_candidate_lands_in_best_native_rank_cluster(self):
        """Adversarial U11 case: candidate matches both 'primary' (rank 5)
        and 'secondary' (rank 1). Lands in 'secondary' (best rank).
        """
        cands = [
            _candidate(
                "a",
                subquery_labels=["primary", "secondary"],
                native_ranks={
                    "primary:methodology.data_ops": 5,
                    "secondary:methodology.data_ops": 1,
                },
            ),
        ]
        plan = _plan("primary", "secondary")
        clusters = group_by_subquery(cands, plan)
        # 'a' should be in 'secondary' only
        secondary = next(c for c in clusters if c.subquery_label == "secondary")
        primary = next((c for c in clusters if c.subquery_label == "primary"), None)
        self.assertEqual(len(secondary.candidates), 1)
        self.assertEqual(secondary.candidates[0].candidate_id, "a")
        self.assertIsNone(primary)  # primary cluster wasn't created — no candidates

    def test_orphan_label_falls_through(self):
        """A candidate produced under a label not in the plan still gets a
        cluster — defensive case (label drift between phases).
        """
        cands = [
            _candidate("a", subquery_labels=["primary"], native_ranks={"primary:methodology.data_ops": 1}),
            _candidate(
                "b",
                subquery_labels=["mystery"],
                native_ranks={"mystery:methodology.data_ops": 1},
            ),
        ]
        plan = _plan("primary")
        clusters = group_by_subquery(cands, plan)
        labels = [c.subquery_label for c in clusters]
        self.assertIn("primary", labels)
        self.assertIn("mystery", labels)


class TestWarningsFor(unittest.TestCase):
    def test_warnings_empty_when_healthy(self):
        cands = [
            _candidate(
                f"x{i}",
                scope=f"methodology.scope_{i % 3}",
                subquery_labels=["primary"],
                native_ranks={"primary:methodology.data_ops": i + 1},
            )
            for i in range(12)
        ]
        out = warnings_for(
            sorted_cands=cands,
            plan_source="host-llm",
            streams_errored=0,
        )
        self.assertEqual(out, [])

    def test_thin_evidence_fires_under_5(self):
        cands = [_candidate(f"x{i}") for i in range(3)]
        out = warnings_for(
            sorted_cands=cands,
            plan_source="host-llm",
            streams_errored=0,
        )
        self.assertIn("thin-evidence", out)
        # All from same scope => also scope-concentration
        self.assertIn("scope-concentration", out)

    def test_scope_concentration_top_5_one_scope(self):
        cands = [_candidate(f"x{i}", scope="methodology.data_ops") for i in range(8)]
        out = warnings_for(
            sorted_cands=cands,
            plan_source="host-llm",
            streams_errored=0,
        )
        self.assertIn("scope-concentration", out)
        self.assertNotIn("thin-evidence", out)

    def test_plan_fallback_fires(self):
        cands = [
            _candidate(f"x{i}", scope=f"methodology.scope_{i % 3}")
            for i in range(12)
        ]
        out = warnings_for(
            sorted_cands=cands,
            plan_source="deterministic",
            streams_errored=0,
        )
        self.assertIn("plan-fallback", out)

    def test_scope_errors_fires(self):
        cands = [_candidate(f"x{i}", scope=f"methodology.scope_{i % 3}") for i in range(12)]
        out = warnings_for(
            sorted_cands=cands,
            plan_source="host-llm",
            streams_errored=2,
        )
        self.assertIn("scope-errors", out)

    def test_no_usable_items_short_circuits(self):
        out = warnings_for(
            sorted_cands=[],
            plan_source="host-llm",
            streams_errored=0,
        )
        self.assertIn("no-usable-items", out)
        # Doesn't add thin-evidence or scope-concentration when empty.
        self.assertNotIn("thin-evidence", out)
        self.assertNotIn("scope-concentration", out)

    def test_seed_warnings_carried_forward_no_duplicates(self):
        cands = [_candidate("a")]
        out = warnings_for(
            sorted_cands=cands,
            plan_source="deterministic",
            streams_errored=0,
            seed_warnings=["rerank-fallback", "plan-fallback"],
        )
        # No duplicates of plan-fallback
        self.assertEqual(out.count("plan-fallback"), 1)
        self.assertIn("rerank-fallback", out)


if __name__ == "__main__":
    unittest.main()
