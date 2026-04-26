"""Tests for corpus_lib.schema dataclasses.

Uses stdlib unittest (pytest not yet installed). The same test classes run
unchanged under `python3 -m unittest` and `pytest` once it's added.
"""

from __future__ import annotations

import json
import sys
import unittest
from dataclasses import asdict
from pathlib import Path

# Make the engine package importable when running from repo root.
SCRIPTS_DIR = Path(__file__).resolve().parent.parent / "scripts"
sys.path.insert(0, str(SCRIPTS_DIR))

from corpus_lib.schema import (  # noqa: E402
    ALLOWED_INTENTS,
    ALLOWED_WARNINGS,
    Candidate,
    Cluster,
    QueryPlan,
    Report,
    RetrievalBundle,
    SourceItem,
    SubQuery,
    TraceSummary,
)


class TestSubQuery(unittest.TestCase):
    def test_construct_and_roundtrip(self):
        sq = SubQuery(
            label="primary",
            search_query="star schema dbt",
            ranking_query="What does the corpus say about modeling star schemas in dbt?",
            scopes=["methodology.data_ops"],
            weight=1.0,
        )
        d = asdict(sq)
        self.assertEqual(d["label"], "primary")
        self.assertEqual(d["scopes"], ["methodology.data_ops"])
        self.assertEqual(d["weight"], 1.0)

    def test_two_query_forms_are_distinct_fields(self):
        """Conflating search_query and ranking_query is the canonical failure
        mode. Schema must keep them separate."""
        sq = SubQuery(
            label="q1",
            search_query="metabase backup",
            ranking_query="How should I back up Metabase?",
            scopes=["methodology.metabase"],
        )
        self.assertNotEqual(sq.search_query, sq.ranking_query)


class TestSourceItem(unittest.TestCase):
    def test_minimum_fields(self):
        item = SourceItem(
            source_id="src_abc123",
            scope="methodology.data_ops",
            snippet="Star schemas separate facts from dimensions.",
        )
        self.assertEqual(item.item_id, "src_abc123")
        self.assertIsNone(item.source_title)  # filled lazily after rerank

    def test_item_id_with_citation_number(self):
        item = SourceItem(
            source_id="src_abc123",
            scope="methodology.data_ops",
            snippet="...",
            citation_number=4,
        )
        self.assertEqual(item.item_id, "src_abc123#4")


class TestCandidate(unittest.TestCase):
    def test_construct(self):
        c = Candidate(
            candidate_id="src_abc123",
            source_id="src_abc123",
            scope="methodology.data_ops",
            snippet="...",
        )
        self.assertEqual(c.rrf_score, 0.0)
        self.assertEqual(c.final_score, 0.0)
        self.assertEqual(c.sources, [])
        self.assertEqual(c.subquery_labels, [])

    def test_keyed_by_source_id(self):
        """candidate_id matches source_id at construction time. The
        per-source-id diversity cap (U7) relies on this stable key."""
        c = Candidate(
            candidate_id="src_xyz",
            source_id="src_xyz",
            scope="methodology.metabase",
            snippet="...",
        )
        self.assertEqual(c.candidate_id, c.source_id)


class TestReport(unittest.TestCase):
    def test_empty_report_serializes(self):
        """Edge case from plan U1: Report with empty ranked_candidates and
        no warnings still serializes to JSON cleanly."""
        plan = QueryPlan(intent="convention")
        trace = TraceSummary(
            plan_source="deterministic",
            n_subqueries=1,
            n_streams_run=0,
            n_streams_errored=0,
        )
        report = Report(
            question="test",
            intent="convention",
            plan=plan,
            ranked_candidates=[],
            clusters=[],
            warnings=[],
            trace_summary=trace,
        )
        as_json = json.dumps(asdict(report))
        roundtrip = json.loads(as_json)
        self.assertEqual(roundtrip["question"], "test")
        self.assertEqual(roundtrip["ranked_candidates"], [])
        self.assertEqual(roundtrip["warnings"], [])

    def test_report_with_clusters_serializes(self):
        plan = QueryPlan(
            intent="design",
            subqueries=[
                SubQuery(
                    label="primary",
                    search_query="x",
                    ranking_query="y",
                    scopes=["methodology.data_ops"],
                )
            ],
        )
        candidate = Candidate(
            candidate_id="src_1",
            source_id="src_1",
            scope="methodology.data_ops",
            snippet="snippet text",
            final_score=85.0,
        )
        cluster = Cluster(
            theme="What about X?", subquery_label="primary", candidates=[candidate]
        )
        report = Report(
            question="test",
            intent="design",
            plan=plan,
            ranked_candidates=[candidate],
            clusters=[cluster],
            warnings=["thin-evidence"],
            trace_summary=TraceSummary(
                plan_source="host-llm",
                n_subqueries=1,
                n_streams_run=3,
                n_streams_errored=0,
            ),
        )
        as_json = json.dumps(asdict(report))
        roundtrip = json.loads(as_json)
        self.assertEqual(len(roundtrip["clusters"]), 1)
        self.assertEqual(roundtrip["warnings"], ["thin-evidence"])


class TestRetrievalBundle(unittest.TestCase):
    def test_add_items_populates_both_views(self):
        bundle = RetrievalBundle()
        items = [
            SourceItem(
                source_id="src_a",
                scope="methodology.data_ops",
                snippet="...",
            )
        ]
        bundle.add_items("primary", "methodology.data_ops", items)
        self.assertEqual(len(bundle.items_by_source["methodology.data_ops"]), 1)
        self.assertEqual(
            len(bundle.items_by_source_and_query[("primary", "methodology.data_ops")]),
            1,
        )

    def test_add_items_appends_not_replaces(self):
        bundle = RetrievalBundle()
        bundle.add_items(
            "q1",
            "methodology.data_ops",
            [SourceItem(source_id="a", scope="methodology.data_ops", snippet="...")],
        )
        bundle.add_items(
            "q2",
            "methodology.data_ops",
            [SourceItem(source_id="b", scope="methodology.data_ops", snippet="...")],
        )
        # Same scope, different subquery labels — items_by_source accumulates
        self.assertEqual(len(bundle.items_by_source["methodology.data_ops"]), 2)
        # Per-(label, scope) view keeps them separate
        self.assertEqual(
            len(bundle.items_by_source_and_query[("q1", "methodology.data_ops")]),
            1,
        )
        self.assertEqual(
            len(bundle.items_by_source_and_query[("q2", "methodology.data_ops")]),
            1,
        )


class TestConstants(unittest.TestCase):
    def test_intents_match_planner_contract(self):
        # These are the corpus-domain intents documented in the plan.
        # If you change this set, update planner.py INTENT_DEFAULT_SCOPES too.
        self.assertEqual(
            ALLOWED_INTENTS,
            frozenset({"convention", "ops", "howto", "history", "design"}),
        )

    def test_warnings_include_rerank_fallback(self):
        """rerank-fallback warning was added during the deepening pass to make
        host-LLM-malformed-scores degradation visible. Don't drop it."""
        self.assertIn("rerank-fallback", ALLOWED_WARNINGS)
        self.assertIn("plan-fallback", ALLOWED_WARNINGS)


if __name__ == "__main__":
    unittest.main()
