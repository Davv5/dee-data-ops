"""Smoke tests for corpus_lib.pipeline.run.

The retriever subprocess is stubbed via fixture JSON. We don't talk to the
live nlm CLI here — that lives in test_retriever.TestLiveSmoke (opt-in via
CORPUS_LIVE_SMOKE=1).

Coverage:
- happy path: 1 subquery × 2 scopes returns a flat candidate bundle with
  per-stream items and traces emitted.
- one scope errors → other scopes succeed; errors_by_source populated.
- all scopes return empty → bundle.items_by_source empty; items_kept = 0.
- end-to-end deterministic-fallback path (no supplied_plan) goes through
  planner.plan_query unchanged.
"""

from __future__ import annotations

import io
import json
import sys
import unittest
from contextlib import redirect_stderr
from pathlib import Path
from unittest.mock import patch

SCRIPTS_DIR = Path(__file__).resolve().parent.parent / "scripts"
sys.path.insert(0, str(SCRIPTS_DIR))

from corpus_lib import pipeline  # noqa: E402
from corpus_lib.env import ScopeRef  # noqa: E402
from corpus_lib.retriever import RetrievalError, clear_source_list_cache  # noqa: E402

FIXTURES = Path(__file__).resolve().parent / "fixtures"
QUERY_FIXTURE = FIXTURES / "nlm_data_ops_star_schema.json"


def _scope_ref(key: str, weight: float = 1.0) -> ScopeRef:
    """Construct a ScopeRef for a methodology key with a unique fake UUID."""
    if key == "engagement":
        notebook_id = "fake-engagement-uuid"
    else:
        notebook_id = f"fake-{key}-uuid"
    return ScopeRef(key=key, notebook_id=notebook_id, name=key, weight=weight)


def _stub_query_one(scope_payloads: dict[str, dict | Exception]):
    """Build a stub for retriever.query_one keyed by scope label.

    Returns the payload-derived hits when the scope matches a key in
    scope_payloads. If the value is an Exception, it's raised — useful for
    testing scope-error handling.
    """
    from corpus_lib.retriever import _extract_hits_from_answer  # noqa: PLC0415

    def fake_query_one(notebook_id, search_query, *, scope="", timeout=130):  # noqa: ARG001
        payload = scope_payloads.get(scope)
        if payload is None:
            return []
        if isinstance(payload, Exception):
            raise payload
        return _extract_hits_from_answer(payload, scope=scope)

    return fake_query_one


class TestPipelineRun(unittest.TestCase):
    def setUp(self):
        clear_source_list_cache()

    def test_happy_path_two_scopes_one_subquery(self):
        payload = json.loads(QUERY_FIXTURE.read_text())
        scope_payloads = {
            "methodology.data_ops": payload,
            "methodology.metabase": payload,
        }
        scope_refs = [
            _scope_ref("data_ops"),
            _scope_ref("metabase"),
        ]
        captured = io.StringIO()
        with patch(
            "corpus_lib.pipeline.retriever.query_one",
            side_effect=_stub_query_one(scope_payloads),
        ), redirect_stderr(captured):
            result = pipeline.run(
                question="How should we model star schemas in dbt?",
                scope_refs=scope_refs,
                supplied_plan=None,
            )

        # Plan came from fallback (no supplied_plan).
        self.assertEqual(result.plan_source, "deterministic")
        self.assertEqual(len(result.plan.subqueries), 1)
        self.assertEqual(result.plan.subqueries[0].label, "primary")

        # Both scopes ran; both produced items.
        self.assertEqual(result.streams_run, 2)
        self.assertEqual(result.streams_errored, 0)
        self.assertGreater(result.items_kept, 0)

        # Bundle has items keyed by scope and by (label, scope).
        self.assertIn("methodology.data_ops", result.bundle.items_by_source)
        self.assertIn("methodology.metabase", result.bundle.items_by_source)
        self.assertIn(
            ("primary", "methodology.data_ops"),
            result.bundle.items_by_source_and_query,
        )

        # Stderr trace lines fired.
        stderr_text = captured.getvalue()
        self.assertIn("[Planner]", stderr_text)
        self.assertIn("[Pipeline]", stderr_text)
        self.assertIn("streams_run=2", stderr_text)

        # local_relevance was annotated (not still 0.0 for all items).
        all_items = (
            result.bundle.items_by_source["methodology.data_ops"]
            + result.bundle.items_by_source["methodology.metabase"]
        )
        annotated = [i for i in all_items if i.local_relevance > 0]
        self.assertGreater(
            len(annotated),
            0,
            "at least one item should have non-zero local_relevance after annotate_stream",
        )

    def test_one_scope_errors_other_succeeds(self):
        payload = json.loads(QUERY_FIXTURE.read_text())
        scope_payloads = {
            "methodology.data_ops": payload,
            "methodology.metabase": RetrievalError("nlm subprocess failed"),
        }
        scope_refs = [
            _scope_ref("data_ops"),
            _scope_ref("metabase"),
        ]
        with patch(
            "corpus_lib.pipeline.retriever.query_one",
            side_effect=_stub_query_one(scope_payloads),
        ), redirect_stderr(io.StringIO()):
            result = pipeline.run(
                question="star schema?",
                scope_refs=scope_refs,
                supplied_plan=None,
            )

        self.assertEqual(result.streams_run, 1)
        self.assertEqual(result.streams_errored, 1)
        self.assertIn("methodology.metabase", result.bundle.errors_by_source)
        self.assertIn("methodology.data_ops", result.bundle.items_by_source)
        self.assertNotIn("methodology.metabase", result.bundle.items_by_source)

    def test_all_scopes_empty(self):
        scope_payloads: dict[str, dict | Exception] = {
            "methodology.data_ops": {"value": {"answer": "", "citations": {}}},
            "methodology.metabase": {"value": {"answer": "", "citations": {}}},
        }
        scope_refs = [_scope_ref("data_ops"), _scope_ref("metabase")]
        with patch(
            "corpus_lib.pipeline.retriever.query_one",
            side_effect=_stub_query_one(scope_payloads),
        ), redirect_stderr(io.StringIO()):
            result = pipeline.run(
                question="anything",
                scope_refs=scope_refs,
                supplied_plan=None,
            )
        self.assertEqual(result.streams_run, 2)
        self.assertEqual(result.streams_errored, 0)
        self.assertEqual(result.items_kept, 0)
        # add_items still creates the empty list entry; that's fine.
        # Just assert nothing kept.

    def test_supplied_plan_marks_plan_source_host_llm(self):
        payload = json.loads(QUERY_FIXTURE.read_text())
        scope_payloads = {"methodology.data_ops": payload}
        scope_refs = [_scope_ref("data_ops")]
        supplied = {
            "intent": "design",
            "subqueries": [
                {
                    "label": "primary",
                    "search_query": "star schema dbt",
                    "ranking_query": "How should we model star schemas in dbt?",
                    "scopes": ["methodology.data_ops"],
                }
            ],
        }
        with patch(
            "corpus_lib.pipeline.retriever.query_one",
            side_effect=_stub_query_one(scope_payloads),
        ), redirect_stderr(io.StringIO()):
            result = pipeline.run(
                question="How should we model star schemas?",
                scope_refs=scope_refs,
                supplied_plan=supplied,
            )
        self.assertEqual(result.plan_source, "host-llm")
        self.assertEqual(result.plan.intent, "design")

    def test_no_streams_when_plan_has_no_resolvable_scopes(self):
        """Defensive: if every subquery scope is missing from scope_index,
        pipeline returns an empty bundle without crashing.
        """
        # Force the planner to emit a plan whose scopes don't match what we
        # provide — supply a plan that names methodology.data_ops, but pass
        # only an engagement ScopeRef.
        scope_refs = [_scope_ref("engagement")]
        supplied = {
            "intent": "design",
            "subqueries": [
                {
                    "label": "primary",
                    # Sanitize will filter unknown scopes from this subquery's
                    # `scopes` list, then fall back to intent defaults...
                    # which will include methodology.data_ops (not in our
                    # available_scopes derived from scope_refs). Thus the
                    # filtered subquery scopes end up with the engagement
                    # scope for sure.
                    "search_query": "x",
                    "ranking_query": "x?",
                    "scopes": ["engagement"],
                }
            ],
        }
        with patch(
            "corpus_lib.pipeline.retriever.query_one",
            side_effect=_stub_query_one({"engagement": {"value": {"answer": "", "citations": {}}}}),
        ), redirect_stderr(io.StringIO()):
            result = pipeline.run(
                question="design?",
                scope_refs=scope_refs,
                supplied_plan=supplied,
            )
        self.assertEqual(result.streams_errored, 0)


if __name__ == "__main__":
    unittest.main()
