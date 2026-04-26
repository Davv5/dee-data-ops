"""Tests for corpus_lib.planner.plan_query and helpers.

Covers the U4 plan scenarios:
- happy path: well-formed plan JSON sanitizes to a QueryPlan
- subquery missing ranking_query is dropped, others kept, warning logged
- intent "frobnicate" is reset to _infer_intent(question), warning logged
- empty/None plan triggers _fallback_plan + host-LLM nudge trace
- scope "methodology.bogus" is filtered; if subquery scopes empty after,
  intent-default scopes are substituted
"""

from __future__ import annotations

import io
import math
import sys
import unittest
from contextlib import redirect_stderr
from pathlib import Path

SCRIPTS_DIR = Path(__file__).resolve().parent.parent / "scripts"
sys.path.insert(0, str(SCRIPTS_DIR))

from corpus_lib.planner import (  # noqa: E402
    INTENT_DEFAULT_SCOPES,
    SCOPE_CAPABILITIES,
    _infer_intent,
    plan_query,
)
from corpus_lib.schema import ALLOWED_INTENTS  # noqa: E402

AVAILABLE = [
    "methodology.data_ops",
    "methodology.metabase",
    "methodology.metabase_learn",
    "engagement",
]


def _capture_stderr(fn, *args, **kwargs):
    """Run fn, return (result, stderr_text)."""
    buf = io.StringIO()
    with redirect_stderr(buf):
        result = fn(*args, **kwargs)
    return result, buf.getvalue()


class TestPlanQueryHappyPath(unittest.TestCase):
    def test_well_formed_plan_sanitizes_to_query_plan(self):
        raw = {
            "intent": "design",
            "scope_weights": {
                "methodology.data_ops": 2.0,
                "methodology.metabase": 1.0,
            },
            "subqueries": [
                {
                    "label": "primary",
                    "search_query": "star schema dbt",
                    "ranking_query": "How should we model star schemas in dbt?",
                    "scopes": ["methodology.data_ops"],
                    "weight": 2.0,
                },
                {
                    "label": "metabase-angle",
                    "search_query": "metabase wide table",
                    "ranking_query": "What does Metabase prefer for wide marts?",
                    "scopes": ["methodology.metabase"],
                    "weight": 1.0,
                },
            ],
            "notes": ["host-supplied"],
        }
        plan, _ = _capture_stderr(
            plan_query,
            question="How should we model star schemas in dbt?",
            available_scopes=AVAILABLE,
            supplied_plan=raw,
        )

        self.assertEqual(plan.intent, "design")
        self.assertEqual(len(plan.subqueries), 2)
        labels = [s.label for s in plan.subqueries]
        self.assertEqual(labels, ["primary", "metabase-angle"])

        # weights normalized to sum to 1.0
        total = sum(s.weight for s in plan.subqueries)
        self.assertAlmostEqual(total, 1.0, places=6)
        # 2:1 input ratio preserved after normalization
        self.assertAlmostEqual(
            plan.subqueries[0].weight / plan.subqueries[1].weight, 2.0, places=6
        )

        # scope_weights covers all available scopes (defaults to 1.0 for any
        # not in raw) and is normalized
        for scope in AVAILABLE:
            self.assertIn(scope, plan.scope_weights)
        self.assertAlmostEqual(sum(plan.scope_weights.values()), 1.0, places=6)
        self.assertEqual(plan.notes, ["host-supplied"])

    def test_legacy_sources_key_accepted_as_scopes(self):
        """Host LLM may emit `sources` (last30days legacy) instead of `scopes`."""
        raw = {
            "intent": "convention",
            "subqueries": [
                {
                    "label": "primary",
                    "search_query": "naming",
                    "ranking_query": "What do we name marts?",
                    "sources": ["methodology.data_ops"],
                }
            ],
        }
        plan, _ = _capture_stderr(
            plan_query,
            question="naming convention",
            available_scopes=AVAILABLE,
            supplied_plan=raw,
        )
        self.assertEqual(plan.subqueries[0].scopes, ["methodology.data_ops"])


class TestPlanQuerySanitizationWarnings(unittest.TestCase):
    def test_subquery_missing_ranking_query_is_dropped(self):
        raw = {
            "intent": "design",
            "subqueries": [
                {
                    "label": "good",
                    "search_query": "star schema",
                    "ranking_query": "How should we model?",
                    "scopes": ["methodology.data_ops"],
                },
                {
                    "label": "missing-ranking",
                    "search_query": "wide tables",
                    # ranking_query absent
                    "scopes": ["methodology.data_ops"],
                },
            ],
        }
        plan, stderr_text = _capture_stderr(
            plan_query,
            question="design question",
            available_scopes=AVAILABLE,
            supplied_plan=raw,
        )
        self.assertEqual(len(plan.subqueries), 1)
        self.assertEqual(plan.subqueries[0].label, "good")
        self.assertIn("missing-ranking", stderr_text)
        self.assertIn("missing search_query or ranking_query", stderr_text)

    def test_subquery_missing_search_query_is_dropped(self):
        raw = {
            "intent": "convention",
            "subqueries": [
                {
                    "label": "missing-search",
                    "ranking_query": "what do we call this?",
                    "scopes": ["methodology.data_ops"],
                },
                {
                    "label": "good",
                    "search_query": "naming",
                    "ranking_query": "naming?",
                    "scopes": ["methodology.data_ops"],
                },
            ],
        }
        plan, stderr_text = _capture_stderr(
            plan_query,
            question="naming question",
            available_scopes=AVAILABLE,
            supplied_plan=raw,
        )
        self.assertEqual([s.label for s in plan.subqueries], ["good"])
        self.assertIn("missing-search", stderr_text)

    def test_unknown_intent_resets_to_inferred(self):
        raw = {
            "intent": "frobnicate",
            "subqueries": [
                {
                    "label": "primary",
                    "search_query": "deploy metabase",
                    "ranking_query": "How do we deploy Metabase?",
                    "scopes": ["methodology.metabase"],
                }
            ],
        }
        question = "How do we deploy Metabase to Cloud Run?"
        plan, stderr_text = _capture_stderr(
            plan_query,
            question=question,
            available_scopes=AVAILABLE,
            supplied_plan=raw,
        )
        # "deploy" wins -> "ops"
        self.assertEqual(plan.intent, "ops")
        self.assertIn("frobnicate", stderr_text)
        self.assertIn("not in allowed set", stderr_text)
        self.assertIn(plan.intent, ALLOWED_INTENTS)

    def test_non_dict_subquery_is_dropped_with_warning(self):
        raw = {
            "intent": "convention",
            "subqueries": [
                "not-a-dict",
                {
                    "label": "good",
                    "search_query": "naming",
                    "ranking_query": "naming?",
                    "scopes": ["methodology.data_ops"],
                },
            ],
        }
        plan, stderr_text = _capture_stderr(
            plan_query,
            question="naming?",
            available_scopes=AVAILABLE,
            supplied_plan=raw,
        )
        self.assertEqual(len(plan.subqueries), 1)
        self.assertIn("subquery #1 is not a dict", stderr_text)


class TestPlanQueryFallback(unittest.TestCase):
    def test_none_plan_triggers_fallback_with_host_llm_nudge(self):
        question = "How should we model star schemas?"
        plan, stderr_text = _capture_stderr(
            plan_query,
            question=question,
            available_scopes=AVAILABLE,
            supplied_plan=None,
        )
        # Single primary subquery covering all available scopes.
        self.assertEqual(len(plan.subqueries), 1)
        self.assertEqual(plan.subqueries[0].label, "primary")
        self.assertEqual(plan.subqueries[0].search_query, question)
        self.assertEqual(plan.subqueries[0].ranking_query, question)
        self.assertEqual(plan.subqueries[0].scopes, AVAILABLE)
        self.assertEqual(plan.notes, ["fallback-plan"])

        # Stderr nudge: tell the host LLM that IT is the planner.
        self.assertIn("YOU ARE the planner", stderr_text)
        self.assertIn("--plan", stderr_text)

    def test_empty_subqueries_falls_back_with_warning(self):
        raw = {"intent": "design", "subqueries": []}
        plan, stderr_text = _capture_stderr(
            plan_query,
            question="how should we design this?",
            available_scopes=AVAILABLE,
            supplied_plan=raw,
        )
        self.assertEqual(len(plan.subqueries), 1)
        self.assertEqual(plan.notes, ["fallback-plan"])
        self.assertIn("no valid subqueries", stderr_text)

    def test_all_subqueries_invalid_falls_back(self):
        raw = {
            "intent": "design",
            "subqueries": [
                {"label": "a", "search_query": "x"},  # no ranking_query
                {"label": "b", "ranking_query": "y"},  # no search_query
            ],
        }
        plan, stderr_text = _capture_stderr(
            plan_query,
            question="design?",
            available_scopes=AVAILABLE,
            supplied_plan=raw,
        )
        self.assertEqual(len(plan.subqueries), 1)
        self.assertEqual(plan.notes, ["fallback-plan"])
        self.assertIn("no valid subqueries", stderr_text)

    def test_fallback_with_no_available_scopes_uses_intent_defaults(self):
        plan, _ = _capture_stderr(
            plan_query,
            question="What did we decide about Speed-to-Lead?",
            available_scopes=[],
            supplied_plan=None,
        )
        # _infer_intent picks "history"; INTENT_DEFAULT_SCOPES["history"] = ["engagement"]
        self.assertEqual(plan.intent, "history")
        self.assertEqual(plan.subqueries[0].scopes, ["engagement"])


class TestScopeFiltering(unittest.TestCase):
    def test_bogus_scope_is_filtered_out_kept_scopes_survive(self):
        raw = {
            "intent": "convention",
            "subqueries": [
                {
                    "label": "primary",
                    "search_query": "naming",
                    "ranking_query": "naming?",
                    "scopes": [
                        "methodology.bogus",
                        "methodology.data_ops",
                    ],
                }
            ],
        }
        plan, _ = _capture_stderr(
            plan_query,
            question="naming?",
            available_scopes=AVAILABLE,
            supplied_plan=raw,
        )
        self.assertEqual(plan.subqueries[0].scopes, ["methodology.data_ops"])

    def test_all_scopes_bogus_falls_back_to_intent_defaults(self):
        raw = {
            "intent": "ops",
            "subqueries": [
                {
                    "label": "primary",
                    "search_query": "deploy",
                    "ranking_query": "how to deploy?",
                    "scopes": ["methodology.bogus", "methodology.also_bogus"],
                }
            ],
        }
        plan, stderr_text = _capture_stderr(
            plan_query,
            question="how to deploy metabase?",
            available_scopes=AVAILABLE,
            supplied_plan=raw,
        )
        # ops defaults: ["methodology.metabase", "methodology.data_ops"]
        self.assertEqual(
            plan.subqueries[0].scopes,
            INTENT_DEFAULT_SCOPES["ops"],
        )
        self.assertIn("intent-default scopes", stderr_text)

    def test_unknown_scope_in_scope_weights_is_filtered(self):
        raw = {
            "intent": "design",
            "scope_weights": {
                "methodology.bogus": 5.0,
                "methodology.data_ops": 2.0,
            },
            "subqueries": [
                {
                    "label": "primary",
                    "search_query": "x",
                    "ranking_query": "x?",
                    "scopes": ["methodology.data_ops"],
                }
            ],
        }
        plan, _ = _capture_stderr(
            plan_query,
            question="design?",
            available_scopes=AVAILABLE,
            supplied_plan=raw,
        )
        self.assertNotIn("methodology.bogus", plan.scope_weights)
        # methodology.data_ops dominates after normalization (was 2.0, others
        # default to 1.0 each across 3 remaining scopes).
        top_scope = max(plan.scope_weights, key=plan.scope_weights.get)
        self.assertEqual(top_scope, "methodology.data_ops")


class TestInferIntent(unittest.TestCase):
    def test_history_signals(self):
        for q in [
            "What did we decide about the speed-to-lead mart?",
            "history of the GHL extractor",
            "back when we picked Cloud Run",
            "When did we cut over to BigQuery?",
        ]:
            with self.subTest(q=q):
                self.assertEqual(_infer_intent(q), "history")

    def test_howto_signals(self):
        for q in [
            "how to back up Metabase",
            "How do I add a new staging model?",
            "step by step setup",
        ]:
            with self.subTest(q=q):
                self.assertEqual(_infer_intent(q), "howto")

    def test_ops_signals(self):
        for q in [
            "deploy metabase to cloud run",
            "rotate the service account",
            "schedule a cron for dbt build",
        ]:
            with self.subTest(q=q):
                self.assertEqual(_infer_intent(q), "ops")

    def test_design_signals(self):
        for q in [
            "what is the right schema design here",
            "should we use star schema",
            "tradeoffs of medallion vs kimball",
        ]:
            with self.subTest(q=q):
                self.assertEqual(_infer_intent(q), "design")

    def test_convention_signals(self):
        for q in [
            "what do we call new marts",
            "naming convention for staging models",
            "what's our style guide for SQL",
        ]:
            with self.subTest(q=q):
                self.assertEqual(_infer_intent(q), "convention")

    def test_default_is_convention(self):
        self.assertEqual(_infer_intent("blah blah"), "convention")
        self.assertEqual(_infer_intent(""), "convention")


class TestConstants(unittest.TestCase):
    def test_intent_default_scopes_cover_all_intents(self):
        for intent in ALLOWED_INTENTS:
            self.assertIn(intent, INTENT_DEFAULT_SCOPES)
            self.assertGreater(len(INTENT_DEFAULT_SCOPES[intent]), 0)

    def test_intent_default_scopes_reference_known_capabilities(self):
        for intent, scopes in INTENT_DEFAULT_SCOPES.items():
            for scope in scopes:
                self.assertIn(
                    scope,
                    SCOPE_CAPABILITIES,
                    msg=f"intent {intent!r} references unknown scope {scope!r}",
                )


class TestWeightNormalization(unittest.TestCase):
    def test_subquery_weights_sum_to_one(self):
        raw = {
            "intent": "design",
            "subqueries": [
                {
                    "label": f"q{i}",
                    "search_query": f"q{i}",
                    "ranking_query": f"q{i}?",
                    "scopes": ["methodology.data_ops"],
                    "weight": float(i + 1),
                }
                for i in range(4)
            ],
        }
        plan, _ = _capture_stderr(
            plan_query,
            question="design?",
            available_scopes=AVAILABLE,
            supplied_plan=raw,
        )
        total = sum(s.weight for s in plan.subqueries)
        self.assertTrue(math.isclose(total, 1.0, abs_tol=1e-9))

    def test_negative_weights_clamped_to_floor(self):
        raw = {
            "intent": "design",
            "subqueries": [
                {
                    "label": "neg",
                    "search_query": "x",
                    "ranking_query": "x?",
                    "scopes": ["methodology.data_ops"],
                    "weight": -5.0,
                },
                {
                    "label": "pos",
                    "search_query": "y",
                    "ranking_query": "y?",
                    "scopes": ["methodology.data_ops"],
                    "weight": 1.0,
                },
            ],
        }
        plan, _ = _capture_stderr(
            plan_query,
            question="design?",
            available_scopes=AVAILABLE,
            supplied_plan=raw,
        )
        # Both kept; negative clamped to >=0.05; weights normalize to sum=1.
        self.assertEqual(len(plan.subqueries), 2)
        self.assertAlmostEqual(
            sum(s.weight for s in plan.subqueries), 1.0, places=6
        )
        self.assertGreater(plan.subqueries[0].weight, 0)


if __name__ == "__main__":
    unittest.main()
