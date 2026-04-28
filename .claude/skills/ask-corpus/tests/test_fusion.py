"""Tests for corpus_lib.fusion.weighted_rrf + the diversity guard.

Covers U7 scenarios:
- happy path: candidate appearing in 2 streams accumulates score; ordering
  matches expected RRF math.
- collapse: same source_id across streams becomes one Candidate with summed
  score and merged provenance.
- per-scope diversity guard (parity ratio >0.6) reserves slots for the
  weaker scope.
- adversarial-fix edge case: parity ratio <0.6 → no reservation, RRF only.
- low-relevance scope (best item below threshold) → no reservation.
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

from corpus_lib.fusion import (  # noqa: E402
    DEFAULT_MIN_PER_SCOPE,
    DIVERSITY_RELEVANCE_THRESHOLD,
    QUALITY_PARITY_FLOOR,
    RRF_K,
    _diversify_pool,
    weighted_rrf,
)
from corpus_lib.schema import QueryPlan, SourceItem, SubQuery  # noqa: E402


def _item(
    source_id: str,
    scope: str,
    snippet: str = "snippet text",
    *,
    local_relevance: float = 0.5,
    citation_number: int = 1,
) -> SourceItem:
    return SourceItem(
        source_id=source_id,
        scope=scope,
        snippet=snippet,
        citation_number=citation_number,
        local_relevance=local_relevance,
    )


def _plan(
    *subqueries: SubQuery,
    scope_weights: dict[str, float] | None = None,
) -> QueryPlan:
    return QueryPlan(
        intent="design",
        subqueries=list(subqueries),
        scope_weights=scope_weights or {},
    )


class TestWeightedRRFMath(unittest.TestCase):
    def test_single_stream_single_item_score(self):
        sub = SubQuery(
            label="primary",
            search_query="x",
            ranking_query="x?",
            scopes=["methodology.data_ops"],
            weight=1.0,
        )
        plan = _plan(sub, scope_weights={"methodology.data_ops": 1.0})
        streams = {
            ("primary", "methodology.data_ops"): [
                _item("src_a", "methodology.data_ops"),
            ]
        }
        with redirect_stderr(io.StringIO()):
            pool = weighted_rrf(streams, plan)
        self.assertEqual(len(pool), 1)
        # rrf_score = 1.0 * 1.0 / (60 + 1) = 1/61
        self.assertAlmostEqual(pool[0].rrf_score, 1.0 / (RRF_K + 1), places=10)

    def test_candidate_in_two_streams_accumulates_score(self):
        sub = SubQuery(
            label="primary",
            search_query="x",
            ranking_query="x?",
            scopes=["methodology.data_ops", "methodology.metabase"],
            weight=1.0,
        )
        plan = _plan(
            sub,
            scope_weights={
                "methodology.data_ops": 1.0,
                "methodology.metabase": 1.0,
            },
        )
        streams = {
            ("primary", "methodology.data_ops"): [
                _item("src_a", "methodology.data_ops"),
                _item("src_b", "methodology.data_ops"),
            ],
            ("primary", "methodology.metabase"): [
                _item("src_a", "methodology.metabase"),
            ],
        }
        with redirect_stderr(io.StringIO()):
            pool = weighted_rrf(streams, plan)
        # src_a appears at rank 1 in both streams → score = 2/(60+1)
        # src_b appears once at rank 2 → score = 1/(60+2)
        ids = {c.source_id: c for c in pool}
        self.assertAlmostEqual(
            ids["src_a"].rrf_score,
            2.0 / (RRF_K + 1),
            places=10,
        )
        self.assertAlmostEqual(
            ids["src_b"].rrf_score,
            1.0 / (RRF_K + 2),
            places=10,
        )
        # src_a outranks src_b
        self.assertEqual(pool[0].source_id, "src_a")

    def test_subquery_weight_multiplies_scope_weight(self):
        sub_a = SubQuery(
            label="a",
            search_query="x",
            ranking_query="x?",
            scopes=["methodology.data_ops"],
            weight=2.0,
        )
        sub_b = SubQuery(
            label="b",
            search_query="y",
            ranking_query="y?",
            scopes=["methodology.data_ops"],
            weight=1.0,
        )
        plan = _plan(
            sub_a, sub_b,
            scope_weights={"methodology.data_ops": 1.5},
        )
        streams = {
            ("a", "methodology.data_ops"): [_item("src_a", "methodology.data_ops")],
            ("b", "methodology.data_ops"): [_item("src_a", "methodology.data_ops")],
        }
        with redirect_stderr(io.StringIO()):
            pool = weighted_rrf(streams, plan)
        # Combined: (2 * 1.5)/(61) + (1 * 1.5)/(61) = 4.5/61
        self.assertAlmostEqual(pool[0].rrf_score, 4.5 / 61, places=10)

    def test_collapse_merges_provenance_and_subquery_labels(self):
        sub_a = SubQuery(label="a", search_query="x", ranking_query="x?",
                         scopes=["methodology.data_ops"], weight=1.0)
        sub_b = SubQuery(label="b", search_query="y", ranking_query="y?",
                         scopes=["methodology.metabase"], weight=1.0)
        plan = _plan(
            sub_a, sub_b,
            scope_weights={"methodology.data_ops": 1.0, "methodology.metabase": 1.0},
        )
        streams = {
            ("a", "methodology.data_ops"): [_item("src_x", "methodology.data_ops")],
            ("b", "methodology.metabase"): [_item("src_x", "methodology.metabase")],
        }
        with redirect_stderr(io.StringIO()):
            pool = weighted_rrf(streams, plan)
        cand = pool[0]
        self.assertEqual(cand.source_id, "src_x")
        self.assertEqual(set(cand.subquery_labels), {"a", "b"})
        self.assertEqual(set(cand.sources), {"methodology.data_ops", "methodology.metabase"})
        self.assertEqual(len(cand.metadata["provenance"]), 2)
        self.assertEqual(len(cand.source_items), 2)


class TestDiversityGuardQualityAware(unittest.TestCase):
    def test_two_scopes_above_threshold_with_parity_both_reserved(self):
        """Adversarial fix happy case: two scopes both meet threshold AND
        have parity ratio >= 0.6 → both get reserved slots.
        """
        # Build: scope A has 5 above-threshold items (top 0.85), scope B has
        # 3 items at 0.55 (top 0.55). Parity ratio = 0.55/0.85 ≈ 0.65 > 0.6.
        # Both qualify for reservation.
        candidates = []
        for i in range(5):
            candidates.append(
                _build_candidate(
                    source_id=f"a{i}",
                    scope="methodology.data_ops",
                    rrf_score=1.0 / (RRF_K + i + 1),
                    local_relevance=0.85 - i * 0.05,
                )
            )
        for i in range(3):
            candidates.append(
                _build_candidate(
                    source_id=f"b{i}",
                    scope="methodology.metabase",
                    rrf_score=0.5 / (RRF_K + i + 1),
                    local_relevance=0.55 - i * 0.05,
                )
            )

        pool = _diversify_pool(candidates, pool_limit=4, min_per_scope=2)
        scopes = [c.scope for c in pool]
        # scope B should have at least 2 in the pool (reserved)
        self.assertGreaterEqual(scopes.count("methodology.metabase"), 2)

    def test_low_parity_ratio_below_floor_no_reservation(self):
        """Adversarial-fix critical case: scope's top is above the absolute
        threshold but its parity ratio is below QUALITY_PARITY_FLOOR. Should
        NOT reserve slots — competes on RRF merit only.
        """
        # Dominant: 0.85; weak: 0.32 above threshold but ratio = 0.376 < 0.6
        candidates = []
        for i in range(10):
            candidates.append(
                _build_candidate(
                    source_id=f"a{i}",
                    scope="methodology.data_ops",
                    rrf_score=1.0 / (RRF_K + i + 1),
                    local_relevance=0.85 - i * 0.05,
                )
            )
        for i in range(2):
            candidates.append(
                _build_candidate(
                    source_id=f"b{i}",
                    scope="methodology.metabase",
                    rrf_score=0.01 / (RRF_K + i + 1),  # very low rrf
                    local_relevance=0.32 - i * 0.05,
                )
            )

        # Sanity check: parity ratio < QUALITY_PARITY_FLOOR
        self.assertLess(0.32 / 0.85, QUALITY_PARITY_FLOOR)
        # And the weak scope's top IS above the absolute threshold
        self.assertGreaterEqual(0.32, DIVERSITY_RELEVANCE_THRESHOLD)

        pool = _diversify_pool(candidates, pool_limit=4, min_per_scope=2)
        # No scope-B reservation: low rrf means scope B doesn't win on merit
        # in the top-4 either.
        scopes = [c.scope for c in pool]
        self.assertEqual(scopes.count("methodology.metabase"), 0)

    def test_below_absolute_threshold_no_reservation(self):
        """Scope's top item is below DIVERSITY_RELEVANCE_THRESHOLD: never
        reserves, regardless of parity ratio.
        """
        candidates = []
        for i in range(5):
            candidates.append(
                _build_candidate(
                    source_id=f"a{i}",
                    scope="methodology.data_ops",
                    rrf_score=1.0 / (RRF_K + i + 1),
                    local_relevance=0.20 - i * 0.02,  # dominant=0.20 (< 0.30)
                )
            )
        for i in range(3):
            candidates.append(
                _build_candidate(
                    source_id=f"b{i}",
                    scope="methodology.metabase",
                    rrf_score=0.5 / (RRF_K + i + 1),
                    local_relevance=0.18 - i * 0.02,  # also < threshold
                )
            )
        # Parity ratio 0.18/0.20 = 0.9 (above floor) but neither meets
        # absolute threshold → no reservation.
        pool = _diversify_pool(candidates, pool_limit=4, min_per_scope=2)
        # Pool keeps top by RRF without injecting reserved slots.
        # scope a's higher rrf_scores dominate.
        self.assertGreater(
            sum(1 for c in pool if c.scope == "methodology.data_ops"),
            sum(1 for c in pool if c.scope == "methodology.metabase"),
        )

    def test_pool_limit_respected(self):
        candidates = [
            _build_candidate(
                source_id=f"x{i}",
                scope="methodology.data_ops",
                rrf_score=1.0 / (RRF_K + i + 1),
                local_relevance=0.5,
            )
            for i in range(20)
        ]
        pool = _diversify_pool(candidates, pool_limit=5)
        self.assertEqual(len(pool), 5)


class TestDeterministicOrdering(unittest.TestCase):
    def test_sort_stable_on_ties(self):
        sub = SubQuery(label="a", search_query="x", ranking_query="x?",
                       scopes=["methodology.data_ops", "methodology.metabase"],
                       weight=1.0)
        plan = _plan(
            sub,
            scope_weights={"methodology.data_ops": 1.0, "methodology.metabase": 1.0},
        )
        # Two distinct candidates with identical rrf_score and local_relevance
        # → tiebreak by scope label (alpha), then source_id.
        streams = {
            ("a", "methodology.data_ops"): [
                _item("z_src", "methodology.data_ops", local_relevance=0.5),
            ],
            ("a", "methodology.metabase"): [
                _item("a_src", "methodology.metabase", local_relevance=0.5),
            ],
        }
        with redirect_stderr(io.StringIO()):
            pool = weighted_rrf(streams, plan)
        # Same rrf_score, same local_relevance. Tiebreak: scope alpha.
        # methodology.data_ops < methodology.metabase
        self.assertEqual(pool[0].scope, "methodology.data_ops")


def _build_candidate(*, source_id, scope, rrf_score, local_relevance):
    """Build a Candidate directly (bypassing fusion) to test the diversifier."""
    from corpus_lib.schema import Candidate
    return Candidate(
        candidate_id=source_id,
        source_id=source_id,
        scope=scope,
        snippet="snippet",
        rrf_score=rrf_score,
        local_relevance=local_relevance,
    )


if __name__ == "__main__":
    unittest.main()
