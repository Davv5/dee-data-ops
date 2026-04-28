"""Tests for corpus_lib.retriever — nlm subprocess wrappers + answer parsing.

Subprocess calls are stubbed via fixture JSON files captured from real nlm
runs against the live Data Ops notebook (2026-04-26).
"""

from __future__ import annotations

import json
import sys
import unittest
from pathlib import Path
from unittest.mock import patch

# Make engine package importable
SCRIPTS_DIR = Path(__file__).resolve().parent.parent / "scripts"
sys.path.insert(0, str(SCRIPTS_DIR))

from corpus_lib.retriever import (  # noqa: E402
    RetrievalError,
    _extract_hits_from_answer,
    clear_source_list_cache,
    list_sources,
    query_one,
)

FIXTURES = Path(__file__).resolve().parent / "fixtures"
QUERY_FIXTURE = FIXTURES / "nlm_data_ops_star_schema.json"
SOURCE_LIST_FIXTURE = FIXTURES / "nlm_source_list_data_ops.json"


def _stub_subprocess_run(stdout_path: Path, returncode: int = 0):
    """Build a mock that returns the fixture as if it were nlm stdout."""
    payload_text = stdout_path.read_text()

    class FakeCompleted:
        def __init__(self) -> None:
            self.stdout = payload_text
            self.stderr = ""
            self.returncode = returncode

    return lambda *args, **kwargs: FakeCompleted()


class TestExtractHits(unittest.TestCase):
    """Direct tests of the answer-parsing function with fixture payload."""

    def test_real_payload_yields_hits(self):
        payload = json.loads(QUERY_FIXTURE.read_text())
        hits = _extract_hits_from_answer(payload, scope="methodology.data_ops")
        self.assertGreater(len(hits), 0)
        # Each hit has the expected fields
        for hit in hits:
            self.assertTrue(hit.source_id)
            self.assertEqual(hit.scope, "methodology.data_ops")
            self.assertTrue(hit.snippet)
            self.assertIsInstance(hit.citation_numbers, list)
            self.assertGreater(len(hit.citation_numbers), 0)

    def test_unique_source_ids(self):
        payload = json.loads(QUERY_FIXTURE.read_text())
        hits = _extract_hits_from_answer(payload, scope="methodology.data_ops")
        source_ids = [h.source_id for h in hits]
        self.assertEqual(
            len(source_ids), len(set(source_ids)), "source_id should be unique per hit"
        )

    def test_handles_range_citations(self):
        """nlm uses [3-5] for ranges. Make sure the parser expands them."""
        payload = {
            "value": {
                "answer": "First fact [1]. Second fact spans many sources [3-5]. End.",
                "citations": {
                    "1": "src_a",
                    "3": "src_c",
                    "4": "src_d",
                    "5": "src_e",
                },
            }
        }
        hits = _extract_hits_from_answer(payload, scope="methodology.data_ops")
        ids = {h.source_id for h in hits}
        self.assertIn("src_a", ids)
        self.assertIn("src_c", ids)
        self.assertIn("src_d", ids)
        self.assertIn("src_e", ids)

    def test_handles_mixed_citations(self):
        """[5, 15-17] should expand to 5 + 15 + 16 + 17."""
        payload = {
            "value": {
                "answer": "Mixed citation form [5, 15-17] is common in nlm output.",
                "citations": {
                    "5": "src_e",
                    "15": "src_o",
                    "16": "src_p",
                    "17": "src_q",
                },
            }
        }
        hits = _extract_hits_from_answer(payload, scope="x")
        ids = {h.source_id for h in hits}
        self.assertEqual(ids, {"src_e", "src_o", "src_p", "src_q"})

    def test_empty_answer_returns_empty(self):
        hits = _extract_hits_from_answer({"value": {"answer": ""}}, scope="x")
        self.assertEqual(hits, [])

    def test_missing_value_key_returns_empty(self):
        hits = _extract_hits_from_answer({}, scope="x")
        self.assertEqual(hits, [])

    def test_snippet_truncates_at_1200(self):
        """Long answers shouldn't blow up downstream rerank prompts."""
        long_text = "Sentence one [1]. " + ("More long text. " * 200) + "End [1]."
        payload = {"value": {"answer": long_text, "citations": {"1": "src_a"}}}
        hits = _extract_hits_from_answer(payload, scope="x")
        self.assertEqual(len(hits), 1)
        self.assertLessEqual(len(hits[0].snippet), 1200)


class TestQueryOne(unittest.TestCase):
    def setUp(self):
        clear_source_list_cache()

    def test_query_one_with_stub(self):
        with patch(
            "corpus_lib.retriever.subprocess.run",
            side_effect=_stub_subprocess_run(QUERY_FIXTURE),
        ):
            hits = query_one(
                "fake-uuid",
                "star schema",
                scope="methodology.data_ops",
            )
        self.assertGreater(len(hits), 0)
        self.assertEqual(hits[0].scope, "methodology.data_ops")

    def test_query_one_retries_on_nonzero_exit(self):
        attempts = {"count": 0}
        payload_text = QUERY_FIXTURE.read_text()

        class FakeCompleted:
            def __init__(self, returncode: int) -> None:
                self.stdout = payload_text if returncode == 0 else ""
                self.stderr = "transient" if returncode != 0 else ""
                self.returncode = returncode

        def fake_run(*args, **kwargs):
            attempts["count"] += 1
            return FakeCompleted(returncode=1 if attempts["count"] == 1 else 0)

        with patch("corpus_lib.retriever.subprocess.run", side_effect=fake_run):
            hits = query_one("fake-uuid", "x", scope="test")
        self.assertEqual(attempts["count"], 2, "should retry once on transient failure")
        self.assertGreater(len(hits), 0)

    def test_query_one_raises_after_two_failures(self):
        class FakeCompleted:
            stdout = ""
            stderr = "permanent"
            returncode = 2

        with patch(
            "corpus_lib.retriever.subprocess.run",
            return_value=FakeCompleted(),
        ):
            with self.assertRaises(RetrievalError):
                query_one("fake-uuid", "x", scope="test")


class TestListSources(unittest.TestCase):
    def setUp(self):
        clear_source_list_cache()

    def test_list_sources_returns_id_to_title(self):
        with patch(
            "corpus_lib.retriever.subprocess.run",
            side_effect=_stub_subprocess_run(SOURCE_LIST_FIXTURE),
        ):
            titles = list_sources("fake-uuid")
        self.assertGreater(len(titles), 0)
        # All values are strings; all keys are strings (UUIDs)
        for src_id, title in titles.items():
            self.assertIsInstance(src_id, str)
            self.assertIsInstance(title, str)
            self.assertTrue(title)

    def test_list_sources_caches_per_process(self):
        call_count = {"n": 0}

        def fake_run_counting(*args, **kwargs):
            call_count["n"] += 1
            return _stub_subprocess_run(SOURCE_LIST_FIXTURE)()

        with patch(
            "corpus_lib.retriever.subprocess.run",
            side_effect=fake_run_counting,
        ):
            list_sources("uuid-x")
            list_sources("uuid-x")
            list_sources("uuid-x")
        self.assertEqual(call_count["n"], 1, "subsequent calls should hit cache")


class TestLiveSmoke(unittest.TestCase):
    """Optional live smoke against the real Data Ops notebook. Skipped unless
    the env explicitly opts in — the test suite stays fixture-driven by default
    so it doesn't make NotebookLM API calls on every CI run.
    """

    def test_live_query_against_data_ops(self):
        import os
        if os.environ.get("CORPUS_LIVE_SMOKE") != "1":
            self.skipTest("set CORPUS_LIVE_SMOKE=1 to run")
        clear_source_list_cache()
        hits = query_one(
            "7c7cd5d4-22df-4ef0-8b74-ed87e0ca4e6a",
            "star schema",
            scope="methodology.data_ops",
        )
        self.assertGreater(len(hits), 0)
        # Resolve titles for the first few hits
        titles = list_sources("7c7cd5d4-22df-4ef0-8b74-ed87e0ca4e6a")
        for hit in hits[:3]:
            self.assertIn(hit.source_id, titles)


if __name__ == "__main__":
    unittest.main()
