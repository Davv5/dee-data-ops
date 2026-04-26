"""Integration tests for the corpus_research.py CLI entry point.

Stubs the retriever subprocess; runs the full --phase=retrieve →
--phase=finalize round-trip against fixture nlm payloads. Verifies:

- retrieve writes shortlist.json + rerank_prompt.md and prints both paths
- finalize without rerank-scores triggers local fallback + emits warning
- finalize with valid rerank-scores produces a Report keyed by candidate
- exit code 2 on usage errors (missing --question, missing --shortlist,
  unknown scope)
- exit code 4 on malformed --plan JSON
"""

from __future__ import annotations

import io
import json
import sys
import tempfile
import unittest
from contextlib import redirect_stderr, redirect_stdout
from pathlib import Path
from unittest.mock import patch

SCRIPTS_DIR = Path(__file__).resolve().parent.parent / "scripts"
sys.path.insert(0, str(SCRIPTS_DIR))

import corpus_research  # noqa: E402
from corpus_lib.retriever import _extract_hits_from_answer, clear_source_list_cache  # noqa: E402

FIXTURES = Path(__file__).resolve().parent / "fixtures"
QUERY_FIXTURE = FIXTURES / "nlm_data_ops_star_schema.json"
CORPUS_YAML_FIXTURE = FIXTURES / "corpus_minimal.yaml"


def _stub_query_one(payload):
    def fake(notebook_id, search_query, *, scope="", timeout=130):  # noqa: ARG001
        return _extract_hits_from_answer(payload, scope=scope)
    return fake


class TestRetrievePhase(unittest.TestCase):
    def setUp(self):
        clear_source_list_cache()
        self.tmpdir = Path(tempfile.mkdtemp(prefix="corpus-research-test-"))

    def test_retrieve_writes_shortlist_and_prompt_paths_to_stdout(self):
        payload = json.loads(QUERY_FIXTURE.read_text())
        argv = [
            "--phase=retrieve",
            "--question", "How should we model star schemas in dbt?",
            "--scope", "methodology.data_ops",
            "--out-dir", str(self.tmpdir),
        ]
        captured_out = io.StringIO()
        captured_err = io.StringIO()
        with patch(
            "corpus_lib.pipeline.retriever.query_one",
            side_effect=_stub_query_one(payload),
        ), patch(
            "corpus_research.resolve_scopes",
            wraps=corpus_research.resolve_scopes,
        ) as resolve_mock, redirect_stdout(captured_out), redirect_stderr(captured_err):
            # Patch resolve_scopes to use the fixture corpus.yaml
            def fake_resolve(scope, config_path=None):
                from corpus_lib.env import resolve_scopes
                return resolve_scopes(scope, config_path=CORPUS_YAML_FIXTURE)
            resolve_mock.side_effect = fake_resolve
            rc = corpus_research.main(argv)

        self.assertEqual(rc, 0, msg=f"stderr: {captured_err.getvalue()}")
        printed = json.loads(captured_out.getvalue().strip())
        self.assertIn("shortlist", printed)
        self.assertIn("rerank_prompt", printed)
        shortlist_path = Path(printed["shortlist"])
        prompt_path = Path(printed["rerank_prompt"])
        self.assertTrue(shortlist_path.exists())
        self.assertTrue(prompt_path.exists())

        shortlist = json.loads(shortlist_path.read_text())
        # plan_source is "deterministic" (no --plan supplied)
        self.assertEqual(shortlist["plan_source"], "deterministic")
        self.assertIn("plan-fallback", shortlist["warnings"])
        self.assertIn("question", shortlist)
        self.assertIn("primary_entity", shortlist)
        self.assertIn("candidates", shortlist)
        self.assertGreater(len(shortlist["candidates"]), 0)

        prompt_md = prompt_path.read_text()
        self.assertIn("<untrusted_content>", prompt_md)
        self.assertIn("</untrusted_content>", prompt_md)


class TestFinalizePhase(unittest.TestCase):
    def setUp(self):
        clear_source_list_cache()
        self.tmpdir = Path(tempfile.mkdtemp(prefix="corpus-research-finalize-"))
        # Build a minimal shortlist.json by running retrieve first.
        payload = json.loads(QUERY_FIXTURE.read_text())
        argv = [
            "--phase=retrieve",
            "--question", "star schema dbt",
            "--scope", "methodology.data_ops",
            "--out-dir", str(self.tmpdir),
        ]
        with patch(
            "corpus_lib.pipeline.retriever.query_one",
            side_effect=_stub_query_one(payload),
        ), patch("corpus_research.resolve_scopes") as resolve_mock, \
             redirect_stdout(io.StringIO()), redirect_stderr(io.StringIO()):
            from corpus_lib.env import resolve_scopes as real_resolve
            resolve_mock.side_effect = lambda scope, config_path=None: real_resolve(
                scope, config_path=CORPUS_YAML_FIXTURE
            )
            corpus_research.main(argv)
        self.shortlist_path = self.tmpdir / "shortlist.json"
        self.assertTrue(self.shortlist_path.exists())

    def test_finalize_without_scores_uses_local_fallback(self):
        argv = [
            "--phase=finalize",
            "--shortlist", str(self.shortlist_path),
            "--out-dir", str(self.tmpdir),
        ]
        captured_out = io.StringIO()
        captured_err = io.StringIO()
        with redirect_stdout(captured_out), redirect_stderr(captured_err):
            rc = corpus_research.main(argv)
        self.assertEqual(rc, 0, msg=f"stderr: {captured_err.getvalue()}")
        printed = json.loads(captured_out.getvalue().strip())
        report_path = Path(printed["report"])
        self.assertTrue(report_path.exists())
        report = json.loads(report_path.read_text())
        self.assertIn("ranked_candidates", report)
        self.assertIn("clusters", report)
        self.assertIn("warnings", report)
        # Local fallback fired → rerank-fallback warning present
        self.assertIn("rerank-fallback", report["warnings"])

    def test_finalize_with_valid_scores_keys_to_candidate_ids(self):
        shortlist = json.loads(self.shortlist_path.read_text())
        scores_payload = {
            "scores": [
                {"candidate_id": c["candidate_id"], "relevance": 75, "reason": "ok"}
                for c in shortlist["candidates"]
            ]
        }
        scores_path = self.tmpdir / "scores.json"
        scores_path.write_text(json.dumps(scores_payload))

        argv = [
            "--phase=finalize",
            "--shortlist", str(self.shortlist_path),
            "--rerank-scores", str(scores_path),
            "--out-dir", str(self.tmpdir),
        ]
        captured_out = io.StringIO()
        with redirect_stdout(captured_out), redirect_stderr(io.StringIO()):
            rc = corpus_research.main(argv)
        self.assertEqual(rc, 0)
        printed = json.loads(captured_out.getvalue().strip())
        report_path = Path(printed["report"])
        report = json.loads(report_path.read_text())

        # rerank-fallback NOT present (valid scores consumed without fallback)
        self.assertNotIn("rerank-fallback", report["warnings"])
        # All candidates have non-zero final_score
        for c in report["ranked_candidates"]:
            self.assertGreater(c["final_score"], 0)


class TestExitCodes(unittest.TestCase):
    def test_missing_question_exits_2(self):
        rc = corpus_research.main(["--phase=retrieve", "--scope", "methodology.data_ops"])
        self.assertEqual(rc, 2)

    def test_missing_shortlist_exits_2(self):
        rc = corpus_research.main(["--phase=finalize"])
        self.assertEqual(rc, 2)

    def test_malformed_plan_exits_4(self):
        with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as fh:
            fh.write("this is not json")
            bad_plan = Path(fh.name)
        captured_err = io.StringIO()
        with redirect_stderr(captured_err):
            rc = corpus_research.main([
                "--phase=retrieve",
                "--question", "x",
                "--scope", "methodology.data_ops",
                "--plan", str(bad_plan),
            ])
        self.assertEqual(rc, 4)
        self.assertIn("[Planner]", captured_err.getvalue())

    def test_unknown_scope_exits_2(self):
        captured_err = io.StringIO()
        # Patch resolve_scopes to use the fixture corpus.yaml so the
        # missing-file fallback doesn't silently succeed.
        with patch("corpus_research.resolve_scopes") as resolve_mock:
            from corpus_lib.env import resolve_scopes as real_resolve
            resolve_mock.side_effect = lambda scope, config_path=None: real_resolve(
                scope, config_path=CORPUS_YAML_FIXTURE
            )
            with redirect_stderr(captured_err):
                rc = corpus_research.main([
                    "--phase=retrieve",
                    "--question", "x",
                    "--scope", "methodology.bogus_scope",
                ])
        self.assertEqual(rc, 2)


if __name__ == "__main__":
    unittest.main()
