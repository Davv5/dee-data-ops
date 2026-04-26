"""Tests for corpus_lib.env.resolve_scopes."""

from __future__ import annotations

import io
import sys
import unittest
from contextlib import redirect_stderr
from pathlib import Path

# Make the engine package importable when running from repo root.
SCRIPTS_DIR = Path(__file__).resolve().parent.parent / "scripts"
sys.path.insert(0, str(SCRIPTS_DIR))

from corpus_lib.env import (  # noqa: E402
    ScopeRef,
    UnknownScopeError,
    resolve_scopes,
)

FIXTURE = Path(__file__).resolve().parent / "fixtures" / "corpus_minimal.yaml"


class TestResolveScopes(unittest.TestCase):
    def test_specific_methodology_key_returns_one(self):
        refs = resolve_scopes("methodology.data_ops", FIXTURE)
        self.assertEqual(len(refs), 1)
        self.assertEqual(refs[0].key, "data_ops")
        self.assertEqual(refs[0].notebook_id, "7c7cd5d4-22df-4ef0-8b74-ed87e0ca4e6a")
        self.assertEqual(refs[0].name, "Data Ops")
        self.assertEqual(refs[0].scope_label, "methodology.data_ops")

    def test_methodology_default_returns_all(self):
        refs = resolve_scopes("methodology", FIXTURE)
        self.assertEqual(len(refs), 2)
        self.assertEqual(refs[0].key, "data_ops")
        self.assertEqual(refs[1].key, "metabase")

    def test_none_scope_treated_as_methodology(self):
        refs_none = resolve_scopes(None, FIXTURE)
        refs_explicit = resolve_scopes("methodology", FIXTURE)
        self.assertEqual([r.key for r in refs_none], [r.key for r in refs_explicit])

    def test_empty_string_treated_as_methodology(self):
        refs = resolve_scopes("", FIXTURE)
        self.assertEqual(len(refs), 2)

    def test_engagement_returns_engagement_entry(self):
        refs = resolve_scopes("engagement", FIXTURE)
        self.assertEqual(len(refs), 1)
        self.assertEqual(refs[0].key, "engagement")
        self.assertEqual(refs[0].name, "D-DEE Engagement Memory")
        self.assertEqual(refs[0].scope_label, "engagement")

    def test_optional_weight_and_size_hint_pass_through(self):
        refs = resolve_scopes("methodology.metabase", FIXTURE)
        self.assertEqual(refs[0].weight, 1.5)
        self.assertEqual(refs[0].size_hint, 30)

    def test_default_weight_when_absent(self):
        refs = resolve_scopes("methodology.data_ops", FIXTURE)
        self.assertEqual(refs[0].weight, 1.0)
        self.assertIsNone(refs[0].size_hint)


class TestResolveScopesErrors(unittest.TestCase):
    def test_unknown_methodology_key_raises(self):
        with self.assertRaises(UnknownScopeError) as ctx:
            resolve_scopes("methodology.bogus", FIXTURE)
        self.assertIn("bogus", str(ctx.exception))

    def test_unknown_scope_string_raises(self):
        with self.assertRaises(UnknownScopeError) as ctx:
            resolve_scopes("not_a_scope", FIXTURE)
        self.assertIn("not_a_scope", str(ctx.exception))

    def test_missing_corpus_yaml_falls_back_with_warning(self):
        """Edge case from plan U2: missing corpus.yaml returns hardcoded
        Data Ops UUID and emits one stderr warning."""
        nonexistent = Path("/tmp/definitely-does-not-exist-corpus.yaml")
        if nonexistent.exists():
            self.skipTest("test fixture path collision")
        captured = io.StringIO()
        with redirect_stderr(captured):
            refs = resolve_scopes(None, nonexistent)
        self.assertEqual(len(refs), 1)
        self.assertEqual(refs[0].notebook_id, "7c7cd5d4-22df-4ef0-8b74-ed87e0ca4e6a")
        self.assertIn("falling back", captured.getvalue())


class TestScopeRefSerialization(unittest.TestCase):
    def test_scope_ref_is_hashable(self):
        ref = ScopeRef(
            key="data_ops",
            notebook_id="abc",
            name="Data Ops",
        )
        # Frozen dataclass should be hashable; needed for use in dict keys / sets
        self.assertEqual(hash(ref), hash(ref))

    def test_scope_label_for_engagement(self):
        ref = ScopeRef(key="engagement", notebook_id="abc", name="D-DEE")
        self.assertEqual(ref.scope_label, "engagement")

    def test_scope_label_for_methodology(self):
        ref = ScopeRef(key="data_ops", notebook_id="abc", name="Data Ops")
        self.assertEqual(ref.scope_label, "methodology.data_ops")


class TestRealCorpusYaml(unittest.TestCase):
    """Smoke test against the real .claude/corpus.yaml in the repo."""

    REAL = Path(__file__).resolve().parents[4] / ".claude" / "corpus.yaml"

    def test_real_corpus_yaml_resolves_methodology(self):
        if not self.REAL.exists():
            self.skipTest(f"real corpus.yaml not found at {self.REAL}")
        refs = resolve_scopes("methodology", self.REAL)
        # Live config has 3 methodology entries: data_ops, metabase, metabase_learn
        self.assertGreaterEqual(len(refs), 3)
        keys = [r.key for r in refs]
        self.assertIn("data_ops", keys)
        self.assertIn("metabase", keys)
        self.assertIn("metabase_learn", keys)

    def test_real_corpus_yaml_resolves_engagement(self):
        if not self.REAL.exists():
            self.skipTest(f"real corpus.yaml not found at {self.REAL}")
        refs = resolve_scopes("engagement", self.REAL)
        self.assertEqual(len(refs), 1)
        self.assertEqual(refs[0].key, "engagement")
        self.assertTrue(refs[0].notebook_id)


if __name__ == "__main__":
    unittest.main()
