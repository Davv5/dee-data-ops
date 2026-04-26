"""Tests for corpus_lib.dedupe.dedupe_within_stream."""

from __future__ import annotations

import sys
import unittest
from pathlib import Path

SCRIPTS_DIR = Path(__file__).resolve().parent.parent / "scripts"
sys.path.insert(0, str(SCRIPTS_DIR))

from corpus_lib.dedupe import dedupe_within_stream  # noqa: E402
from corpus_lib.schema import SourceItem  # noqa: E402


def _item(source_id: str, snippet: str, scope: str = "x") -> SourceItem:
    return SourceItem(source_id=source_id, scope=scope, snippet=snippet)


class TestDedupeWithinStream(unittest.TestCase):
    def test_identical_source_id_and_snippet_collapse(self):
        items = [
            _item("src_a", "Star schema is a Kimball-style modeling approach."),
            _item("src_a", "Star schema is a Kimball-style modeling approach."),
        ]
        result = dedupe_within_stream(items)
        self.assertEqual(len(result), 1)

    def test_same_source_id_different_snippets_kept(self):
        items = [
            _item("src_a", "Star schema is a Kimball-style modeling approach."),
            _item("src_a", "Wide marts beat narrow marts for client-facing tables."),
        ]
        result = dedupe_within_stream(items)
        self.assertEqual(len(result), 2)

    def test_same_snippet_different_source_id_kept(self):
        items = [
            _item("src_a", "Same snippet text shared across sources."),
            _item("src_b", "Same snippet text shared across sources."),
        ]
        result = dedupe_within_stream(items)
        self.assertEqual(len(result), 2)

    def test_snippet_diverges_after_120_chars_collapses(self):
        """Documented behavior: dedupe key is snippet[:120]; identical
        prefixes collapse even if the tails differ. This is intentional —
        a near-duplicate citation that varies by trailing whitespace or
        sentence-boundary noise should collapse.
        """
        prefix = "x" * 120
        items = [
            _item("src_a", prefix + " tail-one"),
            _item("src_a", prefix + " tail-two-which-differs-substantially"),
        ]
        result = dedupe_within_stream(items)
        self.assertEqual(len(result), 1)

    def test_order_preserved_first_occurrence_wins(self):
        items = [
            _item("src_a", "first"),
            _item("src_b", "second"),
            _item("src_a", "first"),
            _item("src_c", "third"),
        ]
        result = dedupe_within_stream(items)
        snippets = [i.snippet for i in result]
        self.assertEqual(snippets, ["first", "second", "third"])

    def test_empty_input_returns_empty(self):
        self.assertEqual(dedupe_within_stream([]), [])


if __name__ == "__main__":
    unittest.main()
