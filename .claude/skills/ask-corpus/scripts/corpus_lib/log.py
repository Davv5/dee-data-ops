"""Stderr trace formatter. Always-on, never blocking.

Per plan R8: emit `[Planner]`, `[Retriever]`, `[Pipeline]`, `[Fusion]`,
`[Rerank]`, `[Cluster]`, `[SourceList]` lines as the engine executes.
Mirrors last30days's pipeline.py:242-257 always-on trace style.
"""

from __future__ import annotations

import sys


def trace(component: str, msg: str) -> None:
    """Emit one structured trace line to stderr.

    Format: `[<Component>] <message>`. Never raises; if stderr write fails,
    silently no-op (better to lose a trace than crash the engine).
    """
    try:
        print(f"[{component}] {msg}", file=sys.stderr, flush=True)
    except Exception:  # noqa: BLE001 - tracing must never crash the caller
        pass
