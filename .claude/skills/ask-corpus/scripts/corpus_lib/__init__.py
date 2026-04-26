"""ask-corpus v2 engine library.

Planner-driven, fan-out-fuse-rerank corpus research engine modeled on the
last30days-skill architecture. The retrieval primitive is `nlm notebook query
--json`; the host LLM is the planner and the reranker via a two-phase JSON
handshake (--phase=retrieve -> host scores -> --phase=finalize).

See docs/plans/2026-04-26-001-feat-corpus-research-engine-plan.md for design.
"""

__version__ = "2.0.0-dev"
