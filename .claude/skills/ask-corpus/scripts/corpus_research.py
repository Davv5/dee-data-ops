#!/usr/bin/env python3
"""ask-corpus v2 — engine entry point.

Two-phase JSON handshake the host LLM drives:

  --phase=retrieve  → engine plans, fans out, fuses, writes shortlist.json
                      + rerank_prompt.md to --out-dir; prints both paths.
  --phase=finalize  → engine loads shortlist + rerank scores, applies them,
                      builds the Report, writes report.json; prints path.

Exit codes:
  0  success
  2  usage error (bad args, unknown scope, malformed shortlist)
  3  retrieval failure (all scopes errored, no streams ran)
  4  plan validation failure (--plan path bad / unparseable)

Stderr trace lines fire throughout: [Planner], [Retriever], [Pipeline],
[Fusion], [Rerank]. Always-on; redirect to a log file with `2>...` if needed.
"""

from __future__ import annotations

import argparse
import dataclasses
import json
import sys
import tempfile
from pathlib import Path

SCRIPT_DIR = Path(__file__).resolve().parent
if str(SCRIPT_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPT_DIR))

from corpus_lib import fusion, pipeline, rerank  # noqa: E402
from corpus_lib.env import UnknownScopeError, resolve_scopes  # noqa: E402
from corpus_lib.log import trace  # noqa: E402
from corpus_lib.schema import (  # noqa: E402
    Candidate,
    QueryPlan,
    SourceItem,
    SubQuery,
    TraceSummary,
)

EXIT_OK = 0
EXIT_USAGE = 2
EXIT_RETRIEVAL = 3
EXIT_PLAN_VALIDATION = 4

DEFAULT_SHORTLIST_SIZE = 30


def main(argv: list[str] | None = None) -> int:
    args = _build_parser().parse_args(argv)
    if args.phase == "retrieve":
        return _cmd_retrieve(args)
    return _cmd_finalize(args)


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="corpus_research",
        description="ask-corpus v2 — fan-out / fuse / rerank engine.",
    )
    parser.add_argument(
        "--phase",
        choices=["retrieve", "finalize"],
        required=True,
        help="Two-phase handshake: retrieve emits a rerank prompt; finalize "
        "consumes host-LLM scores and writes the Report.",
    )
    parser.add_argument(
        "--question",
        help="The user's question. Required for --phase=retrieve.",
    )
    parser.add_argument(
        "--scope",
        default=None,
        help="Scope identifier (methodology / methodology.<key> / engagement). "
        "Default: methodology (cross-query all craft notebooks).",
    )
    parser.add_argument(
        "--plan",
        type=Path,
        default=None,
        help="Path to a JSON file containing the host LLM's plan. Optional; "
        "absent means deterministic-fallback plan.",
    )
    parser.add_argument(
        "--shortlist",
        type=Path,
        default=None,
        help="Path to shortlist.json from a prior --phase=retrieve. Required "
        "for --phase=finalize.",
    )
    parser.add_argument(
        "--rerank-scores",
        type=Path,
        default=None,
        help="Path to a JSON file with the host LLM's rerank scores. "
        "Optional for --phase=finalize; absence triggers local fallback.",
    )
    parser.add_argument(
        "--out-dir",
        type=Path,
        default=None,
        help="Where to write outputs. Default: a fresh tempdir.",
    )
    parser.add_argument(
        "--depth",
        choices=["quick", "default", "deep"],
        default="default",
        help="Retrieval breadth (forward-compat for U9/U10-deferred).",
    )
    parser.add_argument(
        "--shortlist-size",
        type=int,
        default=DEFAULT_SHORTLIST_SIZE,
        help=f"Top-N candidates surfaced for rerank (default {DEFAULT_SHORTLIST_SIZE}).",
    )
    return parser


# ---------------------------------------------------------------------------
# retrieve


def _cmd_retrieve(args: argparse.Namespace) -> int:
    if not args.question:
        print("[Usage] --question is required for --phase=retrieve", file=sys.stderr)
        return EXIT_USAGE

    try:
        scope_refs = resolve_scopes(args.scope)
    except UnknownScopeError as exc:
        print(f"[Env] {exc}", file=sys.stderr)
        return EXIT_USAGE

    supplied_plan = None
    if args.plan is not None:
        try:
            supplied_plan = json.loads(args.plan.read_text())
        except (OSError, json.JSONDecodeError) as exc:
            trace("Planner", f"could not load --plan {args.plan}: {exc}")
            return EXIT_PLAN_VALIDATION
        if not isinstance(supplied_plan, dict):
            trace("Planner", "--plan content must be a JSON object")
            return EXIT_PLAN_VALIDATION

    try:
        result = pipeline.run(
            question=args.question,
            scope_refs=scope_refs,
            supplied_plan=supplied_plan,
            depth=args.depth,
        )
    except Exception as exc:  # noqa: BLE001 — surface unexpected to operator
        trace("Pipeline", f"unhandled error: {type(exc).__name__}: {exc}")
        return EXIT_RETRIEVAL

    candidates = fusion.weighted_rrf(
        result.bundle.items_by_source_and_query,
        result.plan,
        pool_limit=fusion.DEFAULT_POOL_LIMIT,
    )

    if (
        result.streams_errored > 0
        and result.streams_run == 0
        and not candidates
    ):
        trace("Pipeline", "all streams errored and no candidates produced")
        return EXIT_RETRIEVAL

    primary_entity = rerank.extract_primary_entity(args.question)
    shortlist = candidates[: args.shortlist_size]
    prompt_md = rerank.build_rerank_prompt(
        question=args.question,
        plan=result.plan,
        candidates=shortlist,
        primary_entity=primary_entity,
    )

    out_dir = _resolve_out_dir(args.out_dir, prefix="ask-corpus-retrieve-")
    out_dir.mkdir(parents=True, exist_ok=True)
    shortlist_path = out_dir / "shortlist.json"
    prompt_path = out_dir / "rerank_prompt.md"

    trace_summary = TraceSummary(
        plan_source=result.plan_source,
        n_subqueries=len(result.plan.subqueries),
        n_streams_run=result.streams_run,
        n_streams_errored=result.streams_errored,
    )
    payload = {
        "question": args.question,
        "intent": result.plan.intent,
        "primary_entity": primary_entity,
        "plan_source": result.plan_source,
        "plan": dataclasses.asdict(result.plan),
        "candidates": [dataclasses.asdict(c) for c in shortlist],
        "warnings": _initial_warnings(result, shortlist),
        "trace_summary": dataclasses.asdict(trace_summary),
        "errors_by_scope": dict(result.bundle.errors_by_source),
    }
    shortlist_path.write_text(json.dumps(payload, indent=2, default=str))
    prompt_path.write_text(prompt_md)

    print(
        json.dumps(
            {
                "shortlist": str(shortlist_path),
                "rerank_prompt": str(prompt_path),
            }
        )
    )
    return EXIT_OK


# ---------------------------------------------------------------------------
# finalize


def _cmd_finalize(args: argparse.Namespace) -> int:
    if args.shortlist is None:
        print(
            "[Usage] --shortlist is required for --phase=finalize",
            file=sys.stderr,
        )
        return EXIT_USAGE
    try:
        shortlist_payload = json.loads(args.shortlist.read_text())
    except (OSError, json.JSONDecodeError) as exc:
        print(
            f"[Usage] could not load --shortlist {args.shortlist}: {exc}",
            file=sys.stderr,
        )
        return EXIT_USAGE

    rerank_payload: dict | None = None
    if args.rerank_scores is not None:
        try:
            rerank_payload = json.loads(args.rerank_scores.read_text())
        except (OSError, json.JSONDecodeError) as exc:
            trace(
                "Rerank",
                f"could not load --rerank-scores {args.rerank_scores}: {exc} "
                f"— continuing with local fallback",
            )

    try:
        candidates = [_dict_to_candidate(d) for d in shortlist_payload["candidates"]]
        plan = _dict_to_plan(shortlist_payload["plan"])
    except (KeyError, TypeError) as exc:
        print(
            f"[Usage] shortlist payload missing required fields: {exc}",
            file=sys.stderr,
        )
        return EXIT_USAGE

    primary_entity = shortlist_payload.get("primary_entity", "")
    warnings: list[str] = list(shortlist_payload.get("warnings", []))

    sorted_cands = rerank.apply_scores(
        candidates,
        payload=rerank_payload,
        primary_entity=primary_entity,
        warnings=warnings,
    )

    clusters = pipeline.group_by_subquery(sorted_cands, plan)
    warnings = pipeline.warnings_for(
        sorted_cands=sorted_cands,
        plan_source=shortlist_payload.get("plan_source", "host-llm"),
        streams_errored=int(
            shortlist_payload.get("trace_summary", {}).get("n_streams_errored", 0)
        ),
        seed_warnings=warnings,
    )

    report_payload = {
        "question": shortlist_payload.get("question", ""),
        "intent": shortlist_payload.get("intent", plan.intent),
        "primary_entity": primary_entity,
        "plan": shortlist_payload["plan"],
        "ranked_candidates": [dataclasses.asdict(c) for c in sorted_cands],
        "clusters": [dataclasses.asdict(c) for c in clusters],
        "warnings": warnings,
        "trace_summary": shortlist_payload.get("trace_summary", {}),
    }

    out_dir = _resolve_out_dir(args.out_dir, prefix="ask-corpus-finalize-")
    out_dir.mkdir(parents=True, exist_ok=True)
    report_path = out_dir / "report.json"
    report_path.write_text(json.dumps(report_payload, indent=2, default=str))

    print(json.dumps({"report": str(report_path)}))
    return EXIT_OK


# ---------------------------------------------------------------------------
# helpers


def _resolve_out_dir(supplied: Path | None, *, prefix: str) -> Path:
    if supplied is not None:
        return supplied
    return Path(tempfile.mkdtemp(prefix=prefix))


def _dict_to_candidate(d: dict) -> Candidate:
    """Reconstruct a Candidate dataclass from JSON-serialized dict."""
    source_items_raw = d.get("source_items") or []
    source_items = [SourceItem(**si) for si in source_items_raw]
    d_copy = {**d, "source_items": source_items}
    return Candidate(**d_copy)


def _dict_to_plan(d: dict) -> QueryPlan:
    subqueries_raw = d.get("subqueries") or []
    subqueries = [SubQuery(**s) for s in subqueries_raw]
    d_copy = {**d, "subqueries": subqueries}
    return QueryPlan(**d_copy)


def _initial_warnings(
    result: pipeline.PipelineResult,
    shortlist: list[Candidate],
) -> list[str]:
    """Retrieve-phase seed warnings. Finalize composes the full set via
    ``pipeline.warnings_for``.
    """
    out: list[str] = []
    if result.streams_errored > 0:
        out.append("scope-errors")
    if result.plan_source == "deterministic":
        out.append("plan-fallback")
    if not shortlist:
        out.append("no-usable-items")
    return out


if __name__ == "__main__":
    sys.exit(main())
