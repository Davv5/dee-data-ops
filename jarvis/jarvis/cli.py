"""`jarvis` command-line entry point."""

from __future__ import annotations

import argparse
import sys

from .config import load_config
from .brain.llm import OllamaClient, OllamaError


def _cmd_chat(args: argparse.Namespace) -> int:
    from .loop import text

    config = load_config(args.config)
    text.run(config)
    return 0


def _cmd_serve(args: argparse.Namespace) -> int:
    from . import server

    config = load_config(args.config)
    server.serve(config, port=args.port)
    return 0


def _cmd_doctor(args: argparse.Namespace) -> int:
    """Check that everything Jarvis needs is in place."""
    config = load_config(args.config)
    ok = True
    print(f"config: {config.source}")
    print(f"brain:  {config.brain.model} @ {config.brain.host}")

    client = OllamaClient(host=config.brain.host, model=config.brain.model)
    try:
        models = client.list_models()
        print(f"  [ok] Ollama reachable, {len(models)} model(s) installed")
        if any(m == config.brain.model or m.startswith(config.brain.model) for m in models):
            print(f"  [ok] model '{config.brain.model}' is pulled")
        else:
            ok = False
            print(f"  [!!] model '{config.brain.model}' not found — run: ollama pull {config.brain.model}")
    except OllamaError as exc:
        ok = False
        print(f"  [!!] {exc}")
        print("       start it with: ollama serve")

    # Voice deps (Phase 2+) — informational only.
    try:
        import sounddevice  # noqa: F401
        import pywhispercpp  # noqa: F401
        print("  [ok] voice dependencies present")
    except ImportError:
        print("  [..] voice deps not installed (needed for `jarvis voice`/`run`):")
        print("       pip install '.[voice]'")

    print("\nall good." if ok else "\nfix the [!!] items above.")
    return 0 if ok else 1


def _cmd_unavailable(phase: str):
    def handler(args: argparse.Namespace) -> int:
        print(f"`jarvis {args.command}` arrives in {phase}. For now use: jarvis chat")
        return 1

    return handler


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="jarvis", description="Local voice assistant.")
    parser.add_argument("-c", "--config", help="path to config.yaml")
    sub = parser.add_subparsers(dest="command", required=True)

    sub.add_parser("chat", help="text REPL (Phase 1)").set_defaults(func=_cmd_chat)
    serve_p = sub.add_parser("serve", help="run the HTTP brain for the macOS HUD")
    serve_p.add_argument("--port", type=int, default=11500, help="port (default 11500)")
    serve_p.set_defaults(func=_cmd_serve)
    sub.add_parser("doctor", help="check Ollama, model, and deps").set_defaults(func=_cmd_doctor)
    sub.add_parser("voice", help="push-to-talk (Phase 2)").set_defaults(
        func=_cmd_unavailable("Phase 2")
    )
    sub.add_parser("run", help="hands-free wake-word loop (Phase 3)").set_defaults(
        func=_cmd_unavailable("Phase 3")
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv if argv is not None else sys.argv[1:])
    return args.func(args)


if __name__ == "__main__":
    raise SystemExit(main())
