"""Phase 1 text REPL — prove the brain and the tool layer without any audio."""

from __future__ import annotations

from typing import Any

from ..brain.agent import Agent
from ..config import Config


def _confirm(name: str, args: dict[str, Any]) -> bool:
    print(f"\n  ⚠  Jarvis wants to run [{name}] with:")
    for key, val in (args or {}).items():
        shown = str(val)
        if len(shown) > 300:
            shown = shown[:300] + "…"
        print(f"       {key}: {shown}")
    answer = input("  Allow? [y/N] ").strip().lower()
    return answer in ("y", "yes")


def _show_activity(name: str, args: dict[str, Any]) -> None:
    print(f"  · {name}({', '.join(f'{k}={v!r}' for k, v in (args or {}).items())[:120]})")


def run(config: Config) -> None:
    agent = Agent(config, confirm_cb=_confirm, activity_cb=_show_activity)
    print("Jarvis (text mode). Type 'exit' or Ctrl-D to quit.")
    print(f"Model: {config.brain.model} @ {config.brain.host}")
    print(
        "Confirmations: "
        + ("ON (destructive actions ask first)" if config.safety.confirm_destructive else "OFF (autonomous)")
    )
    print()

    while True:
        try:
            user = input("you › ").strip()
        except (EOFError, KeyboardInterrupt):
            print("\nbye.")
            return
        if not user:
            continue
        if user.lower() in ("exit", "quit"):
            print("bye.")
            return

        try:
            reply = agent.say(user)
        except Exception as exc:  # keep the REPL alive on any failure
            print(f"jarvis › [error: {exc}]\n")
            continue
        print(f"jarvis › {reply}\n")
