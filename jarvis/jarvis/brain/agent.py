"""Agent: conversation memory + the tool-calling loop."""

from __future__ import annotations

from typing import Any, Callable, Optional

from ..config import Config
from .llm import OllamaClient
from .prompts import SYSTEM_PROMPT
from .tools import run_tool, tool_schemas

# (tool_name, args) -> bool. Returns True to allow a destructive call.
ConfirmCallback = Callable[[str, dict[str, Any]], bool]

# Called with each tool name + args as it runs, for UI/voice feedback.
ActivityCallback = Callable[[str, dict[str, Any]], None]

# Guards against runaway tool loops within a single user turn.
MAX_TOOL_ROUNDS = 8


class Agent:
    def __init__(
        self,
        config: Config,
        *,
        confirm_cb: Optional[ConfirmCallback] = None,
        activity_cb: Optional[ActivityCallback] = None,
    ) -> None:
        self.config = config
        self.llm = OllamaClient(
            host=config.brain.host,
            model=config.brain.model,
            keep_alive=config.brain.keep_alive,
            temperature=config.brain.temperature,
        )
        self.confirm_cb = confirm_cb
        self.activity_cb = activity_cb
        self.history: list[dict[str, Any]] = [
            {"role": "system", "content": SYSTEM_PROMPT}
        ]

    def _trim_history(self) -> None:
        """Keep the system prompt plus the most recent N turns."""
        limit = self.config.brain.max_history * 2  # rough: user+assistant per turn
        if len(self.history) > limit + 1:
            self.history = [self.history[0]] + self.history[-limit:]

    def say(self, user_text: str) -> str:
        """Process one user utterance; return the assistant's final text reply."""
        self.history.append({"role": "user", "content": user_text})

        for _ in range(MAX_TOOL_ROUNDS):
            message = self.llm.chat(self.history, tools=tool_schemas())
            self.history.append(message)

            tool_calls = message.get("tool_calls") or []
            if not tool_calls:
                self._trim_history()
                return (message.get("content") or "").strip()

            for call in tool_calls:
                fn = call.get("function", {})
                name = fn.get("name", "")
                args = fn.get("arguments") or {}
                if self.activity_cb:
                    self.activity_cb(name, args)
                result = run_tool(
                    name,
                    args,
                    confirm_destructive=self.config.safety.confirm_destructive,
                    confirm_cb=self.confirm_cb,
                )
                self.history.append(
                    {"role": "tool", "name": name, "content": result}
                )

        # Hit the loop cap — ask the model to wrap up with what it has.
        self.history.append(
            {
                "role": "user",
                "content": "Stop calling tools and give me your best answer now.",
            }
        )
        message = self.llm.chat(self.history)
        self.history.append(message)
        self._trim_history()
        return (message.get("content") or "").strip()
