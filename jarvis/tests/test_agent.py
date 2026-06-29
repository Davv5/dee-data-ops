"""Phase 0-1 tests: tool registry + agent loop with a fake LLM (no Ollama needed)."""

from __future__ import annotations

import json

from jarvis.config import load_config
from jarvis.brain.agent import Agent
from jarvis.brain.tools import run_tool, tool_schemas


def test_tools_registered():
    names = {s["function"]["name"] for s in tool_schemas()}
    assert {"run_shell", "open_app", "close_app", "run_applescript", "read_file", "write_file", "web_search"} <= names


def test_destructive_tool_blocked_without_confirm():
    # confirm_cb returns False -> call should be cancelled, nothing executed.
    out = run_tool("run_shell", {"command": "echo hi"}, confirm_destructive=True, confirm_cb=lambda n, a: False)
    assert json.loads(out)["status"] == "cancelled"


def test_destructive_tool_allowed_when_confirmed():
    # The HUD/server path: confirm_cb allows -> the tool actually runs.
    out = run_tool("run_shell", {"command": "echo hi"}, confirm_destructive=True, confirm_cb=lambda n, a: True)
    assert "hi" in out


def test_nondestructive_read_file(tmp_path):
    f = tmp_path / "note.txt"
    f.write_text("hello jarvis")
    out = run_tool("read_file", {"path": str(f)}, confirm_destructive=True, confirm_cb=lambda n, a: False)
    assert "hello jarvis" in out


class _FakeLLM:
    """Replays a scripted sequence of assistant messages."""

    def __init__(self, messages):
        self._messages = list(messages)

    def chat(self, history, tools=None):
        return self._messages.pop(0)


def _agent_with(messages):
    agent = Agent(load_config(), confirm_cb=lambda n, a: True)
    agent.llm = _FakeLLM(messages)
    return agent


def test_agent_plain_reply():
    agent = _agent_with([{"role": "assistant", "content": "Hello."}])
    assert agent.say("hi") == "Hello."


def test_agent_runs_tool_then_answers(tmp_path):
    target = tmp_path / "out.txt"
    scripted = [
        {
            "role": "assistant",
            "content": "",
            "tool_calls": [
                {"function": {"name": "write_file", "arguments": {"path": str(target), "content": "x"}}}
            ],
        },
        {"role": "assistant", "content": "Done — file written."},
    ]
    agent = _agent_with(scripted)
    reply = agent.say("write a file")
    assert reply == "Done — file written."
    assert target.read_text() == "x"
