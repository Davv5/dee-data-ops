"""Task tools — the bridge to the macOS HUD's task board.

These don't mutate anything directly; they emit actions that the Electron app
applies to its own JSON store (the source of truth). When no app is connected
(plain CLI), they report that gracefully instead of pretending to succeed.
"""

from __future__ import annotations

import json

from ..actions import app_connected, current_tasks, emit_action
from .registry import tool


@tool(
    name="add_task",
    description=(
        "Add a task / reminder to the user's JARVIS board. Use this when the user "
        "wants to be reminded of or track something (e.g. 'remind me to call mum at 5', "
        "'add gym tomorrow morning'). Provide a natural title and, if a time is implied, "
        "an ISO-8601 due timestamp."
    ),
    parameters={
        "type": "object",
        "properties": {
            "title": {"type": "string", "description": "Short natural task title, e.g. 'call mum'."},
            "due": {
                "type": "string",
                "description": "Optional ISO-8601 datetime, e.g. '2026-06-29T17:00:00'. Omit if no time.",
            },
            "category": {
                "type": "string",
                "description": (
                    "One of: critical (urgent), deadline (hard due date), work (deep "
                    "work/calls/clients), personal (health/errands/life), idea "
                    "(research/creative), standard. Default standard."
                ),
            },
        },
        "required": ["title"],
    },
)
def add_task(title: str, due: str | None = None, category: str = "standard") -> str:
    if not app_connected():
        return "[task board only available when the JARVIS app is running]"
    emit_action({"type": "add_task", "task": {"title": title, "due": due, "category": category}})
    when = f" (due {due})" if due else ""
    return f"queued task: {title}{when}"


@tool(
    name="list_tasks",
    description="List the user's current tasks on the JARVIS board.",
    parameters={"type": "object", "properties": {}},
)
def list_tasks() -> str:
    if not app_connected():
        return "[task board only available when the JARVIS app is running]"
    tasks = current_tasks()
    if not tasks:
        return "the board is clear — no tasks."
    lines = []
    for t in tasks:
        status = "done" if t.get("done") else "open"
        due = f" due {t['due']}" if t.get("due") else ""
        lines.append(f"- [{status}] {t.get('title', 'untitled')}{due} (id={t.get('id')})")
    return "\n".join(lines)


@tool(
    name="complete_task",
    description="Mark a task done, or remove it. Pass the task's id (from list_tasks).",
    parameters={
        "type": "object",
        "properties": {
            "id": {"type": "string", "description": "The task id to complete/remove."},
            "remove": {"type": "boolean", "description": "If true, delete it instead of marking done."},
        },
        "required": ["id"],
    },
)
def complete_task(id: str, remove: bool = False) -> str:
    if not app_connected():
        return "[task board only available when the JARVIS app is running]"
    emit_action({"type": "remove_task" if remove else "complete_task", "id": id})
    return json.dumps({"ok": True, "id": id, "removed": remove})
