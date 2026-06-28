"""macOS automation via AppleScript / `open`."""

from __future__ import annotations

import subprocess

from .registry import tool


@tool(
    name="open_app",
    description="Open (launch or focus) a macOS application by name, e.g. 'Safari', 'Mail', 'Music'.",
    parameters={
        "type": "object",
        "properties": {
            "name": {"type": "string", "description": "Application name as it appears in /Applications."},
        },
        "required": ["name"],
    },
)
def open_app(name: str) -> str:
    proc = subprocess.run(["open", "-a", name], capture_output=True, text=True)
    if proc.returncode == 0:
        return f"opened {name}"
    return f"could not open {name}: {proc.stderr.strip() or 'unknown error'}"


@tool(
    name="run_applescript",
    description=(
        "Run an AppleScript snippet for deeper macOS automation — controlling apps, "
        "system settings, dialogs, Music, Notes, etc. Returns the script's result."
    ),
    parameters={
        "type": "object",
        "properties": {
            "script": {
                "type": "string",
                "description": "AppleScript source, e.g. 'tell application \"Music\" to play'.",
            },
        },
        "required": ["script"],
    },
    destructive=True,
)
def run_applescript(script: str) -> str:
    proc = subprocess.run(["osascript", "-e", script], capture_output=True, text=True)
    if proc.returncode == 0:
        return proc.stdout.strip() or "[ok]"
    return f"applescript error: {proc.stderr.strip()}"
