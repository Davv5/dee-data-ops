"""Shell command execution."""

from __future__ import annotations

import subprocess

from .registry import tool


@tool(
    name="run_shell",
    description=(
        "Run a shell command on the user's Mac and return its output. Use for "
        "anything the command line can do: file management, git, system info, "
        "launching scripts. Returns combined stdout/stderr and the exit code."
    ),
    parameters={
        "type": "object",
        "properties": {
            "command": {
                "type": "string",
                "description": "The shell command to execute, e.g. 'ls -la ~/Downloads'.",
            },
            "timeout": {
                "type": "integer",
                "description": "Max seconds to wait. Default 60.",
            },
        },
        "required": ["command"],
    },
    destructive=True,
)
def run_shell(command: str, timeout: int = 60) -> str:
    try:
        proc = subprocess.run(
            command,
            shell=True,
            capture_output=True,
            text=True,
            timeout=timeout,
        )
    except subprocess.TimeoutExpired:
        return f"[timed out after {timeout}s]"

    out = (proc.stdout or "") + (proc.stderr or "")
    out = out.strip() or "[no output]"
    if len(out) > 6000:
        out = out[:6000] + "\n…[truncated]"
    return f"exit={proc.returncode}\n{out}"
