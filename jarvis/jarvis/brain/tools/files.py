"""File read / search / write."""

from __future__ import annotations

import os
import subprocess
from pathlib import Path

from .registry import tool


def _expand(path: str) -> Path:
    return Path(os.path.expanduser(os.path.expandvars(path))).resolve()


@tool(
    name="read_file",
    description="Read and return the text contents of a file on the Mac.",
    parameters={
        "type": "object",
        "properties": {
            "path": {"type": "string", "description": "Path to the file, e.g. '~/Desktop/notes.txt'."},
            "max_chars": {"type": "integer", "description": "Truncate after this many characters. Default 8000."},
        },
        "required": ["path"],
    },
)
def read_file(path: str, max_chars: int = 8000) -> str:
    p = _expand(path)
    if not p.is_file():
        return f"[no such file: {p}]"
    text = p.read_text(errors="replace")
    if len(text) > max_chars:
        text = text[:max_chars] + "\n…[truncated]"
    return text


@tool(
    name="search_files",
    description=(
        "Search for files by name or for text inside files, starting from a directory. "
        "Returns matching paths (and lines, for content search)."
    ),
    parameters={
        "type": "object",
        "properties": {
            "query": {"type": "string", "description": "Filename glob or text to search for."},
            "path": {"type": "string", "description": "Directory to search from. Default '~'."},
            "content": {
                "type": "boolean",
                "description": "If true, search inside file contents; otherwise match filenames.",
            },
        },
        "required": ["query"],
    },
)
def search_files(query: str, path: str = "~", content: bool = False) -> str:
    root = _expand(path)
    if not root.is_dir():
        return f"[no such directory: {root}]"

    if content:
        # Prefer ripgrep when present, fall back to grep -r.
        cmd = ["rg", "-n", "--max-count", "5", query, str(root)]
        proc = subprocess.run(cmd, capture_output=True, text=True)
        if proc.returncode == 127:  # rg not installed
            proc = subprocess.run(
                ["grep", "-rn", "--max-count=5", query, str(root)],
                capture_output=True,
                text=True,
            )
        out = proc.stdout.strip()
    else:
        matches = [str(p) for p in root.rglob(query)][:50]
        out = "\n".join(matches)

    return out or "[no matches]"


@tool(
    name="write_file",
    description="Create or overwrite a text file on the Mac with the given contents.",
    parameters={
        "type": "object",
        "properties": {
            "path": {"type": "string", "description": "Destination path, e.g. '~/Desktop/todo.md'."},
            "content": {"type": "string", "description": "Full text to write."},
        },
        "required": ["path", "content"],
    },
    destructive=True,
)
def write_file(path: str, content: str) -> str:
    p = _expand(path)
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(content)
    return f"wrote {len(content)} chars to {p}"
