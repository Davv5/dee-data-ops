"""Tool registry and dispatch.

Tools are plain functions decorated with `@tool(...)`. Importing this package
registers every tool in the submodules. The agent passes `tool_schemas()` to the
LLM and routes tool calls back through `run_tool()`.
"""

from __future__ import annotations

from .registry import run_tool, tool, tool_schemas

# Import submodules for their registration side effects.
from . import shell, applescript, files, web  # noqa: E402,F401

__all__ = ["run_tool", "tool", "tool_schemas"]
