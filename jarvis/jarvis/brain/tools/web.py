"""Lightweight web search (DuckDuckGo HTML, no API key)."""

from __future__ import annotations

import html
import re

import httpx

from .registry import tool

_RESULT_RE = re.compile(
    r'<a[^>]*class="result__a"[^>]*>(.*?)</a>.*?'
    r'<a[^>]*class="result__snippet"[^>]*>(.*?)</a>',
    re.DOTALL,
)
_TAG_RE = re.compile(r"<[^>]+>")


def _clean(fragment: str) -> str:
    return html.unescape(_TAG_RE.sub("", fragment)).strip()


@tool(
    name="web_search",
    description=(
        "Search the web and return the top result titles and snippets. Use for current "
        "information, facts, or to look something up before answering."
    ),
    parameters={
        "type": "object",
        "properties": {
            "query": {"type": "string", "description": "What to search for."},
            "count": {"type": "integer", "description": "Number of results to return. Default 5."},
        },
        "required": ["query"],
    },
)
def web_search(query: str, count: int = 5) -> str:
    try:
        resp = httpx.get(
            "https://html.duckduckgo.com/html/",
            params={"q": query},
            headers={"User-Agent": "Mozilla/5.0 (Macintosh; Apple Silicon) Jarvis/0.1"},
            timeout=20,
            follow_redirects=True,
        )
        resp.raise_for_status()
    except httpx.HTTPError as exc:
        return f"[web search failed: {exc}]"

    results = []
    for title, snippet in _RESULT_RE.findall(resp.text)[:count]:
        results.append(f"- {_clean(title)}: {_clean(snippet)}")
    return "\n".join(results) or "[no results]"
