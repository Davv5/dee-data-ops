"""Resolve corpus.yaml scopes to notebook IDs.

The v1 SKILL.md inline bash+python snippet is replaced here by a tested
library function. Same semantics, additive optional `weight` and `size_hint`
keys per plan U2.

scope strings:
- "methodology.<key>" -> exactly that one methodology entry
- "methodology" or "" or None -> all methodology entries (in declaration order)
- "engagement" -> the engagement entry
- anything else -> UnknownScopeError
"""

from __future__ import annotations

import sys
from dataclasses import dataclass
from pathlib import Path

import yaml


@dataclass(frozen=True)
class ScopeRef:
    """One concrete notebook a scope resolves to."""

    key: str  # e.g. "data_ops" or "engagement"
    notebook_id: str
    name: str
    weight: float = 1.0
    size_hint: int | None = None

    @property
    def scope_label(self) -> str:
        """The scope string a SubQuery would carry — e.g. 'methodology.data_ops'."""
        if self.key == "engagement":
            return "engagement"
        return f"methodology.{self.key}"


class UnknownScopeError(ValueError):
    """Raised when a caller asks for a scope that isn't declared in corpus.yaml."""


# Hardcoded fallback used only when corpus.yaml is missing entirely. Matches
# the safety-fallback behavior of the v1 SKILL.md inline snippet so a partial
# template adoption doesn't break the skill.
_FALLBACK_DATA_OPS = ScopeRef(
    key="data_ops",
    notebook_id="7c7cd5d4-22df-4ef0-8b74-ed87e0ca4e6a",
    name="Data Ops (fallback)",
)


def resolve_scopes(
    scope: str | None,
    config_path: Path | str = ".claude/corpus.yaml",
) -> list[ScopeRef]:
    """Resolve a scope string to a list of ScopeRefs.

    Args:
        scope: scope identifier. None or "methodology" -> all methodology
            entries. "methodology.<key>" -> exactly that one. "engagement" ->
            the engagement entry.
        config_path: path to corpus.yaml (default: ".claude/corpus.yaml"
            relative to cwd).

    Returns:
        List of ScopeRef in declaration order. Always non-empty on success.

    Raises:
        UnknownScopeError: if the scope string is unrecognized or the
            requested methodology key isn't declared.
    """
    cfg_path = Path(config_path)

    if not cfg_path.exists():
        # Same fallback behavior as the v1 SKILL.md snippet — single warning,
        # return Data Ops, keep the skill working for a partial template.
        print(
            f"[Env] WARNING corpus.yaml missing at {cfg_path}; "
            f"falling back to hardcoded Data Ops notebook",
            file=sys.stderr,
        )
        return [_FALLBACK_DATA_OPS]

    config = yaml.safe_load(cfg_path.read_text()) or {}
    scope_value = (scope or "methodology").strip()

    if scope_value == "engagement":
        engagement = config.get("engagement")
        if not isinstance(engagement, dict) or "notebook_id" not in engagement:
            raise UnknownScopeError(
                f"corpus.yaml at {cfg_path} has no 'engagement' entry"
            )
        return [_build_scope_ref("engagement", engagement)]

    if scope_value.startswith("methodology."):
        target_key = scope_value.split(".", 1)[1]
        for entry in config.get("methodology", []) or []:
            if entry.get("key") == target_key:
                return [_build_scope_ref(target_key, entry)]
        raise UnknownScopeError(
            f"unknown methodology key: {target_key} (config: {cfg_path})"
        )

    if scope_value == "methodology":
        entries = config.get("methodology", []) or []
        if not entries:
            raise UnknownScopeError(
                f"corpus.yaml at {cfg_path} has no 'methodology' entries"
            )
        return [_build_scope_ref(e.get("key", ""), e) for e in entries]

    raise UnknownScopeError(f"unknown scope: {scope_value!r}")


def _build_scope_ref(key: str, entry: dict) -> ScopeRef:
    """Construct ScopeRef from a corpus.yaml entry, applying additive defaults."""
    notebook_id = entry.get("notebook_id")
    if not notebook_id:
        raise UnknownScopeError(f"corpus.yaml entry for {key!r} is missing notebook_id")
    return ScopeRef(
        key=key,
        notebook_id=notebook_id,
        name=entry.get("name") or key,
        weight=float(entry.get("weight", 1.0)),
        size_hint=entry.get("size_hint"),
    )
