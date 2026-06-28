"""Configuration loading and defaults."""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import yaml

_DEFAULTS: dict[str, Any] = {
    "brain": {
        "host": "http://localhost:11434",
        "model": "qwen3:8b",
        "keep_alive": "30m",
        "temperature": 0.6,
        "max_history": 20,
    },
    "safety": {
        "confirm_destructive": True,
    },
    "voice": {
        "wake_word": "hey_jarvis",
        "whisper_model": "base.en",
        "piper_voice": "en_US-lessac-medium",
        "hotkey": "space",
    },
}

_SEARCH_PATHS = [
    Path.cwd() / "config.yaml",
    Path.home() / ".config" / "jarvis" / "config.yaml",
]


def _deep_merge(base: dict, override: dict) -> dict:
    out = dict(base)
    for key, val in override.items():
        if isinstance(val, dict) and isinstance(out.get(key), dict):
            out[key] = _deep_merge(out[key], val)
        else:
            out[key] = val
    return out


@dataclass
class BrainConfig:
    host: str
    model: str
    keep_alive: str
    temperature: float
    max_history: int


@dataclass
class SafetyConfig:
    confirm_destructive: bool


@dataclass
class VoiceConfig:
    wake_word: str
    whisper_model: str
    piper_voice: str
    hotkey: str


@dataclass
class Config:
    brain: BrainConfig
    safety: SafetyConfig
    voice: VoiceConfig
    source: str = field(default="defaults")


def load_config(path: str | os.PathLike | None = None) -> Config:
    """Load config, layering an optional YAML file over built-in defaults."""
    data = _DEFAULTS
    source = "defaults"

    candidates = [Path(path)] if path else _SEARCH_PATHS
    for candidate in candidates:
        if candidate.is_file():
            with open(candidate) as fh:
                user_cfg = yaml.safe_load(fh) or {}
            data = _deep_merge(_DEFAULTS, user_cfg)
            source = str(candidate)
            break

    return Config(
        brain=BrainConfig(**data["brain"]),
        safety=SafetyConfig(**data["safety"]),
        voice=VoiceConfig(**data["voice"]),
        source=source,
    )
