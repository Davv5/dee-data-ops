#!/usr/bin/env bash
# One-command launch: bring up the local brain (Ollama + jarvis serve), then
# the FRIDAY HUD. If the brain isn't installed, the HUD still runs in
# local-only mode (task parsing only) — it just won't think or act.
set -euo pipefail
HERE="$(cd "$(dirname "$0")" && pwd)"

step() { printf "\033[36m==> %s\033[0m\n" "$1"; }
warn() { printf "\033[33m%s\033[0m\n" "$1"; }

# 1. Ollama --------------------------------------------------------------
if ! curl -fsS http://localhost:11434/api/tags >/dev/null 2>&1; then
  step "Starting Ollama"
  (ollama serve >/tmp/ollama.log 2>&1 &) || warn "could not start ollama (is it installed?)"
  sleep 3
fi

# 2. Brain (jarvis serve) ------------------------------------------------
# Find the jarvis CLI: on PATH, in the default worktree venv, or via FRIDAY_VENV.
BRAIN_BIN=""
if command -v jarvis >/dev/null 2>&1; then BRAIN_BIN="jarvis"
elif [ -x "$HOME/jarvis-brain/jarvis/.venv/bin/jarvis" ]; then BRAIN_BIN="$HOME/jarvis-brain/jarvis/.venv/bin/jarvis"
elif [ -n "${FRIDAY_VENV:-}" ] && [ -x "$FRIDAY_VENV/bin/jarvis" ]; then BRAIN_BIN="$FRIDAY_VENV/bin/jarvis"
fi

if [ -n "$BRAIN_BIN" ]; then
  if ! curl -fsS http://localhost:11500/health >/dev/null 2>&1; then
    step "Starting FRIDAY brain"
    ("$BRAIN_BIN" serve >/tmp/friday-brain.log 2>&1 &) || warn "brain failed to start (see /tmp/friday-brain.log)"
    sleep 2
  fi
else
  warn "⚠ Brain CLI not found — the HUD will run in local-only mode."
  warn "  Install it: clone the jarvis/ folder, run ./install.sh, then re-run this."
fi

# 3. HUD -----------------------------------------------------------------
if [ -d "$HERE/FRIDAY.app" ]; then
  step "Launching FRIDAY.app"
  open "$HERE/FRIDAY.app"
else
  step "Building FRIDAY.app (first run)"
  bash "$HERE/setup-macos.sh"
fi

echo
echo "FRIDAY up. Press ⇧⌘Space for the HUD. Brain log: /tmp/friday-brain.log"
