#!/usr/bin/env bash
# Jarvis installer for macOS (Apple Silicon). Idempotent — safe to re-run.
set -euo pipefail

cyan() { printf "\033[36m%s\033[0m\n" "$1"; }
warn() { printf "\033[33m%s\033[0m\n" "$1"; }

cyan "==> Jarvis setup (Apple Silicon)"

if [[ "$(uname -s)" != "Darwin" ]]; then
  warn "This installer targets macOS. On other platforms install the pieces manually."
fi

# 1. Homebrew ---------------------------------------------------------------
if ! command -v brew >/dev/null 2>&1; then
  cyan "==> Installing Homebrew"
  /bin/bash -c "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/HEAD/install.sh)"
fi

# 2. Ollama -----------------------------------------------------------------
if ! command -v ollama >/dev/null 2>&1; then
  cyan "==> Installing Ollama"
  brew install ollama
fi

# Make sure the server is running, then pull the model.
if ! curl -fsS http://localhost:11434/api/tags >/dev/null 2>&1; then
  cyan "==> Starting Ollama (brew services)"
  brew services start ollama || (ollama serve >/tmp/ollama.log 2>&1 &)
  sleep 3
fi

MODEL="${JARVIS_MODEL:-qwen3:8b}"
cyan "==> Pulling model: $MODEL"
ollama pull "$MODEL"

# 3. Python environment -----------------------------------------------------
cyan "==> Creating Python venv (.venv)"
python3 -m venv .venv
# shellcheck disable=SC1091
source .venv/bin/activate
pip install --quiet --upgrade pip
cyan "==> Installing Jarvis (text + tools)"
pip install --quiet -e .

# 4. Config -----------------------------------------------------------------
if [[ ! -f config.yaml ]]; then
  cp config.example.yaml config.yaml
  cyan "==> Wrote config.yaml (edit to taste)"
fi

cyan "==> Done. Try it:"
echo "    source .venv/bin/activate"
echo "    jarvis doctor"
echo "    jarvis chat"
echo
warn "Voice phases (2+): run ./scripts/download_models.sh and 'pip install -e .[voice]'"
