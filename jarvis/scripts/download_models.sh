#!/usr/bin/env bash
# Download the speech models used from Phase 2 onward (whisper.cpp + Piper).
# Safe to run once you're ready for voice. Models land in ./models/.
set -euo pipefail

MODELS_DIR="${JARVIS_MODELS_DIR:-./models}"
WHISPER_MODEL="${JARVIS_WHISPER_MODEL:-base.en}"
PIPER_VOICE="${JARVIS_PIPER_VOICE:-en_US-lessac-medium}"

mkdir -p "$MODELS_DIR/whisper" "$MODELS_DIR/piper"

echo "==> whisper.cpp model: $WHISPER_MODEL"
curl -fSL -o "$MODELS_DIR/whisper/ggml-${WHISPER_MODEL}.bin" \
  "https://huggingface.co/ggerganov/whisper.cpp/resolve/main/ggml-${WHISPER_MODEL}.bin"

echo "==> Piper voice: $PIPER_VOICE"
BASE="https://huggingface.co/rhasspy/piper-voices/resolve/main"
# Voice path pattern: en/en_US/lessac/medium/en_US-lessac-medium.onnx
LANG="${PIPER_VOICE%%_*}"                 # en
LOCALE="$(echo "$PIPER_VOICE" | cut -d- -f1)"   # en_US
NAME="$(echo "$PIPER_VOICE" | cut -d- -f2)"     # lessac
QUALITY="$(echo "$PIPER_VOICE" | cut -d- -f3)"  # medium
VOICE_URL="$BASE/$LANG/$LOCALE/$NAME/$QUALITY/$PIPER_VOICE"
curl -fSL -o "$MODELS_DIR/piper/$PIPER_VOICE.onnx"      "$VOICE_URL.onnx"
curl -fSL -o "$MODELS_DIR/piper/$PIPER_VOICE.onnx.json" "$VOICE_URL.onnx.json"

echo "==> Speech models ready in $MODELS_DIR"
echo "    openWakeWord pretrained models download automatically on first use."
