#!/usr/bin/env bash
#
# setup-macos.sh — one-shot, fix-everything installer for JARVIS.
#
# Safe to run from ANY state (fresh clone, half-installed, old version still
# running). It kills stale processes, removes old app copies, repairs the
# Electron runtime (including the download this Mac's npm blocker prevents),
# rebuilds JARVIS.app from current code, and launches it.
#
#   cd ~/Documents/dee-data-ops/jarvis-todo
#   git pull
#   bash setup-macos.sh

set -uo pipefail
HERE="$(cd "$(dirname "$0")" && pwd)"
cd "$HERE"

EV="31.7.7"
ELDIR="node_modules/electron/dist"

echo "==> [1/7] Pulling latest code ..."
git pull --ff-only 2>/dev/null && echo "    up to date." \
  || echo "    (git pull skipped/failed — building with current files.)"

echo "==> [2/7] Stopping any running JARVIS (old versions included) ..."
pkill -9 -f "jarvis-todo" 2>/dev/null || true
pkill -9 -f "JARVIS.app" 2>/dev/null || true
osascript -e 'tell application "JARVIS" to quit' 2>/dev/null || true
sleep 1

echo "==> [3/7] Deleting old built app copies ..."
rm -rf "$HERE/JARVIS.app" "/Applications/JARVIS.app" 2>/dev/null || true

echo "==> [4/7] Ensuring node modules ..."
if [ ! -d node_modules/electron ]; then
  npm install >/dev/null 2>&1 || npm install || true
fi

echo "==> [5/7] Repairing the Electron runtime if needed ..."
if [ ! -d "$ELDIR/Electron.app/Contents/Frameworks" ]; then
  echo "    Runtime incomplete — downloading Electron $EV directly ..."
  ARCH="$(uname -m)"; [ "$ARCH" = "arm64" ] && EARCH=arm64 || EARCH=x64
  echo "    (architecture: $EARCH)"
  if ! curl -L --fail -o /tmp/jarvis-electron.zip \
        "https://github.com/electron/electron/releases/download/v$EV/electron-v$EV-darwin-$EARCH.zip"; then
    echo "    ✘ Could not download Electron. Check your internet and re-run."
    exit 1
  fi
  rm -rf "$ELDIR"; mkdir -p "$ELDIR"
  ditto -x -k /tmp/jarvis-electron.zip "$ELDIR"
  xattr -dr com.apple.quarantine "$ELDIR/Electron.app" 2>/dev/null || true
  echo "    runtime installed."
else
  echo "    runtime OK."
fi
printf 'Electron.app/Contents/MacOS/Electron' > node_modules/electron/path.txt

echo "==> [6/7] Building JARVIS.app from current code ..."
if ! bash make-app.sh; then
  echo "✘ Build failed. Paste the output above to Claude."
  exit 1
fi

echo "==> [7/7] Launching ..."
open "$HERE/JARVIS.app"

echo ""
echo "════════════════════════════════════════════════════════════"
echo " ✔ JARVIS is running — look in the MENU BAR (top-right) for"
echo "   the gold reactor icon. Press  ⇧⌘Space  to summon the HUD."
echo ""
echo "   Keep it: drag this into Applications →  $HERE/JARVIS.app"
echo "   Tasks saved at: ~/Library/Application Support/JARVIS/"
echo "════════════════════════════════════════════════════════════"
