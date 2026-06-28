#!/usr/bin/env bash
#
# make-app.sh — assemble a standalone, double-clickable JARVIS.app from the
# Electron runtime that's ALREADY in node_modules. No npm, no downloads — so it
# sidesteps the install-script blocker that fought us during setup.
#
# Usage:   bash make-app.sh
# Result:  ./JARVIS.app  (drag it into /Applications, then double-click)

set -euo pipefail

HERE="$(cd "$(dirname "$0")" && pwd)"
cd "$HERE"

ELECTRON_APP="node_modules/electron/dist/Electron.app"
OUT="JARVIS.app"
PLISTBUDDY="/usr/libexec/PlistBuddy"

if [ ! -d "$ELECTRON_APP/Contents/Frameworks" ]; then
  echo "✘ Electron isn't fully installed yet."
  echo "  Run 'npm start' once (it must launch without the 'Library not loaded'"
  echo "  error) so the full runtime exists, then re-run this script."
  exit 1
fi

echo "▸ Cloning the Electron runtime into $OUT ..."
rm -rf "$OUT"
ditto "$ELECTRON_APP" "$OUT"

echo "▸ Injecting the JARVIS app code ..."
APPDIR="$OUT/Contents/Resources/app"
mkdir -p "$APPDIR"
# Runtime needs only these — the app has no third-party runtime deps.
cp -R main.js preload.js src renderer package.json "$APPDIR/"

echo "▸ Renaming the executable and tagging the bundle ..."
mv "$OUT/Contents/MacOS/Electron" "$OUT/Contents/MacOS/JARVIS"

PLIST="$OUT/Contents/Info.plist"
set_plist() {
  "$PLISTBUDDY" -c "Set :$1 $2" "$PLIST" 2>/dev/null || \
  "$PLISTBUDDY" -c "Add :$1 string $2" "$PLIST"
}
set_plist CFBundleExecutable  JARVIS
set_plist CFBundleName         JARVIS
set_plist CFBundleDisplayName  JARVIS
set_plist CFBundleIdentifier   com.david.jarvis.todo
# LSUIElement = menu-bar-only app (no Dock icon), matching app.dock.hide().
"$PLISTBUDDY" -c "Set :LSUIElement true" "$PLIST" 2>/dev/null || \
"$PLISTBUDDY" -c "Add :LSUIElement bool true" "$PLIST"

# Optional custom icon: if you generate build/icon.icns it gets used.
if [ -f "build/icon.icns" ]; then
  cp "build/icon.icns" "$OUT/Contents/Resources/electron.icns"
fi

echo "▸ Clearing quarantine + ad-hoc signing (so macOS will open it) ..."
xattr -cr "$OUT" 2>/dev/null || true
if command -v codesign >/dev/null 2>&1; then
  codesign --force --deep --sign - "$OUT" >/dev/null 2>&1 \
    && echo "  signed." \
    || echo "  (codesign skipped — app will still run; right-click → Open first time.)"
fi

echo ""
echo "✔ Built $HERE/$OUT"
echo ""
echo "  Try it now:     open \"$OUT\""
echo "  Keep it:        drag JARVIS.app into your Applications folder"
echo "  First launch:   if macOS warns, right-click the app → Open → Open"
echo ""
echo "  Your tasks are stored at:"
echo "    ~/Library/Application Support/JARVIS/jarvis-tasks.json"
