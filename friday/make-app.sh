#!/usr/bin/env bash
#
# make-app.sh — assemble a standalone, double-clickable FRIDAY.app from the
# Electron runtime that's ALREADY in node_modules. No npm, no downloads — so it
# sidesteps the install-script blocker that fought us during setup.
#
# Usage:   bash make-app.sh
# Result:  ./FRIDAY.app  (drag it into /Applications, then double-click)

set -euo pipefail

HERE="$(cd "$(dirname "$0")" && pwd)"
cd "$HERE"

ELECTRON_APP="node_modules/electron/dist/Electron.app"
OUT="FRIDAY.app"
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

echo "▸ Injecting the FRIDAY app code ..."
APPDIR="$OUT/Contents/Resources/app"
mkdir -p "$APPDIR"
# Runtime needs only these — the app has no third-party runtime deps.
cp -R main.js preload.js src renderer package.json "$APPDIR/"

echo "▸ Renaming the executable and tagging the bundle ..."
mv "$OUT/Contents/MacOS/Electron" "$OUT/Contents/MacOS/FRIDAY"

PLIST="$OUT/Contents/Info.plist"
set_plist() {
  "$PLISTBUDDY" -c "Set :$1 $2" "$PLIST" 2>/dev/null || \
  "$PLISTBUDDY" -c "Add :$1 string $2" "$PLIST"
}
set_plist CFBundleExecutable  FRIDAY
set_plist CFBundleName         FRIDAY
set_plist CFBundleDisplayName  FRIDAY
set_plist CFBundleIdentifier   com.david.friday
# LSUIElement = menu-bar-only app (no Dock icon), matching app.dock.hide().
"$PLISTBUDDY" -c "Set :LSUIElement true" "$PLIST" 2>/dev/null || \
"$PLISTBUDDY" -c "Add :LSUIElement bool true" "$PLIST"

# App icon: build a macOS .icns from build/icon.png using the system tools.
ICON_SRC="build/icon.png"
if [ -f "$ICON_SRC" ] && command -v sips >/dev/null 2>&1 && command -v iconutil >/dev/null 2>&1; then
  echo "▸ Building the FRIDAY app icon ..."
  ICONSET="$(mktemp -d)/jarvis.iconset"
  mkdir -p "$ICONSET"
  gen() { sips -z "$2" "$2" "$ICON_SRC" --out "$ICONSET/$1" >/dev/null 2>&1; }
  gen icon_16x16.png 16;      gen icon_16x16@2x.png 32
  gen icon_32x32.png 32;      gen icon_32x32@2x.png 64
  gen icon_128x128.png 128;   gen icon_128x128@2x.png 256
  gen icon_256x256.png 256;   gen icon_256x256@2x.png 512
  gen icon_512x512.png 512;   gen icon_512x512@2x.png 1024
  if iconutil -c icns "$ICONSET" -o "$OUT/Contents/Resources/electron.icns" >/dev/null 2>&1; then
    set_plist CFBundleIconFile electron.icns
    echo "  icon set."
  else
    echo "  (iconutil failed — default icon kept.)"
  fi
else
  echo "▸ Skipping custom icon (build/icon.png or sips/iconutil missing)."
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
echo "  Keep it:        drag FRIDAY.app into your Applications folder"
echo "  First launch:   if macOS warns, right-click the app → Open → Open"
echo ""
echo "  Your tasks are stored at:"
echo "    ~/Library/Application Support/FRIDAY/friday-tasks.json"
