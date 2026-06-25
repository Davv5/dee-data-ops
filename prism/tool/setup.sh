#!/usr/bin/env bash
#
# Regenerate Prism's native Android + macOS project scaffolding.
#
# To keep the repo tiny we commit only lib/, pubspec.yaml, tool/ and
# platform_overlay/. The android/ and macos/ folders are generated here and then
# patched with our overlay files (notification permissions, macOS network/sandbox
# entitlements, app name). Safe to run repeatedly.
#
#   bash tool/setup.sh
#
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

ORG="com.prismreminders"
NAME="prism"

echo "▸ Scaffolding android + macos platform folders…"
# --no-overwrite preserves our lib/ and pubspec.yaml; only missing files are made.
flutter create \
  --org "$ORG" \
  --project-name "$NAME" \
  --platforms=android,macos \
  --no-overwrite \
  .

echo "▸ Applying platform overlay (permissions, entitlements, app name)…"
# Copy the *contents* of platform_overlay over the generated tree, overwriting
# the generated AndroidManifest / entitlements / xcconfig with our versions.
cp -R platform_overlay/. .

echo "▸ Enabling Android core library desugaring (required by flutter_local_notifications)…"
# Append-merge into the generated app Gradle file rather than replacing it, so we
# stay resilient across Flutter template versions. Handles Groovy and Kotlin DSL.
GROOVY="android/app/build.gradle"
KTS="android/app/build.gradle.kts"
if [ -f "$GROOVY" ] && ! grep -q "coreLibraryDesugaringEnabled" "$GROOVY"; then
  cat >> "$GROOVY" <<'GRADLE'

// --- Added by tool/setup.sh: flutter_local_notifications needs desugaring ---
android {
    compileOptions {
        coreLibraryDesugaringEnabled = true
        sourceCompatibility = JavaVersion.VERSION_1_8
        targetCompatibility = JavaVersion.VERSION_1_8
    }
}
dependencies {
    coreLibraryDesugaring "com.android.tools:desugar_jdk_libs:2.0.4"
}
GRADLE
elif [ -f "$KTS" ] && ! grep -q "isCoreLibraryDesugaringEnabled" "$KTS"; then
  cat >> "$KTS" <<'GRADLE'

// --- Added by tool/setup.sh: flutter_local_notifications needs desugaring ---
android {
    compileOptions {
        isCoreLibraryDesugaringEnabled = true
        sourceCompatibility = JavaVersion.VERSION_1_8
        targetCompatibility = JavaVersion.VERSION_1_8
    }
}
dependencies {
    add("coreLibraryDesugaring", "com.android.tools:desugar_jdk_libs:2.0.4")
}
GRADLE
fi

echo "▸ Fetching Dart packages…"
flutter pub get

echo "✓ Setup complete. Build with:"
echo "    flutter build apk --release      # Android (Vivo V27)"
echo "    flutter build macos --release    # macOS (M4)"
