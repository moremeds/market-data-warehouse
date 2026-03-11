#!/bin/zsh

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
MACOS_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
BUILD_DIR="$MACOS_DIR/build"
APP_NAME="Market Data Warehouse.app"
APP_DIR="$BUILD_DIR/$APP_NAME"
CONTENTS_DIR="$APP_DIR/Contents"
MACOS_CONTENTS_DIR="$CONTENTS_DIR/MacOS"
RESOURCES_DIR="$CONTENTS_DIR/Resources"
PLIST_PATH="$CONTENTS_DIR/Info.plist"
EXECUTABLE_NAME="MarketDataWarehouseApp"
METAL_SOURCE="$MACOS_DIR/Sources/OperatorPilotMetal/Shaders/OperatorPilotMetalShaders.metal"
METAL_LIBRARY_PATH="$RESOURCES_DIR/OperatorPilotMetalShaders.metallib"

export PATH="/opt/homebrew/bin:/opt/homebrew/sbin:/usr/bin:/bin:/usr/sbin:/sbin:/Applications/Xcode.app/Contents/Developer/usr/bin:${PATH:-}"

/usr/bin/xcrun swift build --package-path "$MACOS_DIR" >/dev/null

BIN_DIR="$("/usr/bin/xcrun" swift build --package-path "$MACOS_DIR" --show-bin-path)"
APP_BINARY="$BIN_DIR/$EXECUTABLE_NAME"

if [[ ! -x "$APP_BINARY" ]]; then
  print -u2 -- "Built app binary not found at $APP_BINARY"
  exit 1
fi

/bin/rm -rf "$APP_DIR"
/bin/mkdir -p "$MACOS_CONTENTS_DIR" "$RESOURCES_DIR"
/bin/cp "$APP_BINARY" "$MACOS_CONTENTS_DIR/$EXECUTABLE_NAME"
/bin/chmod +x "$MACOS_CONTENTS_DIR/$EXECUTABLE_NAME"

if [[ -f "$METAL_SOURCE" ]]; then
  if ! "$SCRIPT_DIR/compile_metal_library.sh" "$METAL_SOURCE" "$METAL_LIBRARY_PATH" >/dev/null 2>&1; then
    print -u2 -- "Warning: failed to precompile Metal shaders; the app will fall back to runtime shader compilation."
  fi
fi

/usr/bin/plutil -create xml1 "$PLIST_PATH"
/usr/bin/plutil -replace CFBundleDevelopmentRegion -string "en" "$PLIST_PATH"
/usr/bin/plutil -replace CFBundleExecutable -string "$EXECUTABLE_NAME" "$PLIST_PATH"
/usr/bin/plutil -replace CFBundleIdentifier -string "local.market-data-warehouse.macos" "$PLIST_PATH"
/usr/bin/plutil -replace CFBundleInfoDictionaryVersion -string "6.0" "$PLIST_PATH"
/usr/bin/plutil -replace CFBundleName -string "Market Data Warehouse" "$PLIST_PATH"
/usr/bin/plutil -replace CFBundlePackageType -string "APPL" "$PLIST_PATH"
/usr/bin/plutil -replace CFBundleShortVersionString -string "0.1.0" "$PLIST_PATH"
/usr/bin/plutil -replace CFBundleVersion -string "1" "$PLIST_PATH"
/usr/bin/plutil -replace LSMinimumSystemVersion -string "15.0" "$PLIST_PATH"
/usr/bin/plutil -replace NSHighResolutionCapable -bool YES "$PLIST_PATH"
/usr/bin/plutil -replace NSPrincipalClass -string "NSApplication" "$PLIST_PATH"

print -- "$APP_DIR"
