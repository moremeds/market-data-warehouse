#!/bin/zsh

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
MACOS_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
LAUNCHER_SOURCE="$MACOS_DIR/launcher/Launch Market Data Warehouse.applescript"
LAUNCHER_APP="$MACOS_DIR/launcher/Launch Market Data Warehouse.app"

/bin/rm -rf "$LAUNCHER_APP"
/usr/bin/osacompile -o "$LAUNCHER_APP" "$LAUNCHER_SOURCE"

print -- "$LAUNCHER_APP"
