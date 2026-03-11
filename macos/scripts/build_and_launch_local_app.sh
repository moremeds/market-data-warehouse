#!/bin/zsh

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
MACOS_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
LOG_DIR="$MACOS_DIR/logs"
LOG_FILE="$LOG_DIR/build-and-launch.log"
BUILD_SCRIPT="$MACOS_DIR/scripts/build_local_macos_app.sh"

mkdir -p "$LOG_DIR"

run() {
  {
    print "[$(/bin/date '+%Y-%m-%d %H:%M:%S')] Building Market Data Warehouse.app"
    local app_bundle
    app_bundle="$("$BUILD_SCRIPT")"

    if [[ ! -d "$app_bundle" ]]; then
      print -u2 "Built app bundle not found at $app_bundle"
      return 1
    fi

    print "[$(/bin/date '+%Y-%m-%d %H:%M:%S')] Opening $app_bundle"
    /usr/bin/open "$app_bundle"
    print "[$(/bin/date '+%Y-%m-%d %H:%M:%S')] Done"
  } >>"$LOG_FILE" 2>&1
}

if run; then
  print -- "$LOG_FILE"
else
  print -u2 -- "Build or launch failed. See log: $LOG_FILE"
  exit 1
fi
