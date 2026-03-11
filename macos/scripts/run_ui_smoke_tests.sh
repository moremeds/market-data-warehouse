#!/bin/zsh

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
MACOS_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
LOG_DIR="$MACOS_DIR/logs"
LOG_PATH="$LOG_DIR/ui-smoke-tests.log"
APP_PATH="$("$SCRIPT_DIR/build_local_macos_app.sh")"
APP_EXECUTABLE="$APP_PATH/Contents/MacOS/MarketDataWarehouseApp"
TMP_DIR="$(mktemp -d)"
SESSION_FILE="$TMP_DIR/session.json"
FIXTURE_PATH="$TMP_DIR/uismoke.parquet"
FIXTURE_NAME="${FIXTURE_PATH:t}"
WINDOW_CAPTURE_PATH="$TMP_DIR/window.png"
SESSION_ENV_KEY="MARKET_DATA_WAREHOUSE_SESSION_FILE"
SOURCE_PICK_ENV_KEY="MARKET_DATA_WAREHOUSE_AUTOMATION_PICK_SOURCE"

export PATH="/opt/homebrew/bin:/opt/homebrew/sbin:/usr/bin:/bin:/usr/sbin:/sbin:/Applications/Xcode.app/Contents/Developer/usr/bin:${PATH:-}"

mkdir -p "$LOG_DIR"

cleanup() {
  /bin/launchctl unsetenv "$SESSION_ENV_KEY" >/dev/null 2>&1 || true
  /bin/launchctl unsetenv "$SOURCE_PICK_ENV_KEY" >/dev/null 2>&1 || true
  if [[ -n "${APP_PID:-}" ]] && kill -0 "$APP_PID" >/dev/null 2>&1; then
    kill "$APP_PID" >/dev/null 2>&1 || true
    wait "$APP_PID" >/dev/null 2>&1 || true
  fi
  /usr/bin/pkill -x MarketDataWarehouseApp >/dev/null 2>&1 || true
  rm -rf "$TMP_DIR"
}
trap cleanup EXIT

window_bounds() {
  /usr/bin/osascript - "$APP_PID" <<'APPLESCRIPT'
on run argv
  set pidValue to item 1 of argv as integer
  tell application "System Events"
    try
      tell (first application process whose unix id is pidValue)
        set frontmost to true
        if (count of windows) is 0 then
          return ""
        end if
        tell window 1
          set {xPos, yPos} to position
          set {winWidth, winHeight} to size
          return (xPos as text) & "," & (yPos as text) & "," & (winWidth as text) & "," & (winHeight as text)
        end tell
      end tell
    on error
      return ""
    end try
  end tell
end run
APPLESCRIPT
}

window_text() {
  local bounds
  bounds="$(window_bounds)"
  if [[ -z "$bounds" ]]; then
    return 0
  fi
  /usr/sbin/screencapture -x -R"$bounds" "$WINDOW_CAPTURE_PATH"
  /usr/bin/swift "$SCRIPT_DIR/ocr_window_text.swift" "$WINDOW_CAPTURE_PATH"
}

wait_for_text() {
  local expected="$1"
  local timeout="${2:-30}"
  local started_at="$SECONDS"
  while (( SECONDS - started_at < timeout )); do
    if window_text | /usr/bin/grep -Fqi "$expected"; then
      return 0
    fi
    /bin/sleep 0.75
  done

  echo "Timed out waiting for UI text: $expected" | tee -a "$LOG_PATH" >&2
  return 1
}

send_key_code() {
  local key_code="$1"
  /usr/bin/osascript - "$APP_PID" "$key_code" <<'APPLESCRIPT'
on run argv
  set pidValue to item 1 of argv as integer
  set keyCodeValue to item 2 of argv as integer
  tell application "System Events"
    tell (first application process whose unix id is pidValue)
      set frontmost to true
    end tell
    key code keyCodeValue
  end tell
end run
APPLESCRIPT
}

send_shortcut() {
  local key="$1"
  local modifiers="$2"
  /usr/bin/osascript - "$APP_PID" "$key" "$modifiers" <<'APPLESCRIPT'
on run argv
  set pidValue to item 1 of argv as integer
  set keyValue to item 2 of argv
  set modifierSpec to item 3 of argv

  set modifierList to {}
  if modifierSpec contains "command" then set end of modifierList to command down
  if modifierSpec contains "shift" then set end of modifierList to shift down
  if modifierSpec contains "option" then set end of modifierList to option down
  if modifierSpec contains "control" then set end of modifierList to control down

  tell application "System Events"
    tell (first application process whose unix id is pidValue)
      set frontmost to true
    end tell
    keystroke keyValue using modifierList
  end tell
end run
APPLESCRIPT
}

type_text() {
  local text="$1"
  /usr/bin/osascript - "$APP_PID" "$text" <<'APPLESCRIPT'
on run argv
  set pidValue to item 1 of argv as integer
  set typedText to item 2 of argv
  tell application "System Events"
    tell (first application process whose unix id is pidValue)
      set frontmost to true
    end tell
    keystroke typedText
  end tell
end run
APPLESCRIPT
}

log_step() {
  local message="$1"
  echo "[$(/bin/date '+%Y-%m-%d %H:%M:%S')] $message" | tee -a "$LOG_PATH"
}

{
  echo "[$(/bin/date '+%Y-%m-%d %H:%M:%S')] Starting macOS UI smoke tests"
  echo "App: $APP_PATH"
  echo "Fixture: $FIXTURE_PATH"
} >>"$LOG_PATH"

/usr/bin/osascript -e 'tell application id "local.market-data-warehouse.macos" to quit' >/dev/null 2>&1 || true
/usr/bin/pkill -x MarketDataWarehouseApp >/dev/null 2>&1 || true
for _ in {1..50}; do
  if ! /usr/bin/pgrep -x MarketDataWarehouseApp >/dev/null 2>&1; then
    break
  fi
  /bin/sleep 0.2
done

duckdb ":memory:" -c "COPY (SELECT 1 AS id, 'alpha' AS name UNION ALL SELECT 2 AS id, 'beta' AS name) TO '$FIXTURE_PATH' (FORMAT PARQUET);" >/dev/null

/bin/launchctl setenv "$SESSION_ENV_KEY" "$SESSION_FILE"
/bin/launchctl setenv "$SOURCE_PICK_ENV_KEY" "$FIXTURE_PATH"
/usr/bin/open -na "$APP_PATH"
/usr/bin/osascript -e 'tell application id "local.market-data-warehouse.macos" to activate' >/dev/null 2>&1 || true

for _ in {1..100}; do
  APP_PID="$(/usr/bin/pgrep -nx MarketDataWarehouseApp || true)"
  if [[ -n "$APP_PID" ]]; then
    break
  fi
  /bin/sleep 0.2
done

if [[ -z "${APP_PID:-}" ]]; then
  echo "Failed to locate launched app process." | tee -a "$LOG_PATH" >&2
  exit 1
fi

/bin/launchctl unsetenv "$SESSION_ENV_KEY"
/bin/launchctl unsetenv "$SOURCE_PICK_ENV_KEY"

log_step "Waiting for first-run setup"
wait_for_text "Welcome to Market Data Warehouse" 30

log_step "Completing setup"
send_key_code 36
wait_for_text "Chat with your workspace" 30

log_step "Navigating to transcripts"
send_shortcut "2" "command"
wait_for_text "Transcript Archive" 10

log_step "Navigating to setup summary"
send_shortcut "3" "command"
wait_for_text "Current Configuration" 10

log_step "Navigating to settings"
send_shortcut "4" "command"
wait_for_text "Settings" 10

log_step "Reopening setup from settings"
send_shortcut "r" "command,shift"
wait_for_text "Save Changes" 10

log_step "Canceling setup sheet"
send_key_code 53
wait_for_text "Settings" 10

log_step "Returning to assistant"
send_shortcut "1" "command"
wait_for_text "Chat with your workspace" 10

log_step "Opening diagnostics"
send_shortcut "d" "command,shift"
wait_for_text "Diagnostics" 10

log_step "Running raw DuckDB command through composer"
send_shortcut "l" "command"
type_text "/duckdb --help"
send_key_code 36
wait_for_text "OPTIONS include:" 30

log_step "Running provider-backed chat"
send_shortcut "l" "command"
type_text "Reply with exactly: UI PROVIDER OK"
send_key_code 36
wait_for_text "UI PROVIDER OK" 120

log_step "Importing parquet source through the open-source flow"
send_shortcut "o" "command"
wait_for_text "$FIXTURE_NAME" 15

log_step "Previewing imported parquet data"
send_shortcut "l" "command"
type_text "Preview this parquet file"
send_key_code 36
wait_for_text "Command Result" 30

echo "[$(/bin/date '+%Y-%m-%d %H:%M:%S')] UI smoke tests passed" >>"$LOG_PATH"
echo "UI smoke test passed."
