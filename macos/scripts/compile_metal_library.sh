#!/bin/zsh

set -euo pipefail

if [[ "$#" -ne 2 ]]; then
  print -u2 -- "Usage: compile_metal_library.sh <source.metal> <output.metallib>"
  exit 1
fi

SOURCE_PATH="$1"
OUTPUT_PATH="$2"
AIR_PATH="${OUTPUT_PATH%.metallib}.air"

export PATH="/opt/homebrew/bin:/opt/homebrew/sbin:/usr/bin:/bin:/usr/sbin:/sbin:/Applications/Xcode.app/Contents/Developer/usr/bin:${PATH:-}"

if ! /usr/bin/xcrun --toolchain Metal metal -v >/dev/null 2>&1; then
  print -u2 -- "Metal Toolchain unavailable. Install it with: xcodebuild -downloadComponent metalToolchain"
  exit 1
fi

/bin/mkdir -p "$(dirname "$OUTPUT_PATH")"
/usr/bin/xcrun --toolchain Metal metal -c "$SOURCE_PATH" -o "$AIR_PATH"
/usr/bin/xcrun --toolchain Metal metallib "$AIR_PATH" -o "$OUTPUT_PATH"
/bin/rm -f "$AIR_PATH"

print -- "$OUTPUT_PATH"
