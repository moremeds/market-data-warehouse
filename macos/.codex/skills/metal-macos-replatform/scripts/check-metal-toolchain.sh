#!/bin/zsh

set -euo pipefail

export PATH="/opt/homebrew/bin:/opt/homebrew/sbin:/usr/bin:/bin:/usr/sbin:/sbin:/Applications/Xcode.app/Contents/Developer/usr/bin:${PATH:-}"

if /usr/bin/xcrun --toolchain Metal metal -v >/dev/null 2>&1; then
  /usr/bin/xcrun --toolchain Metal metal -v
else
  print -u2 -- "Metal Toolchain unavailable."
  print -u2 -- "Install it with: xcodebuild -downloadComponent metalToolchain"
  exit 1
fi
