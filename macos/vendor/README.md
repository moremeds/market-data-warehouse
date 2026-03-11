# Metal Vendor References

This directory holds externally sourced reference artifacts used during the Metal replatform.

## Contents

- `apple/Metal-Feature-Set-Tables.pdf`
  downloaded from `https://developer.apple.com/metal/Metal-Feature-Set-Tables.pdf`
- `metal-guide/README.md`
  downloaded from `https://raw.githubusercontent.com/mikeroyal/Metal-Guide/master/README.md`

## Notes

- These files are references, not runtime dependencies.
- The runtime Metal implementation in this repo uses Apple system frameworks: `Metal` and `MetalKit`.
- The local machine also required the optional Metal Toolchain component so the repo can precompile `.metallib` files:
  `xcodebuild -downloadComponent metalToolchain`
