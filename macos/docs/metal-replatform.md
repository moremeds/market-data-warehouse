# Metal Replatform Guide

This document captures the repo-specific rules for using Metal in the native macOS client.

## Why Hybrid Metal

This app is not a game engine, and it should not pretend to be one. The practical architecture is:

- keep windowing, navigation, forms, accessibility, text input, settings, and chat controls in SwiftUI/AppKit
- move dense, animated, or frequently invalidated visual surfaces onto MetalKit
- use GPU rendering for the workspace status canvas and future high-volume data surfaces such as heatmaps, mini-maps, and large result overviews

That gives the performance upside of Metal without rebuilding ordinary desktop controls in a lower-level renderer.

## Current Implementation

The live app now uses a dedicated `OperatorPilotMetal` target for the workspace status panels. The renderer is wired into:

- assistant workspace
- transcript archive
- setup summary
- settings

The renderer currently:

- hosts itself in `MTKView`
- pauses when the app is idle and redraws on demand
- animates only while a command is actively running
- prefers a precompiled `.metallib` in the app bundle
- falls back to loading the checked-in `.metal` source file during local development

## Best Practices

1. Use `MTKView` for drawable management and render-loop integration. Do not hand-roll a `CAMetalLayer` path unless `MTKView` is provably limiting the product surface.
2. Precompile shader libraries for the shipped app bundle. Runtime source compilation is acceptable only as a local-development fallback.
3. Keep Metal surfaces narrow and purposeful. Use them for throughput-heavy visualizations, not basic settings forms or text entry.
4. Prefer demand-driven redraw when content is static. This repo pauses the Metal workspace surface when no command is running.
5. Set `framebufferOnly = true` unless the drawable must be sampled or written by compute passes later.
6. Keep small uniform payloads small. For tiny per-frame values, `setFragmentBytes` is fine; move to persistent buffers only when payload size or synchronization needs justify it.
7. Gate future advanced features against Apple feature-family support and the Metal feature tables. Do not assume one Mac GPU profile.
8. Keep a CPU-accessible fallback path for local development and debugging. This repo uses a checked-in shader source file plus a compiled-bundle path.
9. Separate pure visualization math from GPU plumbing. `MetalWorkspaceVisualization` exists so the signal-generation logic stays testable without a live GPU.
10. Validate Metal work with both unit tests and UI smoke tests. A renderer that compiles but breaks setup, navigation, or diagnostics is still a regression.

## Toolchain And Build Rules

- Local Metal compiler access on this machine required `xcodebuild -downloadComponent metalToolchain`
- Precompiled shader output is produced by `macos/scripts/compile_metal_library.sh`
- App-bundle assembly copies `OperatorPilotMetalShaders.metallib` into `Market Data Warehouse.app/Contents/Resources/`
- `swift test` verifies the pure Swift Metal snapshot and visualization seams without requiring a live Metal app session

## Downloaded References

Repo-local reference artifacts live under `macos/vendor/`:

- `macos/vendor/apple/Metal-Feature-Set-Tables.pdf`
- `macos/vendor/metal-guide/README.md`

## Primary Sources

- Apple Metal overview: https://developer.apple.com/metal/
- Apple Metal documentation: https://developer.apple.com/documentation/metal
- Apple MetalKit `MTKView`: https://developer.apple.com/documentation/metalkit/mtkview
- Apple site update for Swift package distribution: https://developer.apple.com/news/site-updates/?id=03262024b
- Apple Xcode asset download for toolchains/components: https://developer.apple.com/documentation/xcode/installing-additional-xcode-components
- Apple Metal feature tables PDF: https://developer.apple.com/metal/Metal-Feature-Set-Tables.pdf
- Mike Royal Metal Guide: https://github.com/mikeroyal/Metal-Guide
