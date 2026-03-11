---
name: metal-macos-replatform
description: Use this skill when planning, auditing, or implementing Metal-backed macOS desktop surfaces, especially hybrid SwiftUI/AppKit plus MetalKit apps, shader packaging, MTKView integration, and Metal toolchain setup.
---

# Metal macOS Replatform

## Overview

Use this skill for native macOS work that needs real Metal acceleration without turning a desktop productivity app into a game engine. The default architecture is hybrid: keep shell controls in SwiftUI/AppKit, and move dense or frequently invalidated visual surfaces onto `MTKView`.

## Workflow

1. Confirm that Metal is justified.
   Use Metal for high-throughput rendering surfaces such as dense status canvases, mini-maps, heatmaps, or large data previews. Do not rebuild ordinary settings forms, text fields, or menus in Metal.
2. Validate the local toolchain first.
   Run `scripts/check-metal-toolchain.sh`. If the compiler is missing, install it with `xcodebuild -downloadComponent metalToolchain`.
3. Read the references that match the task.
   Read `references/best-practices.md` for rendering rules and `references/toolchain-and-packaging.md` for shader packaging and build-path guidance.
4. Keep render math testable.
   Put palette selection, signal generation, and visualization-state transforms in pure Swift so unit tests do not depend on a live GPU.
5. Prefer a precompiled `.metallib` for the runnable app.
   Runtime source compilation is acceptable as a local fallback, not the primary shipping path.
6. Verify both code and workflow.
   Run unit tests plus any repo-local UI or smoke harness after changing a Metal surface.

## Rules

- Default to `MTKView` before considering lower-level `CAMetalLayer` plumbing.
- Pause redraw when the surface is idle; animate only when the product state actually changes or a command is running.
- Keep `framebufferOnly = true` unless the drawable must be sampled or written by compute work.
- Gate advanced effects against feature-family support instead of assuming all Macs expose the same GPU features.
- Keep accessibility, menus, text input, and standard form controls in native macOS frameworks.

## References

- Read `references/best-practices.md` when choosing architecture, pacing, or renderer structure.
- Read `references/toolchain-and-packaging.md` when wiring `.metal` sources, `.metallib` output, Swift package layout, or app bundle assembly.
