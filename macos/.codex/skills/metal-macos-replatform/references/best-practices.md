# Metal macOS best practices

## Core rule

For desktop productivity apps, use a hybrid architecture:

- SwiftUI/AppKit for window chrome, forms, text entry, accessibility, menus, and standard controls
- MetalKit for dense or animated visualization surfaces

## Rendering guidance

- Prefer `MTKView` for drawable lifecycle management.
- Redraw on demand when the surface is static.
- Unpause or animate only while work is active or the surface genuinely changes.
- Keep tiny per-frame inputs small; use `setVertexBytes` or `setFragmentBytes` only for small payloads.
- Move repeated, deterministic visualization math into pure Swift helpers so it is unit-testable.
- Precompile `.metallib` files for the runnable app bundle when possible.
- Keep a local-development fallback that can still load shader source if the bundle library is missing.

## Capability guidance

- Check Apple feature-family support before adopting advanced effects.
- Use the Metal feature tables when choosing optional features across Apple Silicon and older Macs.
- Do not assume one GPU memory model; inspect unified-memory and working-set facts if the renderer grows.

## Operational guidance

- If `xcrun metal` is unavailable, install the Xcode component with `xcodebuild -downloadComponent metalToolchain`.
- Use GPU tooling and frame capture when a surface behaves incorrectly or regresses in frame pacing.

## Sources

- https://developer.apple.com/metal/
- https://developer.apple.com/documentation/metal
- https://developer.apple.com/documentation/metalkit/mtkview
- https://developer.apple.com/metal/Metal-Feature-Set-Tables.pdf
- https://github.com/mikeroyal/Metal-Guide
