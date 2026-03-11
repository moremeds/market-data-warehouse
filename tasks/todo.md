# Active Plan

Use this file for the current task only. Replace it at the start of each non-trivial task.

## Objective
- Research current macOS Metal best practices, create a reusable Codex skill for Metal work, download the supporting references/assets needed for this repo, and begin rewriting the native macOS client around a Metal-backed rendering architecture.

## Success Criteria
- A new Metal-focused skill exists under `macos/.codex/skills/` with concise instructions and bundled reference material.
- Repo-local documentation captures the Metal replatform decision, the research findings, and the practical guardrails for this app.
- The macOS client gains a Metal-backed rendering foundation that is actually wired into the live app rather than remaining design-only.
- Any downloaded references, sample assets, or vendored libraries needed for the Metal rewrite are stored in a deliberate repo location and documented.
- Verification proves the rewritten macOS app still builds and its tests pass; new coverage is added where the refactor introduces new behavior.

## Dependency Graph
- T1 -> T2 -> T3 -> T4 -> T5 -> T6

## Tasks
- [x] T1 Audit the current macOS app structure and capture the Metal replatform plan
  depends_on: []
- [x] T2 Research current Metal guidance from Apple and curated references, then distill repo-specific best practices
  depends_on: [T1]
- [x] T3 Create the new Codex skill under `macos/.codex/skills/` with references/assets for future Metal work
  depends_on: [T2]
- [x] T4 Download and organize the external libraries, sample assets, or reference artifacts needed for the Metal rewrite
  depends_on: [T2]
- [x] T5 Refactor the macOS client to introduce a Metal-backed rendering architecture and wire it into the live app
  depends_on: [T3, T4]
- [x] T6 Verify the Metal rewrite, update the docs, and record the review notes
  depends_on: [T5]

## Review
- Outcome: Researched current Apple Metal guidance, created a reusable app-local Codex skill at `macos/.codex/skills/metal-macos-replatform`, downloaded repo-local Metal reference artifacts under `macos/vendor/`, and refactored the native macOS client onto a hybrid SwiftUI plus MetalKit path with precompiled shader-library support for the app bundle.
- Verification: `cd macos && swift test` passed with 31 tests, and `cd macos && ./scripts/build_local_macos_app.sh` produced `macos/build/Market Data Warehouse.app` with `OperatorPilotMetalShaders.metallib` in `Contents/Resources/`.
- Residual risk: This is intentionally a hybrid replatform. It accelerates workspace surfaces with Metal without rewriting every desktop control in a bespoke renderer, which remains the correct tradeoff for a productivity app.
