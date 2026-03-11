# Active Plan

Use this file for the current task only. Replace it at the start of each non-trivial task.

## Objective
- Update the repo and agent documentation to match the implemented macOS client, then commit and push the result.

## Success Criteria
- Root operator docs and agent-facing guides describe the current macOS app, setup flow, settings, launcher flow, and smoke harness accurately.
- macOS-local docs remain aligned with the latest implementation and verification path.
- Final verification confirms the documented macOS workflows still work.
- The resulting changes are committed and pushed to the current remote branch.

## Dependency Graph
- T1 -> T2 -> T3 -> T4 -> T5

## Tasks
- [x] T1 Record the documentation-and-release plan and capture the latest documentation correction in lessons learned
  depends_on: []
- [x] T2 Audit README, agent guides, and macOS docs for stale or missing implementation details
  depends_on: [T1]
- [x] T3 Update the relevant docs so they describe the live macOS client and verification flow consistently
  depends_on: [T2]
- [x] T4 Run a final verification pass for the documented macOS workflows
  depends_on: [T3]
- [x] T5 Commit the documentation updates and push the branch
  depends_on: [T4]

## Review
- Outcome: Updated the root operator docs, native macOS README, and agent-facing guides so they all describe the live macOS client, setup flow, settings, keyboard commands, launcher/build path, provider-backed chat, and raw DuckDB CLI passthrough consistently.
- Verification: `cd macos && swift test` passed with 26 tests, and `cd macos && ./scripts/run_ui_smoke_tests.sh` passed through first-run setup, navigation, rerun-setup cancel flow, diagnostics, `/duckdb --help`, provider chat, source import, and parquet preview.
- Residual risk: The repo still uses a smoke harness rather than a full XCTest UI target for macOS UI verification.
