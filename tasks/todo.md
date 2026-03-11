# Active Plan

Use this file for the current task only. Replace it at the start of each non-trivial task.

## Objective
- Research current chatbot UI and AI assistant UX best practices from the provided sites, produce an HTML report for the macOS app project, and create a reusable app-local Codex skill under `macos/.codex/skills/ai-chat-ux-best-practices`.

## Success Criteria
- The cited sites are reviewed and their actionable UI or UX guidance is captured with source attribution.
- A repo-local HTML report exists with synthesized findings, patterns, and implementation guidance relevant to this macOS app.
- A correctly formatted skill exists under `macos/.codex/skills/ai-chat-ux-best-practices` with concise instructions and supporting references.
- The report and skill are specific enough to guide future chatbot UX work for this application.

## Dependency Graph
- T1 -> T2 -> T3 -> T4 -> T5

## Tasks
- [ ] T1 Audit current app-local `.codex` layout and record the research plan
  depends_on: []
- [ ] T2 Research the provided chatbot UX sources and extract concrete best practices
  depends_on: [T1]
- [ ] T3 Synthesize the findings into an HTML report for the macOS application
  depends_on: [T2]
- [ ] T4 Create the app-local skill under `macos/.codex/skills/ai-chat-ux-best-practices`
  depends_on: [T2, T3]
- [ ] T5 Verify the skill structure and document the result
  depends_on: [T4]

## Review
- Outcome: In progress.
- Verification: Pending after the research synthesis, report generation, and skill validation.
- Residual risk: Third-party design articles may overlap or conflict, so the final guidance should privilege recurring patterns and stronger primary-source recommendations.
