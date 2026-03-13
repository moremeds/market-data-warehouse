# Active Plan

Use this file for the current task only. Replace it at the start of each non-trivial task.

## Objective
- Update the repo docs so they reflect the new generic breadth strategy, then commit and push the relevant change set.

## Success Criteria
- Root operator docs, agent-facing guides, and repo-specific backtesting docs describe the generic breadth strategy entry point and its supported universe and signal modes.
- Durable project memory is updated if the strategy shape is now a stable repo fact.
- Only the relevant files are committed.
- The commit is pushed to the current branch.

## Dependency Graph
- T1 -> T2
- T2 -> T3
- T3 -> T4

## Tasks
- [x] T1 Audit the repo docs that should mention the generic breadth strategy
  depends_on: []
- [x] T2 Update the selected docs to reflect the current strategy behavior
  depends_on: [T1]
- [x] T3 Verify the doc changes and prepare a focused commit
  depends_on: [T2]
- [x] T4 Commit and push the doc update change set
  depends_on: [T3]

## Review
- Outcome:
  - Updated `README.md`, `CLAUDE.md`, `AGENTS.md`, `.codex/project-memory.md`, and `docs/backtesting.md` so they document the generic breadth strategy entry point, supported universe selectors, supported signal modes, and the `ndx100`-only point-in-time membership caveat.
  - Added operator-facing CLI examples in `README.md` and reflected the strategy modules in the `CLAUDE.md` repo layout.
- Verification:
  - `source ~/market-warehouse/.venv/bin/activate && python strategies/breadth_washout.py --help | sed -n '1,220p'`
  - `rg -n "breadth_washout.py|oversold|overbought|all-stocks|point-in-time membership" README.md CLAUDE.md AGENTS.md .codex/project-memory.md docs/backtesting.md`
  - `git diff --check -- README.md CLAUDE.md AGENTS.md .codex/project-memory.md docs/backtesting.md tasks/todo.md`
- Residual risk:
  - The docs correctly state the current limitation that official point-in-time membership is only implemented for `ndx100`; if that expands later, these docs will need another sweep.
