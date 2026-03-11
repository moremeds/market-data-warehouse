# Design Comparison

## Shared Visual Direction

All three concepts assume:

- graphite-dark default appearance
- quiet surfaces, thin separators, minimal card chrome
- full-height sidebar and unified toolbar
- bottom-aligned composer
- strict separation between generated suggestions and executed DuckDB commands

## Selected Direction

The user selected `Option 3: Operator Pilot` on March 10, 2026, and the runnable scaffold in `macos/` now follows that direction.

## Comparison Matrix

| Option | Best For | DuckDB CLI Parity Visibility | Parquet Discoverability | Match To Reference Images | Implementation Complexity |
| --- | --- | --- | --- | --- | --- |
| Navigator Console | mixed browse + chat workflows | strongest | strongest | high | medium-high |
| Workbench Canvas | comparative analysis and saved research trails | medium | high | medium | high |
| Operator Pilot | chat-first operator workflow | medium-low unless drawer is open | medium-low | strongest | medium |

## Original Recommendation

Recommend `Option 1: Navigator Console`.

Why:

- It preserves the chat-first product direction without hiding the real DuckDB surface.
- It gives parquet and DuckDB assets a strong native explorer model.
- It is the most defensible path to the user's hard requirement of DuckDB CLI parity.
- It still matches the reference screenshots closely enough once styled with a quiet dark sidebar, subdued toolbar, and centered empty state.

## When To Choose Each

Choose `Option 1` if:

- the product should feel like a serious data workstation
- direct inspection and raw command transparency matter as much as chat convenience
- debugging and supportability are important from day one

Choose `Option 2` if:

- users will build long-form investigative sessions
- result comparison and saved analytical context matter more than pure operational speed
- the product should lean toward a research notebook

Choose `Option 3` if:

- the app's identity is primarily assistant-driven
- the reference screenshots should be followed as closely as possible
- the team accepts a higher burden to keep command disclosure and trust surfaces explicit

## Proposed Next Step After Selection

Once a direction is chosen:

1. Create the Xcode workspace under `macos/`.
2. Lock the module boundaries from `technical-foundation.md`.
3. Build the auth adapter and DuckDB CLI adapter before investing in deeper UI polish.
4. Add coverage gates for the macOS source set from the first commit.

For the current repo state, the implementation work has started from `Option 3` instead.
