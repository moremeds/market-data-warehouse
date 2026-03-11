# Option 2: Workbench Canvas

## Summary

This version treats each conversation turn or command run as a reusable analysis block. It is the most analyst-friendly option for comparing parquet previews, SQL results, and narrative conclusions side by side.

## Layout

```text
+-------------------+---------------------------------------------------+------------------+
| Sidebar           | Analysis canvas                                   | Inspector        |
|                   |                                                   |                  |
| Sources           | Intro card / prompt suggestions                   | Schema           |
| Sessions          | Query card: SQL + status + actions                | Stats            |
| Snippets          | Result card: table or chart                       | Provenance       |
| Exports           | Notes card: model summary / rationale             | Provider status  |
|                   | Diff card: compare two result sets                |                  |
|                   | Sticky composer at bottom                         |                  |
+-------------------+---------------------------------------------------+------------------+
```

## Why It Fits The Product

- Great for longer research sessions where users compare multiple datasets or save intermediate findings.
- Works well when parquet inspection, SQL, charts, and explanation need to remain visible together.
- Gives the app a stronger research-workbench identity than a plain assistant window.

## Core Native Components

- split view shell with a scrollable content canvas
- `Table` for dense result cards
- inspector for source metadata and query diagnostics
- native share/export panels for cards and result snapshots
- command palette for insert query block, compare runs, export, or reopen source

## Interaction Model

### Primary flow

1. User starts a chat prompt or opens a saved workspace.
2. Each assistant suggestion or user command becomes a block on the canvas.
3. Blocks can be rerun, edited, pinned, or exported.
4. Comparison blocks show schema or result diffs between parquet files or DuckDB runs.

### Best features

- strongest support for narrative analysis
- easy to preserve context from one run to the next
- excellent for export and reporting workflows

## Auth Posture

- Provider selector and model controls are visible in a compact toolbar region
- Auth details stay mostly in the inspector and settings to reduce noise

## Testing Notes

- good for reducer-style state testing because each card can be modeled as immutable session state
- slightly heavier UI test burden because drag, reorder, pin, and compare interactions multiply states

## Tradeoffs

- Raw DuckDB CLI parity is still present, but less visually dominant
- Less faithful to the attached reference images than the other two concepts
- Risk of over-feeling like a notebook if the chrome gets too card-heavy
