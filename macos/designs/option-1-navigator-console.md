# Option 1: Navigator Console

## Summary

This is the recommended direction. It balances chat-first interaction with a visible raw-command lane and the strongest native-data-explorer affordances.

## Layout

```text
+-------------------+--------------------------------------+------------------+
| Sidebar           | Workspace                            | Inspector        |
|                   |                                      |                  |
| Data Roots        | Header: source | model | run | find  | Schema           |
| DuckDB Files      |--------------------------------------| File metadata    |
| Sessions          | Result tabs: Table / SQL / Plan      | Query profile    |
| Saved Queries     |--------------------------------------| Provider status  |
| Diagnostics       | Chat transcript + prompt composer    |                  |
|                   |--------------------------------------|                  |
|                   | Collapsible DuckDB console drawer    |                  |
+-------------------+--------------------------------------+------------------+
```

## Why It Fits The Product

- Users can browse parquet roots and DuckDB files directly, without relying entirely on the model.
- The chat layer feels primary, but the raw DuckDB command path remains obvious and inspectable.
- It maps cleanly to native macOS split-view patterns and the reference screenshots.

## Core Native Components

- `NavigationSplitView` for the shell
- `Table` for result sets and parquet previews
- trailing inspector for schema, provider, and run metadata
- searchable toolbar field for symbols, files, and sessions
- settings scene for providers, auth, and data roots
- contextual menus and keyboard shortcuts for every major action

## Interaction Model

### Primary flow

1. User opens a parquet root or `.duckdb` file.
2. Sidebar shows datasets, sessions, and saved queries.
3. User asks a question in chat or types a raw DuckDB command.
4. The model proposes SQL or CLI commands.
5. The app shows the exact command and lets the user run, edit, or reject it.
6. Results land in tabs while the raw transcript remains available in the console drawer.

### Best features

- query plan view beside results
- one-click copy of generated SQL and raw DuckDB transcript
- fast pivot between parquet metadata, query result, and chat context
- strong debugging surface because every run is visible

## Auth Posture

- Provider status lives in the inspector and toolbar
- Auth sheets handle subscription login or API-key override
- Diagnostics view exposes token expiry, auth source, and last refresh error

## Testing Notes

- easiest option to validate with parity tests because the console drawer can expose exact command text
- straightforward UI automation for sidebar, result tabs, and inspector states

## Tradeoffs

- More chrome than the pure chat-first concept
- Slightly steeper implementation complexity because three panes plus a console drawer must coordinate cleanly
