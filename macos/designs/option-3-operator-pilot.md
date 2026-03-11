# Option 3: Operator Pilot

## Summary

This is the most chat-native concept and the closest visual match to the supplied screenshots. It gives the model transcript pride of place and treats data sources, results, and diagnostics as supporting surfaces.

## Layout

```text
+-------------------+----------------------------------------------------------+
| Sidebar           | Main transcript                                          |
|                   |                                                          |
| Assistant         | Quiet empty state / session summary                      |
| Live Sessions     |----------------------------------------------------------|
| Transcripts       | Conversation turns                                       |
| Tools             | Suggested prompts                                        |
| Costs             | Inline result cards                                      |
| Setup             | Inline SQL / command previews                            |
| Settings          |----------------------------------------------------------|
|                   | Bottom composer + attachment/source chips                |
+-------------------+----------------------------------------------------------+
| Optional bottom drawer: raw DuckDB terminal, logs, and provider diagnostics         |
+--------------------------------------------------------------------------------------+
```

## Why It Fits The Product

- Strongest alignment with the provided visual references
- Lowest cognitive overhead for users who want to ask questions first and inspect details second
- Cleanest onboarding story for subscription-backed model usage

## Core Native Components

- single primary content column with native sidebar navigation
- optional bottom drawer for terminal output and logs
- sheets for connection/auth/file-open flows
- settings scene for providers and workspace indexing
- inspector only when explicitly opened for schema or metadata

## Interaction Model

### Primary flow

1. User opens the app and sees recent sessions and quick prompts.
2. User asks the assistant to inspect parquet, run DuckDB SQL, or explain a result.
3. The transcript shows generated commands inline before execution.
4. Result cards open inline, with a drawer available for the full raw terminal transcript.

### Best features

- best empty state and onboarding story
- most legible for users already comfortable with AI chat workflows
- easiest to align with the existing look-and-feel references

## Auth Posture

- auth feels like a first-run setup flow, not an admin screen
- provider health and model choice remain visible but subdued

## Testing Notes

- simple primary view hierarchy
- higher product risk because more core functionality depends on chat-state correctness and disclosure design

## Tradeoffs

- weakest direct discoverability for parquet structure and DuckDB assets
- easier for important raw-command details to become hidden behind transcript expansion
- highest risk of feeling magical unless the command-preview and diagnostics surfaces are disciplined
