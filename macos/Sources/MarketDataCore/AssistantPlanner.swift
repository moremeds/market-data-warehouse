import Foundation

public struct DuckDBCommandPlan: Equatable, Sendable {
    public let source: DataSource
    public let sql: String
    public let explanation: String

    public init(source: DataSource, sql: String, explanation: String) {
        self.source = source
        self.sql = sql
        self.explanation = explanation
    }
}

public enum AssistantAction: Equatable, Sendable {
    case assistantReply(String)
    case command(DuckDBCommandPlan)
    case providerPrompt(String)
    case rawCommand(String)
}

public enum PromptLibrary {
    public static func prompts(for source: DataSource?) -> [PromptChip] {
        guard let source else {
            return [
                PromptChip(title: "Open a parquet file", prompt: "How do I get started with a parquet file?"),
                PromptChip(title: "Open a DuckDB database", prompt: "How do I get started with a DuckDB database?"),
                PromptChip(title: "DuckDB CLI help", prompt: "/duckdb --help"),
                PromptChip(title: "What can you do?", prompt: "What can you do?"),
            ]
        }

        switch source.kind {
        case .parquet:
            return [
                PromptChip(title: "Preview rows", prompt: "Preview this parquet file"),
                PromptChip(title: "Show schema", prompt: "Show the schema"),
                PromptChip(title: "Count rows", prompt: "Count rows"),
                PromptChip(title: "DuckDB CLI help", prompt: "/duckdb --help"),
            ]
        case .duckdb:
            return [
                PromptChip(title: "Show tables", prompt: "Show tables"),
                PromptChip(title: "Database info", prompt: "Show database info"),
                PromptChip(title: "Paste SQL", prompt: "/sql SELECT * FROM sqlite_master LIMIT 5;"),
                PromptChip(title: "DuckDB CLI help", prompt: "/duckdb --help"),
            ]
        }
    }
}

public enum AssistantPlanner {
    public static func plan(prompt: String, source: DataSource?) -> AssistantAction {
        let trimmed = prompt.trimmingCharacters(in: .whitespacesAndNewlines)
        guard !trimmed.isEmpty else {
            return .assistantReply("Ask a question, paste SQL with `/sql ...`, or use one of the quick prompts.")
        }

        if trimmed.caseInsensitiveCompare("What can you do?") == .orderedSame {
            return .assistantReply(
                "I can open a local `.duckdb` or `.parquet` source, map common prompts into real DuckDB commands, run raw DuckDB CLI arguments with `/duckdb ...`, and route broader analysis prompts through the selected provider."
            )
        }

        if trimmed.caseInsensitiveCompare("/duckdb") == .orderedSame {
            return .assistantReply("Use `/duckdb ...` to run raw DuckDB CLI arguments exactly as you would in Terminal after the `duckdb` binary name. Example: `/duckdb --help`.")
        }

        if let rawCommand = rawDuckDBCommand(from: trimmed) {
            return .rawCommand(rawCommand)
        }

        guard let source else {
            if trimmed.localizedCaseInsensitiveContains("parquet") || trimmed.localizedCaseInsensitiveContains("duckdb") {
                return .assistantReply("Use the toolbar to open a `.parquet` or `.duckdb` source first, then I can run real DuckDB commands against it.")
            }

            if trimmed.localizedCaseInsensitiveContains("show tables") || trimmed.localizedCaseInsensitiveContains("preview this") {
                return .assistantReply("Open a local `.duckdb` or `.parquet` source first. After that you can ask for a preview, schema, row count, or paste raw SQL with `/sql ...`.")
            }

            return .providerPrompt(trimmed)
        }

        if let sql = rawSQL(from: trimmed) {
            return .command(
                DuckDBCommandPlan(
                    source: source,
                    sql: sql,
                    explanation: "Running your SQL directly against \(source.displayName)."
                )
            )
        }

        switch source.kind {
        case .parquet:
            return planForParquet(prompt: trimmed, source: source)
        case .duckdb:
            return planForDuckDB(prompt: trimmed, source: source)
        }
    }

    private static func planForParquet(prompt: String, source: DataSource) -> AssistantAction {
        let escapedPath = escapeLiteral(source.path)
        let lowercased = prompt.lowercased()

        if lowercased.contains("schema") {
            return .command(
                DuckDBCommandPlan(
                    source: source,
                    sql: "DESCRIBE SELECT * FROM read_parquet('\(escapedPath)');",
                    explanation: "Inspecting the parquet schema for \(source.displayName)."
                )
            )
        }

        if lowercased.contains("count") {
            return .command(
                DuckDBCommandPlan(
                    source: source,
                    sql: "SELECT COUNT(*) AS row_count FROM read_parquet('\(escapedPath)');",
                    explanation: "Counting rows in \(source.displayName)."
                )
            )
        }

        return .command(
            DuckDBCommandPlan(
                source: source,
                sql: "SELECT * FROM read_parquet('\(escapedPath)') LIMIT 25;",
                explanation: "Previewing up to 25 rows from \(source.displayName)."
            )
        )
    }

    private static func planForDuckDB(prompt: String, source: DataSource) -> AssistantAction {
        let lowercased = prompt.lowercased()

        if lowercased.contains("database info") || lowercased.contains("database size") {
            return .command(
                DuckDBCommandPlan(
                    source: source,
                    sql: "PRAGMA database_list;",
                    explanation: "Showing attached database information for \(source.displayName)."
                )
            )
        }

        if lowercased.contains("show tables") || lowercased.contains("list tables") {
            return .command(
                DuckDBCommandPlan(
                    source: source,
                    sql: "SHOW TABLES;",
                    explanation: "Listing tables in \(source.displayName)."
                )
            )
        }

        if looksLikeSQL(prompt) {
            return .command(
                DuckDBCommandPlan(
                    source: source,
                    sql: prompt,
                    explanation: "Running your SQL directly against \(source.displayName)."
                )
            )
        }

        return .providerPrompt(prompt)
    }

    private static func rawSQL(from prompt: String) -> String? {
        let prefix = "/sql "
        guard prompt.lowercased().hasPrefix(prefix) else {
            return nil
        }

        return String(prompt.dropFirst(prefix.count)).trimmingCharacters(in: .whitespacesAndNewlines)
    }

    private static func rawDuckDBCommand(from prompt: String) -> String? {
        let prefix = "/duckdb "
        guard prompt.lowercased().hasPrefix(prefix) else {
            return nil
        }

        return String(prompt.dropFirst(prefix.count)).trimmingCharacters(in: .whitespacesAndNewlines)
    }

    private static func looksLikeSQL(_ prompt: String) -> Bool {
        let keywords = ["select", "show", "describe", "pragma", "with", "from"]
        let firstToken = prompt
            .trimmingCharacters(in: .whitespacesAndNewlines)
            .split(separator: " ")
            .first?
            .lowercased() ?? ""
        return keywords.contains(firstToken)
    }

    private static func escapeLiteral(_ path: String) -> String {
        path.replacingOccurrences(of: "'", with: "''")
    }
}
