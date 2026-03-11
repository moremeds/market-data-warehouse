import Foundation

public enum DuckDBBinaryLocator {
    public static func locate(
        environment: [String: String] = ProcessInfo.processInfo.environment,
        fileManager: FileManager = .default
    ) -> String? {
        candidatePaths(environment: environment).first(where: { fileManager.isExecutableFile(atPath: $0) })
    }

    public static func candidatePaths(environment: [String: String]) -> [String] {
        var paths: [String] = []

        if let explicit = environment["DUCKDB_BINARY"], !explicit.isEmpty {
            paths.append(explicit)
        }

        if let pathValue = environment["PATH"], !pathValue.isEmpty {
            for segment in pathValue.split(separator: ":") {
                paths.append(String(segment) + "/duckdb")
            }
        }

        paths.append("/opt/homebrew/bin/duckdb")
        paths.append("/usr/local/bin/duckdb")
        paths.append("/usr/bin/duckdb")

        var deduped: [String] = []
        var seen = Set<String>()
        for candidate in paths where seen.insert(candidate).inserted {
            deduped.append(candidate)
        }
        return deduped
    }
}
