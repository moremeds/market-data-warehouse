import Foundation
@testable import DuckDBCLIAdapter
@testable import MarketDataCore
@testable import OperatorPilotKit

struct MockExecutor: CommandExecuting {
    let result: DuckDBExecutionResult

    func execute(plan: DuckDBCommandPlan) async throws -> DuckDBExecutionResult {
        result
    }

    func executeRaw(argumentsLine: String) async throws -> DuckDBExecutionResult {
        result
    }
}

struct MockChatResponder: ProviderResponding {
    let response: ProviderChatResponse

    func respond(
        prompt: String,
        source: DataSource?,
        transcript: [TranscriptItem],
        settings: AppSettings,
        providerStatuses: [ProviderStatus]
    ) async throws -> ProviderChatResponse {
        response
    }
}

final class MemorySessionStore: AppSessionPersisting, @unchecked Sendable {
    var snapshot: AppSessionSnapshot?
    var lastSaved: AppSessionSnapshot?

    init(snapshot: AppSessionSnapshot? = nil) {
        self.snapshot = snapshot
    }

    func loadSnapshot() -> AppSessionSnapshot? {
        snapshot
    }

    func saveSnapshot(_ snapshot: AppSessionSnapshot) throws {
        lastSaved = snapshot
        self.snapshot = snapshot
    }
}

final class MemorySecretStore: ProviderSecretStoring, @unchecked Sendable {
    var keys: [ProviderKind: String]

    init(keys: [ProviderKind: String] = [:]) {
        self.keys = keys
    }

    func apiKey(for provider: ProviderKind) -> String? {
        keys[provider]
    }

    func saveAPIKey(_ key: String, for provider: ProviderKind) throws {
        keys[provider] = key
    }

    func removeAPIKey(for provider: ProviderKind) throws {
        keys.removeValue(forKey: provider)
    }
}
