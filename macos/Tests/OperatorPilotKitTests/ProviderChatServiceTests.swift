import Foundation
import XCTest
@testable import MarketDataCore
@testable import OperatorPilotKit

private final class CapturingProcessExecutor: ProcessExecuting, @unchecked Sendable {
    struct Invocation {
        let command: String
        let arguments: [String]
        let environment: [String: String]
    }

    var invocation: Invocation?
    var handler: ((String, [String], [String: String]) throws -> ProcessExecutionResult)?

    func execute(
        command: String,
        arguments: [String],
        environment: [String: String]
    ) async throws -> ProcessExecutionResult {
        invocation = Invocation(command: command, arguments: arguments, environment: environment)
        if let handler {
            return try handler(command, arguments, environment)
        }
        return ProcessExecutionResult(stdout: "", stderr: "", exitCode: 0)
    }
}

final class ProviderChatServiceTests: XCTestCase {
    func testClaudeJSONResponseIsParsed() async throws {
        let executor = CapturingProcessExecutor()
        executor.handler = { _, _, _ in
            ProcessExecutionResult(
                stdout: #"{"result":"CLAUDE_REPLY"}"#,
                stderr: "",
                exitCode: 0
            )
        }

        let service = ProviderChatService(
            processExecutor: executor,
            secretStore: MemorySecretStore(),
            baseEnvironment: [:]
        )

        var settings = AppSettings(hasCompletedSetup: true, defaultProvider: .claude)
        settings.setPreference(ProviderPreference(authMode: .localCLI, customModel: "sonnet"), for: .claude)

        let response = try await service.respond(
            prompt: "Test",
            source: nil,
            transcript: [],
            settings: settings,
            providerStatuses: [ProviderStatus(provider: .claude, cliInstalled: true, cliPath: "/usr/bin/claude", apiKeyPresent: false, environmentKeyPresent: false)]
        )

        XCTAssertEqual(response.text, "CLAUDE_REPLY")
        XCTAssertEqual(executor.invocation?.arguments.contains("--output-format"), true)
    }

    func testLocalSubscriptionFallsBackToStoredAPIKey() async throws {
        let executor = CapturingProcessExecutor()
        var invocationCount = 0
        executor.handler = { _, _, environment in
            invocationCount += 1
            if invocationCount == 1 {
                XCTAssertNil(environment["ANTHROPIC_API_KEY"])
                return ProcessExecutionResult(stdout: "", stderr: "authentication required", exitCode: 1)
            }

            XCTAssertEqual(environment["ANTHROPIC_API_KEY"], "fallback-key")
            return ProcessExecutionResult(
                stdout: #"{"result":"FALLBACK_REPLY"}"#,
                stderr: "",
                exitCode: 0
            )
        }

        let service = ProviderChatService(
            processExecutor: executor,
            secretStore: MemorySecretStore(keys: [.claude: "fallback-key"]),
            baseEnvironment: [:]
        )

        var settings = AppSettings(hasCompletedSetup: true, defaultProvider: .claude)
        settings.setPreference(ProviderPreference(authMode: .localCLI, customModel: ""), for: .claude)

        let response = try await service.respond(
            prompt: "Test",
            source: nil,
            transcript: [],
            settings: settings,
            providerStatuses: [ProviderStatus(provider: .claude, cliInstalled: true, cliPath: "/usr/bin/claude", apiKeyPresent: true, environmentKeyPresent: false)]
        )

        XCTAssertEqual(invocationCount, 2)
        XCTAssertEqual(response.text, "FALLBACK_REPLY")
    }

    func testAPIKeyModeInjectsStoredSecret() async throws {
        let executor = CapturingProcessExecutor()
        executor.handler = { _, _, environment in
            XCTAssertEqual(environment["ANTHROPIC_API_KEY"], "secret-key")
            return ProcessExecutionResult(stdout: #"{"result":"OK"}"#, stderr: "", exitCode: 0)
        }

        let service = ProviderChatService(
            processExecutor: executor,
            secretStore: MemorySecretStore(keys: [.claude: "secret-key"]),
            baseEnvironment: [:]
        )

        var settings = AppSettings(hasCompletedSetup: true, defaultProvider: .claude)
        settings.setPreference(ProviderPreference(authMode: .apiKey, customModel: ""), for: .claude)

        _ = try await service.respond(
            prompt: "Test",
            source: nil,
            transcript: [],
            settings: settings,
            providerStatuses: [ProviderStatus(provider: .claude, cliInstalled: true, cliPath: "/usr/bin/claude", apiKeyPresent: true, environmentKeyPresent: false)]
        )
    }

    func testCodexReadsLastMessageFile() async throws {
        let executor = CapturingProcessExecutor()
        executor.handler = { _, arguments, _ in
            guard let index = arguments.firstIndex(of: "--output-last-message") else {
                return ProcessExecutionResult(stdout: "", stderr: "missing output path", exitCode: 1)
            }
            let path = arguments[index + 1]
            try "CODEX_REPLY".write(toFile: path, atomically: true, encoding: .utf8)
            return ProcessExecutionResult(stdout: "noise", stderr: "", exitCode: 0)
        }

        let service = ProviderChatService(
            processExecutor: executor,
            secretStore: MemorySecretStore(),
            baseEnvironment: [:]
        )

        var settings = AppSettings(hasCompletedSetup: true, defaultProvider: .openAI)
        settings.setPreference(ProviderPreference(authMode: .localCLI, customModel: ""), for: .openAI)

        let response = try await service.respond(
            prompt: "Test",
            source: nil,
            transcript: [],
            settings: settings,
            providerStatuses: [ProviderStatus(provider: .openAI, cliInstalled: true, cliPath: "/usr/bin/codex", apiKeyPresent: false, environmentKeyPresent: false)]
        )

        XCTAssertEqual(response.text, "CODEX_REPLY")
    }
}
