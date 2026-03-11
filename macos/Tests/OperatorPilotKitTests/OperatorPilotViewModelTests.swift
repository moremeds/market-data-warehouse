import Foundation
import XCTest
@testable import DuckDBCLIAdapter
@testable import MarketDataCore
@testable import OperatorPilotKit

@MainActor
final class OperatorPilotViewModelTests: XCTestCase {
    func testProviderDiagnosticsDetectsCliAndKeys() {
        let secretStore = MemorySecretStore(keys: [.openAI: "secret"])
        let statuses = ProviderDiagnostics.detect(
            environment: [
                "PATH": "/tooling:/bin",
            ],
            secretStore: secretStore,
            executableExists: { path in
                path == "/tooling/claude" || path == "/tooling/codex"
            }
        )

        XCTAssertEqual(statuses.first(where: { $0.provider == .claude })?.cliInstalled, true)
        XCTAssertEqual(statuses.first(where: { $0.provider == .openAI })?.apiKeyPresent, true)
        XCTAssertEqual(statuses.first(where: { $0.provider == .gemini })?.cliInstalled, false)
    }

    func testInitialStateRequiresSetupWhenNoSnapshotExists() {
        let viewModel = OperatorPilotViewModel(
            executor: nil,
            chatResponder: MockChatResponder(response: .init(provider: .claude, text: "ignored")),
            sessionStore: MemorySessionStore(),
            secretStore: MemorySecretStore(),
            environment: ["PATH": "/bin"]
        )

        XCTAssertTrue(viewModel.requiresInitialSetup)
        XCTAssertTrue(viewModel.isSetupFlowPresented)
    }

    func testCompleteSetupPersistsSettingsAndStoresAPIKey() {
        let sessionStore = MemorySessionStore()
        let secretStore = MemorySecretStore()
        let viewModel = OperatorPilotViewModel(
            executor: nil,
            chatResponder: MockChatResponder(response: .init(provider: .claude, text: "ignored")),
            sessionStore: sessionStore,
            secretStore: secretStore,
            environment: ["PATH": "/bin"]
        )

        viewModel.completeSetup(
            defaultProvider: .gemini,
            authMode: .apiKey,
            model: "gemini-2.5-flash",
            apiKey: "gem-key"
        )

        XCTAssertFalse(viewModel.requiresInitialSetup)
        XCTAssertEqual(sessionStore.lastSaved?.settings.defaultProvider, .gemini)
        XCTAssertEqual(secretStore.keys[.gemini], "gem-key")
    }

    func testImportSourceSelectsParquet() {
        let viewModel = OperatorPilotViewModel(
            executor: nil,
            chatResponder: MockChatResponder(response: .init(provider: .claude, text: "ignored")),
            sessionStore: MemorySessionStore(snapshot: .init(settings: AppSettings(hasCompletedSetup: true), sources: [], selectedSourceID: nil, transcript: [])),
            secretStore: MemorySecretStore(),
            environment: ["PATH": "/bin"]
        )
        let url = URL(fileURLWithPath: "/tmp/prices.parquet")

        viewModel.importSource(url: url)

        XCTAssertEqual(viewModel.selectedSource?.displayName, "prices.parquet")
        XCTAssertEqual(viewModel.sources.count, 1)
    }

    func testSendPromptWithoutSourceAppendsGuidance() async {
        let viewModel = OperatorPilotViewModel(
            executor: nil,
            chatResponder: MockChatResponder(response: .init(provider: .claude, text: "ignored")),
            sessionStore: MemorySessionStore(snapshot: .init(settings: AppSettings(hasCompletedSetup: true), sources: [], selectedSourceID: nil, transcript: [])),
            secretStore: MemorySecretStore(),
            environment: ["PATH": "/bin"]
        )
        viewModel.composerText = "show tables"

        await viewModel.sendPrompt()

        XCTAssertEqual(viewModel.transcript.last?.role, .assistant)
        XCTAssertTrue(viewModel.transcript.last?.body.contains("Open a local `.duckdb` or `.parquet` source first") == true)
    }

    func testSendPromptExecutesAndStoresLastResult() async {
        let result = DuckDBExecutionResult(
            binaryPath: "/opt/homebrew/bin/duckdb",
            arguments: [":memory:", "-table", "-c", "SELECT 1;"],
            sql: "SELECT 1;",
            stdout: "answer\n1\n",
            stderr: "",
            exitCode: 0,
            startedAt: Date(),
            endedAt: Date()
        )

        let viewModel = OperatorPilotViewModel(
            executor: MockExecutor(result: result),
            chatResponder: MockChatResponder(response: .init(provider: .claude, text: "ignored")),
            sessionStore: MemorySessionStore(snapshot: .init(settings: AppSettings(hasCompletedSetup: true), sources: [], selectedSourceID: nil, transcript: [])),
            secretStore: MemorySecretStore(),
            environment: ["PATH": "/bin"]
        )
        viewModel.importSource(url: URL(fileURLWithPath: "/tmp/prices.parquet"))

        await viewModel.triggerPrompt("Preview this parquet file")

        XCTAssertEqual(viewModel.lastExecution?.stdout, "answer\n1\n")
        XCTAssertTrue(viewModel.isDiagnosticsDrawerPresented)
        XCTAssertEqual(viewModel.transcript.last?.title, "Command Result")
    }

    func testProviderPromptUsesChatResponder() async {
        let response = ProviderChatResponse(provider: .claude, text: "Use rolling windows and watch regime shifts.")
        let viewModel = OperatorPilotViewModel(
            executor: nil,
            chatResponder: MockChatResponder(response: response),
            sessionStore: MemorySessionStore(snapshot: .init(settings: AppSettings(hasCompletedSetup: true), sources: [], selectedSourceID: nil, transcript: [])),
            secretStore: MemorySecretStore(),
            environment: ["PATH": "/bin"]
        )
        viewModel.composerText = "Explain momentum factor construction"

        await viewModel.sendPrompt()

        XCTAssertEqual(viewModel.transcript.last?.title, "Claude")
        XCTAssertEqual(viewModel.transcript.last?.body, response.text)
    }

    func testRawDuckDBPromptExecutesThroughExecutor() async {
        let result = DuckDBExecutionResult(
            binaryPath: "/opt/homebrew/bin/duckdb",
            arguments: ["--help"],
            sql: "",
            stdout: "usage",
            stderr: "",
            exitCode: 0,
            startedAt: Date(),
            endedAt: Date()
        )

        let viewModel = OperatorPilotViewModel(
            executor: MockExecutor(result: result),
            chatResponder: MockChatResponder(response: .init(provider: .claude, text: "ignored")),
            sessionStore: MemorySessionStore(snapshot: .init(settings: AppSettings(hasCompletedSetup: true), sources: [], selectedSourceID: nil, transcript: [])),
            secretStore: MemorySecretStore(),
            environment: ["PATH": "/bin"]
        )
        viewModel.composerText = "/duckdb --help"

        await viewModel.sendPrompt()

        XCTAssertEqual(viewModel.lastExecution?.arguments, ["--help"])
        XCTAssertEqual(viewModel.transcript.last?.title, "DuckDB CLI Result")
    }

    func testSnapshotRestoresSelectedSourceAndTranscript() {
        let source = DataSource(url: URL(fileURLWithPath: "/tmp/market.duckdb"), kind: .duckdb)
        let snapshot = AppSessionSnapshot(
            settings: AppSettings(hasCompletedSetup: true, defaultProvider: .openAI),
            sources: [source],
            selectedSourceID: source.id,
            transcript: [
                TranscriptItem(role: .assistant, title: "Saved", body: "Restored conversation")
            ]
        )

        let viewModel = OperatorPilotViewModel(
            executor: nil,
            chatResponder: MockChatResponder(response: .init(provider: .openAI, text: "ignored")),
            sessionStore: MemorySessionStore(snapshot: snapshot),
            secretStore: MemorySecretStore(),
            environment: ["PATH": "/bin"]
        )

        XCTAssertEqual(viewModel.selectedProvider, .openAI)
        XCTAssertEqual(viewModel.selectedSource, source)
        XCTAssertEqual(viewModel.transcript.last?.body, "Restored conversation")
        XCTAssertFalse(viewModel.isSetupFlowPresented)
    }
}
