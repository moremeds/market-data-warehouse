import Foundation
import DuckDBCLIAdapter
import MarketDataCore

@MainActor
public protocol CommandExecuting: Sendable {
    func execute(plan: DuckDBCommandPlan) async throws -> DuckDBExecutionResult
    func executeRaw(argumentsLine: String) async throws -> DuckDBExecutionResult
}

extension DuckDBCLIExecutor: CommandExecuting {}

public enum SidebarDestination: String, CaseIterable, Hashable, Identifiable, Codable {
    case assistant = "Assistant"
    case transcripts = "Transcripts"
    case setup = "Setup"
    case settings = "Settings"

    public var id: String { rawValue }
}

@MainActor
public final class OperatorPilotViewModel: ObservableObject {
    @Published public var selectedDestination: SidebarDestination? = .assistant
    @Published public var composerText = ""
    @Published public private(set) var transcript: [TranscriptItem]
    @Published public private(set) var sources: [DataSource]
    @Published public private(set) var selectedSource: DataSource?
    @Published public private(set) var lastExecution: DuckDBExecutionResult?
    @Published public private(set) var providerStatuses: [ProviderStatus]
    @Published public private(set) var settings: AppSettings
    @Published public var manualDuckDBArguments = ""
    @Published public var composerFocusRequestID = 0
    @Published public var isDiagnosticsDrawerPresented = false
    @Published public var isSetupFlowPresented = false
    @Published public private(set) var isRunning = false

    private let executor: (any CommandExecuting)?
    private let chatResponder: any ProviderResponding
    private let sessionStore: any AppSessionPersisting
    private let secretStore: any ProviderSecretStoring
    private let environment: [String: String]

    public init(
        executor: (any CommandExecuting)? = try? DuckDBCLIExecutor(),
        chatResponder: any ProviderResponding = ProviderChatService(),
        sessionStore: any AppSessionPersisting = AppSessionStore(),
        secretStore: any ProviderSecretStoring = KeychainStore(),
        environment: [String: String] = ProcessInfo.processInfo.environment
    ) {
        self.executor = executor
        self.chatResponder = chatResponder
        self.sessionStore = sessionStore
        self.secretStore = secretStore
        self.environment = environment

        let snapshot = sessionStore.loadSnapshot()
        self.settings = snapshot?.settings ?? AppSettings()
        self.sources = snapshot?.sources ?? []
        self.transcript = snapshot?.transcript.isEmpty == false ? snapshot!.transcript : Self.initialTranscript
        if let selectedSourceID = snapshot?.selectedSourceID {
            self.selectedSource = snapshot?.sources.first(where: { $0.id == selectedSourceID })
        } else {
            self.selectedSource = nil
        }
        self.providerStatuses = ProviderDiagnostics.detect(
            environment: environment,
            secretStore: secretStore
        )
        self.selectedDestination = settings.hasCompletedSetup ? .assistant : .setup
        self.isSetupFlowPresented = !settings.hasCompletedSetup
    }

    public static var initialTranscript: [TranscriptItem] {
        [
            TranscriptItem(
                role: .assistant,
                title: "Assistant",
                body: "Welcome. Finish setup, open a `.duckdb` or `.parquet` source, then ask for SQL or use the selected provider for broader analysis."
            ),
        ]
    }

    public var promptChips: [PromptChip] {
        PromptLibrary.prompts(for: selectedSource)
    }

    public var requiresInitialSetup: Bool {
        !settings.hasCompletedSetup
    }

    public var selectedProvider: ProviderKind {
        settings.defaultProvider
    }

    public func preference(for provider: ProviderKind) -> ProviderPreference {
        settings.preference(for: provider)
    }

    public func status(for provider: ProviderKind) -> ProviderStatus? {
        providerStatuses.first(where: { $0.provider == provider })
    }

    public func importSource(url: URL) {
        guard let source = DataSource.from(url: url) else {
            appendTranscript(
                TranscriptItem(
                    role: .system,
                    title: "Unsupported Source",
                    body: "Only `.duckdb`, `.db`, and `.parquet` files are supported."
                )
            )
            return
        }

        if !sources.contains(source) {
            sources.insert(source, at: 0)
        }
        selectedSource = source
        selectedDestination = .assistant

        appendTranscript(
            TranscriptItem(
                role: .system,
                title: "Source Attached",
                body: "Attached `\(source.displayName)` as the active data source."
            )
        )
        persistSnapshot()
    }

    @MainActor
    public func promptForSourceImport() {
        guard let url = SourcePicker.pickURL() else {
            return
        }
        importSource(url: url)
    }

    public func selectSource(_ source: DataSource) {
        selectedSource = source
        selectedDestination = .assistant
        persistSnapshot()
    }

    public func triggerPrompt(_ prompt: String) async {
        composerText = prompt
        await sendPrompt()
    }

    public func sendPrompt() async {
        let prompt = composerText.trimmingCharacters(in: .whitespacesAndNewlines)
        guard !prompt.isEmpty else { return }

        composerText = ""
        appendTranscript(
            TranscriptItem(
                role: .user,
                title: "You",
                body: prompt
            )
        )

        let action = AssistantPlanner.plan(prompt: prompt, source: selectedSource)
        switch action {
        case let .assistantReply(reply):
            appendTranscript(
                TranscriptItem(
                    role: .assistant,
                    title: "Assistant",
                    body: reply
                )
            )

        case let .command(plan):
            appendTranscript(
                TranscriptItem(
                    role: .assistant,
                    title: "Command Preview",
                    body: plan.explanation,
                    kind: .commandPreview(sql: plan.sql, sourceName: plan.source.displayName)
                )
            )

            guard let executor else {
                appendTranscript(
                    TranscriptItem(
                        role: .system,
                        title: "DuckDB Unavailable",
                        body: "The app could not locate the `duckdb` binary. Install DuckDB or set DUCKDB_BINARY."
                    )
                )
                return
            }

            isRunning = true
            isDiagnosticsDrawerPresented = true
            defer { isRunning = false }

            do {
                let result = try await executor.execute(plan: plan)
                lastExecution = result
                appendTranscript(
                    TranscriptItem(
                        role: .assistant,
                        title: result.exitCode == 0 ? "Command Result" : "Command Error",
                        body: result.exitCode == 0 ? "DuckDB finished running the command." : "DuckDB returned a non-zero exit code.",
                        kind: .commandResult(
                            exitCode: result.exitCode,
                            stdout: result.stdout,
                            stderr: result.stderr
                        )
                    )
                )
            } catch {
                appendTranscript(
                    TranscriptItem(
                        role: .system,
                        title: "Execution Failed",
                        body: error.localizedDescription
                    )
                )
            }

        case let .providerPrompt(providerPrompt):
            await sendProviderPrompt(providerPrompt)

        case let .rawCommand(argumentsLine):
            await executeRawDuckDB(argumentsLine: argumentsLine, source: .chatComposer)
        }
    }

    public func completeSetup(
        defaultProvider: ProviderKind,
        authMode: ProviderAuthMode,
        model: String,
        apiKey: String
    ) {
        settings.defaultProvider = defaultProvider
        settings.hasCompletedSetup = true
        settings.setPreference(
            ProviderPreference(authMode: authMode, customModel: model),
            for: defaultProvider
        )

        if authMode == .apiKey {
            let trimmed = apiKey.trimmingCharacters(in: .whitespacesAndNewlines)
            if !trimmed.isEmpty {
                try? secretStore.saveAPIKey(trimmed, for: defaultProvider)
            }
        }

        refreshProviderStatuses()
        isSetupFlowPresented = false
        selectedDestination = .assistant
        appendTranscript(
            TranscriptItem(
                role: .system,
                title: "Setup Complete",
                body: "\(defaultProvider.displayName) is now the default provider. You can change this anytime in Settings."
            )
        )
        persistSnapshot()
    }

    public func reopenSetup() {
        isSetupFlowPresented = true
        selectedDestination = .setup
    }

    public func updateDefaultProvider(_ provider: ProviderKind) {
        settings.defaultProvider = provider
        if settings.providerPreferences[provider.rawValue] == nil {
            settings.setPreference(.default(for: provider), for: provider)
        }
        refreshProviderStatuses()
        persistSnapshot()
    }

    public func updateAuthMode(_ mode: ProviderAuthMode, for provider: ProviderKind) {
        var preference = settings.preference(for: provider)
        preference.authMode = mode
        settings.setPreference(preference, for: provider)
        refreshProviderStatuses()
        persistSnapshot()
    }

    public func updateCustomModel(_ model: String, for provider: ProviderKind) {
        var preference = settings.preference(for: provider)
        preference.customModel = model
        settings.setPreference(preference, for: provider)
        persistSnapshot()
    }

    public func saveAPIKey(_ key: String, for provider: ProviderKind) {
        let trimmed = key.trimmingCharacters(in: .whitespacesAndNewlines)
        guard !trimmed.isEmpty else { return }
        try? secretStore.saveAPIKey(trimmed, for: provider)
        refreshProviderStatuses()
        persistSnapshot()
    }

    public func removeAPIKey(for provider: ProviderKind) {
        try? secretStore.removeAPIKey(for: provider)
        refreshProviderStatuses()
        persistSnapshot()
    }

    public func hasStoredAPIKey(for provider: ProviderKind) -> Bool {
        secretStore.apiKey(for: provider)?.isEmpty == false
    }

    public func clearConversation() {
        transcript = Self.initialTranscript
        lastExecution = nil
        persistSnapshot()
    }

    public func refreshProviderStatuses() {
        providerStatuses = ProviderDiagnostics.detect(
            environment: environment,
            secretStore: secretStore
        )
    }

    public func requestComposerFocus() {
        composerFocusRequestID += 1
    }

    public func runRawDuckDBCommand() async {
        let argumentsLine = manualDuckDBArguments.trimmingCharacters(in: .whitespacesAndNewlines)
        guard !argumentsLine.isEmpty else {
            return
        }

        await executeRawDuckDB(argumentsLine: argumentsLine, source: .diagnosticsDrawer)
        manualDuckDBArguments = ""
    }

    private func sendProviderPrompt(_ prompt: String) async {
        isRunning = true
        defer { isRunning = false }

        do {
            let response = try await chatResponder.respond(
                prompt: prompt,
                source: selectedSource,
                transcript: transcript,
                settings: settings,
                providerStatuses: providerStatuses
            )
            appendTranscript(
                TranscriptItem(
                    role: .assistant,
                    title: response.provider.displayName,
                    body: response.text
                )
            )
        } catch {
            appendTranscript(
                TranscriptItem(
                    role: .system,
                    title: "Provider Error",
                    body: error.localizedDescription
                )
            )
        }
    }

    private func appendTranscript(_ item: TranscriptItem) {
        transcript.append(item)
        persistSnapshot()
    }

    private func executeRawDuckDB(argumentsLine: String, source: DuckDBRunSource) async {
        appendTranscript(
            TranscriptItem(
                role: .assistant,
                title: "DuckDB CLI",
                body: "Running raw DuckDB CLI arguments exactly as entered.",
                kind: .rawCommandPreview(command: argumentsLine)
            )
        )

        guard let executor else {
            appendTranscript(
                TranscriptItem(
                    role: .system,
                    title: "DuckDB Unavailable",
                    body: "The app could not locate the `duckdb` binary. Install DuckDB or set DUCKDB_BINARY."
                )
            )
            return
        }

        isRunning = true
        isDiagnosticsDrawerPresented = true
        defer { isRunning = false }

        do {
            let result = try await executor.executeRaw(argumentsLine: argumentsLine)
            lastExecution = result
            appendTranscript(
                TranscriptItem(
                    role: .assistant,
                    title: result.exitCode == 0 ? "DuckDB CLI Result" : "DuckDB CLI Error",
                    body: source.successMessage,
                    kind: .commandResult(
                        exitCode: result.exitCode,
                        stdout: result.stdout,
                        stderr: result.stderr
                    )
                )
            )
        } catch {
            appendTranscript(
                TranscriptItem(
                    role: .system,
                    title: "Execution Failed",
                    body: error.localizedDescription
                )
            )
        }
    }

    private func persistSnapshot() {
        try? sessionStore.saveSnapshot(
            AppSessionSnapshot(
                settings: settings,
                sources: sources,
                selectedSourceID: selectedSource?.id,
                transcript: transcript
            )
        )
    }
}

private enum DuckDBRunSource {
    case chatComposer
    case diagnosticsDrawer

    var successMessage: String {
        switch self {
        case .chatComposer:
            "DuckDB finished running the raw CLI command from the chat composer."
        case .diagnosticsDrawer:
            "DuckDB finished running the raw CLI command from the diagnostics drawer."
        }
    }
}
