import Foundation
import MarketDataCore

public struct ProviderChatResponse: Equatable, Sendable {
    public let provider: ProviderKind
    public let text: String
}

public enum ProviderChatError: Error, LocalizedError, Equatable {
    case cliUnavailable(String)
    case missingAPIKey(String)
    case processFailure(String)
    case malformedOutput(String)

    public var errorDescription: String? {
        switch self {
        case let .cliUnavailable(message),
             let .missingAPIKey(message),
             let .processFailure(message),
             let .malformedOutput(message):
            return message
        }
    }
}

public protocol ProviderResponding: Sendable {
    func respond(
        prompt: String,
        source: DataSource?,
        transcript: [TranscriptItem],
        settings: AppSettings,
        providerStatuses: [ProviderStatus]
    ) async throws -> ProviderChatResponse
}

public struct ProcessExecutionResult: Equatable, Sendable {
    public let stdout: String
    public let stderr: String
    public let exitCode: Int32
}

public protocol ProcessExecuting: Sendable {
    func execute(
        command: String,
        arguments: [String],
        environment: [String: String]
    ) async throws -> ProcessExecutionResult
}

public final class SystemProcessExecutor: ProcessExecuting, @unchecked Sendable {
    public init() {}

    public func execute(
        command: String,
        arguments: [String],
        environment: [String: String]
    ) async throws -> ProcessExecutionResult {
        try await Task.detached(priority: .userInitiated) {
            let process = Process()
            process.executableURL = URL(fileURLWithPath: command)
            process.arguments = arguments
            process.environment = environment

            let stdoutPipe = Pipe()
            let stderrPipe = Pipe()
            process.standardOutput = stdoutPipe
            process.standardError = stderrPipe

            do {
                try process.run()
            } catch {
                throw ProviderChatError.processFailure("Failed to launch \(command): \(error.localizedDescription)")
            }

            process.waitUntilExit()
            let stdout = String(decoding: stdoutPipe.fileHandleForReading.readDataToEndOfFile(), as: UTF8.self)
            let stderr = String(decoding: stderrPipe.fileHandleForReading.readDataToEndOfFile(), as: UTF8.self)

            return ProcessExecutionResult(stdout: stdout, stderr: stderr, exitCode: process.terminationStatus)
        }.value
    }
}

public final class ProviderChatService: ProviderResponding, @unchecked Sendable {
    private let processExecutor: any ProcessExecuting
    private let secretStore: any ProviderSecretStoring
    private let baseEnvironment: [String: String]

    public init(
        processExecutor: any ProcessExecuting = SystemProcessExecutor(),
        secretStore: any ProviderSecretStoring = KeychainStore(),
        baseEnvironment: [String: String] = ProcessInfo.processInfo.environment
    ) {
        self.processExecutor = processExecutor
        self.secretStore = secretStore
        self.baseEnvironment = baseEnvironment
    }

    public func respond(
        prompt: String,
        source: DataSource?,
        transcript: [TranscriptItem],
        settings: AppSettings,
        providerStatuses: [ProviderStatus]
    ) async throws -> ProviderChatResponse {
        let provider = settings.defaultProvider
        let preference = settings.preference(for: provider)

        guard let status = providerStatuses.first(where: { $0.provider == provider }), status.cliInstalled, let cliPath = status.cliPath else {
            throw ProviderChatError.cliUnavailable("`\(provider.cliCommand)` is not available on this Mac. Install the local CLI first. This build uses the provider CLI for both subscription and API-key-backed chat.")
        }

        let fallbackAPIKey = apiKey(for: provider, environment: baseEnvironment)
        var environment = preparedEnvironment(for: provider, authMode: preference.authMode)
        let wrappedPrompt = wrappedPrompt(prompt: prompt, source: source, transcript: transcript)
        let arguments: [String]
        var codexOutputPath: String?

        switch provider {
        case .claude:
            arguments = claudeArguments(prompt: wrappedPrompt, model: preference.customModel)
        case .openAI:
            let invocation = codexInvocation(prompt: wrappedPrompt, model: preference.customModel)
            arguments = invocation.arguments
            codexOutputPath = invocation.outputPath
        case .gemini:
            arguments = geminiArguments(prompt: wrappedPrompt, model: preference.customModel)
        }

        if preference.authMode == .apiKey {
            let key = apiKey(for: provider, environment: environment)
            guard !key.isEmpty else {
                throw ProviderChatError.missingAPIKey("No API key is configured for \(provider.displayName). Add one in Settings or complete setup with API key mode.")
            }
            environment[provider.preferredAPIKeyEnvironmentName] = key
        }

        var result = try await processExecutor.execute(
            command: cliPath,
            arguments: arguments,
            environment: environment
        )

        if result.exitCode != 0,
           preference.authMode == .localCLI,
           !fallbackAPIKey.isEmpty {
            var fallbackEnvironment = preparedEnvironment(for: provider, authMode: .apiKey)
            fallbackEnvironment[provider.preferredAPIKeyEnvironmentName] = fallbackAPIKey
            result = try await processExecutor.execute(
                command: cliPath,
                arguments: arguments,
                environment: fallbackEnvironment
            )
        }

        guard result.exitCode == 0 else {
            let message = result.stderr.trimmingCharacters(in: .whitespacesAndNewlines)
            throw ProviderChatError.processFailure(message.isEmpty ? "\(provider.displayName) exited with status \(result.exitCode)." : message)
        }

        let text = try parseResponse(from: result.stdout, provider: provider, codexOutputPath: codexOutputPath)
        cleanupTemporaryOutput(at: codexOutputPath)

        return ProviderChatResponse(
            provider: provider,
            text: text
        )
    }

    private func wrappedPrompt(
        prompt: String,
        source: DataSource?,
        transcript: [TranscriptItem]
    ) -> String {
        let transcriptTail = transcript.suffix(4).map { item in
            "[\(item.role.rawValue)] \(item.title): \(item.body)"
        }.joined(separator: "\n")

        if let source {
            return """
            You are the assistant inside Market Data Warehouse, a native macOS app for local parquet and DuckDB analysis.

            Current source:
            - name: \(source.displayName)
            - kind: \(source.kind.rawValue)
            - path: \(source.path)

            Recent transcript:
            \(transcriptTail)

            Respond concisely. If a DuckDB query would help, include it plainly and say what it does.

            User prompt:
            \(prompt)
            """
        }

        return """
        You are the assistant inside Market Data Warehouse, a native macOS app for local parquet and DuckDB analysis.

        Recent transcript:
        \(transcriptTail)

        Respond concisely. If the user needs to run SQL, say so clearly.

        User prompt:
        \(prompt)
        """
    }

    private func preparedEnvironment(for provider: ProviderKind, authMode: ProviderAuthMode) -> [String: String] {
        var environment = baseEnvironment

        if authMode == .localCLI {
            for key in provider.apiKeyEnvironmentNames {
                environment.removeValue(forKey: key)
            }
        }

        return environment
    }

    private func apiKey(for provider: ProviderKind, environment: [String: String]) -> String {
        if let stored = secretStore.apiKey(for: provider), !stored.isEmpty {
            return stored
        }

        for key in provider.apiKeyEnvironmentNames {
            if let value = environment[key], !value.isEmpty {
                return value
            }
        }

        return ""
    }

    private func claudeArguments(prompt: String, model: String) -> [String] {
        var arguments = [
            "-p",
            prompt,
            "--output-format",
            "json",
            "--no-session-persistence",
            "--tools",
            "",
        ]
        if !model.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty {
            arguments.append(contentsOf: ["--model", model])
        }
        return arguments
    }

    private func geminiArguments(prompt: String, model: String) -> [String] {
        var arguments = [
            "--prompt",
            prompt,
            "--output-format",
            "json",
        ]
        if !model.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty {
            arguments.append(contentsOf: ["--model", model])
        }
        return arguments
    }

    private func codexInvocation(prompt: String, model: String) -> (arguments: [String], outputPath: String) {
        let outputPath = FileManager.default.temporaryDirectory
            .appendingPathComponent(UUID().uuidString)
            .appendingPathExtension("txt")
            .path

        var arguments: [String] = []
        if !model.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty {
            arguments.append(contentsOf: ["-m", model])
        }
        arguments.append(contentsOf: [
            "exec",
            prompt,
            "--output-last-message",
            outputPath,
            "--sandbox",
            "read-only",
            "--ephemeral",
            "--skip-git-repo-check",
            "--color",
            "never",
        ])
        return (arguments, outputPath)
    }

    private func parseResponse(from output: String, provider: ProviderKind, codexOutputPath: String?) throws -> String {
        switch provider {
        case .claude:
            let data = Data(output.utf8)
            let decoded = try JSONDecoder().decode(ClaudeResponse.self, from: data)
            return decoded.result.trimmingCharacters(in: .whitespacesAndNewlines)
        case .gemini:
            guard let start = output.firstIndex(of: "{") else {
                throw ProviderChatError.malformedOutput("Gemini did not return JSON output.")
            }
            let jsonString = String(output[start...])
            let data = Data(jsonString.utf8)
            let decoded = try JSONDecoder().decode(GeminiResponse.self, from: data)
            return decoded.response.trimmingCharacters(in: .whitespacesAndNewlines)
        case .openAI:
            guard let codexOutputPath else {
                throw ProviderChatError.malformedOutput("Codex did not provide an output file path.")
            }
            let fileOutput = (try? String(contentsOfFile: codexOutputPath, encoding: .utf8))?.trimmingCharacters(in: .whitespacesAndNewlines) ?? ""
            if fileOutput.isEmpty {
                throw ProviderChatError.malformedOutput("Codex did not return a final message.")
            }
            return fileOutput
        }
    }

    private func cleanupTemporaryOutput(at path: String?) {
        guard let path else { return }
        try? FileManager.default.removeItem(atPath: path)
    }
}

private struct ClaudeResponse: Decodable {
    let result: String
}

private struct GeminiResponse: Decodable {
    let response: String
}
