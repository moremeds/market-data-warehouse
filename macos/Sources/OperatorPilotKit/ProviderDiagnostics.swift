import Foundation
import MarketDataCore

public struct ProviderStatus: Identifiable, Equatable, Sendable {
    public let provider: ProviderKind
    public let cliInstalled: Bool
    public let cliPath: String?
    public let apiKeyPresent: Bool
    public let environmentKeyPresent: Bool

    public init(
        provider: ProviderKind,
        cliInstalled: Bool,
        cliPath: String?,
        apiKeyPresent: Bool,
        environmentKeyPresent: Bool
    ) {
        self.provider = provider
        self.cliInstalled = cliInstalled
        self.cliPath = cliPath
        self.apiKeyPresent = apiKeyPresent
        self.environmentKeyPresent = environmentKeyPresent
    }

    public var id: String { provider.rawValue }
    public var name: String { provider.displayName }
    public var cliName: String { provider.cliCommand }
    public var apiKeyName: String { provider.preferredAPIKeyEnvironmentName }
    public var statusSummary: String {
        if cliInstalled {
            return "CLI ready"
        }
        if apiKeyPresent || environmentKeyPresent {
            return "API key available"
        }
        return "Needs configuration"
    }
}

public enum ProviderDiagnostics {
    public static func detect(
        environment: [String: String] = ProcessInfo.processInfo.environment,
        secretStore: (any ProviderSecretStoring)? = nil,
        executableExists: (String) -> Bool = { FileManager.default.isExecutableFile(atPath: $0) }
    ) -> [ProviderStatus] {
        let pathSegments = environment["PATH"]?.split(separator: ":").map(String.init) ?? []

        func resolve(_ command: String) -> String? {
            for segment in pathSegments {
                let candidate = segment + "/" + command
                if executableExists(candidate) {
                    return candidate
                }
            }
            return nil
        }

        return ProviderKind.allCases.map { provider in
            let cliPath = resolve(provider.cliCommand)
            let environmentKeyPresent = provider.apiKeyEnvironmentNames.contains {
                environment[$0]?.isEmpty == false
            }
            let storedKeyPresent = secretStore?.apiKey(for: provider)?.isEmpty == false

            return ProviderStatus(
                provider: provider,
                cliInstalled: cliPath != nil,
                cliPath: cliPath,
                apiKeyPresent: storedKeyPresent,
                environmentKeyPresent: environmentKeyPresent
            )
        }
    }
}
