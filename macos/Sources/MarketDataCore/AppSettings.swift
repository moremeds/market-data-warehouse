import Foundation

public enum ProviderKind: String, CaseIterable, Codable, Sendable, Identifiable {
    case claude
    case openAI
    case gemini

    public var id: String { rawValue }

    public var displayName: String {
        switch self {
        case .claude:
            return "Claude"
        case .openAI:
            return "OpenAI"
        case .gemini:
            return "Gemini"
        }
    }

    public var cliCommand: String {
        switch self {
        case .claude:
            return "claude"
        case .openAI:
            return "codex"
        case .gemini:
            return "gemini"
        }
    }

    public var apiKeyEnvironmentNames: [String] {
        switch self {
        case .claude:
            return ["ANTHROPIC_API_KEY"]
        case .openAI:
            return ["OPENAI_API_KEY"]
        case .gemini:
            return ["GEMINI_API_KEY", "GOOGLE_API_KEY"]
        }
    }

    public var preferredAPIKeyEnvironmentName: String {
        apiKeyEnvironmentNames[0]
    }

    public var suggestedModel: String {
        switch self {
        case .claude:
            return "sonnet"
        case .openAI:
            return ""
        case .gemini:
            return ""
        }
    }
}

public enum ProviderAuthMode: String, CaseIterable, Codable, Sendable, Identifiable {
    case localCLI
    case apiKey

    public var id: String { rawValue }

    public var displayName: String {
        switch self {
        case .localCLI:
            return "Local Subscription"
        case .apiKey:
            return "API Key"
        }
    }
}

public struct ProviderPreference: Codable, Equatable, Sendable {
    public var authMode: ProviderAuthMode
    public var customModel: String

    public init(authMode: ProviderAuthMode = .localCLI, customModel: String = "") {
        self.authMode = authMode
        self.customModel = customModel
    }

    public static func `default`(for provider: ProviderKind) -> ProviderPreference {
        ProviderPreference(authMode: .localCLI, customModel: provider.suggestedModel)
    }
}

public struct AppSettings: Codable, Equatable, Sendable {
    public var hasCompletedSetup: Bool
    public var defaultProvider: ProviderKind
    public var providerPreferences: [String: ProviderPreference]

    public init(
        hasCompletedSetup: Bool = false,
        defaultProvider: ProviderKind = .claude,
        providerPreferences: [String: ProviderPreference] = [:]
    ) {
        self.hasCompletedSetup = hasCompletedSetup
        self.defaultProvider = defaultProvider
        self.providerPreferences = providerPreferences
    }

    public func preference(for provider: ProviderKind) -> ProviderPreference {
        providerPreferences[provider.rawValue] ?? .default(for: provider)
    }

    public mutating func setPreference(_ preference: ProviderPreference, for provider: ProviderKind) {
        providerPreferences[provider.rawValue] = preference
    }
}

public struct AppSessionSnapshot: Codable, Equatable, Sendable {
    public var settings: AppSettings
    public var sources: [DataSource]
    public var selectedSourceID: UUID?
    public var transcript: [TranscriptItem]

    public init(
        settings: AppSettings,
        sources: [DataSource],
        selectedSourceID: UUID?,
        transcript: [TranscriptItem]
    ) {
        self.settings = settings
        self.sources = sources
        self.selectedSourceID = selectedSourceID
        self.transcript = transcript
    }
}
