import Foundation

public enum TranscriptRole: String, Codable, Sendable {
    case assistant
    case user
    case system
}

public enum TranscriptKind: Equatable, Sendable, Codable {
    case text
    case commandPreview(sql: String, sourceName: String)
    case rawCommandPreview(command: String)
    case commandResult(exitCode: Int32, stdout: String, stderr: String)

    private enum CodingKeys: String, CodingKey {
        case type
        case sql
        case sourceName
        case command
        case exitCode
        case stdout
        case stderr
    }

    private enum KindType: String, Codable {
        case text
        case commandPreview
        case rawCommandPreview
        case commandResult
    }

    public init(from decoder: any Decoder) throws {
        let container = try decoder.container(keyedBy: CodingKeys.self)
        let type = try container.decode(KindType.self, forKey: .type)

        switch type {
        case .text:
            self = .text
        case .commandPreview:
            self = .commandPreview(
                sql: try container.decode(String.self, forKey: .sql),
                sourceName: try container.decode(String.self, forKey: .sourceName)
            )
        case .rawCommandPreview:
            self = .rawCommandPreview(
                command: try container.decode(String.self, forKey: .command)
            )
        case .commandResult:
            self = .commandResult(
                exitCode: try container.decode(Int32.self, forKey: .exitCode),
                stdout: try container.decode(String.self, forKey: .stdout),
                stderr: try container.decode(String.self, forKey: .stderr)
            )
        }
    }

    public func encode(to encoder: any Encoder) throws {
        var container = encoder.container(keyedBy: CodingKeys.self)

        switch self {
        case .text:
            try container.encode(KindType.text, forKey: .type)
        case let .commandPreview(sql, sourceName):
            try container.encode(KindType.commandPreview, forKey: .type)
            try container.encode(sql, forKey: .sql)
            try container.encode(sourceName, forKey: .sourceName)
        case let .rawCommandPreview(command):
            try container.encode(KindType.rawCommandPreview, forKey: .type)
            try container.encode(command, forKey: .command)
        case let .commandResult(exitCode, stdout, stderr):
            try container.encode(KindType.commandResult, forKey: .type)
            try container.encode(exitCode, forKey: .exitCode)
            try container.encode(stdout, forKey: .stdout)
            try container.encode(stderr, forKey: .stderr)
        }
    }
}

public struct TranscriptItem: Identifiable, Equatable, Sendable, Codable {
    public let id: UUID
    public let role: TranscriptRole
    public let title: String
    public let body: String
    public let kind: TranscriptKind
    public let timestamp: Date

    public init(
        id: UUID = UUID(),
        role: TranscriptRole,
        title: String,
        body: String,
        kind: TranscriptKind = .text,
        timestamp: Date = Date()
    ) {
        self.id = id
        self.role = role
        self.title = title
        self.body = body
        self.kind = kind
        self.timestamp = timestamp
    }
}

public struct PromptChip: Identifiable, Equatable, Sendable {
    public let id: UUID
    public let title: String
    public let prompt: String

    public init(id: UUID = UUID(), title: String, prompt: String) {
        self.id = id
        self.title = title
        self.prompt = prompt
    }
}
