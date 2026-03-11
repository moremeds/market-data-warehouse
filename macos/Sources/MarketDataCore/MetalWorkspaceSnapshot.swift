import Foundation

public enum MetalWorkspaceDestination: String, Codable, Sendable {
    case assistant
    case transcripts
    case setup
    case settings
}

public enum MetalExecutionState: String, Codable, Sendable {
    case idle
    case success
    case failure
}

public struct MetalWorkspaceSnapshot: Equatable, Codable, Sendable {
    public let destination: MetalWorkspaceDestination
    public let provider: ProviderKind
    public let sourceKind: DataSourceKind?
    public let sourceCount: Int
    public let transcriptCount: Int
    public let providerReadiness: Int
    public let executionState: MetalExecutionState
    public let commandDurationMilliseconds: Double
    public let commandOutputBytes: Int
    public let isRunning: Bool

    public init(
        destination: MetalWorkspaceDestination,
        provider: ProviderKind,
        sourceKind: DataSourceKind?,
        sourceCount: Int,
        transcriptCount: Int,
        providerReadiness: Int,
        executionState: MetalExecutionState,
        commandDurationMilliseconds: Double,
        commandOutputBytes: Int,
        isRunning: Bool
    ) {
        self.destination = destination
        self.provider = provider
        self.sourceKind = sourceKind
        self.sourceCount = sourceCount
        self.transcriptCount = transcriptCount
        self.providerReadiness = providerReadiness
        self.executionState = executionState
        self.commandDurationMilliseconds = commandDurationMilliseconds
        self.commandOutputBytes = commandOutputBytes
        self.isRunning = isRunning
    }
}
