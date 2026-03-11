import Foundation
import MarketDataCore

struct MetalWorkspaceVisualization: Equatable, Sendable {
    let transcriptDensity: Float
    let sourceDensity: Float
    let readinessFraction: Float
    let commandStateWeight: Float
    let runIntensity: Float
    let commandDurationMilliseconds: Float
    let commandOutputScale: Float
    let signalBars: [Float]

    init(snapshot: MetalWorkspaceSnapshot, signalCount: Int = 48) {
        let transcriptDensity = min(Float(snapshot.transcriptCount) / 18, 1)
        let sourceDensity = min(Float(snapshot.sourceCount) / 6, 1)
        let readinessFraction = min(Float(snapshot.providerReadiness) / 3, 1)
        let commandStateWeight: Float
        switch snapshot.executionState {
        case .idle:
            commandStateWeight = 0
        case .success:
            commandStateWeight = 0.72
        case .failure:
            commandStateWeight = 1
        }

        let duration = min(Float(snapshot.commandDurationMilliseconds) / 1800, 1)
        let outputScale = min(Float(snapshot.commandOutputBytes) / 16_384, 1)

        self.transcriptDensity = transcriptDensity
        self.sourceDensity = sourceDensity
        self.readinessFraction = readinessFraction
        self.commandStateWeight = commandStateWeight
        self.runIntensity = snapshot.isRunning ? 1 : 0
        self.commandDurationMilliseconds = Float(snapshot.commandDurationMilliseconds)
        self.commandOutputScale = outputScale
        self.signalBars = Self.makeSignalBars(
            snapshot: snapshot,
            signalCount: max(signalCount, 12),
            transcriptDensity: transcriptDensity,
            sourceDensity: sourceDensity,
            readinessFraction: readinessFraction,
            commandStateWeight: commandStateWeight,
            duration: duration,
            outputScale: outputScale
        )
    }

    private static func makeSignalBars(
        snapshot: MetalWorkspaceSnapshot,
        signalCount: Int,
        transcriptDensity: Float,
        sourceDensity: Float,
        readinessFraction: Float,
        commandStateWeight: Float,
        duration: Float,
        outputScale: Float
    ) -> [Float] {
        let providerSeed: Float
        switch snapshot.provider {
        case .claude:
            providerSeed = 0.37
        case .openAI:
            providerSeed = 0.73
        case .gemini:
            providerSeed = 1.11
        }

        let destinationSeed: Float
        switch snapshot.destination {
        case .assistant:
            destinationSeed = 0.19
        case .transcripts:
            destinationSeed = 0.41
        case .setup:
            destinationSeed = 0.67
        case .settings:
            destinationSeed = 0.89
        }

        let sourceSeed: Float
        switch snapshot.sourceKind {
        case .parquet:
            sourceSeed = 0.23
        case .duckdb:
            sourceSeed = 0.59
        case nil:
            sourceSeed = 0.11
        }

        let base = Float(snapshot.transcriptCount * 13 + snapshot.sourceCount * 29 + snapshot.providerReadiness * 31)
            + Float(snapshot.commandOutputBytes % 251)
            + providerSeed * 19
            + destinationSeed * 23
            + sourceSeed * 17

        return (0..<signalCount).map { index in
            let position = Float(index) / Float(max(signalCount - 1, 1))
            let waveA = 0.5 + 0.5 * sin(base * 0.014 + Float(index) * 0.49 + providerSeed * 3.1)
            let waveB = 0.5 + 0.5 * cos(base * 0.021 + Float(index) * 0.27 + destinationSeed * 4.4)
            let ridge = 0.5 + 0.5 * sin((position + sourceSeed) * 10.5)
            let density = transcriptDensity * 0.34 + sourceDensity * 0.22 + readinessFraction * 0.16
            let commandLift = commandStateWeight * 0.12 + duration * 0.08 + outputScale * 0.08
            return min(
                1,
                0.12 + waveA * 0.28 + waveB * 0.18 + ridge * 0.16 + density + commandLift
            )
        }
    }
}
