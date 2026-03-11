import XCTest
@testable import MarketDataCore
@testable import OperatorPilotMetal

final class MetalWorkspaceSurfaceTests: XCTestCase {
    func testVisualizationNormalizesSnapshotIntoSignals() {
        let snapshot = MetalWorkspaceSnapshot(
            destination: .assistant,
            provider: .claude,
            sourceKind: .parquet,
            sourceCount: 2,
            transcriptCount: 7,
            providerReadiness: 2,
            executionState: .success,
            commandDurationMilliseconds: 420,
            commandOutputBytes: 2_048,
            isRunning: true
        )

        let visualization = MetalWorkspaceVisualization(snapshot: snapshot, signalCount: 32)

        XCTAssertEqual(visualization.signalBars.count, 32)
        XCTAssertTrue(visualization.signalBars.allSatisfy { $0 >= 0 && $0 <= 1 })
        XCTAssertEqual(visualization.runIntensity, 1)
        XCTAssertGreaterThan(visualization.transcriptDensity, 0)
        XCTAssertGreaterThan(visualization.commandStateWeight, 0)
    }

    func testVisualizationRespondsToFailureState() {
        let success = MetalWorkspaceVisualization(
            snapshot: MetalWorkspaceSnapshot(
                destination: .assistant,
                provider: .openAI,
                sourceKind: .duckdb,
                sourceCount: 1,
                transcriptCount: 4,
                providerReadiness: 1,
                executionState: .success,
                commandDurationMilliseconds: 300,
                commandOutputBytes: 800,
                isRunning: false
            ),
            signalCount: 24
        )
        let failure = MetalWorkspaceVisualization(
            snapshot: MetalWorkspaceSnapshot(
                destination: .assistant,
                provider: .openAI,
                sourceKind: .duckdb,
                sourceCount: 1,
                transcriptCount: 4,
                providerReadiness: 1,
                executionState: .failure,
                commandDurationMilliseconds: 300,
                commandOutputBytes: 800,
                isRunning: false
            ),
            signalCount: 24
        )

        XCTAssertGreaterThan(failure.commandStateWeight, success.commandStateWeight)
        XCTAssertNotEqual(failure.signalBars, success.signalBars)
    }

    func testShaderSourceURLPointsAtBundledMetalSource() {
        let sourceURL = MetalShaderLibrary.shaderSourceURL()

        XCTAssertEqual(sourceURL.lastPathComponent, "OperatorPilotMetalShaders.metal")
    }
}
