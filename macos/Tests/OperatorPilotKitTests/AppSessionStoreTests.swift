import Foundation
import XCTest
@testable import MarketDataCore
@testable import OperatorPilotKit

final class AppSessionStoreTests: XCTestCase {
    func testSaveAndLoadSnapshotRoundTrips() throws {
        let rootURL = FileManager.default.temporaryDirectory.appendingPathComponent(UUID().uuidString, isDirectory: true)
        let fileURL = rootURL.appendingPathComponent("session.json")
        let store = AppSessionStore(fileURL: fileURL)

        let source = DataSource(url: URL(fileURLWithPath: "/tmp/prices.parquet"), kind: .parquet)
        let snapshot = AppSessionSnapshot(
            settings: AppSettings(hasCompletedSetup: true, defaultProvider: .gemini),
            sources: [source],
            selectedSourceID: source.id,
            transcript: [
                TranscriptItem(role: .assistant, title: "Saved", body: "Round trip")
            ]
        )

        try store.saveSnapshot(snapshot)
        let restored = store.loadSnapshot()

        XCTAssertEqual(restored, snapshot)
        XCTAssertTrue(FileManager.default.fileExists(atPath: fileURL.path))

        try? FileManager.default.removeItem(at: rootURL)
    }
}
