import Foundation
import MarketDataCore

public protocol AppSessionPersisting: Sendable {
    func loadSnapshot() -> AppSessionSnapshot?
    func saveSnapshot(_ snapshot: AppSessionSnapshot) throws
}

public final class AppSessionStore: AppSessionPersisting, @unchecked Sendable {
    private let fileURL: URL
    private let fileManager: FileManager

    public init(
        fileURL: URL? = nil,
        fileManager: FileManager = .default
    ) {
        self.fileManager = fileManager
        self.fileURL = fileURL ?? Self.defaultFileURL(fileManager: fileManager)
    }

    public func loadSnapshot() -> AppSessionSnapshot? {
        guard let data = try? Data(contentsOf: fileURL) else {
            return nil
        }

        return try? JSONDecoder().decode(AppSessionSnapshot.self, from: data)
    }

    public func saveSnapshot(_ snapshot: AppSessionSnapshot) throws {
        try fileManager.createDirectory(at: fileURL.deletingLastPathComponent(), withIntermediateDirectories: true)
        let data = try JSONEncoder().encode(snapshot)
        try data.write(to: fileURL, options: .atomic)
    }

    private static func defaultFileURL(fileManager: FileManager) -> URL {
        if let override = ProcessInfo.processInfo.environment["MARKET_DATA_WAREHOUSE_SESSION_FILE"],
           !override.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty {
            return URL(fileURLWithPath: override)
        }

        let appSupport = fileManager.urls(for: .applicationSupportDirectory, in: .userDomainMask).first
            ?? URL(fileURLWithPath: NSHomeDirectory()).appendingPathComponent("Library/Application Support", isDirectory: true)
        return appSupport
            .appendingPathComponent("MarketDataWarehouseMac", isDirectory: true)
            .appendingPathComponent("session.json")
    }
}
