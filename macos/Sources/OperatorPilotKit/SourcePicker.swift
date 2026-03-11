import AppKit
import Foundation

enum SourcePicker {
    @MainActor
    static func pickURL() -> URL? {
        if let automationPath = ProcessInfo.processInfo.environment["MARKET_DATA_WAREHOUSE_AUTOMATION_PICK_SOURCE"],
           !automationPath.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty {
            return URL(fileURLWithPath: automationPath)
        }

        let panel = NSOpenPanel()
        panel.canChooseFiles = true
        panel.canChooseDirectories = false
        panel.allowsMultipleSelection = false
        panel.allowedContentTypes = []
        panel.message = "Choose a local .duckdb, .db, or .parquet file."
        panel.prompt = "Open"

        return panel.runModal() == .OK ? panel.url : nil
    }
}
