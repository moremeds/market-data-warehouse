import XCTest
@testable import MarketDataCore

final class AssistantPlannerTests: XCTestCase {
    func testNoSourceReturnsGuidance() {
        let action = AssistantPlanner.plan(prompt: "preview this", source: nil)

        guard case let .assistantReply(reply) = action else {
            return XCTFail("Expected assistant reply")
        }

        XCTAssertTrue(reply.contains("Open a local `.duckdb` or `.parquet` source first"))
    }

    func testParquetPreviewUsesReadParquet() {
        let source = DataSource(url: URL(fileURLWithPath: "/tmp/prices.parquet"), kind: .parquet)
        let action = AssistantPlanner.plan(prompt: "Preview this parquet file", source: source)

        guard case let .command(plan) = action else {
            return XCTFail("Expected command plan")
        }

        XCTAssertTrue(plan.sql.contains("read_parquet('/tmp/prices.parquet')"))
        XCTAssertTrue(plan.sql.contains("LIMIT 25"))
    }

    func testDuckDBShowTablesUsesShowTables() {
        let source = DataSource(url: URL(fileURLWithPath: "/tmp/market.duckdb"), kind: .duckdb)
        let action = AssistantPlanner.plan(prompt: "Show tables", source: source)

        guard case let .command(plan) = action else {
            return XCTFail("Expected command plan")
        }

        XCTAssertEqual(plan.sql, "SHOW TABLES;")
    }

    func testRawSQLPassthrough() {
        let source = DataSource(url: URL(fileURLWithPath: "/tmp/market.duckdb"), kind: .duckdb)
        let action = AssistantPlanner.plan(prompt: "/sql SELECT 42 AS answer;", source: source)

        guard case let .command(plan) = action else {
            return XCTFail("Expected command plan")
        }

        XCTAssertEqual(plan.sql, "SELECT 42 AS answer;")
    }

    func testRawDuckDBCommandPassthrough() {
        let action = AssistantPlanner.plan(prompt: "/duckdb --help", source: nil)

        guard case let .rawCommand(argumentsLine) = action else {
            return XCTFail("Expected raw command")
        }

        XCTAssertEqual(argumentsLine, "--help")
    }

    func testGeneralPromptWithoutSourceUsesProvider() {
        let action = AssistantPlanner.plan(prompt: "Explain factor momentum", source: nil)

        guard case let .providerPrompt(prompt) = action else {
            return XCTFail("Expected provider prompt")
        }

        XCTAssertEqual(prompt, "Explain factor momentum")
    }

    func testDuckDBUnknownPromptUsesProvider() {
        let source = DataSource(url: URL(fileURLWithPath: "/tmp/market.duckdb"), kind: .duckdb)
        let action = AssistantPlanner.plan(prompt: "How should I analyze drawdowns here?", source: source)

        guard case let .providerPrompt(prompt) = action else {
            return XCTFail("Expected provider prompt")
        }

        XCTAssertEqual(prompt, "How should I analyze drawdowns here?")
    }
}
