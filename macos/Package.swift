// swift-tools-version: 6.2
import PackageDescription

let package = Package(
    name: "MarketDataWarehouseMac",
    platforms: [
        .macOS(.v15),
    ],
    products: [
        .executable(
            name: "MarketDataWarehouseApp",
            targets: ["MarketDataWarehouseApp"]
        ),
    ],
    targets: [
        .target(
            name: "MarketDataCore"
        ),
        .target(
            name: "DuckDBCLIAdapter",
            dependencies: ["MarketDataCore"]
        ),
        .target(
            name: "OperatorPilotMetal",
            dependencies: ["MarketDataCore"],
            exclude: ["Shaders"]
        ),
        .target(
            name: "OperatorPilotKit",
            dependencies: ["MarketDataCore", "DuckDBCLIAdapter", "OperatorPilotMetal"]
        ),
        .executableTarget(
            name: "MarketDataWarehouseApp",
            dependencies: ["OperatorPilotKit"]
        ),
        .testTarget(
            name: "MarketDataCoreTests",
            dependencies: ["MarketDataCore"]
        ),
        .testTarget(
            name: "DuckDBCLIAdapterTests",
            dependencies: ["DuckDBCLIAdapter", "MarketDataCore"]
        ),
        .testTarget(
            name: "OperatorPilotMetalTests",
            dependencies: ["OperatorPilotMetal", "MarketDataCore"]
        ),
        .testTarget(
            name: "OperatorPilotKitTests",
            dependencies: ["OperatorPilotKit", "OperatorPilotMetal", "MarketDataCore", "DuckDBCLIAdapter"]
        ),
    ]
)
