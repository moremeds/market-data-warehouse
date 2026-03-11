import Foundation
import Metal

enum MetalShaderLibrary {
    static let libraryBaseName = "OperatorPilotMetalShaders"

    static func makeWorkspaceLibrary(
        device: MTLDevice,
        mainBundle: Bundle = .main
    ) throws -> MTLLibrary {
        if let url = mainBundle.url(forResource: libraryBaseName, withExtension: "metallib") {
            return try device.makeLibrary(URL: url)
        }

        let sourceURL = shaderSourceURL()
        let source = try String(contentsOf: sourceURL, encoding: .utf8)
        return try device.makeLibrary(source: source, options: nil)
    }

    static func shaderSourceURL() -> URL {
        URL(fileURLWithPath: #filePath)
            .deletingLastPathComponent()
            .appendingPathComponent("Shaders", isDirectory: true)
            .appendingPathComponent("\(libraryBaseName).metal")
    }
}
