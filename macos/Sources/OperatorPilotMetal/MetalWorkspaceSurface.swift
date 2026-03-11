import AppKit
import MarketDataCore
import Metal
import MetalKit
import SwiftUI
import simd

public struct MetalWorkspaceSurface: View {
    private let snapshot: MetalWorkspaceSnapshot

    public init(snapshot: MetalWorkspaceSnapshot) {
        self.snapshot = snapshot
    }

    public var body: some View {
        Group {
            if MetalWorkspaceRenderer.isSupported {
                MetalWorkspaceSurfaceRepresentable(snapshot: snapshot)
            } else {
                fallbackSurface
            }
        }
    }

    private var fallbackSurface: some View {
        LinearGradient(
            colors: [
                Color(red: 0.08, green: 0.10, blue: 0.12),
                Color(red: 0.12, green: 0.16, blue: 0.20),
                Color(red: 0.08, green: 0.12, blue: 0.18),
            ],
            startPoint: .topLeading,
            endPoint: .bottomTrailing
        )
    }
}

@MainActor
private struct MetalWorkspaceSurfaceRepresentable: NSViewRepresentable {
    let snapshot: MetalWorkspaceSnapshot

    func makeCoordinator() -> Coordinator {
        Coordinator()
    }

    func makeNSView(context: Context) -> MTKView {
        let view = context.coordinator.makeView()
        context.coordinator.apply(snapshot: snapshot, to: view)
        return view
    }

    func updateNSView(_ nsView: MTKView, context: Context) {
        context.coordinator.apply(snapshot: snapshot, to: nsView)
    }

    @MainActor
    final class Coordinator {
        private let renderer = MetalWorkspaceRenderer.isSupported ? MetalWorkspaceRenderer() : nil

        func makeView() -> MTKView {
            guard let renderer else {
                return MTKView()
            }
            return renderer.makeView()
        }

        func apply(snapshot: MetalWorkspaceSnapshot, to view: MTKView) {
            renderer?.update(snapshot: snapshot, in: view)
        }
    }
}

private struct MetalWorkspaceUniforms {
    var resolution: SIMD2<Float>
    var time: Float
    var destination: Float
    var provider: Float
    var sourceKind: Float
    var transcriptDensity: Float
    var sourceDensity: Float
    var readiness: Float
    var commandState: Float
    var isRunning: Float
    var barCount: UInt32
    var commandDuration: Float
    var commandOutputScale: Float
}

@MainActor
private final class MetalWorkspaceRenderer: NSObject, MTKViewDelegate {
    static let isSupported = MTLCreateSystemDefaultDevice() != nil

    private let device: MTLDevice
    private let commandQueue: MTLCommandQueue
    private let pipelineState: MTLRenderPipelineState
    private var snapshot = MetalWorkspaceSnapshot(
        destination: .assistant,
        provider: .claude,
        sourceKind: nil,
        sourceCount: 0,
        transcriptCount: 0,
        providerReadiness: 0,
        executionState: .idle,
        commandDurationMilliseconds: 0,
        commandOutputBytes: 0,
        isRunning: false
    )
    private var visualization = MetalWorkspaceVisualization(
        snapshot: MetalWorkspaceSnapshot(
            destination: .assistant,
            provider: .claude,
            sourceKind: nil,
            sourceCount: 0,
            transcriptCount: 0,
            providerReadiness: 0,
            executionState: .idle,
            commandDurationMilliseconds: 0,
            commandOutputBytes: 0,
            isRunning: false
        )
    )
    private var startedAt = CACurrentMediaTime()

    override init() {
        guard let device = MTLCreateSystemDefaultDevice(),
              let commandQueue = device.makeCommandQueue() else {
            fatalError("MetalWorkspaceRenderer requires a default Metal device.")
        }

        self.device = device
        self.commandQueue = commandQueue
        self.pipelineState = try! Self.makePipelineState(device: device)
        super.init()
    }

    func makeView() -> MTKView {
        let view = MTKView(frame: .zero, device: device)
        view.delegate = self
        view.enableSetNeedsDisplay = false
        view.isPaused = true
        view.preferredFramesPerSecond = 30
        view.framebufferOnly = true
        view.clearColor = MTLClearColor(red: 0.06, green: 0.08, blue: 0.10, alpha: 1.0)
        view.colorPixelFormat = .bgra8Unorm
        view.depthStencilPixelFormat = .invalid
        view.sampleCount = 1
        return view
    }

    func update(snapshot: MetalWorkspaceSnapshot, in view: MTKView) {
        self.snapshot = snapshot
        self.visualization = MetalWorkspaceVisualization(snapshot: snapshot)

        if snapshot.isRunning {
            if view.isPaused {
                startedAt = CACurrentMediaTime()
            }
            view.isPaused = false
        } else {
            view.isPaused = true
            view.draw()
        }
    }

    func draw(in view: MTKView) {
        guard let descriptor = view.currentRenderPassDescriptor,
              let drawable = view.currentDrawable,
              let commandBuffer = commandQueue.makeCommandBuffer(),
              let encoder = commandBuffer.makeRenderCommandEncoder(descriptor: descriptor) else {
            return
        }

        var uniforms = MetalWorkspaceUniforms(
            resolution: SIMD2<Float>(
                Float(max(view.drawableSize.width, 1)),
                Float(max(view.drawableSize.height, 1))
            ),
            time: snapshot.isRunning ? Float(CACurrentMediaTime() - startedAt) : 0,
            destination: destinationValue(snapshot.destination),
            provider: providerValue(snapshot.provider),
            sourceKind: sourceKindValue(snapshot.sourceKind),
            transcriptDensity: visualization.transcriptDensity,
            sourceDensity: visualization.sourceDensity,
            readiness: visualization.readinessFraction,
            commandState: visualization.commandStateWeight,
            isRunning: visualization.runIntensity,
            barCount: UInt32(visualization.signalBars.count),
            commandDuration: visualization.commandDurationMilliseconds,
            commandOutputScale: visualization.commandOutputScale
        )

        encoder.setRenderPipelineState(pipelineState)
        encoder.setFragmentBytes(&uniforms, length: MemoryLayout<MetalWorkspaceUniforms>.stride, index: 0)
        visualization.signalBars.withUnsafeBytes { buffer in
            if let baseAddress = buffer.baseAddress {
                encoder.setFragmentBytes(
                    baseAddress,
                    length: buffer.count,
                    index: 1
                )
            }
        }
        encoder.drawPrimitives(type: .triangle, vertexStart: 0, vertexCount: 3)
        encoder.endEncoding()

        commandBuffer.present(drawable)
        commandBuffer.commit()
    }

    func mtkView(_ view: MTKView, drawableSizeWillChange size: CGSize) {
        if !snapshot.isRunning {
            view.draw()
        }
    }

    private func destinationValue(_ destination: MetalWorkspaceDestination) -> Float {
        switch destination {
        case .assistant:
            0
        case .transcripts:
            1
        case .setup:
            2
        case .settings:
            3
        }
    }

    private func providerValue(_ provider: ProviderKind) -> Float {
        switch provider {
        case .claude:
            0
        case .openAI:
            1
        case .gemini:
            2
        }
    }

    private func sourceKindValue(_ sourceKind: DataSourceKind?) -> Float {
        switch sourceKind {
        case .parquet:
            0
        case .duckdb:
            1
        case nil:
            -1
        }
    }

    private static func makePipelineState(device: MTLDevice) throws -> MTLRenderPipelineState {
        let library = try MetalShaderLibrary.makeWorkspaceLibrary(device: device)
        let descriptor = MTLRenderPipelineDescriptor()
        descriptor.vertexFunction = library.makeFunction(name: "workspaceVertex")
        descriptor.fragmentFunction = library.makeFunction(name: "workspaceFragment")
        descriptor.colorAttachments[0].pixelFormat = .bgra8Unorm

        return try device.makeRenderPipelineState(descriptor: descriptor)
    }
}
