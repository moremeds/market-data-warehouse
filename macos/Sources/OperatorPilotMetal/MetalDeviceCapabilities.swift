import Metal

public struct MetalDeviceCapabilities: Equatable, Sendable {
    public let name: String
    public let isLowPower: Bool
    public let hasUnifiedMemory: Bool
    public let supportsDynamicLibraries: Bool
    public let recommendedMaxWorkingSetSizeMB: Int?

    public var summaryBadges: [String] {
        var badges = [name]
        badges.append(hasUnifiedMemory ? "Unified memory" : "Discrete memory")
        badges.append(isLowPower ? "Low power" : "High throughput")
        if supportsDynamicLibraries {
            badges.append("Dynamic libraries")
        }
        if let recommendedMaxWorkingSetSizeMB {
            badges.append("\(recommendedMaxWorkingSetSizeMB) MB working set")
        }
        return badges
    }
}

public enum MetalDeviceInspector {
    public static func current(
        makeDefaultDevice: () -> MTLDevice? = MTLCreateSystemDefaultDevice
    ) -> MetalDeviceCapabilities? {
        guard let device = makeDefaultDevice() else {
            return nil
        }

        let workingSetBytes = Int64(device.recommendedMaxWorkingSetSize)
        let workingSetMB = workingSetBytes > 0 ? Int(workingSetBytes / 1_048_576) : nil

        return MetalDeviceCapabilities(
            name: device.name,
            isLowPower: device.isLowPower,
            hasUnifiedMemory: device.hasUnifiedMemory,
            supportsDynamicLibraries: device.supportsDynamicLibraries,
            recommendedMaxWorkingSetSizeMB: workingSetMB
        )
    }
}
