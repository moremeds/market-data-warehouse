import MarketDataCore
import OperatorPilotMetal
import SwiftUI

struct MetalStatusPanel: View {
    let snapshot: MetalWorkspaceSnapshot
    let eyebrow: String
    let title: String
    let subtitle: String
    let metrics: [String]
    private let deviceBadges = MetalDeviceInspector.current()?.summaryBadges ?? []

    var body: some View {
        ZStack(alignment: .bottomLeading) {
            MetalWorkspaceSurface(snapshot: snapshot)

            LinearGradient(
                colors: [
                    .black.opacity(0.06),
                    .black.opacity(0.22),
                    .black.opacity(0.46),
                ],
                startPoint: .top,
                endPoint: .bottom
            )

            VStack(alignment: .leading, spacing: 12) {
                Text(eyebrow.uppercased())
                    .font(.caption.weight(.semibold))
                    .foregroundStyle(.secondary)
                    .tracking(1.1)

                Text(title)
                    .font(.title2.weight(.semibold))

                Text(subtitle)
                    .foregroundStyle(.secondary)
                    .fixedSize(horizontal: false, vertical: true)

                if !metrics.isEmpty {
                    HStack(spacing: 8) {
                        ForEach(Array(metrics.enumerated()), id: \.offset) { _, metric in
                            Text(metric)
                                .font(.caption.weight(.medium))
                                .padding(.horizontal, 10)
                                .padding(.vertical, 6)
                                .background(.thinMaterial)
                                .clipShape(Capsule())
                        }
                    }
                }

                if !deviceBadges.isEmpty {
                    ScrollView(.horizontal, showsIndicators: false) {
                        HStack(spacing: 8) {
                            ForEach(Array(deviceBadges.enumerated()), id: \.offset) { _, badge in
                                Text(badge)
                                    .font(.caption2.weight(.medium))
                                    .padding(.horizontal, 8)
                                    .padding(.vertical, 5)
                                    .background(.black.opacity(0.18))
                                    .clipShape(Capsule())
                            }
                        }
                    }
                }
            }
            .padding(18)
        }
        .frame(maxWidth: .infinity, minHeight: 220, maxHeight: 220, alignment: .leading)
        .clipShape(RoundedRectangle(cornerRadius: 18, style: .continuous))
        .overlay(
            RoundedRectangle(cornerRadius: 18, style: .continuous)
                .strokeBorder(.white.opacity(0.08), lineWidth: 1)
        )
    }
}
