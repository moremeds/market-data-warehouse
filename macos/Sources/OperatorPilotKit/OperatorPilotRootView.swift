import SwiftUI
import DuckDBCLIAdapter
import MarketDataCore

public struct OperatorPilotRootView: View {
    private enum FocusTarget: Hashable {
        case composer
        case rawDuckDB
    }

    @ObservedObject private var viewModel: OperatorPilotViewModel
    @FocusState private var focusedField: FocusTarget?

    public init(viewModel: OperatorPilotViewModel) {
        self.viewModel = viewModel
    }

    public var body: some View {
        NavigationSplitView {
            sidebar
        } detail: {
            detail
        }
        .navigationSplitViewStyle(.balanced)
        .onAppear {
            viewModel.refreshProviderStatuses()
        }
        .toolbar {
            ToolbarItemGroup(placement: .primaryAction) {
                Button("Open Source") {
                    viewModel.promptForSourceImport()
                }
                .accessibilityLabel("Open Source")
                .accessibilityIdentifier("toolbar-open-source")

                Button(viewModel.isDiagnosticsDrawerPresented ? "Hide Diagnostics" : "Show Diagnostics") {
                    viewModel.isDiagnosticsDrawerPresented.toggle()
                }
                .accessibilityLabel("Toggle Diagnostics")
                .accessibilityIdentifier("toolbar-toggle-diagnostics")
            }

            ToolbarItem(placement: .principal) {
                VStack(alignment: .leading, spacing: 2) {
                    Text("Operator Pilot")
                        .font(.headline)
                    Text(toolbarSubtitle)
                        .font(.caption)
                        .foregroundStyle(.secondary)
                }
            }
        }
        .sheet(isPresented: $viewModel.isSetupFlowPresented) {
            SetupFlowView(viewModel: viewModel, isInitialLaunch: false)
                .frame(minWidth: 760, minHeight: 640)
        }
        .onChange(of: viewModel.composerFocusRequestID) { _, _ in
            focusedField = .composer
        }
    }

    private var toolbarSubtitle: String {
        if let source = viewModel.selectedSource {
            return "\(source.displayName) • \(viewModel.selectedProvider.displayName)"
        }
        return "No source selected • \(viewModel.selectedProvider.displayName)"
    }

    private var sidebar: some View {
        List {
            Section("Workspace") {
                ForEach(SidebarDestination.allCases) { destination in
                    Button {
                        viewModel.selectedDestination = destination
                    } label: {
                        SidebarNavigationRow(
                            title: destination.rawValue,
                            systemImage: icon(for: destination),
                            isSelected: viewModel.selectedDestination == destination
                        )
                    }
                    .buttonStyle(.plain)
                    .accessibilityLabel("\(destination.rawValue) Navigation")
                    .accessibilityIdentifier("sidebar-destination-\(destination.id.lowercased())")
                }
            }

            Section("Sources") {
                if viewModel.sources.isEmpty {
                    Text("No local sources yet")
                        .foregroundStyle(.secondary)
                } else {
                    ForEach(viewModel.sources) { source in
                        Button {
                            viewModel.selectSource(source)
                        } label: {
                            SidebarSourceRow(
                                source: source,
                                isSelected: viewModel.selectedSource == source
                            )
                        }
                        .buttonStyle(.plain)
                        .accessibilityLabel("Source \(source.displayName)")
                    }
                }
            }
        }
        .listStyle(.sidebar)
    }

    @ViewBuilder
    private var detail: some View {
        switch viewModel.selectedDestination ?? .assistant {
        case .assistant:
            assistantWorkspace
        case .transcripts:
            transcriptArchive
        case .setup:
            SetupSummaryView(viewModel: viewModel)
        case .settings:
            SettingsPaneView(viewModel: viewModel)
        }
    }

    private var assistantWorkspace: some View {
        ScrollViewReader { proxy in
            ScrollView {
                VStack(alignment: .leading, spacing: 18) {
                    heroCard

                    promptChips

                    ForEach(viewModel.transcript) { item in
                        TranscriptItemView(item: item)
                            .id(item.id)
                    }
                }
                .padding(24)
            }
            .background(Color(nsColor: .windowBackgroundColor))
            .safeAreaInset(edge: .bottom) {
                VStack(spacing: 0) {
                    if viewModel.isDiagnosticsDrawerPresented {
                        DiagnosticsDrawer(viewModel: viewModel)
                    }

                    composerBar
                }
                .background(.ultraThinMaterial)
            }
            .onChange(of: viewModel.transcript.count) { _, _ in
                if let lastID = viewModel.transcript.last?.id {
                    withAnimation {
                        proxy.scrollTo(lastID, anchor: .bottom)
                    }
                }
            }
        }
    }

    private var heroCard: some View {
        MetalStatusPanel(
            snapshot: viewModel.metalSnapshot,
            eyebrow: "Metal Workspace",
            title: "Chat with your workspace",
            subtitle: "Default provider: \(viewModel.selectedProvider.displayName). Open a parquet or DuckDB source for direct query help, or ask broader questions and the selected provider CLI will respond.",
            metrics: assistantMetrics
        )
    }

    private var promptChips: some View {
        ScrollView(.horizontal, showsIndicators: false) {
            HStack(spacing: 10) {
                ForEach(viewModel.promptChips) { chip in
                    Button(chip.title) {
                        Task { await viewModel.triggerPrompt(chip.prompt) }
                    }
                    .buttonStyle(.bordered)
                    .controlSize(.regular)
                    .accessibilityLabel("Prompt \(chip.title)")
                    .accessibilityIdentifier(promptChipIdentifier(chip.title))
                }
            }
        }
    }

    private var composerBar: some View {
        HStack(spacing: 12) {
            TextField("Ask about your local data, request SQL, or use `/sql ...`", text: $viewModel.composerText)
                .textFieldStyle(.roundedBorder)
                .focused($focusedField, equals: .composer)
                .accessibilityLabel("Prompt Composer")
                .accessibilityIdentifier("assistant-composer")
                .onSubmit {
                    Task { await viewModel.sendPrompt() }
                }

            Button {
                Task { await viewModel.sendPrompt() }
            } label: {
                if viewModel.isRunning {
                    ProgressView()
                        .controlSize(.small)
                } else {
                    Image(systemName: "arrow.up.circle.fill")
                }
            }
            .buttonStyle(.plain)
            .font(.title2)
            .disabled(viewModel.composerText.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty || viewModel.isRunning)
            .accessibilityLabel("Send Prompt")
            .accessibilityIdentifier("assistant-send-prompt")
        }
        .padding(.horizontal, 20)
        .padding(.vertical, 14)
    }

    private var transcriptArchive: some View {
        ScrollView {
            VStack(alignment: .leading, spacing: 16) {
                HStack {
                    Text("Transcript Archive")
                        .font(.title2.weight(.semibold))
                    Spacer()
                    Button("Clear Conversation") {
                        viewModel.clearConversation()
                    }
                }

                Text("The current session is persisted locally and restored on the next launch.")
                    .foregroundStyle(.secondary)

                MetalStatusPanel(
                    snapshot: viewModel.metalSnapshot,
                    eyebrow: "Session Playback",
                    title: "Transcript density at a glance",
                    subtitle: "The Metal surface below is redrawn on demand and shifts into timed updates only while work is actively running.",
                    metrics: transcriptMetrics
                )

                ForEach(viewModel.transcript) { item in
                    TranscriptItemView(item: item)
                }
            }
            .padding(24)
        }
    }

    private func icon(for destination: SidebarDestination) -> String {
        switch destination {
        case .assistant:
            "bubble.left.and.bubble.right"
        case .transcripts:
            "text.bubble"
        case .setup:
            "wrench.and.screwdriver"
        case .settings:
            "gearshape"
        }
    }

    private func promptChipIdentifier(_ title: String) -> String {
        let normalized = title
            .lowercased()
            .replacingOccurrences(of: " ", with: "-")
        return "prompt-chip-\(normalized)"
    }

    private var assistantMetrics: [String] {
        var metrics = [
            "\(viewModel.sources.count) source\(viewModel.sources.count == 1 ? "" : "s")",
            "\(viewModel.transcript.count) message\(viewModel.transcript.count == 1 ? "" : "s")",
        ]

        if let source = viewModel.selectedSource {
            metrics.append(source.kind == .parquet ? "Parquet attached" : "DuckDB attached")
        } else {
            metrics.append("No source selected")
        }

        metrics.append(viewModel.isRunning ? "Live GPU redraw" : "Demand-driven redraw")
        return metrics
    }

    private var transcriptMetrics: [String] {
        let commandMetric: String
        if let lastExecution = viewModel.lastExecution {
            commandMetric = lastExecution.exitCode == 0 ? "Last command succeeded" : "Last command failed"
        } else {
            commandMetric = "No command yet"
        }

        return [
            viewModel.isRunning ? "Command running" : "Idle",
            "\(viewModel.transcript.count) archived item\(viewModel.transcript.count == 1 ? "" : "s")",
            commandMetric,
        ]
    }
}

private struct SidebarNavigationRow: View {
    let title: String
    let systemImage: String
    let isSelected: Bool

    var body: some View {
        HStack(spacing: 10) {
            Image(systemName: systemImage)
                .frame(width: 16)
            Text(title)
            Spacer(minLength: 0)
        }
        .padding(.horizontal, 10)
        .padding(.vertical, 8)
        .frame(maxWidth: .infinity, alignment: .leading)
        .contentShape(RoundedRectangle(cornerRadius: 10, style: .continuous))
        .background(
            RoundedRectangle(cornerRadius: 10, style: .continuous)
                .fill(isSelected ? AnyShapeStyle(.tint.opacity(0.16)) : AnyShapeStyle(.clear))
        )
    }
}

private struct SidebarSourceRow: View {
    let source: DataSource
    let isSelected: Bool

    var body: some View {
        HStack(spacing: 10) {
            Image(systemName: source.kind == .parquet ? "shippingbox" : "cylinder.split.1x2")
                .frame(width: 16)
            Text(source.displayName)
                .lineLimit(1)
            Spacer(minLength: 0)
            if isSelected {
                Image(systemName: "checkmark.circle.fill")
                    .foregroundStyle(.tint)
            }
        }
        .padding(.horizontal, 10)
        .padding(.vertical, 8)
        .frame(maxWidth: .infinity, alignment: .leading)
        .contentShape(RoundedRectangle(cornerRadius: 10, style: .continuous))
        .background(
            RoundedRectangle(cornerRadius: 10, style: .continuous)
                .fill(isSelected ? AnyShapeStyle(.tint.opacity(0.12)) : AnyShapeStyle(.clear))
        )
    }
}

private struct SetupSummaryView: View {
    @ObservedObject var viewModel: OperatorPilotViewModel

    var body: some View {
        ScrollView {
            VStack(alignment: .leading, spacing: 16) {
                Text("Setup")
                    .font(.title2.weight(.semibold))
                Text("The first-run setup decides which provider the app uses by default and whether that provider should run through the local CLI session or a Keychain-stored API key.")
                    .foregroundStyle(.secondary)

                MetalStatusPanel(
                    snapshot: viewModel.metalSnapshot,
                    eyebrow: "Render State",
                    title: "Hybrid Metal setup preview",
                    subtitle: "The app keeps configuration controls in SwiftUI while using MetalKit surfaces for dense visual feedback and execution state.",
                    metrics: [
                        "Default: \(viewModel.selectedProvider.displayName)",
                        viewModel.isSetupFlowPresented ? "Setup sheet open" : "Setup sheet closed",
                        "\(viewModel.providerStatuses.count) providers tracked",
                    ]
                )

                GroupBox("Current Configuration") {
                    VStack(alignment: .leading, spacing: 8) {
                        Text("Default provider: \(viewModel.selectedProvider.displayName)")
                        Text("Auth mode: \(viewModel.preference(for: viewModel.selectedProvider).authMode.displayName)")
                        let model = viewModel.preference(for: viewModel.selectedProvider).customModel
                        Text(model.isEmpty ? "Model: provider default" : "Model: \(model)")
                            .foregroundStyle(.secondary)
                    }
                    .frame(maxWidth: .infinity, alignment: .leading)
                }

                GroupBox("Providers") {
                    VStack(alignment: .leading, spacing: 12) {
                        ForEach(viewModel.providerStatuses) { status in
                            VStack(alignment: .leading, spacing: 4) {
                                Text(status.name)
                                    .font(.headline)
                                Text(status.statusSummary)
                                    .foregroundStyle(status.cliInstalled ? .green : .secondary)
                                Text("CLI: \(status.cliPath ?? status.cliName)")
                                    .font(.caption)
                                    .foregroundStyle(.secondary)
                            }
                            if status.id != viewModel.providerStatuses.last?.id {
                                Divider()
                            }
                        }
                    }
                    .frame(maxWidth: .infinity, alignment: .leading)
                }

                Button("Run Setup Again") {
                    viewModel.reopenSetup()
                }
                .buttonStyle(.borderedProminent)
            }
            .padding(24)
        }
    }
}

private struct TranscriptItemView: View {
    let item: TranscriptItem

    var body: some View {
        VStack(alignment: item.role == .user ? .trailing : .leading, spacing: 8) {
            Text(item.title)
                .font(.caption.weight(.semibold))
                .foregroundStyle(.secondary)

            VStack(alignment: .leading, spacing: 10) {
                Text(item.body)
                    .frame(maxWidth: .infinity, alignment: .leading)

                switch item.kind {
                case .text:
                    EmptyView()

                case let .commandPreview(sql, sourceName):
                    VStack(alignment: .leading, spacing: 6) {
                        Text(sourceName)
                            .font(.caption)
                            .foregroundStyle(.secondary)
                        Text(sql)
                            .font(.system(.body, design: .monospaced))
                            .textSelection(.enabled)
                    }
                    .padding(12)
                    .background(
                        RoundedRectangle(cornerRadius: 12, style: .continuous)
                            .fill(.black.opacity(0.18))
                    )

                case let .rawCommandPreview(command):
                    VStack(alignment: .leading, spacing: 6) {
                        Text("duckdb")
                            .font(.caption)
                            .foregroundStyle(.secondary)
                        Text(command)
                            .font(.system(.body, design: .monospaced))
                            .textSelection(.enabled)
                    }
                    .padding(12)
                    .background(
                        RoundedRectangle(cornerRadius: 12, style: .continuous)
                            .fill(.black.opacity(0.18))
                    )

                case let .commandResult(exitCode, stdout, stderr):
                    VStack(alignment: .leading, spacing: 6) {
                        Text("Exit code: \(exitCode)")
                            .font(.caption)
                            .foregroundStyle(exitCode == 0 ? .green : .red)

                        if !stdout.isEmpty {
                            ScrollView(.horizontal) {
                                Text(stdout)
                                    .font(.system(.body, design: .monospaced))
                                    .textSelection(.enabled)
                                    .frame(maxWidth: .infinity, alignment: .leading)
                            }
                            .frame(maxHeight: 220)
                        }

                        if !stderr.isEmpty {
                            Text(stderr)
                                .font(.system(.body, design: .monospaced))
                                .foregroundStyle(.red)
                                .textSelection(.enabled)
                        }
                    }
                    .padding(12)
                    .background(
                        RoundedRectangle(cornerRadius: 12, style: .continuous)
                            .fill(.black.opacity(0.18))
                    )
                }
            }
            .padding(14)
            .frame(maxWidth: 760, alignment: .leading)
            .background(
                RoundedRectangle(cornerRadius: 18, style: .continuous)
                    .fill(backgroundStyle)
            )
        }
        .frame(maxWidth: .infinity, alignment: item.role == .user ? .trailing : .leading)
    }

    private var backgroundStyle: some ShapeStyle {
        switch item.role {
        case .assistant:
            return AnyShapeStyle(.quaternary.opacity(0.25))
        case .user:
            return AnyShapeStyle(.tint.opacity(0.18))
        case .system:
            return AnyShapeStyle(.tertiary.opacity(0.22))
        }
    }
}

private struct DiagnosticsDrawer: View {
    @ObservedObject var viewModel: OperatorPilotViewModel

    var body: some View {
        VStack(alignment: .leading, spacing: 12) {
            HStack {
                Text("Diagnostics")
                    .font(.headline)
                Spacer()
                if let selectedSource = viewModel.selectedSource {
                    Text(selectedSource.displayName)
                        .font(.caption)
                        .foregroundStyle(.secondary)
                }
            }

            VStack(alignment: .leading, spacing: 10) {
                Text("Raw DuckDB CLI")
                    .font(.caption.weight(.semibold))
                    .foregroundStyle(.secondary)

                HStack(spacing: 10) {
                    TextField("Arguments after `duckdb`", text: $viewModel.manualDuckDBArguments)
                        .textFieldStyle(.roundedBorder)
                        .accessibilityLabel("Raw DuckDB Arguments")
                        .accessibilityIdentifier("diagnostics-raw-duckdb-arguments")
                        .onSubmit {
                            Task { await viewModel.runRawDuckDBCommand() }
                        }

                    Button("Insert Source") {
                        guard let selectedSource = viewModel.selectedSource else {
                            return
                        }

                        let quotedPath = "\"\(selectedSource.path)\""
                        if viewModel.manualDuckDBArguments.isEmpty {
                            viewModel.manualDuckDBArguments = quotedPath
                        } else {
                            viewModel.manualDuckDBArguments += " \(quotedPath)"
                        }
                    }
                    .disabled(viewModel.selectedSource == nil)
                    .accessibilityLabel("Insert Source Into Raw DuckDB Arguments")
                    .accessibilityIdentifier("diagnostics-insert-source")

                    Button("Run") {
                        Task { await viewModel.runRawDuckDBCommand() }
                    }
                    .buttonStyle(.borderedProminent)
                    .disabled(
                        viewModel.manualDuckDBArguments.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty ||
                        viewModel.isRunning
                    )
                    .accessibilityLabel("Run Raw DuckDB Command")
                    .accessibilityIdentifier("diagnostics-run-raw-command")
                }

                Text("Run any DuckDB CLI arguments exactly as you would after the `duckdb` binary name, for example `--help` or `\"/path/to/db.duckdb\" -readonly -c \"SHOW TABLES;\"`.")
                    .font(.caption2)
                    .foregroundStyle(.secondary)
                    .textSelection(.enabled)
            }

            if let lastExecution = viewModel.lastExecution {
                VStack(alignment: .leading, spacing: 8) {
                    Text("Last DuckDB command")
                        .font(.caption.weight(.semibold))
                        .foregroundStyle(.secondary)
                    Text(([lastExecution.binaryPath] + lastExecution.arguments).joined(separator: " "))
                        .font(.system(.caption, design: .monospaced))
                        .textSelection(.enabled)
                    Text("Exit code: \(lastExecution.exitCode)")
                        .font(.caption)
                        .foregroundStyle(lastExecution.exitCode == 0 ? .green : .red)
                }
            } else {
                Text("No DuckDB command has run yet.")
                    .foregroundStyle(.secondary)
            }

            HStack(spacing: 18) {
                ForEach(viewModel.providerStatuses) { status in
                    VStack(alignment: .leading, spacing: 4) {
                        Text(status.name)
                            .font(.caption.weight(.semibold))
                        Text(status.statusSummary)
                            .font(.caption2)
                            .foregroundStyle(status.cliInstalled ? .green : .secondary)
                    }
                }
            }
        }
        .frame(maxWidth: .infinity, alignment: .leading)
        .padding(.horizontal, 20)
        .padding(.top, 12)
        .padding(.bottom, 10)
        .background(.bar)
    }
}
