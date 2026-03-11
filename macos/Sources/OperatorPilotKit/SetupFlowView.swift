import SwiftUI
import MarketDataCore

public struct SetupFlowView: View {
    @ObservedObject private var viewModel: OperatorPilotViewModel
    private let isInitialLaunch: Bool

    @State private var selectedProvider: ProviderKind
    @State private var authMode: ProviderAuthMode
    @State private var customModel: String
    @State private var apiKey: String = ""

    public init(viewModel: OperatorPilotViewModel, isInitialLaunch: Bool) {
        self.viewModel = viewModel
        self.isInitialLaunch = isInitialLaunch

        let provider = viewModel.selectedProvider
        let preference = viewModel.preference(for: provider)
        _selectedProvider = State(initialValue: provider)
        _authMode = State(initialValue: preference.authMode)
        _customModel = State(initialValue: preference.customModel)
    }

    public var body: some View {
        ScrollView {
            VStack(alignment: .leading, spacing: 24) {
                VStack(alignment: .leading, spacing: 10) {
                    Text(isInitialLaunch ? "Welcome to Market Data Warehouse" : "Setup")
                        .font(.largeTitle.weight(.semibold))
                    Text("Choose a default provider, decide whether to use the locally logged-in CLI or an API key, and then start exploring local parquet and DuckDB data.")
                        .foregroundStyle(.secondary)
                }

                VStack(alignment: .leading, spacing: 16) {
                    Text("Default provider")
                        .font(.headline)
                    Picker("Provider", selection: $selectedProvider) {
                        ForEach(ProviderKind.allCases) { provider in
                            Text(provider.displayName).tag(provider)
                        }
                    }
                    .pickerStyle(.segmented)
                    .accessibilityLabel("Default Provider Picker")
                    .accessibilityIdentifier("setup-default-provider-picker")

                    if let status = viewModel.status(for: selectedProvider) {
                        ProviderStatusSummary(status: status)
                    }
                }

                VStack(alignment: .leading, spacing: 16) {
                    Text("Authentication")
                        .font(.headline)
                    Picker("Auth mode", selection: $authMode) {
                        ForEach(ProviderAuthMode.allCases) { mode in
                            Text(mode.displayName).tag(mode)
                        }
                    }
                    .pickerStyle(.segmented)
                    .accessibilityLabel("Authentication Mode Picker")
                    .accessibilityIdentifier("setup-auth-mode-picker")

                    if authMode == .apiKey {
                        SecureField("Paste \(selectedProvider.preferredAPIKeyEnvironmentName)", text: $apiKey)
                            .textFieldStyle(.roundedBorder)
                            .accessibilityLabel("Provider API Key")
                            .accessibilityIdentifier("setup-provider-api-key")
                        Text("The key is stored in the macOS Keychain. Existing stored keys stay hidden.")
                            .font(.caption)
                            .foregroundStyle(.secondary)
                    } else {
                        Text("The app will call the locally installed `\(selectedProvider.cliCommand)` CLI and rely on its existing login/session state.")
                            .font(.caption)
                            .foregroundStyle(.secondary)
                    }
                }

                VStack(alignment: .leading, spacing: 16) {
                    Text("Model override")
                        .font(.headline)
                    TextField("Optional model name", text: $customModel)
                        .textFieldStyle(.roundedBorder)
                        .accessibilityLabel("Model Override")
                        .accessibilityIdentifier("setup-model-override")
                    Text("Leave blank to let the local CLI choose its default model.")
                        .font(.caption)
                        .foregroundStyle(.secondary)
                }

                HStack {
                    Button("Refresh Status") {
                        viewModel.refreshProviderStatuses()
                    }
                    .accessibilityLabel("Refresh Provider Status")
                    .accessibilityIdentifier("setup-refresh-provider-status")

                    if !isInitialLaunch {
                        Button("Cancel") {
                            viewModel.isSetupFlowPresented = false
                        }
                        .keyboardShortcut(.cancelAction)
                        .accessibilityLabel("Cancel Setup")
                        .accessibilityIdentifier("setup-cancel")
                    }

                    Spacer()

                    Button(isInitialLaunch ? "Finish Setup" : "Save Changes") {
                        viewModel.completeSetup(
                            defaultProvider: selectedProvider,
                            authMode: authMode,
                            model: customModel,
                            apiKey: apiKey
                        )
                    }
                    .buttonStyle(.borderedProminent)
                    .keyboardShortcut(.defaultAction)
                    .accessibilityLabel(isInitialLaunch ? "Finish Setup" : "Save Setup Changes")
                    .accessibilityIdentifier(isInitialLaunch ? "setup-finish" : "setup-save")
                }
            }
            .padding(32)
            .frame(maxWidth: 760, alignment: .leading)
        }
        .onAppear {
            viewModel.refreshProviderStatuses()
        }
        .onChange(of: selectedProvider) { _, provider in
            let preference = viewModel.preference(for: provider)
            authMode = preference.authMode
            customModel = preference.customModel
            apiKey = ""
        }
    }
}

public struct SettingsPaneView: View {
    @ObservedObject var viewModel: OperatorPilotViewModel

    public init(viewModel: OperatorPilotViewModel) {
        self.viewModel = viewModel
    }

    public var body: some View {
        ScrollView {
            VStack(alignment: .leading, spacing: 20) {
                Text("Settings")
                    .font(.largeTitle.weight(.semibold))

                Picker("Default provider", selection: Binding(
                    get: { viewModel.selectedProvider },
                    set: { viewModel.updateDefaultProvider($0) }
                )) {
                    ForEach(ProviderKind.allCases) { provider in
                        Text(provider.displayName).tag(provider)
                    }
                }
                .pickerStyle(.segmented)
                .accessibilityLabel("Settings Default Provider Picker")
                .accessibilityIdentifier("settings-default-provider-picker")

                ForEach(ProviderKind.allCases) { provider in
                    ProviderSettingsCard(viewModel: viewModel, provider: provider)
                }

                HStack {
                    Button("Refresh Provider Status") {
                        viewModel.refreshProviderStatuses()
                    }
                    .accessibilityLabel("Refresh Provider Status")
                    .accessibilityIdentifier("settings-refresh-provider-status")

                    Button("Run Setup Again") {
                        viewModel.reopenSetup()
                    }
                    .accessibilityLabel("Run Setup Again")
                    .accessibilityIdentifier("settings-run-setup-again")

                    Button("Clear Conversation") {
                        viewModel.clearConversation()
                    }
                    .accessibilityLabel("Clear Conversation")
                    .accessibilityIdentifier("settings-clear-conversation")
                }
            }
            .padding(24)
        }
        .onAppear {
            viewModel.refreshProviderStatuses()
        }
    }
}

private struct ProviderSettingsCard: View {
    @ObservedObject var viewModel: OperatorPilotViewModel
    let provider: ProviderKind
    @State private var apiKeyDraft: String = ""

    private var authModeBinding: Binding<ProviderAuthMode> {
        Binding(
            get: { viewModel.preference(for: provider).authMode },
            set: { viewModel.updateAuthMode($0, for: provider) }
        )
    }

    private var modelBinding: Binding<String> {
        Binding(
            get: { viewModel.preference(for: provider).customModel },
            set: { viewModel.updateCustomModel($0, for: provider) }
        )
    }

    var body: some View {
        VStack(alignment: .leading, spacing: 14) {
            if let status = viewModel.status(for: provider) {
                ProviderStatusSummary(status: status)
            }

            Picker("Auth mode", selection: authModeBinding) {
                ForEach(ProviderAuthMode.allCases) { mode in
                    Text(mode.displayName).tag(mode)
                }
            }
            .pickerStyle(.segmented)

            TextField("Optional model override", text: modelBinding)
                .textFieldStyle(.roundedBorder)

            HStack {
                SecureField(provider.preferredAPIKeyEnvironmentName, text: $apiKeyDraft)
                    .textFieldStyle(.roundedBorder)

                Button("Save Key") {
                    viewModel.saveAPIKey(apiKeyDraft, for: provider)
                    apiKeyDraft = ""
                }
                .disabled(apiKeyDraft.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty)

                Button("Remove Key") {
                    viewModel.removeAPIKey(for: provider)
                }
                .disabled(!viewModel.hasStoredAPIKey(for: provider))
            }

            Text(viewModel.hasStoredAPIKey(for: provider) ? "A Keychain-stored API key is available." : "No Keychain-stored API key for this provider.")
                .font(.caption)
                .foregroundStyle(.secondary)
        }
        .padding(16)
        .background(.thinMaterial)
        .clipShape(RoundedRectangle(cornerRadius: 16, style: .continuous))
    }
}

private struct ProviderStatusSummary: View {
    let status: ProviderStatus

    var body: some View {
        VStack(alignment: .leading, spacing: 6) {
            HStack {
                Text(status.name)
                    .font(.headline)
                Spacer()
                Text(status.statusSummary)
                    .foregroundStyle(status.cliInstalled ? .green : .secondary)
            }
            Text("CLI: \(status.cliPath ?? status.cliName)")
                .font(.caption)
                .foregroundStyle(.secondary)
            Text("\(status.apiKeyName): \(status.apiKeyPresent || status.environmentKeyPresent ? "available" : "missing")")
                .font(.caption)
                .foregroundStyle(.secondary)
            if !status.cliInstalled {
                Text("Install `\(status.cliName)` to use this provider in the current app build.")
                    .font(.caption)
                    .foregroundStyle(.secondary)
            }
        }
        .frame(maxWidth: .infinity, alignment: .leading)
    }
}
