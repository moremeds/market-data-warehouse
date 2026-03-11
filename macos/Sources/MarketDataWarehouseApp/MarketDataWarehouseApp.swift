import SwiftUI
import OperatorPilotKit

@main
struct MarketDataWarehouseApp: App {
    @StateObject private var viewModel = OperatorPilotViewModel()

    var body: some Scene {
        WindowGroup("Market Data Warehouse") {
            Group {
                if viewModel.requiresInitialSetup {
                    SetupFlowView(viewModel: viewModel, isInitialLaunch: true)
                } else {
                    OperatorPilotRootView(viewModel: viewModel)
                }
            }
                .frame(minWidth: 1100, minHeight: 760)
        }
        .defaultSize(width: 1280, height: 840)
        .commands {
            CommandGroup(after: .newItem) {
                Button("Open Source…") {
                    viewModel.promptForSourceImport()
                }
                .keyboardShortcut("o", modifiers: [.command])

                Button("Run Setup Again") {
                    viewModel.reopenSetup()
                }
                .keyboardShortcut("r", modifiers: [.command, .shift])

                Button("Toggle Diagnostics") {
                    viewModel.isDiagnosticsDrawerPresented.toggle()
                }
                .keyboardShortcut("d", modifiers: [.command, .shift])

                Button("Focus Composer") {
                    viewModel.requestComposerFocus()
                }
                .keyboardShortcut("l", modifiers: [.command])
            }

            CommandMenu("Navigate") {
                Button("Assistant") {
                    viewModel.selectedDestination = .assistant
                }
                .keyboardShortcut("1", modifiers: [.command])

                Button("Transcripts") {
                    viewModel.selectedDestination = .transcripts
                }
                .keyboardShortcut("2", modifiers: [.command])

                Button("Setup") {
                    viewModel.selectedDestination = .setup
                }
                .keyboardShortcut("3", modifiers: [.command])

                Button("Settings") {
                    viewModel.selectedDestination = .settings
                }
                .keyboardShortcut("4", modifiers: [.command])
            }
        }

        Settings {
            SettingsPaneView(viewModel: viewModel)
                .frame(minWidth: 760, minHeight: 640)
        }
    }
}
