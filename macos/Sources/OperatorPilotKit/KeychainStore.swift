import Foundation
import MarketDataCore
import Security

public protocol ProviderSecretStoring: Sendable {
    func apiKey(for provider: ProviderKind) -> String?
    func saveAPIKey(_ key: String, for provider: ProviderKind) throws
    func removeAPIKey(for provider: ProviderKind) throws
}

public enum ProviderSecretStoreError: Error, LocalizedError, Equatable {
    case keychainFailure(OSStatus)

    public var errorDescription: String? {
        switch self {
        case let .keychainFailure(status):
            return "Keychain operation failed with status \(status)."
        }
    }
}

public final class KeychainStore: ProviderSecretStoring, @unchecked Sendable {
    private let service = "local.market-data-warehouse.macos"

    public init() {}

    public func apiKey(for provider: ProviderKind) -> String? {
        let query: [String: Any] = [
            kSecClass as String: kSecClassGenericPassword,
            kSecAttrService as String: service,
            kSecAttrAccount as String: accountName(for: provider),
            kSecReturnData as String: true,
            kSecMatchLimit as String: kSecMatchLimitOne,
        ]

        var result: CFTypeRef?
        let status = SecItemCopyMatching(query as CFDictionary, &result)
        guard status == errSecSuccess, let data = result as? Data else {
            return nil
        }

        return String(data: data, encoding: .utf8)
    }

    public func saveAPIKey(_ key: String, for provider: ProviderKind) throws {
        try removeAPIKeyIfPresent(for: provider)

        let query: [String: Any] = [
            kSecClass as String: kSecClassGenericPassword,
            kSecAttrService as String: service,
            kSecAttrAccount as String: accountName(for: provider),
            kSecValueData as String: Data(key.utf8),
            kSecAttrAccessible as String: kSecAttrAccessibleWhenUnlockedThisDeviceOnly,
        ]

        let status = SecItemAdd(query as CFDictionary, nil)
        guard status == errSecSuccess else {
            throw ProviderSecretStoreError.keychainFailure(status)
        }
    }

    public func removeAPIKey(for provider: ProviderKind) throws {
        let query: [String: Any] = [
            kSecClass as String: kSecClassGenericPassword,
            kSecAttrService as String: service,
            kSecAttrAccount as String: accountName(for: provider),
        ]

        let status = SecItemDelete(query as CFDictionary)
        guard status == errSecSuccess || status == errSecItemNotFound else {
            throw ProviderSecretStoreError.keychainFailure(status)
        }
    }

    private func removeAPIKeyIfPresent(for provider: ProviderKind) throws {
        try removeAPIKey(for: provider)
    }

    private func accountName(for provider: ProviderKind) -> String {
        "provider.apiKey.\(provider.rawValue)"
    }
}
