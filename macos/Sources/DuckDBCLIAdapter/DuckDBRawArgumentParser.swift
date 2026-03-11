import Foundation

public enum DuckDBRawArgumentParser {
    public enum Error: Swift.Error, LocalizedError, Equatable {
        case unterminatedQuote
        case danglingEscape

        public var errorDescription: String? {
            switch self {
            case .unterminatedQuote:
                "The raw DuckDB command has an unterminated quote."
            case .danglingEscape:
                "The raw DuckDB command ends with a dangling escape character."
            }
        }
    }

    public static func parse(_ input: String) throws -> [String] {
        var arguments: [String] = []
        var current = ""
        var activeQuote: Character?
        var isEscaping = false

        for character in input {
            if isEscaping {
                current.append(character)
                isEscaping = false
                continue
            }

            if let quote = activeQuote {
                if character == quote {
                    activeQuote = nil
                } else if character == "\\" && quote != "'" {
                    isEscaping = true
                } else {
                    current.append(character)
                }
                continue
            }

            switch character {
            case "'", "\"":
                activeQuote = character
            case "\\":
                isEscaping = true
            case " ", "\t", "\n":
                if !current.isEmpty {
                    arguments.append(current)
                    current = ""
                }
            default:
                current.append(character)
            }
        }

        if isEscaping {
            throw Error.danglingEscape
        }

        if activeQuote != nil {
            throw Error.unterminatedQuote
        }

        if !current.isEmpty {
            arguments.append(current)
        }

        return arguments
    }
}
