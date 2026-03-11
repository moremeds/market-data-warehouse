# Metal toolchain and packaging

## Toolchain check

Run the bundled checker:

```bash
scripts/check-metal-toolchain.sh
```

If the compiler is missing:

```bash
xcodebuild -downloadComponent metalToolchain
```

## Packaging pattern

Use this pattern for local macOS apps that are assembled outside a full Xcode app target:

1. Keep the canonical shader source checked into the repo as `.metal`.
2. Precompile that source into `.metallib` during app-bundle assembly.
3. Copy the `.metallib` into the final app bundle resources.
4. In code, prefer loading the bundled `.metallib`.
5. Fall back to compiling the checked-in source only for local development or tests.

## Swift package guidance

- A Swift package target can host the Metal renderer code even if the final app bundle is assembled by a script.
- If `.metal` files should not be treated as package resources, exclude them explicitly and handle compilation in build scripts.
- Keep render math separate from GPU plumbing so Swift tests can validate the visualization logic without a live Metal session.

## Verification

- Run unit tests for any pure Swift renderer math.
- Build the app bundle and confirm the `.metallib` lands in `Contents/Resources`.
- Run the repo-local smoke or UI harness after wiring the surface into the live app shell.

## Sources

- https://developer.apple.com/documentation/xcode/installing-additional-xcode-components
- https://developer.apple.com/news/site-updates/?id=03262024b
- https://developer.apple.com/documentation/metalkit/mtkview
