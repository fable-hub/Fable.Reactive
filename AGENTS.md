# Repository Guidelines

## Project Structure & Module Organization

`src/` contains the `Fable.Reactive` library. Core contracts live in `Types.fs`; operators are grouped by purpose in files such as `Create.fs`, `Transform.fs`, `Combine.fs`, and `Filter.fs`; `AsyncObservable.fs` exposes the public `Reactive` API. `test/` contains the shared F# test suite and its executable entry point, `Main.fs`. The optional AsyncSeq integration is under `extra/AsyncSeq/`. Package metadata and dependency locks live at the repository root, while `logo/` holds branding assets.

F# files compile in the order listed in each `.fsproj`. When adding a file, place its `<Compile Include="..." />` entry after every dependency and before its consumers.

## Agent Decision Comments

This repository uses Agent Decision Comments (ADCs). Read
`AGENT_DECISION_COMMENTS.md` and all active ADCs in the affected scope before
modifying code. Preserve them or update them explicitly. Add concise ADCs for
non-obvious decisions, invariants, assumptions, and tradeoffs introduced by a
change; do not annotate mechanics already evident from the code. A change that
introduces a non-obvious engineering constraint is incomplete until its ADCs
match the implementation. The local convention vendors the unpublished 0.1.1
specification from <https://github.com/dbrattli/adc/blob/main/README.md>; review
upstream changes explicitly before refreshing it.

## Build, Test, and Development Commands

Use the `justfile` as the task interface:

- `just restore` restores pinned .NET tools and project dependencies.
- `just build` builds the main library; `just build-release` builds the solution in Release mode.
- `just test` runs the .NET suite, which is the required green gate.
- `just test-js`, `just test-python`, and `just test-beam` transpile and run the same suite on other targets. Some cross-target failures are currently documented in `CLAUDE.md`.
- `just test-all` performs the full cross-target sweep.
- `just format` formats `src/` and `test/` with Fantomas.
- `just pack` creates both NuGet packages in `nupkgs/`.

## Coding Style & Naming Conventions

Follow Fantomas and `.editorconfig`: four-space indentation, final newlines, and a 120-character maximum line length. Use PascalCase for types, modules, and union cases; use camelCase for values, functions, arguments, and test names. Keep operators async-aware with `Async<'T>` and preserve Fable portability; isolate target-specific behavior with existing conditional-compilation patterns.

## Testing Guidelines

Tests use Scriptorium Quill with Nib assertions, not `dotnet test`. Add focused files named `<Feature>Test.fs`, expose `let tests = testList (...)`, register the file in `Tests.fsproj`, and include its list in `Main.fs`. Prefer `TestObserver.WaitUntil` over fixed sleeps for asynchronous positive assertions. There is no enforced coverage threshold; cover success, completion, error, and disposal behavior where relevant.

## Commit & Pull Request Guidelines

Use Conventional Commits, matching history such as `fix(beam): trap exits in the supervisor monitor`. Allowed types include `feat`, `fix`, `test`, `docs`, `refactor`, `chore`, `ci`, and `build`. PR titles are checked in CI. Keep PRs focused, explain behavioral changes and target impact, link relevant issues, and report the commands run. Ensure the Release build, .NET suite, and Fable compile checks pass before review.
