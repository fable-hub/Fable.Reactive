# Fable.Reactive development tasks

src_path := "src"
test_path := "test"
build_path := "build"

# Development mode: compile with a local Fable checkout instead of the pinned dotnet tool.
# Usage: just dev=true test-beam
dev := "false"
fable_repo := justfile_directory() / "../Fable"
fable := if dev == "true" { "dotnet run --project " + fable_repo / "src/Fable.Cli" + " --" } else { "dotnet fable" }

# List available recipes
default:
    @just --list

# --- Build ---

# Build the project
build:
    dotnet build {{src_path}}

# Build in Release mode
build-release:
    dotnet build --configuration Release

# Format source files
format:
    dotnet fantomas {{src_path}} {{test_path}}

# Restore dependencies and tools
restore:
    dotnet tool restore
    dotnet restore

# --- Packaging ---

# Create NuGet packages with versions from changelog
pack:
    #!/usr/bin/env bash
    set -euo pipefail
    get_version() { grep -m1 '^## ' "$1" | sed 's/^## \([^ ]*\).*/\1/'; }
    VERSION=$(get_version CHANGELOG.md)
    dotnet pack {{src_path}} -c Release -o ./nupkgs -p:PackageVersion=$VERSION -p:InformationalVersion=$VERSION
    dotnet pack extra/AsyncSeq -c Release -o ./nupkgs -p:PackageVersion=$VERSION -p:InformationalVersion=$VERSION

# Pack and push all packages to NuGet (used in CI)
release: pack
    dotnet nuget push './nupkgs/*.nupkg' -s https://api.nuget.org/v3/index.json -k $NUGET_KEY

# Run EasyBuild.ShipIt for release management
shipit *args:
    dotnet shipit --pre-release rc {{args}}

# --- Tests ---

# One suite in test/, compiled to each target from the same project. Assertions come from
# Scriptorium.Nib, the runner from Scriptorium.Quill.
# decision: compiles one shared suite for every target so behavioral coverage cannot drift between harnesses
#
# FableCompile=true drops the .NET-only bits (extra/AsyncSeq + AsyncSeqTest.fs, which need
# FSharp.Control.AsyncSeq and AutoResetEvent) from the Fable builds. It reaches the design-time
# build Fable runs internally because MSBuild treats environment variables as global properties
# — Fable has no flag to forward one.
# decision: passes FableCompile through the environment because Fable cannot forward an MSBuild property itself
# invariant: FableCompile excludes every test and project reference that depends on .NET-only primitives
#
# Current pass rates: .NET 86/86, JS 82/84, Python 79/84, BEAM 14/84.
# The Fable-target failures are all pre-existing library gaps this suite is the first to reach,
# not harness problems — see the note on test-beam and `Async.Start'` in src/Core.fs.

# Run the .NET suite — the green gate
test: test-native

# Run the suite on every target (cross-target sweep; not all green yet)
test-all: test-native test-js test-python test-beam

# .NET target
test-native:
    dotnet run --project test

# JS target: compile the suite to JS and run it under Node
test-js:
    rm -rf {{build_path}}/tests-js
    FableCompile=true {{fable}} {{test_path}} --exclude Fable.Core --lang javascript --outDir {{build_path}}/tests-js
    echo '{"type":"module"}' > {{build_path}}/tests-js/package.json
    node {{build_path}}/tests-js/Main.js

# --no-project keeps uv off the repo's stale poetry pyproject.toml.

# Python target: compile the suite to Python and run the generated runner
test-python:
    rm -rf {{build_path}}/tests-py
    FableCompile=true {{fable}} {{test_path}} --exclude Fable.Core --lang python --outDir {{build_path}}/tests-py
    cd {{build_path}}/tests-py && uv run --no-project --with fable-library python main.py

# Fable pulls the Fable.Reactive sources into the same outDir and generates rebar.config, so the
# app is self-contained. Quill calls halt/1, so erl exits non-zero on failure.
#
# KNOWN FAILING (14/84): terminal notifications (OnCompleted/OnError) do not reach the observer
# on BEAM, so every Await/AwaitIgnore blocks until Quill's 5s timeout. The actor primitives
# themselves are fine — all raw-primitive Probe tests pass. Part of the in-flight BEAM port.

# BEAM target: compile the suite to Erlang, build with rebar3, run on the BEAM VM
test-beam:
    rm -rf {{build_path}}/tests-beam
    FableCompile=true {{fable}} {{test_path}} --exclude Fable.Core --lang beam --outDir {{build_path}}/tests-beam
    cd {{build_path}}/tests-beam && rebar3 compile
    cd {{build_path}}/tests-beam && erl -noshell -pa _build/default/lib/*/ebin -eval 'main:main([])'
