# Fable.Reactive development tasks

src_path := "src"
test_path := "test"

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

# Run all tests
test:
    dotnet test

# Run tests with coverage
test-coverage:
    dotnet test --collect:"XPlat Code Coverage"
