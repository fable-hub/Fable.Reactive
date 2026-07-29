# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Fable.Reactive is a lightweight Async Reactive library for F# implementing Async Observables (ReactiveX pattern). Designed for Fable compatibility, enabling the same F# code to run both server-side and transpiled to JavaScript.

## Build Commands

Use `just` as the task runner (see `justfile` for all recipes):

```bash
just              # List available recipes
just build        # Build F# project
just build-release # Build in Release mode
just test         # Run all tests
just format       # Format with Fantomas
just pack         # Create NuGet package
just restore      # Restore dependencies and tools
just shipit       # Run EasyBuild.ShipIt for release management
```

Tools are managed via `.config/dotnet-tools.json` (Paket, Fantomas, EasyBuild.ShipIt).

## Commits

Use [Conventional Commits](https://www.conventionalcommits.org/). PR titles are enforced via CI.

Allowed types: `feat`, `fix`, `chore`, `docs`, `style`, `refactor`, `perf`, `test`, `ci`, `build`, `revert`.

## Test Framework

Uses **Scriptorium** — assertions from `Scriptorium.Nib`, the runner from `Scriptorium.Quill`
(same setup as Fable.Actor and Fable.Giraffe). Tests are in `test/`.

Quill is a plain executable, not a `dotnet test` adapter, so `just test` runs
`dotnet run --project test` and the process exit code is the result. `test/Main.fs` is the single
entry point: it hands the module `testList`s to Quill's `runTestsWith`.

Each test module exposes `let tests = testList ("Name", [ ... ])`.

Test utilities in `test/Utils.fs`:

- `TestObserver<'a>` - Captures OnNext/OnError/OnCompleted notifications. All state lives in an
  actor, so `Notifications` is only as fresh as the last `Await`/`AwaitIgnore`/`Refresh` call —
  after a bare `Async.Sleep`, call `Refresh()` before asserting.
- `fromNotification` - Creates observables from notification sequences
- `waitUntil` - Polls a predicate (no blocking primitives, so it works on every target)
- `containAll` - Nib assertion for order-insensitive containment. Nib's own `haveSameElements`
  sorts, which `Notification<'a>` cannot do: `OnError of exn` leaves the type without a
  comparison constraint.

Example test pattern:

```fsharp
open Scriptorium.Quill
open Scriptorium.Nib.Assertion
open type Scriptorium.Quill.Test

let tests =
    testList (
        "Map",
        [ testAsync (
              "map",
              async {
                  let xs = Reactive.single 42 |> Reactive.map (fun x -> x * 10)
                  let obv = TestObserver<int>()
                  let! _sub = xs.SubscribeAsync obv
                  let! latest = obv.Await()
                  assertThat latest (isEqualTo 420)
              }
          ) ]
    )
```

Tests within a `testList` run in **parallel**; use `testSequenced` if a group ever needs
serializing. The runner raises Quill's "slow test" threshold to 1000ms, since this suite sleeps
on purpose (timers, debounce, subjects).

Scriptorium is Fable-compatible, so this one suite is able to compile and run on every Fable
target, not just .NET.

## Architecture

### Core Types (`src/Types.fs`)

- `IReactiveDisposable` - Async disposable for subscriptions
- `IAsyncObserver<'T>` - Async observer with OnNextAsync/OnErrorAsync/OnCompletedAsync
- `IAsyncObservable<'T>` - Async observable with SubscribeAsync
- `Notification<'T>` - OnNext/OnError/OnCompleted discriminated union
- `AsyncStream<'TSource, 'TResult>` - Function type for operator composition

### Source Modules (`src/`)

|       Module       |                                              Purpose                                              |
| ------------------ | ------------------------------------------------------------------------------------------------- |
| Create.fs          | Observable creation: `single`, `empty`, `never`, `interval`, `timer`, `ofSeq`, `ofAsync`, `defer` |
| Transform.fs       | `map`, `flatMap`, `concatMap`, `catch`, `retry`, `switch`                                         |
| Filter.fs          | `filter`, `take`, `skip`, `choose`, `distinctUntilChanged`, `takeUntil`                           |
| Combine.fs         | `merge`, `concat`, `combineLatest`, `withLatestFrom`, `zip`                                       |
| Aggregate.fs       | `scan`, `reduce`, `groupBy`, `min`, `max`                                                         |
| Timeshift.fs       | `delay`, `debounce`, `sample`                                                                     |
| Subject.fs         | Hot/cold stream subjects for multicast                                                            |
| AsyncObservable.fs | Main API module exporting all operators via `Reactive`                                            |
| Builder.fs         | Query/computation expression builder (`reactive { }`)                                             |

### Key Patterns

- All operators are async-aware using F# `Async<'T>`
- `safeObserver` uses MailboxProcessor to serialize notifications and enforce Rx grammar
- Operators compose via the `AsyncStream` function type
- Conditional compilation (`#if FABLE_COMPILER`) for Fable compatibility

## Formatting

Fantomas with settings in `.editorconfig`:

- Max line length: 120 characters
- Max infix operator expression: 50
