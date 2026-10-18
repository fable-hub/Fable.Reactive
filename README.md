# Fable.Reactive

![Build and Test](https://github.com/dbrattli/Fable.Reactive/workflows/Build%20and%20Test/badge.svg)
[![Nuget](https://img.shields.io/nuget/vpre/Fable.Reactive)](https://www.nuget.org/packages/Fable.Reactive/)

> Fable.Reactive is a lightweight Async Reactive library for F#.

Fable.Reactive is a library for asynchronous reactive
programming, and is the implementation of Async Observables
([ReactiveX](http://reactivex.io/)) for F#. Fable.Reactive makes
it easy to compose and combine streams of asynchronous event-based data
such as timers, mouse-moves, keyboard input, web requests and enables
you to do operations such as:

- Filtering
- Transforming
- Aggregating
- Combining
- Time-shifting

Fable.Reactive is cross-platform via [Fable](http://fable.io/) and is
known to work on .NET, JavaScript, Python and BEAM. The same F# code may
be used both client and server side for full stack software development.

This package was previously named (or known as) FSharp.Control.AsyncRx.

## Upgrading from FSharp.Control.AsyncRx

The namespace has changed from `FSharp.Control` to `Fable.Reactive`. Update
your open statements:

```diff
- open FSharp.Control
+ open Fable.Reactive
```

All sub-namespaces and modules have been renamed accordingly:

| Old | New |
|-----|-----|
| `FSharp.Control` | `Fable.Reactive` |
| `FSharp.Control.Core` | `Fable.Reactive.Core` |
| `AsyncRx` module | `Reactive` module |
| `IAsyncRxDisposable` | `IReactiveDisposable` |

## Documentation

- [Getting started](docs/getting-started.md): subscribe, handle notifications, and dispose
- [Creating streams](docs/creating.md): values, workflows, and subjects
- [Filtering](docs/filtering.md): select, limit, and stop values
- [Transforming](docs/transforming.md): map values and switch or flatten streams
- [Combining](docs/combining.md): concatenate, merge, and pair streams
- [Aggregating](docs/aggregating.md): running state, final results, and groups
- [Error handling](docs/error-handling.md): failures, fallback streams, and retries
- [Time shifting](docs/time-shifting.md): timers, delays, debounce, and sampling
- [Reactive builder](docs/builder.md): write streams with `reactive { ... }`
- [Actor interoperability](docs/actor-interop.md): subscribe actors and build actor-backed streams
- [Interoperability with .NET Reactive Extensions](docs/INTEROPERABILITY.md): adapters and their execution boundaries

## Install

When compiling for JavaScript, Python, or BEAM, use Fable 5.22.0 or newer.
This compiler requirement does not apply to .NET-only applications.

```cmd
dotnet paket add Fable.Reactive --project <project>
```
