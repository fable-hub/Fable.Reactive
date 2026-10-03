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

Documentation is currently being updated.

See [Interoperability with .NET Reactive Extensions](docs/INTEROPERABILITY.md) for existing
observable, observer, and subscription adapters and their asynchronous execution boundaries.

## Install

```cmd
dotnet paket add Fable.Reactive --project <project>
```
