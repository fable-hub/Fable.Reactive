# Interoperability with .NET Reactive Extensions

[FSharp.Control.Reactive](https://github.com/fsprojects/FSharp.Control.Reactive) provides F# wrappers
for .NET Reactive Extensions (`System.Reactive`). Its streams use `System.IObservable<'T>` and
`System.IObserver<'T>`, whose notification methods return `unit`.

Fable.Reactive uses `IAsyncObservable<'T>` and `IAsyncObserver<'T>`, with notification methods
returning `Async<unit>`. It implements its operators in F# for use with Fable. The shared
ReactiveX vocabulary makes the libraries related, but their types and execution contracts differ.

## Consume a .NET observable

Any producer implementing `System.IObservable<'T>`, including a stream composed with
FSharp.Control.Reactive or System.Reactive, can enter a Fable.Reactive pipeline through the
existing `ToAsyncObservable()` extension:

```fsharp
open System
open Fable.Reactive

let subscribeFromRx (source: IObservable<int>) (observer: IAsyncObserver<int>) =
    source.ToAsyncObservable()
    |> Reactive.map (fun value -> value + 1)
    |> Reactive.subscribeAsync observer
```

The function returns `Async<IReactiveDisposable>`. Run that workflow to subscribe, and await
`DisposeAsync()` on the resulting disposable to unsubscribe from the original .NET source.

The conversion uses `IAsyncObserver.ToObserver()`, which starts each asynchronous callback as
fire-and-forget work. A synchronous producer cannot await that work. This adapter therefore
provides no asynchronous backpressure or acknowledgement of downstream processing, and independently
started callbacks are not guaranteed to finish in source order. Use a native asynchronous producer
when its protocol needs to await observer work.

## Stream, observer, and disposal adapters

| Conversion | Existing API | Behavior |
| ---------- | ------------ | -------- |
| .NET stream to async stream | `source.ToAsyncObservable()` | Uses a synchronous observer adapter. |
| Async stream to .NET stream | `Reactive.toObservable source` | Starts subscribing asynchronously. |
| .NET observer to async observer | `observer.ToAsyncObserver()` | Runs the synchronous callback in a workflow. |
| Async observer to .NET observer | `observer.ToObserver()` | Starts callbacks without awaiting them. |
| .NET disposable to async disposable | `subscription.ToAsyncDisposable()` | Runs `Dispose()` in a workflow. |
| Async disposable to .NET disposable | `subscription.ToDisposable()` | Starts disposal without awaiting it. |

`Reactive.toObservable` has a subscription-lifetime limitation in its current implementation:
it returns a disposable before asynchronous subscription setup finishes, and stores the latest
subscription in state shared by subscribers. The returned handle can therefore refer to an empty
or previous subscription. Use `Reactive.subscribeAsync` and await `DisposeAsync()` when reliable
subscription ownership is required.

## Fable and integration scope

The .NET interface bridge does not make a dependency on System.Reactive portable to Fable targets.
A transpiled application still needs a producer and dependencies supported by its target. Compose
portable streams with the `Reactive` operators rather than assuming a compiler replacement for
FSharp.Control.Reactive or System.Reactive.

These adapters are the existing integration boundary; there are no automatic replacements for
the other library's operators. Further integration should specify the conversion direction, target support,
notification ordering, and disposal behavior it needs.
