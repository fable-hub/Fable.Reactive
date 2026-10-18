# Creating streams

The functions below return `IAsyncObservable<'T>`. Each subscription starts its own execution for
`ofSeq` and `ofAsync`; subscribing to a stream is what starts delivery.

| API | What a subscriber receives |
| --- | --- |
| `Reactive.single value` | One value, then completion during subscription |
| `Reactive.ofSeq values` | Values from a sequence, then completion |
| `Reactive.ofAsync workflow` | The workflow result, then completion; workflow failure sends `OnError` |
| `Reactive.empty<'T> ()` | Completion with no values |
| `Reactive.never<'T> ()` | No values or terminal notification |
| `Reactive.fail<'T> error` | `OnError` with the supplied exception |

```fsharp
open Fable.Reactive

let numbers = Reactive.ofSeq [ 1; 2; 3 ]

let fetched =
    Reactive.ofAsync (async { return 42 })
    |> Reactive.map string
```

`ofSeq` and `ofAsync` run in cancellable workers. Disposing a subscription requests cancellation;
the underlying workflow must cooperate with cancellation for it to stop promptly.

## Create per-subscription values

`defer` invokes its factory separately for each subscriber. Use it when stream construction should
read fresh state or acquire a new source for each subscription.

```fsharp
let mutable nextId = 0

let ids =
    Reactive.defer (fun () ->
        nextId <- nextId + 1
        Reactive.single nextId)
```

`Reactive.create` accepts a subscribe function of type
`IAsyncObserver<'T> -> Async<IReactiveDisposable>`. It is the low-level entry point when wrapping
an existing producer; the returned disposable owns that producer's cleanup. For ordinary values
or async work, prefer the constructors above.

## Publish values to subscribers

`Reactive.subject<'T> ()` returns an observer to publish through and a stream to subscribe to.
Subscribers see values published while they are subscribed. `Reactive.replaySubject bufferSize`
retains up to `bufferSize` recent values for new subscribers; its buffer size must be at least one.
Late subscribers also receive its terminal notification after the retained values.

```fsharp
let publisher, events = Reactive.replaySubject<int> 2

let publish =
    async {
        do! publisher.OnNextAsync 1
        do! publisher.OnNextAsync 2
        do! publisher.OnNextAsync 3
        do! publisher.OnCompletedAsync()
    }
```

The separate [time-shifting guide](time-shifting.md) covers `timer` and `interval`.
