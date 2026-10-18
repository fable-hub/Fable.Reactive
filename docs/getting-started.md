# Getting started

Fable.Reactive composes asynchronous streams with `IAsyncObservable<'T>`. A stream sends values to an
`IAsyncObserver<'T>` through `OnNextAsync`, then ends with either `OnCompletedAsync` or `OnErrorAsync`.
These callbacks return `Async<unit>`, so a producer can await an observer's work.

## Install

Add the package to your F# project:

```sh
dotnet paket add Fable.Reactive --project <project>
```

Use Fable 5.22.0 or newer when compiling to JavaScript, Python, or BEAM. This compiler requirement
does not apply to a .NET-only application. Open `Fable.Reactive` to use the stream types and the
qualified `Reactive` module.

## Your first subscription

```fsharp
open System.Threading.Tasks
open Fable.Reactive

let program =
    async {
        let finished = TaskCompletionSource<unit>()

        let stream =
            Reactive.single 21
            |> Reactive.map (fun value -> value * 2)

        let! subscription =
            stream.SubscribeAsync(fun notification ->
                async {
                    match notification with
                    | OnNext value -> printfn "Value: %d" value
                    | OnError error ->
                        printfn "Error: %s" error.Message
                        finished.TrySetResult() |> ignore
                    | OnCompleted ->
                        printfn "Completed"
                        finished.TrySetResult() |> ignore
                })

        do! finished.Task |> Async.AwaitTask
        do! subscription.DisposeAsync()
    }

Async.RunSynchronously program
```

On .NET, this prints `Value: 42`, then `Completed`. The `TaskCompletionSource` lets the host wait for
a terminal callback before disposing. Even `Reactive.single` may finish delivering callbacks after
`SubscribeAsync` returns. Keep a subscription alive until the work you need has finished.

`SubscribeAsync` returns `Async<IReactiveDisposable>`: run the workflow to subscribe, then await
`DisposeAsync()` to stop owning that subscription. Disposing an active subscription is distinct from
receiving `OnCompleted` or `OnError`. The `RunAsync` convenience method discards the disposable;
prefer `SubscribeAsync` when you need to control lifetime.

Operators are curried and put the source last, so pipelines read from left to right:

```fsharp
let values =
    Reactive.ofSeq [ 1; 2; 3; 4 ]
    |> Reactive.filter (fun value -> value % 2 = 0)
    |> Reactive.map (fun value -> value * 10)
```

`values` is a stream description. Subscribe to run it. Start with [creating streams](creating.md),
then choose operators from the [guide index](../README.md#documentation).
