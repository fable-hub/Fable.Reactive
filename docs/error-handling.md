# Error handling

An observer receives values through `OnNext`, then at most one terminal notification:
`OnCompleted` or `OnError`. Handle both when subscribing to a stream; disposal stops a
subscription but is not a completion notification.

```fsharp
open Fable.Reactive

let observe (stream: IAsyncObservable<'T>) =
    stream.SubscribeAsync(fun notification ->
        async {
            match notification with
            | OnNext value -> printfn "Value: %A" value
            | OnError error -> printfn "Failed: %s" error.Message
            | OnCompleted -> printfn "Finished"
        })
```

## Recover with a fallback stream

`Reactive.catch handler source` forwards the source's values. If the source sends `OnError`, it
disposes the failed subscription and subscribes to the stream returned by `handler`.

```fsharp
let recovered =
    Reactive.fail<int> (System.Exception "unavailable")
    |> Reactive.catch (fun _ -> Reactive.single 0)
```

If the fallback fails, `catch` invokes the handler again with that error. A handler that throws is
reported as `OnError`. `Reactive.fail<'T> error` constructs a stream that reports an error when
subscribed to.

## Retry

`Reactive.retry count source` resubscribes after an error up to `count` times per subscription.
`count = 0` forwards the first error. For example, `retry 2` allows the initial subscription and
two retries, then forwards the last error if all three attempts fail. Successful completion stops
retrying. Use `catch` when recovery should switch to a different stream.

```fsharp
let fetchWithRetries (fetch: unit -> Async<int>) =
    Reactive.defer (fun () -> Reactive.ofAsync (fetch ()))
    |> Reactive.retry 2
```

The `defer` factory calls `fetch` for each attempt, including the retries.

See [creating streams](creating.md) for `ofAsync` and `fail`, and [getting started](getting-started.md)
for subscription ownership.
