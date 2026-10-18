# Actor interoperability

Fable.Reactive works with [Fable.Actor](https://github.com/fable-hub/Fable.Actor). Open
`Fable.Reactive`, `Fable.Actor`, and `Fable.Actor.Types` for the examples below. An `Actor<'Msg>` is
the send-only reference; an actor body's `ActorInbox<'Msg>` receives messages. Share
`inbox.Reference` when another component needs to post to an actor created inside a handler.

```fsharp
open Fable.Reactive
open Fable.Actor
open Fable.Actor.Types
```

## Send stream notifications to an actor

`Reactive.subscribeActor actor source` subscribes an actor that receives `Notification<'T>` values.
It returns `Async<IReactiveDisposable>`. Disposal unsubscribes from the source; the caller remains
responsible for the actor's lifetime.

```fsharp
let sink =
    Actor.spawn (fun inbox ->
        let rec loop () =
            actor {
                let! notification = inbox.Receive()

                match notification with
                | OnNext value -> printfn "Value: %d" value
                | OnError error -> printfn "Error: %s" error.Message
                | OnCompleted -> printfn "Completed"

                return! loop ()
            }

        loop ())

let subscription =
    Reactive.interval 0 1000
    |> Reactive.subscribeActor sink
```

Run `subscription` to obtain its disposable. Keep `sink` alive while it may receive notifications;
stop it separately after its work is finished.

## Map through a stateful actor

`mapActor handler initialState` creates an actor for each subscription. Its handler receives the
current state and one source value, then returns `(nextState, result)`. The operator waits for the
actor's reply before forwarding that result, so actor processing applies backpressure to the
source.

```fsharp
let runningTotals =
    Reactive.ofSeq [ 10; 20; 30 ]
    |> Reactive.mapActor (fun total value ->
        let next = total + value
        next, next) 0

// Emits 10, 30, 60.
```

The returned subscription owns this actor as well as the source subscription. Handler failure is
reported as `OnError`.

## Emit from an actor

`flatMapActor` creates a receiving actor for each subscription. The handler gets an `emit` callback
and an `ActorInbox` for source values. It can emit zero, one, or many results per value.

```fsharp
let doubled =
    Reactive.interval 0 1000
    |> Reactive.flatMapActor (fun emit inbox ->
        let rec loop () =
            actor {
                let! value = inbox.Receive()
                emit (value * 2)
                return! loop ()
            }

        loop ())
```

`emit` posts output without waiting for downstream processing. Source completion and error bypass
the actor, so a terminal notification can overtake actor output still in flight. Use `mapActor`
when each input needs an acknowledged, one-to-one result. An actor crash in `flatMapActor` becomes
`OnError`.

## Choose crash behavior

`flatMapActorSupervised decide handler` adds a decision for an actor failure:

| Directive | Behavior |
| --- | --- |
| `Escalate` | Send the failure downstream as `OnError` |
| `Stop` | Leave the child stopped; the crashing value and later values sent to it produce no output |
| `Restart` | Replace the child; the crashing value is lost, and later values go to the replacement |

```fsharp
let restartOnFailure =
    Reactive.interval 0 1000
    |> Reactive.flatMapActorSupervised
        (fun _ -> Directive.Restart)
        (fun emit inbox ->
            let rec loop () =
                actor {
                    let! value = inbox.Receive()
                    emit (value * 2)
                    return! loop ()
                }

            loop ())
```

As with `flatMapActor`, source terminal notifications bypass the child. Dispose the subscription
with `do! subscription.DisposeAsync()` to await its owned source and actor cleanup. If the handler
needs to share its receiving actor, pass `inbox.Reference` to code that expects an `Actor<'Msg>`.

## Create an actor-backed source

`Reactive.ofActor body` starts an actor immediately and returns its send-only reference and a hot
stream of values emitted by the body.

```fsharp
let publisher, events =
    Reactive.ofActor (fun emit inbox ->
        let rec loop () =
            actor {
                let! value = inbox.Receive()
                emit (value * 2)
                return! loop ()
            }

        loop ())
```

Subscribe to `events` before posting to `publisher`. Values emitted before subscription are not
replayed. The caller owns the actor's lifetime; disposing a stream subscription removes that
subscriber but does not stop `publisher`. See [creating streams](creating.md#publish-values-to-subscribers)
for subjects and [the reactive builder](builder.md) for composition syntax.
