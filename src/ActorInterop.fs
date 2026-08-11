namespace Fable.Reactive

open Fable.Actor
open Fable.Actor.Types

open Fable.Reactive.Core

[<RequireQualifiedAccess>]
module internal ActorInterop =

    /// Wraps an Actor<Notification<'T>> as an IAsyncObserver<'T>.
    /// Posts each notification to the actor. The actor owns its own lifecycle.
    let toObserver (actor: Actor<Notification<'T>>) : IAsyncObserver<'T> =
        { new IAsyncObserver<'T> with
            member _.OnNextAsync x = async { actor.Post(OnNext x) }
            member _.OnErrorAsync err = async { actor.Post(OnError err) }
            member _.OnCompletedAsync() = async { actor.Post OnCompleted } }

    /// Subscribe an observable to an actor that receives Notification<'T>.
    /// Actor lifecycle is caller-managed.
    let subscribeActor (actor: Actor<Notification<'T>>) (source: IAsyncObservable<'T>) : Async<IReactiveDisposable> =
        source.SubscribeAsync(toObserver actor)

    /// For each upstream item, posts it to a supervised per-subscription actor.
    /// The actor emits downstream via an emit callback provided at spawn time.
    /// Terminal events bypass the actor and go directly to downstream.
    /// The decider controls crash behavior:
    ///   Escalate → forward as OnErrorAsync (terminates the stream)
    ///   Stop     → actor dies, stream continues (crashed item is lost)
    ///   Restart  → actor is restarted, stream continues (crashed item is lost)
    ///
    /// decision: routes values and linked exits through one monitor so restart never exposes a stale child actor
    /// invariant: terminal source notifications bypass the child and retain control of the stream lifecycle
    /// tradeoff: Stop and Restart keep the stream alive by dropping the item that crashed the child
    let flatMapActorSupervised
        (decide: exn -> Directive)
        (handler: ('TResult -> unit) -> Actor<'TSource> -> ActorOp<unit>)
        (source: IAsyncObservable<'TSource>)
        : IAsyncObservable<'TResult> =
        let subscribeAsync (aobv: IAsyncObserver<'TResult>) =
            async {
                let dispatch, stream = Subjects.subject<'TResult> ()
                let! innerDisp = stream.SubscribeAsync aobv

                let emit value =
                    dispatch.OnNextAsync value |> Async.Start'

                // Nothing supervises the monitor, so downstream termination must not throw back into it.
                // invariant: escalation attempts one terminal notification and never raises into the monitor
                let escalate (ex: exn) =
                    async {
                        try
                            do! aobv.OnErrorAsync ex
                        with _ ->
                            ()
                    }

                // Spawns and supervises the child, replaces it on Restart, and forwards upstream values.
                //
                // decision: multiplexes ChildExited and source values through Actor<obj> to serialize routing
                // invariant: the current child is installed before the monitor dequeues an upstream value
                // tradeoff: runtime casts buy one ordered mailbox for control signals and typed source values
                let monitor =
                    Actor.spawn (fun inbox ->
                        // BEAM implements spawnLinked with spawn_link; trapping converts a child crash into a message.
                        // decision: traps linked exits so the monitor, rather than the runtime, applies the decider
                        Actor.trapExits ()

                        let mutable child = Actor.spawnLinked inbox (handler emit)

                        let rec loop () =
                            actor {
                                let! msg = inbox.Receive()

                                // The monitor is the root of this local supervision tree.
                                // decision: catches loop failures so an unsupervised monitor terminates downstream
                                // invariant: monitor failure escalates and stops instead of leaving an unread mailbox
                                let! alive =
                                    actor {
                                        try
                                            match Actor.tryAsChildExited msg with
                                            | Some exited ->
                                                let ex =
                                                    match exited.Reason with
                                                    | :? exn as e -> e
                                                    | r -> ProcessExitException(sprintf "%A" r)

                                                match decide ex with
                                                | Directive.Escalate -> do! escalate ex
                                                | Directive.Stop -> ()
                                                | Directive.Restart -> child <- Actor.spawnLinked inbox (handler emit)
                                            // Both backends normalize linked exits to ChildExited before this point.
                                            // assumption: every other mailbox item is a boxed TSource value
                                            | None -> child.Post(unbox<'TSource> msg)

                                            return true
                                        with ex ->
                                            do! escalate ex
                                            return false
                                    }

                                if alive then
                                    return! loop ()
                            }

                        loop ())

                let obv =
                    { new IAsyncObserver<'TSource> with
                        member _.OnNextAsync x = async { monitor.Post(box x) }
                        member _.OnErrorAsync err = aobv.OnErrorAsync err
                        member _.OnCompletedAsync() = aobv.OnCompletedAsync() }

                let! sourceDisp = source.SubscribeAsync obv

                return
                    AsyncDisposable.Composite
                        [ sourceDisp
                          innerDisp
                          AsyncDisposable.Create(fun () -> async { Actor.kill monitor }) ]
            }

        { new IAsyncObservable<'TResult> with
            member _.SubscribeAsync o = subscribeAsync o }

    /// For each upstream item, posts it to a per-subscription actor.
    /// The actor emits downstream via an emit callback provided at spawn time.
    /// Terminal events bypass the actor and go directly to downstream.
    /// If the actor crashes, the error is forwarded downstream as OnErrorAsync.
    let flatMapActor
        (handler: ('TResult -> unit) -> Actor<'TSource> -> ActorOp<unit>)
        (source: IAsyncObservable<'TSource>)
        : IAsyncObservable<'TResult> =
        flatMapActorSupervised (fun _ -> Directive.Escalate) handler source

    /// Stateful 1-to-1 transform using an actor with request-reply (call).
    /// Provides backpressure — the pipeline waits for the actor's reply before emitting downstream.
    ///
    /// decision: uses request-reply so actor processing applies backpressure to each upstream notification
    let mapActor
        (handler: 'State -> 'TSource -> 'State * 'TResult)
        (initialState: 'State)
        (source: IAsyncObservable<'TSource>)
        : IAsyncObservable<'TResult> =
        let subscribeAsync (aobv: IAsyncObserver<'TResult>) =
            async {
                let actor =
                    Actor.spawn (fun inbox ->
                        let rec loop state =
                            actor {
                                let! (value, rc: ReplyChannel<'TResult>) = inbox.Receive()
                                let state', result = handler state value
                                rc.Reply result
                                return! loop state'
                            }

                        loop initialState)

                let obv =
                    { new IAsyncObserver<'TSource> with
                        member _.OnNextAsync x =
                            async {
                                let! result = Actor.callAsync actor x
                                do! aobv.OnNextAsync result
                            }

                        member _.OnErrorAsync err = aobv.OnErrorAsync err
                        member _.OnCompletedAsync() = aobv.OnCompletedAsync() }

                let! disp = source.SubscribeAsync obv

                return AsyncDisposable.Composite [ disp; AsyncDisposable.Create(fun () -> async { Actor.kill actor }) ]
            }

        { new IAsyncObservable<'TResult> with
            member _.SubscribeAsync o = subscribeAsync o }

    /// Create an actor-backed subject. The actor body receives an emit callback.
    /// Returns the actor (for posting messages) and an observable (for subscribing).
    /// The observable is hot; values emitted before subscription are not replayed.
    ///
    /// decision: spawns the actor eagerly and multicasts its output so subscribers share one actor lifecycle
    /// tradeoff: emissions before the first subscription can be lost because the backing subject has no replay buffer
    let ofActor (body: ('T -> unit) -> Actor<'Msg> -> ActorOp<unit>) : Actor<'Msg> * IAsyncObservable<'T> =
        let dispatch, stream = Subjects.subject<'T> ()

        let emit value =
            dispatch.OnNextAsync value |> Async.Start'

        let actor = Actor.spawn (body emit)
        actor, stream
