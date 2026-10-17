namespace Fable.Reactive

open Fable.Actor
open Fable.Actor.Types

open Fable.Reactive.Core

[<RequireQualifiedAccess>]
module internal ActorInterop =

    type private SupervisorStop = SupervisorStop

    /// Wraps an Actor<Notification<'T>> as an IAsyncObserver<'T>.
    /// Posts each notification to the actor. The actor owns its own lifecycle.
    let toObserver (actor: Actor<Notification<'T>>) : IAsyncObserver<'T> =
        { new IAsyncObserver<'T> with
            member _.OnNextAsync x = async { actor.Post(OnNext x) }
            member _.OnErrorAsync err = async { actor.Post(OnError err) }
            member _.OnCompletedAsync() = async { actor.Post OnCompleted }
        }

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
        (handler: ('TResult -> unit) -> ActorInbox<'TSource> -> ActorOp<unit>)
        (source: IAsyncObservable<'TSource>)
        : IAsyncObservable<'TResult> =
        let subscribeAsync (aobv: IAsyncObserver<'TResult>) =
            async {
                let closed = ActorLifetime.Flag(0)
                let monitorFailed = ActorLifetime.Flag(0)
                let childCancellation = new System.Threading.CancellationTokenSource()
                let cleanupTimeout = 5000

                let output =
                    ActorLifetime.spawn None ignore (fun inbox ->
                        let rec loop () =
                            actor {
                                let! value = inbox.Receive()

                                if closed.Value = 0 then
                                    try
                                        do! aobv.OnNextAsync value
                                    with ex ->
                                        do! aobv.OnErrorAsync ex

                                return! loop ()
                            }

                        loop ())

                let emit value =
                    if closed.Value = 0 then
                        output.Actor.Post value

                // invariant: escalation never throws back into the unsupervised monitor
                let escalate ex =
                    async {
                        if closed.Value = 0 then
                            try
                                do! aobv.OnErrorAsync ex
                            with _ ->
                                ()
                    }

                let monitor =
                    ActorLifetime.spawn None (fun _ -> monitorFailed.Set 1) (fun inbox ->
                        Actor.trapExits ()

                        let spawnChild () =
                            ActorLifetime.spawnWithToken
                                childCancellation.Token
                                (Some inbox.Reference)
                                (fun ex -> inbox.Post(box ({ Pid = null; Reason = box ex }: ChildExited)))
                                (handler emit)

                        let mutable child = spawnChild ()

                        let rec loop () =
                            actor {
                                let! msg = inbox.Receive()

                                match msg with
                                | :? SupervisorStop -> ()
                                | _ when closed.Value <> 0 -> return! loop ()
                                | _ ->
                                    match Actor.tryAsChildExited msg with
                                    | Some exited ->
                                        let ex =
                                            match exited.Reason with
                                            | :? exn as e -> e
                                            | r -> ProcessExitException(sprintf "%A" r)

                                        match decide ex with
                                        | Directive.Escalate -> do! escalate ex
                                        | Directive.Stop -> return! loop ()
                                        | Directive.Restart ->
                                            do! child.Stop cleanupTimeout

                                            if closed.Value = 0 then
                                                child <- spawnChild ()

                                            return! loop ()
                                    | None ->
                                        child.Actor.Post(unbox<'TSource> msg)
                                        return! loop ()
                            }

                        actor {
                            try
                                do! loop ()
                            with ex ->
                                do! escalate ex

                            try
                                do! child.Stop cleanupTimeout
                            with _ ->
                                monitorFailed.Set 1
                        })

                let close () =
                    closed.Set 1
                    childCancellation.Cancel()

                let stopMonitor () =
                    async {
                        monitor.Actor.Post(box SupervisorStop)

                        try
                            do!
                                ActorLifetime.waitUntil
                                    cleanupTimeout
                                    "Supervisor did not acknowledge shutdown"
                                    monitor.IsStopped
                        with ex ->
                            do! monitor.Stop cleanupTimeout
                            return raise ex

                        do! monitor.Stop cleanupTimeout

                        if monitorFailed.Value <> 0 then
                            failwith "Supervisor child cleanup failed"
                    }

                let cleanup (sourceDisp: IReactiveDisposable) () =
                    ActorLifetime.cleanupAll
                        [
                            stopMonitor
                            (fun () -> output.Stop cleanupTimeout)
                            (fun () ->
                                ActorLifetime.cleanupStep
                                    cleanupTimeout
                                    "Source disposal failed or timed out"
                                    (sourceDisp.DisposeAsync()))
                        ]

                let obv =
                    { new IAsyncObserver<'TSource> with
                        member _.OnNextAsync x =
                            async {
                                if closed.Value = 0 then
                                    monitor.Actor.Post(box x)
                            }

                        member _.OnErrorAsync err =
                            async {
                                if closed.Value = 0 then
                                    do! aobv.OnErrorAsync err
                            }

                        member _.OnCompletedAsync() =
                            async {
                                if closed.Value = 0 then
                                    do! aobv.OnCompletedAsync()
                            }
                    }

                try
                    let! sourceDisp = source.SubscribeAsync obv
                    return ActorLifetime.disposable (cleanupTimeout * 4) close (cleanup sourceDisp)
                with ex ->
                    close ()
                    do! cleanup AsyncDisposable.Empty ()
                    return raise ex
            }

        { new IAsyncObservable<'TResult> with
            member _.SubscribeAsync o = subscribeAsync o
        }

    /// For each upstream item, posts it to a per-subscription actor.
    /// The actor emits downstream via an emit callback provided at spawn time.
    /// Terminal events bypass the actor and go directly to downstream.
    /// If the actor crashes, the error is forwarded downstream as OnErrorAsync.
    let flatMapActor
        (handler: ('TResult -> unit) -> ActorInbox<'TSource> -> ActorOp<unit>)
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
                                let! result = Actor.asyncCall actor x
                                do! aobv.OnNextAsync result
                            }

                        member _.OnErrorAsync err = aobv.OnErrorAsync err
                        member _.OnCompletedAsync() = aobv.OnCompletedAsync()
                    }

                let! disp = source.SubscribeAsync obv

                return AsyncDisposable.Composite [ disp; AsyncDisposable.Create(fun () -> async { Actor.kill actor }) ]
            }

        { new IAsyncObservable<'TResult> with
            member _.SubscribeAsync o = subscribeAsync o
        }

    /// Create an actor-backed subject. The actor body receives an emit callback.
    /// Returns the actor (for posting messages) and an observable (for subscribing).
    /// The observable is hot; values emitted before subscription are not replayed.
    ///
    /// decision: spawns the actor eagerly and multicasts its output so subscribers share one actor lifecycle
    /// tradeoff: emissions before the first subscription can be lost because the backing subject has no replay buffer
    let ofActor (body: ('T -> unit) -> ActorInbox<'Msg> -> ActorOp<unit>) : Actor<'Msg> * IAsyncObservable<'T> =
        let dispatch, stream = Subjects.subject<'T>()

        let emit value =
            dispatch.OnNextAsync value |> Async.Start'

        let actor = Actor.spawn (body emit)
        actor, stream
