namespace Fable.Reactive

open Fable.Actor
open Fable.Actor.Types

open Fable.Reactive.Core

[<RequireQualifiedAccess>]
module internal ActorInterop =

    type private SupervisorStop = SupervisorStop

    // decision: tags forwarded values before the shared monitor sees them so BEAM never tests user records as child exits
    // invariant: the monitor classifies only the outer envelope and never inspects a source value for exit markers
    type private ForwardedValue = ReactiveForwardedValue of obj

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

    let private cleanupTimeout = 5000

    type private SupervisedContext<'TSource, 'TResult> =
        {
            Closed: ActorLifetime.Flag
            MonitorFailed: ActorLifetime.Flag
            ChildCancellation: System.Threading.CancellationTokenSource
            Output: ActorLifetime.Worker<'TResult>
            Downstream: IAsyncObserver<'TResult>
            Decide: exn -> Directive
            Handler: ('TResult -> unit) -> ActorInbox<'TSource> -> ActorOp<unit>
        }

    let private spawnOutput (closed: ActorLifetime.Flag) (downstream: IAsyncObserver<'T>) =
        ActorLifetime.spawn None ignore (fun inbox ->
            let rec loop () =
                actor {
                    let! value = inbox.Receive()

                    if closed.Value = 0 then
                        try
                            do! downstream.OnNextAsync value
                        with ex ->
                            do! downstream.OnErrorAsync ex

                    return! loop ()
                }

            loop ())

    // invariant: escalation never throws back into the unsupervised monitor
    let private escalate (context: SupervisedContext<_, _>) ex =
        async {
            if context.Closed.Value = 0 then
                try
                    do! context.Downstream.OnErrorAsync ex
                with _ ->
                    ()
        }

    let private childExitError (exited: ChildExited) =
        match exited.Reason with
        | :? exn as ex -> ex
        | reason -> ProcessExitException(sprintf "%A" reason)

    let private handleChildExit
        (context: SupervisedContext<'TSource, 'TResult>)
        (spawnChild: unit -> ActorLifetime.Worker<'TSource>)
        (child: ActorLifetime.Worker<'TSource>)
        (exited: ChildExited)
        : ActorOp<ActorLifetime.Worker<'TSource> option> =
        actor {
            let ex = childExitError exited

            match context.Decide ex with
            | Directive.Escalate ->
                do! escalate context ex
                return None
            | Directive.Stop -> return Some child
            | Directive.Restart ->
                do! child.Stop cleanupTimeout

                if context.Closed.Value = 0 then
                    return Some(spawnChild ())
                else
                    return Some child
        }

    let private routeMonitorMessage
        (context: SupervisedContext<'TSource, 'TResult>)
        (spawnChild: unit -> ActorLifetime.Worker<'TSource>)
        (child: ActorLifetime.Worker<'TSource>)
        (msg: obj)
        : ActorOp<ActorLifetime.Worker<'TSource> option> =
        actor {
            match msg with
            | :? SupervisorStop -> return None
            | _ when context.Closed.Value <> 0 -> return Some child
            | :? ForwardedValue as forwarded ->
                let (ReactiveForwardedValue value) = forwarded
                child.Actor.Post(unbox<'TSource> value)
                return Some child
            | _ ->
                match Actor.tryAsChildExited msg with
                | Some exited -> return! handleChildExit context spawnChild child exited
                | None -> return failwith "Unexpected supervised actor message"
        }

    let private startSupervisedMonitor (context: SupervisedContext<'TSource, 'TResult>) =
        ActorLifetime.spawn None (fun _ -> context.MonitorFailed.Set 1) (fun inbox ->
            Actor.trapExits ()

            let emit value =
                if context.Closed.Value = 0 then
                    context.Output.Actor.Post value

            let spawnChild () =
                ActorLifetime.spawnWithToken
                    context.ChildCancellation.Token
                    (Some inbox.Reference)
                    (fun ex -> inbox.Post(box ({ Pid = null; Reason = box ex }: ChildExited)))
                    (context.Handler emit)

            let mutable child = spawnChild ()

            let rec loop () =
                actor {
                    let! msg = inbox.Receive()
                    let! nextChild = routeMonitorMessage context spawnChild child msg

                    match nextChild with
                    | None -> ()
                    | Some next ->
                        child <- next
                        return! loop ()
                }

            actor {
                try
                    do! loop ()
                with ex ->
                    do! escalate context ex

                try
                    do! child.Stop cleanupTimeout
                with _ ->
                    context.MonitorFailed.Set 1
            })

    let private closeSupervision (context: SupervisedContext<_, _>) =
        context.Closed.Set 1
        context.ChildCancellation.Cancel()

    let private stopMonitor (context: SupervisedContext<_, _>) (monitor: ActorLifetime.Worker<obj>) =
        async {
            monitor.Actor.Post(box SupervisorStop)

            try
                do! ActorLifetime.waitUntil cleanupTimeout "Supervisor did not acknowledge shutdown" monitor.IsStopped
            with ex ->
                do! monitor.Stop cleanupTimeout
                return raise ex

            do! monitor.Stop cleanupTimeout

            if context.MonitorFailed.Value <> 0 then
                failwith "Supervisor child cleanup failed"
        }

    let private cleanupSupervision
        (context: SupervisedContext<_, _>)
        (monitor: ActorLifetime.Worker<obj>)
        (sourceDisp: IReactiveDisposable)
        =
        ActorLifetime.cleanupAll
            [
                (fun () -> stopMonitor context monitor)
                (fun () -> context.Output.Stop cleanupTimeout)
                (fun () ->
                    ActorLifetime.cleanupStep
                        cleanupTimeout
                        "Source disposal failed or timed out"
                        (sourceDisp.DisposeAsync()))
            ]

    let private supervisedObserver
        (context: SupervisedContext<'TSource, 'TResult>)
        (monitor: ActorLifetime.Worker<obj>)
        =
        { new IAsyncObserver<'TSource> with
            member _.OnNextAsync x =
                async {
                    if context.Closed.Value = 0 then
                        monitor.Actor.Post(box (ReactiveForwardedValue(box x)))
                }

            member _.OnErrorAsync err =
                async {
                    if context.Closed.Value = 0 then
                        do! context.Downstream.OnErrorAsync err
                }

            member _.OnCompletedAsync() =
                async {
                    if context.Closed.Value = 0 then
                        do! context.Downstream.OnCompletedAsync()
                }
        }

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

                let context =
                    {
                        Closed = closed
                        MonitorFailed = ActorLifetime.Flag(0)
                        ChildCancellation = new System.Threading.CancellationTokenSource()
                        Output = spawnOutput closed aobv
                        Downstream = aobv
                        Decide = decide
                        Handler = handler
                    }

                let monitor = startSupervisedMonitor context

                try
                    let! sourceDisp = source.SubscribeAsync(supervisedObserver context monitor)

                    return
                        ActorLifetime.disposable (cleanupTimeout * 4) (fun () -> closeSupervision context) (fun () ->
                            cleanupSupervision context monitor sourceDisp)
                with ex ->
                    closeSupervision context
                    do! cleanupSupervision context monitor AsyncDisposable.Empty
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
                    Actor.spawn (
                        initialState,
                        fun state (value, rc: ReplyChannel<'TResult>) ->
                            let state', result = handler state value
                            rc.Reply result
                            state'
                    )

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
