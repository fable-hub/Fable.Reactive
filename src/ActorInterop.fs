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

                // Shared mutable ref — upstream posts directly to the child actor.
                // Monitor swaps the ref on Restart.
                let childRef = ref Unchecked.defaultof<Actor<'TSource>>
                let ready = System.Threading.Tasks.TaskCompletionSource<unit>()

                // Monitor actor: receives ChildExited and applies the supervision directive.
                let monitor =
                    Actor.spawn (fun inbox ->
                        childRef.Value <- Actor.spawnLinked inbox (handler emit)
                        ready.SetResult()

                        let rec loop () =
                            async {
                                let! msg = inbox.Receive()

                                match Actor.tryAsChildExited msg with
                                | Some exited ->
                                    let ex =
                                        match exited.Reason with
                                        | :? exn as e -> e
                                        | r -> ProcessExitException(sprintf "%A" r)

                                    match decide ex with
                                    | Directive.Escalate -> do! aobv.OnErrorAsync ex
                                    | Directive.Stop -> ()
                                    | Directive.Restart -> childRef.Value <- Actor.spawnLinked inbox (handler emit)
                                | None -> ()

                                return! loop ()
                            }

                        loop ())

                do! Async.AwaitTask ready.Task

                let obv =
                    { new IAsyncObserver<'TSource> with
                        member _.OnNextAsync x = async { childRef.Value.Post x }
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
                            async {
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
                                let! result = Actor.call actor x
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
    let ofActor (body: ('T -> unit) -> Actor<'Msg> -> ActorOp<unit>) : Actor<'Msg> * IAsyncObservable<'T> =
        let dispatch, stream = Subjects.subject<'T> ()

        let emit value =
            dispatch.OnNextAsync value |> Async.Start'

        let actor = Actor.spawn (body emit)
        actor, stream
