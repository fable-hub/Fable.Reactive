namespace Fable.Reactive

open System
open Fable.Actor
open Fable.Actor.Types
open Fable.Reactive.Core

type AsyncObserver<'T>(fn: Notification<'T> -> Async<unit>) =

    interface IAsyncObserver<'T> with
        member this.OnNextAsync(x: 'T) = OnNext x |> fn
        member this.OnErrorAsync err = OnError err |> fn
        member this.OnCompletedAsync() = OnCompleted |> fn

    static member Create(fn) : IAsyncObserver<'T> =
        AsyncObserver<'T> fn :> IAsyncObserver<'T>

type Observer<'T>(fn: Notification<'T> -> unit) =
    interface IObserver<'T> with
        member this.OnNext(x: 'T) = OnNext x |> fn
        member this.OnError err = OnError err |> fn
        member this.OnCompleted() = OnCompleted |> fn

    static member Create(fn) : IObserver<'T> = Observer<'T> fn :> IObserver<'T>

[<AutoOpen>]
module AsyncObserver =
    // decision: owns operator state in one actor so callbacks from different BEAM processes share ordered state
    let internal statefulObserver (initial: 'State) (update: 'State -> Notification<'T> -> Async<'State>) =
        let owner =
            Actor.spawn (fun inbox ->
                let rec loop state =
                    actor {
                        let! notification = inbox.Receive()
                        let! next = update state notification
                        return! loop next
                    }

                loop initial)

        AsyncObserver.Create(fun notification -> async { owner.Post notification })

    type IAsyncObserver<'T> with

        /// Convert async observer (IAsyncObserver) to an observer (IObserver).
        member this.ToObserver() =
            { new IObserver<'T> with
                member __.OnNext x = this.OnNextAsync x |> Async.Start'
                member __.OnError err = this.OnErrorAsync err |> Async.Start'
                member __.OnCompleted() = this.OnCompletedAsync() |> Async.Start'
            }

    type IObserver<'T> with

        /// Convert observer (IObserver) to an async observer (IAsyncObserver).
        member this.ToAsyncObserver() =
            { new IAsyncObserver<'T> with
                member __.OnNextAsync x = async { this.OnNext x }
                member __.OnErrorAsync err = async { this.OnError err }
                member __.OnCompletedAsync() = async { this.OnCompleted() }
            }

    /// Safe observer that wraps the given observer. Makes sure that invocations are serialized and that the Rx grammar
    /// (OnNext* (OnError|OnCompleted)?) is not violated.
    ///
    /// decision: serializes callbacks through one actor because upstream notifications may arrive concurrently
    /// invariant: the wrapped observer receives no callback after its first OnError or OnCompleted callback
    let safeObserver (obv: IAsyncObserver<'TSource>) (disposable: IReactiveDisposable) : IAsyncObserver<'TSource> =
        let agent =
            Actor.spawn (fun inbox ->
                let rec messageLoop stopped =
                    actor {
                        let! n = inbox.Receive()

                        if stopped then
                            return! messageLoop stopped

                        let! stop =
                            async {
                                match n with
                                | OnNext x ->
                                    try
                                        do! obv.OnNextAsync x
                                        return false
                                    with ex ->
                                        do! obv.OnErrorAsync ex
                                        return true
                                | OnError ex ->
                                    do! disposable.DisposeAsync()
                                    do! obv.OnErrorAsync ex
                                    return true
                                | OnCompleted ->
                                    do! disposable.DisposeAsync()
                                    do! obv.OnCompletedAsync()
                                    return true
                            }

                        return! messageLoop stop
                    }

                messageLoop false)

        { new IAsyncObserver<'TSource> with
            member this.OnNextAsync x = async { OnNext x |> agent.Post }
            member this.OnErrorAsync err = async { OnError err |> agent.Post }
            member this.OnCompletedAsync() = async { OnCompleted |> agent.Post }
        }

    type private Msg =
        | Disposable of IReactiveDisposable
        | Dispose

    let autoDetachObserver
        (obv: IAsyncObserver<'TSource>)
        : IAsyncObserver<'TSource> * (Async<IReactiveDisposable> -> Async<IReactiveDisposable>) =
        let agent =
            Actor.spawn (fun inbox ->
                let rec messageLoop disposables disposed =
                    actor {
                        let! (cmd, reply: ReplyChannel<unit>) = inbox.Receive()

                        let! disposables' =
                            async {
                                match cmd with
                                | Disposable disp when disposed ->
                                    do! disp.DisposeAsync()
                                    return []
                                | Disposable disp -> return disp :: disposables
                                | Dispose ->
                                    for disp in disposables do
                                        do! disp.DisposeAsync()

                                    return []
                            }

                        reply.Reply()

                        return!
                            messageLoop
                                disposables'
                                (disposed
                                 || (match cmd with
                                     | Dispose -> true
                                     | _ -> false))
                    }

                messageLoop [] false)

        let cancel () = Actor.asyncCall agent Dispose

        let safeObv = AsyncDisposable.Create cancel |> safeObserver obv

        // Auto-detaches (disposes) the disposable when the observer completes with success or error.
        // decision: registers subscriptions with an actor so terminal signals and explicit disposal serialize
        // decision: queues registration without waiting so nested eager subscriptions cannot wait on their own cleanup
        // invariant: subscriptions registered after termination are disposed instead of being retained
        let autoDetach (disposable: Async<IReactiveDisposable>) =
            async {
                let! disp = disposable
                Actor.cast agent (Disposable disp)
                return disp
            }

        safeObv, autoDetach
