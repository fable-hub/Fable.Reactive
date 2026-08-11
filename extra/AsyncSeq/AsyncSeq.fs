namespace Fable.Reactive

open System.Threading
open FSharp.Control
open Fable.Reactive

[<CompilationRepresentation(CompilationRepresentationFlags.ModuleSuffix)>]
module Reactive =
    /// Convert async sequence into an async observable.
    let ofAsyncSeq (xs: AsyncSeq<'TSource>) : IAsyncObservable<'TSource> =
        let subscribeAsync (aobv: IAsyncObserver<'TSource>) : Async<IReactiveDisposable> =
            let cancel, token = canceller ()

            async {
                let ie = xs.GetEnumerator()

                let rec loop () =
                    async {
                        let! result =
                            async {
                                try
                                    let! value = ie.MoveNext()
                                    return Ok value
                                with ex ->
                                    return Error ex
                            }

                        match result with
                        | Ok notification ->
                            match notification with
                            | Some x ->
                                do! aobv.OnNextAsync x
                                do! loop ()
                            | None -> do! aobv.OnCompletedAsync()
                        | Error err -> do! aobv.OnErrorAsync err
                    }

                Async.StartImmediate(loop (), token)
                return cancel
            }

        { new IAsyncObservable<'TSource> with
            member __.SubscribeAsync o = subscribeAsync o }


    /// Convert async observable to async sequence, non-blocking. Producer will be awaited until item is consumed by the
    /// async enumerator.
    ///
    /// decision: uses a ping-pong handshake to provide one-item backpressure without a shared blocking queue
    /// invariant: the producer cannot overwrite the pending notification before the consumer acknowledges it
    let toAsyncSeq (source: IAsyncObservable<'TSource>) : AsyncSeq<'TSource> =
        let ping = new AutoResetEvent false
        let pong = new AutoResetEvent false
        let mutable latest: Notification<'TSource> = OnCompleted

        let _obv n =
            async {
                latest <- n
                ping.Set() |> ignore
                do! Async.AwaitWaitHandle pong |> Async.Ignore
            }

        asyncSeq {
            let! dispose = AsyncObserver _obv |> source.SubscribeAsync
            let mutable running = true

            while running do
                do! Async.AwaitWaitHandle ping |> Async.Ignore

                match latest with
                | OnNext x -> yield x
                | OnError ex ->
                    running <- false
                    raise ex
                | OnCompleted -> running <- false

                pong.Set() |> ignore

            do! dispose.DisposeAsync()
        }
