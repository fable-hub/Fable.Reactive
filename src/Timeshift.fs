namespace Fable.Reactive

open System
open Fable.Actor

open Fable.Reactive.Core


[<RequireQualifiedAccess>]
module internal Timeshift =

    type private DebounceMessage<'T> =
        | Schedule of Notification<'T>
        | Emit of value: 'T * generation: int
        | Dispose

    /// Time shifts the observable sequence by the given timeout. The
    /// relative time intervals between the values are preserved.
    ///
    /// decision: queues delayed notifications through one actor to preserve source and terminal ordering
    /// invariant: a terminal notification never overtakes an earlier delayed value
    let delay (msecs: int) (source: IAsyncObservable<'TSource>) : IAsyncObservable<'TSource> =
        let subscribeAsync (aobv: IAsyncObserver<'TSource>) =
            let agent =
                Actor.spawn (fun inbox ->
                    let rec messageLoop () =
                        actor {
                            let! n, dueTime = inbox.Receive()

                            let diff: TimeSpan = dueTime - DateTime.UtcNow
                            let msecs = Convert.ToInt32 diff.TotalMilliseconds

                            if msecs > 0 then
                                do! Async.Sleep msecs

                            match n with
                            | OnNext x -> do! aobv.OnNextAsync x
                            | OnError ex -> do! aobv.OnErrorAsync ex
                            | OnCompleted -> do! aobv.OnCompletedAsync()

                            return! messageLoop ()
                        }

                    messageLoop ())

            async {
                let obv n =
                    async {
                        let dueTime =
                            DateTime.UtcNow
                            + TimeSpan.FromMilliseconds(float msecs)

                        agent.Post(n, dueTime)
                    }

                let! subscription = AsyncObserver obv |> source.SubscribeAsync

                let cancel () =
                    async { do! subscription.DisposeAsync() }

                return AsyncDisposable.Create cancel
            }

        { new IAsyncObservable<'TSource> with
            member _.SubscribeAsync o = subscribeAsync o
        }

    /// Ignores values from an observable sequence which are followed by
    /// another value before the given timeout.
    ///
    /// decision: assigns monotonic generations inside the actor so BEAM processes never share a mutable enumerator
    /// decision: owns generation-tagged Fable.Actor timers to keep scheduling and disposal portable across targets
    /// invariant: only the latest OnNext generation emits after the quiet period
    let debounce (msecs: int) (source: IAsyncObservable<'TSource>) : IAsyncObservable<'TSource> =
        let subscribeAsync (aobv: IAsyncObserver<'TSource>) =
            let safeObv, autoDetach = autoDetachObserver aobv

            let cancelTimers timers =
                timers
                |> Map.iter (fun _ timer -> Actor.cancelTimer timer)

            let agent =
                Actor.spawn (fun inbox ->
                    let rec messageLoop currentIndex timers stopped =
                        actor {
                            let! message = inbox.Receive()

                            match message with
                            | Schedule _ when stopped -> return! messageLoop currentIndex timers stopped
                            | Schedule notification ->
                                let generation = currentIndex + 1

                                match notification with
                                | OnNext value ->
                                    let timer = Actor.schedule msecs (fun () -> inbox.Post(Emit(value, generation)))

                                    return! messageLoop generation (Map.add generation timer timers) false
                                | OnError ex ->
                                    cancelTimers timers
                                    do! safeObv.OnErrorAsync ex
                                    return! messageLoop generation Map.empty true
                                | OnCompleted ->
                                    cancelTimers timers
                                    do! safeObv.OnCompletedAsync()
                                    return! messageLoop generation Map.empty true
                            | Emit(value, generation) ->
                                let remaining = Map.remove generation timers

                                if not stopped && generation = currentIndex then
                                    do! safeObv.OnNextAsync value

                                return! messageLoop currentIndex remaining stopped
                            | Dispose ->
                                cancelTimers timers
                                return! messageLoop currentIndex Map.empty true
                        }

                    messageLoop -1 Map.empty false)

            async {
                let obv (n: Notification<'TSource>) = async { agent.Post(Schedule n) }

                let! dispose =
                    AsyncObserver obv
                    |> source.SubscribeAsync
                    |> autoDetach

                let cancel () =
                    async {
                        agent.Post Dispose
                        do! dispose.DisposeAsync()
                    }

                return AsyncDisposable.Create cancel
            }

        { new IAsyncObservable<'TSource> with
            member _.SubscribeAsync o = subscribeAsync o
        }

    /// Samples the observable sequence at each interval.
    let sample msecs (source: IAsyncObservable<'TSource>) : IAsyncObservable<'TSource> =
        let timer = Create.interval msecs msecs

        if msecs > 0 then
            Combine.withLatestFrom source timer
            |> Transform.map (fun (_, source) -> source)
        else
            source
