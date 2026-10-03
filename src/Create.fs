namespace Fable.Reactive

open System.Threading

open Fable.Reactive.Core

open System.Runtime.CompilerServices

[<assembly: InternalsVisibleTo("Tests")>]
do ()


[<RequireQualifiedAccess>]
module internal Create =

    /// Creates an async observable (`AsyncObservable{'TSource}`) from the
    /// given subscribe function.
    let create (subscribe: IAsyncObserver<'TSource> -> Async<IReactiveDisposable>) : IAsyncObservable<'TSource> =
        { new IAsyncObservable<'TSource> with
            member _.SubscribeAsync o = subscribe o
        }

    // Creates a cold observable from an asynchronous worker.
    //
    // decision: starts a distinct cancellable worker for each subscription so subscribers never share execution
    // assumption: workers cooperate with async cancellation — disposal cannot force blocking foreign code
    let ofAsyncWorker
        (worker: IAsyncObserver<'TSource> -> CancellationToken -> Async<unit>)
        : IAsyncObservable<'TSource> =
        let subscribeAsync (aobv: IAsyncObserver<'TSource>) : Async<IReactiveDisposable> =
            let disposable, token = canceller ()
            let safeObv = safeObserver aobv disposable

            async {
                Async.Start'(worker safeObv token, token)
                return disposable
            }

        { new IAsyncObservable<'TSource> with
            member _.SubscribeAsync o = subscribeAsync o
        }

    /// Returns the async observable sequence whose single element is the result of the given async workflow.
    let ofAsync (workflow: Async<'TSource>) : IAsyncObservable<'TSource> =
        ofAsyncWorker (fun obv _ ->
            async {
                try
                    let! result = workflow
                    do! obv.OnNextAsync result
                    do! obv.OnCompletedAsync()
                with ex ->
                    do! obv.OnErrorAsync ex
            })

    /// Returns an observable sequence containing the single specified element.
    let single (value: 'TSource) =
        let subscribeAsync (aobv: IAsyncObserver<'TSource>) : Async<IReactiveDisposable> =
            let safeObv = safeObserver aobv AsyncDisposable.Empty

            async {
                do! safeObv.OnNextAsync value
                do! safeObv.OnCompletedAsync()
                return AsyncDisposable.Empty
            }

        { new IAsyncObservable<'TSource> with
            member _.SubscribeAsync o = subscribeAsync o
        }

    /// Returns an observable sequence with no elements.
    let inline empty<'TSource> () : IAsyncObservable<'TSource> =
        let subscribeAsync (aobv: IAsyncObserver<_>) : Async<IReactiveDisposable> =
            async {
                do! aobv.OnCompletedAsync()
                return AsyncDisposable.Empty
            }

        { new IAsyncObservable<'TSource> with
            member _.SubscribeAsync o = subscribeAsync o
        }

    /// Returns an empty observable sequence that never completes.
    let inline never<'TSource> () : IAsyncObservable<'TSource> =
        let subscribeAsync (_: IAsyncObserver<_>) : Async<IReactiveDisposable> = async { return AsyncDisposable.Empty }

        { new IAsyncObservable<'TSource> with
            member _.SubscribeAsync o = subscribeAsync o
        }

    /// Returns the observable sequence that terminates exceptionally
    /// with the specified exception.
    let inline fail<'TSource> (error: exn) : IAsyncObservable<'TSource> =
        ofAsyncWorker (fun obv _ -> obv.OnErrorAsync error)

    /// Returns the async observable sequence whose elements are pulled
    /// from the given enumerable sequence.
    let ofSeq (xs: seq<'TSource>) : IAsyncObservable<'TSource> =
        ofAsyncWorker (fun obv _token ->
            async {
                for x in xs do
                    try
                        do! obv.OnNextAsync x
                    with ex ->
                        do! obv.OnErrorAsync ex

                do! obv.OnCompletedAsync()
            })

    // Returns an observable sequence that invokes the specified factory function whenever a new observer subscribes.
    let defer (factory: unit -> IAsyncObservable<'TSource>) : IAsyncObservable<'TSource> =
        let subscribeAsync (aobv: IAsyncObserver<'TSource>) : Async<IReactiveDisposable> =
            async {
                let result =
                    try
                        factory ()
                    with ex ->
                        fail ex

                return! result.SubscribeAsync aobv
            }

        { new IAsyncObservable<'TSource> with
            member _.SubscribeAsync o = subscribeAsync o
        }

    /// Returns an observable sequence that triggers the increasing sequence starting with 0 after the given msecs, and
    /// the after each period.
    let interval (msecs: int) (period: int) : IAsyncObservable<int> =
        let subscribeAsync (aobv: IAsyncObserver<int>) : Async<IReactiveDisposable> =
            let cancel, token = canceller ()

            async {
                let rec handler (msecs: int) next =
                    async {
                        do! Async.Sleep msecs
                        do! aobv.OnNextAsync next

                        if period > 0 then
                            return! handler period (next + 1)
                        else
                            do! aobv.OnCompletedAsync()
                    }

                Async.Start'(handler msecs 0, token)
                return cancel
            }

        { new IAsyncObservable<int> with
            member _.SubscribeAsync o = subscribeAsync o
        }

    /// Returns an observable sequence that triggers the value 0
    /// after the given duetime in milliseconds.
    let timer (dueTime: int) : IAsyncObservable<int> = interval dueTime 0
