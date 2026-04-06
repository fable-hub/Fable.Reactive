namespace Fable.Reactive

open System
open Fable.Actor
open Fable.Reactive.Core

[<RequireQualifiedAccess>]
module internal Transform =
    /// Returns an observable sequence whose elements are the result of invoking the async mapper function on each
    /// element of the source.
    let transformAsync<'TSource, 'TResult>
        (nextAsync: ('TResult -> Async<unit>) -> 'TSource -> Async<unit>)
        (source: IAsyncObservable<'TSource>)
        : IAsyncObservable<'TResult> =
        let subscribeAsync (aobv: IAsyncObserver<'TResult>) : Async<IReactiveDisposable> =
            { new IAsyncObserver<'TSource> with
                member _.OnNextAsync x = nextAsync aobv.OnNextAsync x
                member _.OnErrorAsync err = aobv.OnErrorAsync err
                member _.OnCompletedAsync() = aobv.OnCompletedAsync() }
            |> source.SubscribeAsync

        { new IAsyncObservable<'TResult> with
            member _.SubscribeAsync o = subscribeAsync o }

    /// Returns an observable sequence whose elements are the result of invoking the async mapper function on each
    /// element of the source.
    let mapAsync (mapperAsync: 'TSource -> Async<'TResult>) : AsyncStream<'TSource, 'TResult> =
        transformAsync (fun next x ->
            async {
                let! b = mapperAsync x
                return! next b
            })

    /// Returns an observable sequence whose elements are the result of invoking the mapper function on each element of
    /// the source.
    let map (mapper: 'TSource -> 'TResult) : AsyncStream<'TSource, 'TResult> =
        transformAsync (fun next x -> next (mapper x))

    /// Returns an observable sequence whose elements are the result of invoking the async mapper function by
    /// incorporating the element's index on each element of the source.
    let mapiAsync (mapper: 'TSource * int -> Async<'TResult>) : AsyncStream<'TSource, 'TResult> =
        Combine.zipSeq infinite >> mapAsync mapper

    /// Returns an observable sequence whose elements are the result of invoking the mapper function and incorporating
    /// the element's index on each element of the source.
    let mapi (mapper: 'TSource * int -> 'TResult) : AsyncStream<'TSource, 'TResult> =
        Combine.zipSeq infinite >> map mapper

    /// Projects each element of an observable sequence into an observable sequence and merges the resulting observable
    /// sequences back into one observable sequence.
    let flatMap (mapper: 'TSource -> IAsyncObservable<'TResult>) : AsyncStream<'TSource, 'TResult> =
        map mapper >> Combine.mergeInner 0

    /// Projects each element of an observable sequence into an observable sequence by incorporating the element's index
    /// on each element of the source. Merges the resulting observable sequences back into one observable sequence.
    let flatMapi (mapper: 'TSource * int -> IAsyncObservable<'TResult>) : AsyncStream<'TSource, 'TResult> =
        mapi mapper >> Combine.mergeInner 0

    /// Asynchronously projects each element of an observable sequence into an observable sequence and merges the
    /// resulting observable sequences back into one observable sequence.
    let flatMapAsync (mapper: 'TSource -> Async<IAsyncObservable<'TResult>>) : AsyncStream<'TSource, 'TResult> =
        mapAsync mapper >> Combine.mergeInner 0

    /// Asynchronously projects each element of an observable sequence into an observable sequence by incorporating the
    /// element's index on each element of the source. Merges the resulting observable sequences back into one
    /// observable sequence.
    let flatMapiAsync
        (mapperAsync: 'TSource * int -> Async<IAsyncObservable<'TResult>>)
        : AsyncStream<'TSource, 'TResult> =
        mapiAsync mapperAsync >> Combine.mergeInner 0

    let concatMap (mapper: 'TSource -> IAsyncObservable<'TResult>) : AsyncStream<'TSource, 'TResult> =
        map mapper >> Combine.mergeInner 1

    type InnerSubscriptionCmd<'T> =
        | InnerObservable of IAsyncObservable<'T>
        | InnerCompleted of int
        | Completed
        | Dispose

    /// Transforms an observable sequence of observable sequences into an observable sequence producing values only from
    /// the most recent observable sequence.
    let switchLatest (source: IAsyncObservable<IAsyncObservable<'TSource>>) : IAsyncObservable<'TSource> =
        let subscribeAsync (aobv: IAsyncObserver<'TSource>) =
            let safeObv, autoDetach = autoDetachObserver aobv

            let innerAgent =
                let mutable agentRef: Actor<InnerSubscriptionCmd<'TSource>> option = None

                let obv (id: int) =
                    { new IAsyncObserver<'TSource> with
                        member _.OnNextAsync x = safeObv.OnNextAsync x
                        member _.OnErrorAsync err = safeObv.OnErrorAsync err

                        member _.OnCompletedAsync() =
                            async {
                                match agentRef with
                                | Some a -> a.Post(InnerCompleted id)
                                | None -> ()
                            } }

                let agent =
                    Actor.spawn (fun inbox ->
                        let rec messageLoop (current: IReactiveDisposable option, isStopped, currentId) =
                            async {
                                let! cmd = inbox.Receive()

                                let! current', isStopped', currentId' =
                                    async {
                                        match cmd with
                                        | InnerObservable xs ->
                                            let nextId = currentId + 1

                                            if current.IsSome then
                                                do! current.Value.DisposeAsync()

                                            let! inner = xs.SubscribeAsync(obv nextId)
                                            return Some inner, isStopped, nextId
                                        | InnerCompleted idx ->
                                            if isStopped && idx = currentId then
                                                do! safeObv.OnCompletedAsync()
                                                return (None, true, currentId)
                                            else
                                                return (current, isStopped, currentId)
                                        | Completed ->
                                            if current.IsNone then
                                                do! safeObv.OnCompletedAsync()

                                            return (current, true, currentId)
                                        | Dispose ->
                                            if current.IsSome then
                                                do! current.Value.DisposeAsync()

                                            return (None, true, currentId)
                                    }

                                return! messageLoop (current', isStopped', currentId')
                            }

                        messageLoop (None, false, 0))

                agentRef <- Some agent
                agent

            async {
                let obv (ns: Notification<IAsyncObservable<'TSource>>) =
                    async {
                        match ns with
                        | OnNext xs -> InnerObservable xs |> innerAgent.Post
                        | OnError e -> do! safeObv.OnErrorAsync e
                        | OnCompleted -> innerAgent.Post Completed
                    }

                let! dispose =
                    AsyncObserver obv
                    |> source.SubscribeAsync
                    |> autoDetach

                let cancel () =
                    async {
                        do! dispose.DisposeAsync()
                        innerAgent.Post Dispose
                    }

                return AsyncDisposable.Create cancel
            }

        { new IAsyncObservable<'TSource> with
            member _.SubscribeAsync o = subscribeAsync o }

    /// Asynchronosly transforms the items emitted by an source sequence into observable streams, and mirror those items
    /// emitted by the most-recently transformed observable sequence.
    let flatMapLatestAsync
        (mapperAsync: 'TSource -> Async<IAsyncObservable<'TResult>>)
        : AsyncStream<'TSource, 'TResult> =
        mapAsync mapperAsync >> switchLatest

    /// Transforms the items emitted by an source sequence into observable streams, and mirror those items emitted by
    /// the most-recently transformed observable sequence.
    let flatMapLatest (mapper: 'TSource -> IAsyncObservable<'TResult>) : AsyncStream<'TSource, 'TResult> =
        map mapper >> switchLatest

    /// Returns an observable sequence containing the first sequence's elements, followed by the elements of the handler
    /// sequence in case an exception occurred.
    let catch
        (handler: exn -> IAsyncObservable<'TSource>)
        (source: IAsyncObservable<'TSource>)
        : IAsyncObservable<'TSource> =
        let subscribeAsync (aobv: IAsyncObserver<'TSource>) =
            async {
                let mutable disposable = AsyncDisposable.Empty

                let rec action (source: IAsyncObservable<_>) =
                    async {
                        let _obv =
                            { new IAsyncObserver<'TSource> with
                                member _.OnNextAsync x = aobv.OnNextAsync x

                                member _.OnErrorAsync err =
                                    let nextSource = handler err
                                    action nextSource

                                member _.OnCompletedAsync() = aobv.OnCompletedAsync() }

                        do! disposable.DisposeAsync()
                        let! subscription = source.SubscribeAsync _obv
                        disposable <- subscription
                    }

                do! action source

                return AsyncDisposable.Create(fun () -> disposable.DisposeAsync())
            }

        { new IAsyncObservable<'TSource> with
            member _.SubscribeAsync o = subscribeAsync o }

    let retry (retryCount: int) (source: IAsyncObservable<'TSource>) =
        let mutable count = retryCount

        let factory exn =
            match count with
            | 0 -> Create.fail exn
            | _ ->
                count <- count - 1
                source

        catch factory source

    type Cmd =
        | Connect
        | Dispose

    /// Share a single subscription among multple observers. Returns a new Observable that multicasts (shares) the
    /// original Observable. As long as there is at least one Subscriber this Observable will be subscribed and emitting
    /// data. When all subscribers have unsubscribed it will unsubscribe from the source Observable.
    let share (source: IAsyncObservable<'TSource>) : IAsyncObservable<'TSource> =
        let dispatch, stream = Subjects.subject<'TSource> ()

        let mb =
            Actor.spawn (fun inbox ->
                let rec messageLoop (count: int) (subscription: IReactiveDisposable) =
                    async {
                        let! cmd = inbox.Receive()

                        let! count', subscription' =
                            async {
                                match cmd with
                                | Connect ->
                                    if count = 0 then
                                        let! disposable = source.SubscribeAsync dispatch
                                        return count + 1, disposable
                                    else
                                        return count + 1, subscription
                                | Dispose ->
                                    if count = 1 then
                                        do! subscription.DisposeAsync()
                                        return count - 1, AsyncDisposable.Empty
                                    else
                                        return count - 1, subscription
                            }

                        return! messageLoop count' subscription'
                    }

                messageLoop 0 AsyncDisposable.Empty)

        let subscribeAsync (aobv: IAsyncObserver<'TSource>) =
            async {
                mb.Post Connect

                let! disposable = stream.SubscribeAsync aobv

                let cancel () =
                    mb.Post Dispose
                    disposable.DisposeAsync()

                return AsyncDisposable.Create cancel
            }

        { new IAsyncObservable<'TSource> with
            member _.SubscribeAsync o = subscribeAsync o }

    let toObservable (source: IAsyncObservable<'TSource>) : IObservable<'TSource> =
        let mutable subscription: IReactiveDisposable = AsyncDisposable.Empty

        { new IObservable<'TSource> with
            member _.Subscribe obv =
                async {
                    let aobv = obv.ToAsyncObserver()
                    let! disposable = source.SubscribeAsync aobv
                    subscription <- disposable
                }
                |> Async.Start'

                subscription.ToDisposable() }
