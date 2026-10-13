namespace Fable.Reactive

open Fable.Actor
open Fable.Actor.Types

open Fable.Reactive.Core

module internal Subjects =
    type private SingleSubjectMsg<'TSource> =
        | Notify of Notification<'TSource>
        | Subscribe of IAsyncObserver<'TSource>

    /// A cold stream that only supports a single subscriber.
    /// Notifications arriving before subscription are buffered and replayed.
    ///
    /// decision: buffers before subscription because producer and subscriber startup order differs across targets
    /// invariant: buffered notifications replay in arrival order before live forwarding begins
    let singleSubject<'TSource> () : IAsyncObserver<'TSource> * IAsyncObservable<'TSource> =
        let actor =
            Actor.spawn (fun inbox ->
                let rec waitForSubscriber (buffer: Notification<'TSource> list) =
                    actor {
                        let! msg = inbox.Receive()

                        match msg with
                        | Notify n -> return! waitForSubscriber (buffer @ [ n ])
                        | Subscribe obv ->
                            // Replay buffered notifications
                            for n in buffer do
                                do! deliverNotification obv n

                            return! forwarding obv
                    }

                and forwarding (obv: IAsyncObserver<'TSource>) =
                    actor {
                        let! msg = inbox.Receive()

                        match msg with
                        | Notify n ->
                            do! deliverNotification obv n
                            return! forwarding obv
                        | Subscribe _ -> failwith "singleSubject: Already subscribed"
                    }

                and deliverNotification (obv: IAsyncObserver<'TSource>) (n: Notification<'TSource>) : Async<unit> =
                    async {
                        match n with
                        | OnNext x ->
                            try
                                do! obv.OnNextAsync x
                            with ex ->
                                do! obv.OnErrorAsync ex
                        | OnError e -> do! obv.OnErrorAsync e
                        | OnCompleted -> do! obv.OnCompletedAsync()
                    }

                waitForSubscriber [])

        let subscribeAsync (aobv: IAsyncObserver<'TSource>) : Async<IReactiveDisposable> =
            let sobv = safeObserver aobv AsyncDisposable.Empty
            actor.Post(Subscribe sobv)

            async {
                let cancel () = async { () }
                return AsyncDisposable.Create cancel
            }

        let obv (n: Notification<'TSource>) = async { actor.Post(Notify n) }

        let obs =
            { new IAsyncObservable<'TSource> with
                member __.SubscribeAsync o = subscribeAsync o
            }

        AsyncObserver obv :> IAsyncObserver<'TSource>, obs

    type private SubjectMsg<'T> =
        | Add of IAsyncObserver<'T>
        | Remove of int
        | Broadcast of Notification<'T>

    /// A mailbox subject is a subscribable mailbox. Each message is broadcasted to all subscribed observers.
    ///
    /// decision: owns subscribers inside the actor because BEAM processes cannot share a mutable collection
    /// invariant: acknowledged subscription changes precede subsequent broadcasts from the caller
    let mbSubject<'TSource> () : Actor<Notification<'TSource>> * IAsyncObservable<'TSource> =
        let owner =
            Actor.spawn (fun inbox ->
                let rec messageLoop subscribers nextId =
                    actor {
                        let! (cmd, reply: ReplyChannel<int>) = inbox.Receive()

                        match cmd with
                        | Add obv ->
                            reply.Reply nextId
                            return! messageLoop (subscribers @ [ nextId, obv ]) (nextId + 1)
                        | Remove id ->
                            reply.Reply id

                            return!
                                messageLoop
                                    (subscribers
                                     |> List.filter (fun (key, _) -> key <> id))
                                    nextId
                        | Broadcast n ->
                            for _, obv in subscribers do
                                match n with
                                | OnNext x -> do! obv.OnNextAsync x
                                | OnError err -> do! obv.OnErrorAsync err
                                | OnCompleted -> do! obv.OnCompletedAsync()

                            reply.Reply 0
                            return! messageLoop subscribers nextId
                    }

                messageLoop [] 0)

        let mb =
            Actor.spawn (fun inbox ->
                let rec loop () =
                    actor {
                        let! n = inbox.Receive()
                        Actor.cast owner (Broadcast n)
                        return! loop ()
                    }

                loop ())

        let subscribeAsync (aobv: IAsyncObserver<'TSource>) : Async<IReactiveDisposable> =
            async {
                let sobv = safeObserver aobv AsyncDisposable.Empty
                let! id = Actor.asyncCall owner (Add sobv)

                let cancel () =
                    async {
                        let! _ = Actor.asyncCall owner (Remove id)
                        return ()
                    }

                return AsyncDisposable.Create cancel
            }

        mb,
        { new IAsyncObservable<'TSource> with
            member __.SubscribeAsync o = subscribeAsync o
        }

    /// A stream is both an observable sequence as well as an observer.
    /// Each notification is broadcasted to all subscribed observers.
    let subject<'TSource> () : IAsyncObserver<'TSource> * IAsyncObservable<'TSource> =
        let actor, obs = mbSubject<'TSource>()

        let obv =
            { new IAsyncObserver<'TSource> with
                member this.OnNextAsync x = async { OnNext x |> actor.Post }
                member this.OnErrorAsync err = async { OnError err |> actor.Post }
                member this.OnCompletedAsync() = async { OnCompleted |> actor.Post }
            }

        obv, obs
