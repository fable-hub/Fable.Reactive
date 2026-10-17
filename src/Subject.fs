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

    type private ReplaySubjectMsg<'T> =
        | AddReplay of IAsyncObserver<'T>
        | RemoveReplay of int
        | BroadcastReplay of Notification<'T>

    /// Replays the most recent values to each new subscriber, then forwards live notifications.
    /// Late subscribers also receive the terminal notification after the retained values.
    ///
    /// decision: keeps subscribers, retained values, and terminal state in one actor to order replay before live delivery
    /// invariant: the buffer contains at most bufferSize values, newest first, and never changes after termination
    /// invariant: a completed OnNextAsync updates the buffer before a subsequent SubscribeAsync reads it
    let replaySubject<'TSource> (bufferSize: int) : IAsyncObserver<'TSource> * IAsyncObservable<'TSource> =
        if bufferSize < 1 then
            invalidArg (nameof bufferSize) "Buffer size must be at least one."

        let deliver (obv: IAsyncObserver<'TSource>) (notification: Notification<'TSource>) =
            match notification with
            | OnNext value -> obv.OnNextAsync value
            | OnError error -> obv.OnErrorAsync error
            | OnCompleted -> obv.OnCompletedAsync()

        let owner =
            Actor.spawn (fun inbox ->
                let rec loop
                    (subscribers: (int * IAsyncObserver<'TSource>) list)
                    nextId
                    (buffer: 'TSource list)
                    (terminal: Notification<'TSource> option)
                    =
                    actor {
                        let! (cmd, reply: ReplyChannel<int option>) = inbox.Receive()

                        match cmd with
                        | AddReplay obv ->
                            for value in List.rev buffer do
                                do! obv.OnNextAsync value

                            match terminal with
                            | Some notification ->
                                do! deliver obv notification
                                reply.Reply None
                                return! loop subscribers nextId buffer terminal
                            | None ->
                                reply.Reply(Some nextId)
                                return! loop (subscribers @ [ nextId, obv ]) (nextId + 1) buffer terminal
                        | RemoveReplay id ->
                            reply.Reply None

                            return!
                                loop
                                    (subscribers
                                     |> List.filter (fun (key, _) -> key <> id))
                                    nextId
                                    buffer
                                    terminal
                        | BroadcastReplay notification ->
                            match terminal with
                            | Some _ ->
                                reply.Reply None
                                return! loop subscribers nextId buffer terminal
                            | None ->
                                for _, obv in subscribers do
                                    do! deliver obv notification

                                reply.Reply None

                                match notification with
                                | OnNext value ->
                                    return! loop subscribers nextId (List.truncate bufferSize (value :: buffer)) None
                                | OnError _
                                | OnCompleted -> return! loop [] nextId buffer (Some notification)
                    }

                loop [] 0 [] None)

        let subscribeAsync (aobv: IAsyncObserver<'TSource>) : Async<IReactiveDisposable> =
            async {
                let sobv = safeObserver aobv AsyncDisposable.Empty
                let! id = Actor.asyncCall owner (AddReplay sobv)

                let cancel () =
                    async {
                        match id with
                        | Some key ->
                            let! _ = Actor.asyncCall owner (RemoveReplay key)
                            return ()
                        | None -> return ()
                    }

                return AsyncDisposable.Create cancel
            }

        let post notification =
            async {
                let! _ = Actor.asyncCall owner (BroadcastReplay notification)
                return ()
            }

        let observer = AsyncObserver post :> IAsyncObserver<'TSource>

        observer,
        { new IAsyncObservable<'TSource> with
            member _.SubscribeAsync obv = subscribeAsync obv
        }
