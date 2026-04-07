namespace Fable.Reactive

open System.Collections.Generic
open Fable.Actor

open Fable.Reactive.Core

module internal Subjects =
    type private SingleSubjectMsg<'TSource> =
        | Notify of Notification<'TSource>
        | Subscribe of IAsyncObserver<'TSource>

    /// A cold stream that only supports a single subscriber.
    /// Notifications arriving before subscription are buffered and replayed.
    let singleSubject<'TSource> () : IAsyncObserver<'TSource> * IAsyncObservable<'TSource> =
        let actor =
            Actor.spawn (fun inbox ->
                let rec waitForSubscriber (buffer: Notification<'TSource> list) =
                    async {
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
                    async {
                        let! msg = inbox.Receive()

                        match msg with
                        | Notify n ->
                            do! deliverNotification obv n
                            return! forwarding obv
                        | Subscribe _ -> failwith "singleSubject: Already subscribed"
                    }

                and deliverNotification (obv: IAsyncObserver<'TSource>) (n: Notification<'TSource>) =
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
                member __.SubscribeAsync o = subscribeAsync o }

        AsyncObserver obv :> IAsyncObserver<'TSource>, obs

    /// A mailbox subject is a subscribable mailbox. Each message is broadcasted to all subscribed observers.
    let mbSubject<'TSource> () : Actor<Notification<'TSource>> * IAsyncObservable<'TSource> =
        let obvs = new List<IAsyncObserver<'TSource>>()

        let mb =
            Actor.spawn (fun inbox ->
                let rec messageLoop () =
                    async {
                        let! n = inbox.Receive()

                        match n with
                        | OnNext x ->
                            for aobv in obvs do
                                do! aobv.OnNextAsync x

                        | OnError err ->
                            for aobv in obvs do
                                do! aobv.OnErrorAsync err

                        | OnCompleted ->
                            for aobv in obvs do
                                do! aobv.OnCompletedAsync()

                        return! messageLoop ()
                    }

                messageLoop ())

        let subscribeAsync (aobv: IAsyncObserver<'TSource>) : Async<IReactiveDisposable> =
            async {
                let sobv = safeObserver aobv AsyncDisposable.Empty
                obvs.Add sobv

                let cancel () = async { obvs.Remove sobv |> ignore }
                return AsyncDisposable.Create cancel
            }

        mb,
        { new IAsyncObservable<'TSource> with
            member __.SubscribeAsync o = subscribeAsync o }

    /// A stream is both an observable sequence as well as an observer.
    /// Each notification is broadcasted to all subscribed observers.
    let subject<'TSource> () : IAsyncObserver<'TSource> * IAsyncObservable<'TSource> =
        let actor, obs = mbSubject<'TSource> ()

        let obv =
            { new IAsyncObserver<'TSource> with
                member this.OnNextAsync x = async { OnNext x |> actor.Post }
                member this.OnErrorAsync err = async { OnError err |> actor.Post }
                member this.OnCompletedAsync() = async { OnCompleted |> actor.Post } }

        obv, obs

    type private SafeQueue<'a> () =
        let agent = 
            MailboxProcessor<QueueMessage<'a>>.Start 
            <| fun inbox ->
                let rec loop (queue: Queue<'a>) =
                    async {
                        let! message = inbox.Receive()
                        match message with
                        | Enqueue item -> 
                            queue.Enqueue item
                            return! loop queue
                        | Dequeue channel ->
                            let item = queue.Dequeue()
                            channel.Reply item
                            return! loop queue
                        | IterAsync f ->
                            for msg in queue do 
                              do! f msg
                            return! loop queue
                        | Count channel-> 
                           channel.Reply (queue.Count)
                           return! loop queue
                    }
                loop <| new Queue<'a>()

        member _.Count () = agent.PostAndReply Count
        member _.Enqueue item = agent.Post <| Enqueue item
        member _.Dequeue () = agent.PostAndReply Dequeue
        member _.IterAsync f = agent.Post(IterAsync f)

    and private QueueMessage<'a> =
        | Count of AsyncReplyChannel<int>
        | Enqueue of 'a
        | Dequeue of AsyncReplyChannel<'a>
        | IterAsync of ('a -> Async<unit>)

    let mbReplaySubject<'TSource> (bufferSize : int) : MailboxProcessor<Notification<'TSource>> * IAsyncObservable<'TSource> =
        let obvs = new List<IAsyncObserver<'TSource>>()
        let cts = new CancellationTokenSource()
        let queue = new SafeQueue<Notification<'TSource>> ()
        let mb =
            MailboxProcessor.Start(
                fun inbox ->
                    let rec messageLoop _ =
                        async {
                            let! n = inbox.Receive()

                            match n with
                            | OnNext x ->
                                if queue.Count () < bufferSize then ()
                                else queue.Dequeue() |> ignore 
                                queue.Enqueue (Notification.OnNext x)

                                for aobv in obvs do
                                    do! aobv.OnNextAsync x

                            | OnError err ->
                                for aobv in obvs do
                                    queue.Enqueue (Notification.OnError err)
                                    do! aobv.OnErrorAsync err

                                cts.Cancel()

                            | OnCompleted ->
                                for aobv in obvs do
                                    do! aobv.OnCompletedAsync()
                                    queue.Enqueue Notification.OnCompleted

                                cts.Cancel()

                            return! messageLoop ()
                        }

                    messageLoop ()
                , cts.Token
            )

        let subscribeAsync (aobv: IAsyncObserver<'TSource>) : Async<IAsyncRxDisposable> =
            async {
                let sobv = safeObserver aobv AsyncDisposable.Empty
                queue.IterAsync (function 
                    | Notification.OnNext n -> sobv.OnNextAsync n
                    | Notification.OnError exn -> sobv.OnErrorAsync exn
                    | Notification.OnCompleted -> sobv.OnCompletedAsync ())

                obvs.Add sobv

                let cancel () = async { obvs.Remove sobv |> ignore }
                return AsyncDisposable.Create cancel
            }

        mb,
        { new IAsyncObservable<'TSource> with
            member __.SubscribeAsync o = subscribeAsync o }

    let replaySubject<'TSource> (bufferSize : int) : IAsyncObserver<'TSource> * IAsyncObservable<'TSource> =
        if bufferSize < 1 then
          invalidArg (nameof(bufferSize)) $"Buffer size should be one or greater but it was {bufferSize}."

        let mb, obs = mbReplaySubject<'TSource> bufferSize

        let obv =
            { new IAsyncObserver<'TSource> with
                member this.OnNextAsync x = async { OnNext x |> mb.Post }
                member this.OnErrorAsync err = async { OnError err |> mb.Post }
                member this.OnCompletedAsync() = async { OnCompleted |> mb.Post } }

        obv, obs
