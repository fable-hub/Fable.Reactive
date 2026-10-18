module Tests.Retry

open Scriptorium.Quill
open Scriptorium.Nib.Assertion
open type Scriptorium.Quill.Test

open Fable.Actor
open Fable.Actor.Types
open Fable.Reactive
open Tests.Utils

let tests =
    testList (
        "Retry",
        [
            testAsync (
                "zero retries forwards the first error",
                async {
                    let error = System.Exception "failed"

                    let source =
                        fromNotification [ OnNext 1; OnError error ]
                        |> Reactive.retry 0

                    let observer = TestObserver<int>()
                    let! _sub = source.SubscribeAsync observer
                    do! observer.AwaitIgnore()
                    assertThat (List.ofSeq observer.Notifications) (isEqualTo [ OnNext 1; OnError error ])
                }
            )
            testAsync (
                "positive count allows exactly that many retries",
                async {
                    let error = System.Exception "failed"

                    let source =
                        fromNotification [ OnNext 1; OnError error ]
                        |> Reactive.retry 2

                    let observer = TestObserver<int>()
                    let! _sub = source.SubscribeAsync observer
                    do! observer.AwaitIgnore()

                    assertThat
                        (List.ofSeq observer.Notifications)
                        (isEqualTo [ OnNext 1; OnNext 1; OnNext 1; OnError error ])
                }
            )
            testAsync (
                "completion after a retry stops further subscriptions",
                async {
                    let error = System.Exception "transient"

                    // decision: owns attempt count in an actor so BEAM subscriptions observe one shared count
                    let attempts =
                        Actor.spawn (fun inbox ->
                            let rec loop count =
                                actor {
                                    let! (_, reply: ReplyChannel<int>) = inbox.Receive()
                                    let next = count + 1
                                    reply.Reply next
                                    return! loop next
                                }

                            loop 0)

                    let source =
                        Reactive.create (fun observer ->
                            async {
                                let! attempt = Actor.asyncCall attempts 0

                                if attempt = 1 then
                                    do! observer.OnNextAsync 1
                                    do! observer.OnErrorAsync error
                                else
                                    do! observer.OnNextAsync 2
                                    do! observer.OnCompletedAsync()

                                return AsyncDisposable.Empty
                            })
                        |> Reactive.retry 2

                    let observer = TestObserver<int>()
                    let! _sub = source.SubscribeAsync observer
                    do! observer.AwaitIgnore()
                    assertThat (List.ofSeq observer.Notifications) (isEqualTo [ OnNext 1; OnNext 2; OnCompleted ])
                }
            )
            testAsync (
                "each subscriber starts with a fresh retry budget",
                async {
                    let error = System.Exception "failed"

                    let source =
                        fromNotification [ OnNext 1; OnError error ]
                        |> Reactive.retry 1

                    for _ in 1..2 do
                        let observer = TestObserver<int>()
                        let! _sub = source.SubscribeAsync observer
                        do! observer.AwaitIgnore()
                        assertThat (List.ofSeq observer.Notifications) (isEqualTo [ OnNext 1; OnNext 1; OnError error ])
                }
            )
        ]
    )
