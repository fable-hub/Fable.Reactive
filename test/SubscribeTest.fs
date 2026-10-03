module Tests.Subscribe

open Scriptorium.Quill
open Scriptorium.Nib.Assertion
open type Scriptorium.Quill.Test

open Fable.Reactive
open Tests.Utils

let tests =
    testList (
        "Subscribe",
        [
            testAsync (
                "subscribeAsync terminates a composed pipeline with values and completion",
                async {
                    let observer = TestObserver<int>()

                    let! subscription =
                        fromNotification [ OnNext 1; OnNext 2; OnNext 3; OnCompleted ]
                        |> Reactive.map (fun x -> x + 1)
                        |> Reactive.filter (fun x -> x < 4)
                        |> Reactive.subscribeAsync observer

                    do! observer.WaitUntil(List.exists isOnCompleted)
                    assertThat (observer.Notifications |> Seq.toList) (isEqualTo [ OnNext 2; OnNext 3; OnCompleted ])
                    do! subscription.DisposeAsync()
                }
            )
            testAsync (
                "subscribeAsync forwards source errors",
                async {
                    let observer = TestObserver<int>()
                    let error = System.InvalidOperationException("subscription error")

                    let! subscription =
                        Reactive.fail<int> error
                        |> Reactive.subscribeAsync observer

                    do! observer.WaitUntil(List.exists isOnError)

                    let messages =
                        observer.Notifications
                        |> Seq.choose (function
                            | OnError ex -> Some ex.Message
                            | _ -> None)
                        |> Seq.toList

                    assertThat messages (isEqualTo [ "subscription error" ])
                    do! subscription.DisposeAsync()
                }
            )
            testAsync (
                "subscribeAsync returns the source subscription for disposal",
                async {
                    let mutable disposed = false

                    let source =
                        Reactive.create (fun (_: IAsyncObserver<int>) ->
                            async { return AsyncDisposable.Create(fun () -> async { disposed <- true }) })

                    let! subscription =
                        source
                        |> Reactive.subscribeAsync (TestObserver<int>())

                    assertThat disposed isFalse
                    do! subscription.DisposeAsync()
                    assertThat disposed isTrue
                }
            )
        ]
    )
