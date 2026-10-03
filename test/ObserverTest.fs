module Tests.Observer

open Scriptorium.Quill
open Scriptorium.Nib.Assertion
open type Scriptorium.Quill.Test

open Fable.Reactive
open Fable.Reactive.Core
open Tests.Utils

exception MyError of string

let tests =
    testList (
        "Observer",
        [
            testAsync (
                "safe observer empty sequence",
                async {
                    // Arrange
                    let xs = fromNotification Seq.empty
                    let obv = TestObserver<int>()
                    let safeObv = safeObserver obv AsyncDisposable.Empty

                    // Act
                    let! _dispose = xs.SubscribeAsync safeObv

                    // Assert
                    let actual = obv.Notifications |> Seq.toList
                    let expected: Notification<int> list = []

                    assertThat actual (isEqualTo expected)
                }
            )

            testAsync (
                "safe observer error sequence",
                async {
                    // Arrange
                    let error = MyError "error"
                    let xs = fromNotification [ OnError error ]
                    let obv = TestObserver<int>()
                    let safeObv = safeObserver obv AsyncDisposable.Empty

                    // Act
                    let! _dispose = xs.SubscribeAsync safeObv

                    try
                        do! obv.AwaitIgnore()
                    with _ ->
                        ()

                    // Assert
                    let actual = obv.Notifications |> Seq.toList
                    let expected: Notification<int> list = [ OnError error ]

                    assertThat actual (isEqualTo expected)
                }
            )

            testAsync (
                "safe observer happy",
                async {
                    // Arrange
                    let xs = Reactive.ofSeq [ 1..3 ]
                    let obv = TestObserver<int>()
                    let safeObv = safeObserver obv AsyncDisposable.Empty

                    // Act
                    let! _dispose = xs.SubscribeAsync safeObv
                    do! obv.AwaitIgnore()

                    // Assert
                    let actual = obv.Notifications |> Seq.toList
                    let expected = [ OnNext 1; OnNext 2; OnNext 3; OnCompleted ]

                    assertThat actual (isEqualTo expected)
                }
            )

            testAsync (
                "safe observer stops after completed",
                async {
                    // Arrange
                    let xs = fromNotification [ OnNext 1; OnCompleted; OnNext 2 ]
                    let obv = TestObserver<int>()
                    let safeObv = safeObserver obv AsyncDisposable.Empty

                    // Act
                    let! _dispose = xs.SubscribeAsync safeObv
                    do! obv.AwaitIgnore()

                    // Assert
                    let actual = obv.Notifications |> Seq.toList
                    let expected = [ OnNext 1; OnCompleted ]

                    assertThat actual (isEqualTo expected)
                }
            )

            testAsync (
                "safe observer stops after completed completed",
                async {
                    // Arrange
                    let xs = fromNotification [ OnNext 1; OnCompleted; OnCompleted ]
                    let obv = TestObserver<int>()
                    let safeObv = safeObserver obv AsyncDisposable.Empty

                    // Act
                    let! _dispose = xs.SubscribeAsync safeObv
                    do! obv.AwaitIgnore()

                    // Assert
                    let actual = obv.Notifications |> Seq.toList
                    let expected = [ OnNext 1; OnCompleted ]

                    assertThat actual (isEqualTo expected)
                }
            )

            testAsync (
                "safe observer stops after error",
                async {
                    // Arrange
                    let error = MyError "error"
                    let xs = fromNotification [ OnNext 1; OnError error; OnNext 2 ]
                    let obv = TestObserver<int>()
                    let safeObv = safeObserver obv AsyncDisposable.Empty

                    // Act
                    let! _dispose = xs.SubscribeAsync safeObv

                    try
                        do! obv.AwaitIgnore()
                    with _ ->
                        ()

                    // Assert
                    let actual = obv.Notifications |> Seq.toList
                    let expected = [ OnNext 1; OnError error ]

                    assertThat actual (isEqualTo expected)
                }
            )

            testAsync (
                "safe observer stops after error error",
                async {
                    // Arrange
                    let error = MyError "error"
                    let xs = fromNotification [ OnNext 1; OnError error; OnError error ]
                    let obv = TestObserver<int>()
                    let safeObv = safeObserver obv AsyncDisposable.Empty

                    // Act
                    let! _dispose = xs.SubscribeAsync safeObv

                    try
                        do! obv.AwaitIgnore()
                    with _ ->
                        ()

                    // Assert
                    let actual = obv.Notifications |> Seq.toList
                    let expected = [ OnNext 1; OnError error ]

                    assertThat actual (isEqualTo expected)
                }
            )

            testAsync (
                "auto-detach observer is disposing",
                async {
                    // Arrange
                    let obv = TestObserver<int>()
                    let mutable disposed = false

                    let subscribeAsync (aobv: IAsyncObserver<int>) : Async<IReactiveDisposable> =
                        async {
                            let worker =
                                async {
                                    for x in [ 1..5 ] do
                                        do! aobv.OnNextAsync x
                                }

                            Async.Start' worker
                            let cancel () = async { disposed <- true }
                            return AsyncDisposable.Create cancel
                        }

                    let source =
                        { new IAsyncObservable<int> with
                            member _.SubscribeAsync o = subscribeAsync o
                        }

                    let xs = source |> Reactive.take 4

                    // Act
                    let! _dispose = xs.SubscribeAsync obv
                    do! obv.AwaitIgnore()

                    // Give dispose logic a run on the loop.
                    do! Async.Sleep 10

                    // Assert
                    let actual = obv.Notifications |> Seq.toList
                    let expected = [ OnNext 1; OnNext 2; OnNext 3; OnNext 4; OnCompleted ]

                    assertThat disposed isTrue
                    assertThat actual (isEqualTo expected)
                }
            )
        ]
    )
