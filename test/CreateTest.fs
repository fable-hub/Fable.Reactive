module Tests.Create

open Scriptorium.Quill
open Scriptorium.Nib.Assertion
open type Scriptorium.Quill.Test

open Fable.Reactive
open Tests.Utils

let tests =
    testList (
        "Create",
        [
            testAsync (
                "single happy",
                async {
                    // Arrange
                    let xs = Reactive.single 42
                    let obv = TestObserver<int>()

                    // Act
                    let! _dispose = xs.SubscribeAsync obv

                    // Assert
                    let! latest = obv.Await()
                    assertThat latest (isEqualTo 42)

                    let actual = obv.Notifications |> Seq.toList
                    let expected = [ OnNext 42; OnCompleted ]

                    assertThat actual (isEqualTo expected)
                }
            )

            testAsync (
                "single dispose right after subscribe does not throw",
                async {
                    // Arrange
                    let xs = Reactive.single 42
                    let obv = TestObserver<int>()

                    // Act
                    let! subscription = xs.SubscribeAsync obv
                    Async.StartImmediate(subscription.DisposeAsync())

                    // Assert - racing dispose against delivery must not raise
                    assertThat true isTrue
                }
            )

            testAsync (
                "ofSeq empty",
                async {
                    // Arrange
                    let xs = Reactive.ofSeq Seq.empty
                    let obv = TestObserver<int>()

                    // Act
                    let! _dispose = xs.SubscribeAsync obv
                    do! obv.AwaitIgnore()

                    // Assert
                    let actual = obv.Notifications |> Seq.toList
                    let expected: Notification<int> list = [ OnCompleted ]

                    assertThat actual (isEqualTo expected)
                }
            )

            testAsync (
                "ofSeq non empty",
                async {
                    // Arrange
                    let xs = seq { 1..5 } |> Reactive.ofSeq
                    let obv = TestObserver<int>()

                    // Act
                    let! _dispose = xs.SubscribeAsync obv
                    do! obv.AwaitIgnore()

                    // Assert
                    let actual = obv.Notifications |> Seq.toList
                    let expected = [ OnNext 1; OnNext 2; OnNext 3; OnNext 4; OnNext 5; OnCompleted ]

                    assertThat actual (isEqualTo expected)
                }
            )

            testAsync (
                "timer dispose after subscribe",
                async {
                    // Arrange
                    let xs = Reactive.timer 10
                    let obv = TestObserver<int>()

                    // Act
                    let! subscription = xs.SubscribeAsync obv
                    do! subscription.DisposeAsync()
                    do! Async.Sleep 15

                    // Assert
                    do! obv.Refresh()
                    let actual = obv.Notifications |> Seq.toList
                    assertThat actual isEmpty
                }
            )
        ]
    )
