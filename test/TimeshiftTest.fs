module Tests.Timeshift

open Scriptorium.Quill
open Scriptorium.Nib.Assertion
open type Scriptorium.Quill.Test

open Fable.Reactive
open Tests.Utils

let tests =
    testList (
        "Timeshift",
        [
            testAsync (
                "delay preserves values and order",
                async {
                    // Arrange
                    let xs = fromNotification [ OnNext 1; OnNext 2; OnCompleted ]
                    let delayed = xs |> Reactive.delay 50
                    let obv = TestObserver<int>()

                    // Act
                    let! _sub = delayed.SubscribeAsync obv
                    do! obv.AwaitIgnore()

                    // Assert
                    let actual = obv.Notifications |> Seq.toList
                    let expected: Notification<int> list = [ OnNext 1; OnNext 2; OnCompleted ]
                    assertThat actual (isEqualTo expected)
                }
            )

            testAsync (
                "delay can resubscribe after dispose",
                async {
                    // Arrange - each subscription should get its own CTS
                    let xs = fromNotification [ OnNext 1; OnCompleted ]
                    let delayed = xs |> Reactive.delay 50

                    // Act - first subscription
                    let obv1 = TestObserver<int>()
                    let! sub1 = delayed.SubscribeAsync obv1
                    do! obv1.AwaitIgnore()
                    do! sub1.DisposeAsync()

                    // Second subscription should work (not broken by first dispose)
                    let obv2 = TestObserver<int>()
                    let! _sub2 = delayed.SubscribeAsync obv2
                    do! obv2.AwaitIgnore()

                    // Assert
                    let actual = obv2.Notifications |> Seq.toList
                    let expected: Notification<int> list = [ OnNext 1; OnCompleted ]
                    assertThat actual (isEqualTo expected)
                }
            )
        ]
    )
