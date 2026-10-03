module Tests.Merge

open Scriptorium.Quill
open Scriptorium.Nib.Assertion
open type Scriptorium.Quill.Test

open Fable.Reactive
open Tests.Utils

exception MyError of string

let tests =
    testList (
        "Merge",
        [
            testAsync (
                "merge non empty with empty",
                async {
                    // Arrange
                    let xs = seq { 1..5 } |> Reactive.ofSeq
                    let ys = Reactive.empty<int>()
                    let zs = Reactive.ofSeq [ xs; ys ] |> Reactive.mergeInner
                    let obv = TestObserver<int>()

                    // Act
                    let! _sub = zs.SubscribeAsync obv
                    let! latest = obv.Await()

                    // Assert
                    assertThat latest (isEqualTo 5)
                    let actual = obv.Notifications |> Seq.toList

                    let expected: Notification<int> list =
                        [ OnNext 1; OnNext 2; OnNext 3; OnNext 4; OnNext 5; OnCompleted ]

                    assertThat actual (isEqualTo expected)
                }
            )

            testAsync (
                "merge empty with non empty",
                async {
                    // Arrange
                    let xs = Reactive.empty<int>()
                    let ys = seq { 1..5 } |> Reactive.ofSeq
                    let zs = Reactive.ofSeq [ xs; ys ] |> Reactive.mergeInner
                    let obv = TestObserver<int>()

                    // Act
                    let! _sub = zs.SubscribeAsync obv
                    let! latest = obv.Await()

                    // Assert
                    assertThat latest (isEqualTo 5)
                    let actual = obv.Notifications |> Seq.toList

                    let expected: Notification<int> list =
                        [ OnNext 1; OnNext 2; OnNext 3; OnNext 4; OnNext 5; OnCompleted ]

                    assertThat actual (isEqualTo expected)
                }
            )

            testAsync (
                "merge error with error",
                async {
                    // Arrange
                    let error = MyError "error"
                    let xs = Reactive.fail error
                    let ys = Reactive.fail error
                    let zs = Reactive.ofSeq [ xs; ys ] |> Reactive.mergeInner
                    let obv = TestObserver<int>()

                    // Act
                    let! _sub = zs.SubscribeAsync obv

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
                "merge two",
                async {
                    // Arrange
                    let xs = seq { 1..3 } |> Reactive.ofSeq
                    let ys = seq { 4..5 } |> Reactive.ofSeq
                    let zs = Reactive.ofSeq [ xs; ys ] |> Reactive.mergeInner
                    let obv = TestObserver<int>()

                    // Act
                    let! _sub = zs.SubscribeAsync obv
                    do! obv.AwaitIgnore()

                    // Assert - interleaving is not deterministic, only membership is
                    let actual = obv.Notifications |> Seq.toList

                    assertThat
                        actual
                        (hasSize 6
                         >> containAll [ OnNext 1; OnNext 2; OnNext 3; OnNext 4; OnNext 5; OnCompleted ])
                }
            )

            testAsync (
                "merge delivers to an observer subscribed immediately",
                async {
                    // Arrange
                    let obv, stream = Reactive.singleSubject<int>()

                    let msgs =
                        Create.ofSeq [ stream; Reactive.empty () ]
                        |> Reactive.mergeInner

                    let testObv = TestObserver<int>()

                    // Act
                    let! subscription = msgs.SubscribeAsync testObv
                    do! obv.OnNextAsync 1
                    do! Async.Sleep 100
                    do! obv.OnCompletedAsync()
                    do! testObv.AwaitIgnore()
                    do! subscription.DisposeAsync()

                    // Assert
                    let actual = testObv.Notifications |> Seq.toList
                    assertThat actual (isEqualTo [ OnNext 1; OnCompleted ])
                }
            )
        ]
    )
