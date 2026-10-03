module Tests.Filter

open Scriptorium.Quill
open Scriptorium.Nib.Assertion
open type Scriptorium.Quill.Test

open Fable.Reactive
open Tests.Utils

exception MyError of string

let tests =
    testList (
        "Filter",
        [
            testAsync (
                "filterAsync",
                async {
                    // Arrange
                    let predicate x = async { return x < 3 }

                    let xs =
                        seq { 1..5 }
                        |> Reactive.ofSeq
                        |> Reactive.filterAsync predicate

                    let obv = TestObserver<int>()

                    // Act
                    let! _sub = xs.SubscribeAsync obv
                    let! result = obv.Await()

                    // Assert
                    assertThat result (isEqualTo 2)
                    let actual = obv.Notifications |> Seq.toList
                    let expected = [ OnNext 1; OnNext 2; OnCompleted ]
                    assertThat actual (isEqualTo expected)
                }
            )

            testAsync (
                "filter",
                async {
                    // Arrange
                    let predicate x = x < 3

                    let xs =
                        seq { 1..5 }
                        |> Reactive.ofSeq
                        |> Reactive.filter predicate

                    let obv = TestObserver<int>()

                    // Act
                    let! _sub = xs.SubscribeAsync obv
                    let! result = obv.Await()

                    // Assert
                    assertThat result (isEqualTo 2)
                    let actual = obv.Notifications |> Seq.toList
                    let expected = [ OnNext 1; OnNext 2; OnCompleted ]
                    assertThat actual (isEqualTo expected)
                }
            )

            testAsync (
                "filter predicate throws exception",
                async {
                    // Arrange
                    let error = MyError "error"

                    let predicate _ =
                        async {
                            raise error
                            return true
                        }

                    let xs =
                        seq { 1..5 }
                        |> Reactive.ofSeq
                        |> Reactive.filterAsync predicate

                    let obv = TestObserver<int>()

                    // Act
                    let! _sub = xs.SubscribeAsync obv

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
        ]
    )
