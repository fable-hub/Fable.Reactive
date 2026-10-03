module Tests.Map

open Scriptorium.Quill
open Scriptorium.Nib.Assertion
open type Scriptorium.Quill.Test

open Fable.Reactive
open Tests.Utils

exception MyError of string

let tests =
    testList (
        "Map",
        [
            testAsync (
                "mapAsync",
                async {
                    // Arrange
                    let mapper x = async { return x * 10 }

                    let xs = Reactive.single 42 |> Reactive.mapAsync mapper
                    let obv = TestObserver<int>()

                    // Act
                    let! _sub = xs.SubscribeAsync obv
                    let! latest = obv.Await()

                    // Assert
                    assertThat latest (isEqualTo 420)
                    let actual = obv.Notifications |> Seq.toList
                    let expected: Notification<int> list = [ OnNext 420; OnCompleted ]
                    assertThat actual (isEqualTo expected)
                }
            )

            testAsync (
                "map",
                async {
                    // Arrange
                    let mapper x = x * 10

                    let xs = Reactive.single 42 |> Reactive.map mapper
                    let obv = TestObserver<int>()

                    // Act
                    let! _sub = xs.SubscribeAsync obv
                    let! latest = obv.Await()

                    // Assert
                    assertThat latest (isEqualTo 420)
                    let actual = obv.Notifications |> Seq.toList
                    let expected: Notification<int> list = [ OnNext 420; OnCompleted ]
                    assertThat actual (isEqualTo expected)
                }
            )

            testAsync (
                "map mapper throws exception",
                async {
                    // Arrange
                    let error = MyError "error"
                    let mapper _ = async { raise error }

                    let xs =
                        Reactive.single "error"
                        |> Reactive.mapAsync mapper

                    let obv = TestObserver<unit>()

                    // Act
                    let! _sub = xs.SubscribeAsync obv

                    try
                        do! obv.AwaitIgnore()
                    with _ ->
                        ()

                    // Assert
                    let actual = obv.Notifications |> Seq.toList
                    let expected: Notification<unit> list = [ OnError error ]
                    assertThat actual (isEqualTo expected)
                }
            )
        ]
    )
