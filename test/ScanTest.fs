module Tests.Scan

open Scriptorium.Quill
open Scriptorium.Nib.Assertion
open type Scriptorium.Quill.Test

open Fable.Reactive
open Tests.Utils

exception MyError of string

let tests =
    testList (
        "Scan",
        [ testAsync (
              "scanInitAsync",
              async {
                  // Arrange
                  let scanner acc x = async { return acc + x }

                  let xs =
                      Reactive.ofSeq <| seq { 1..5 }
                      |> Reactive.scanInitAsync 0 scanner

                  let obv = TestObserver<int>()

                  // Act
                  let! _sub = xs.SubscribeAsync obv
                  let! result = obv.Await()

                  // Assert
                  assertThat result (isEqualTo 15)
                  let actual = obv.Notifications |> Seq.toList
                  let expected = [ OnNext 1; OnNext 3; OnNext 6; OnNext 10; OnNext 15; OnCompleted ]
                  assertThat actual (isEqualTo expected)
              }
          )

          testAsync (
              "scanInit",
              async {
                  // Arrange
                  let scanner acc x = acc + x

                  let xs =
                      Reactive.ofSeq <| seq { 1..5 }
                      |> Reactive.scanInit 0 scanner

                  let obv = TestObserver<int>()

                  // Act
                  let! _sub = xs.SubscribeAsync obv
                  let! result = obv.Await()

                  // Assert
                  assertThat result (isEqualTo 15)
                  let actual = obv.Notifications |> Seq.toList
                  let expected = [ OnNext 1; OnNext 3; OnNext 6; OnNext 10; OnNext 15; OnCompleted ]
                  assertThat actual (isEqualTo expected)
              }
          )

          testAsync (
              "scan accumulator fails",
              async {
                  // Arrange
                  let error = MyError "error"

                  let scanner _ _ =
                      raise error
                      0

                  let xs =
                      Reactive.ofSeq <| seq { 1..5 }
                      |> Reactive.scanInit 0 scanner

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
          ) ]
    )
