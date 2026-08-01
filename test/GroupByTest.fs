module Tests.GroupBy

open Scriptorium.Quill
open Scriptorium.Nib.Assertion
open type Scriptorium.Quill.Test

open Fable.Reactive
open Tests.Utils

exception MyError of string

let tests =
    testList (
        "GroupBy",
        [ testAsync (
              "groupBy empty",
              async {
                  // Arrange
                  let xs =
                      Reactive.empty<int> ()
                      |> Reactive.groupBy (fun _ -> 42)
                      |> Reactive.flatMap id

                  let obv = TestObserver<int>()

                  // Act
                  let! _sub = xs.SubscribeAsync obv

                  try
                      do! obv.AwaitIgnore()
                  with _ ->
                      ()

                  // Assert
                  let actual = obv.Notifications |> Seq.toList
                  let expected: Notification<int> list = [ OnCompleted ]
                  assertThat actual (isEqualTo expected)
              }
          )

          testAsync (
              "groupBy error",
              async {
                  // Arrange
                  let error = MyError "error"

                  let xs =
                      Reactive.fail<int> error
                      |> Reactive.groupBy (fun _ -> 42)
                      |> Reactive.flatMap id

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

          testAsync (
              "groupBy 2 groups",
              async {
                  // Arrange
                  let xs =
                      Reactive.ofSeq [ 1; 2; 3; 4; 5; 6 ]
                      |> Reactive.groupBy (fun x -> x % 2)
                      |> Reactive.flatMap (fun x -> x |> Reactive.min)

                  let obv = TestObserver<int>()

                  // Act
                  let! _sub = xs.SubscribeAsync obv

                  try
                      do! obv.AwaitIgnore()
                  with _ ->
                      ()

                  // Assert - the two groups can complete in either order
                  let actual = obv.Notifications |> Seq.toList

                  let accepted =
                      [ [ OnNext 1; OnNext 2; OnCompleted ]; [ OnNext 2; OnNext 1; OnCompleted ] ]

                  assertThat actual (hasSize 3)
                  assertThat accepted (contain actual)
              }
          )

          testAsync (
              "groupBy cancel",
              async {
                  // Arrange
                  let xs =
                      Reactive.ofSeq [ 1; 2; 3; 4; 5; 6 ]
                      |> Reactive.groupBy (fun x -> x % 2)
                      |> Reactive.flatMap id

                  let obv = TestObserver<int>()

                  // Act
                  let! sub = xs.SubscribeAsync obv
                  do! sub.DisposeAsync()

                  // Assert
                  assertThat obv.Notifications.Count (isLessThan 8)
              }
          ) ]
    )
