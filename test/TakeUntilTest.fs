module Tests.TakeUntil

open Scriptorium.Quill
open Scriptorium.Nib.Assertion
open type Scriptorium.Quill.Test

open Fable.Reactive
open Fable.Reactive.Subjects
open Tests.Utils

exception MyError of string

let tests =
    testList (
        "TakeUntil",
        [ testAsync (
              "takeUntil stops at the first notifier value",
              async {
                  // Arrange
                  let obvX, xs = subject<int> ()
                  let obvY, ys = subject<bool> ()
                  let zs = xs |> Reactive.takeUntil ys

                  let obv = TestObserver<int>()

                  // Act
                  let! _sub = zs.SubscribeAsync obv
                  do! Async.Sleep 100
                  do! obvX.OnNextAsync 1
                  do! obvX.OnNextAsync 2
                  do! obvY.OnNextAsync true
                  do! Async.Sleep 500
                  do! obvX.OnNextAsync 3
                  do! obvX.OnCompletedAsync()

                  try
                      do! obv.AwaitIgnore()
                  with _ ->
                      ()

                  // Assert
                  let actual = obv.Notifications |> Seq.toList

                  assertThat
                      actual
                      (hasSize 3
                       >> containAll [ OnNext 1; OnNext 2; OnCompleted ])
              }
          ) ]
    )
