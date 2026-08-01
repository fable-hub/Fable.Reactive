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
                  do! obvX.OnNextAsync 1
                  do! obvX.OnNextAsync 2

                  // `xs` and `ys` are independent subjects whose delivery chains converge on the
                  // single safeObserver inside `takeUntil`, so nothing orders the notifier's
                  // OnCompleted against values still in flight from `xs`. Let them land before
                  // tripping the notifier — once safeObserver sees a terminal notification it
                  // marks the stream stopped and silently drops everything after it.
                  do! obv.WaitUntil(fun ns -> ns |> List.filter isOnNext |> List.length = 2)

                  do! obvY.OnNextAsync true
                  do! obv.WaitUntil(List.exists isOnCompleted)

                  // Values posted after the notifier must be dropped. That is a negative, so
                  // there is no condition to poll on — a plain sleep is the only wait available.
                  do! obvX.OnNextAsync 3
                  do! obvX.OnCompletedAsync()
                  do! Async.Sleep 200
                  do! obv.Refresh()

                  // Assert
                  let actual = obv.Notifications |> Seq.toList

                  assertThat
                      actual
                      (hasSize 3
                       >> containAll [ OnNext 1; OnNext 2; OnCompleted ])
              }
          ) ]
    )
