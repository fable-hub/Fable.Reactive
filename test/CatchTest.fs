module Tests.Catch

open Scriptorium.Quill
open Scriptorium.Nib.Assertion
open type Scriptorium.Quill.Test

open Fable.Reactive
open Tests.Utils

exception MyError of string

let tests =
    testList (
        "Catch",
        [ testAsync (
              "catch no error",
              async {
                  // Arrange
                  let xs = fromNotification [ OnNext 1; OnNext 2; OnNext 3; OnCompleted ]
                  let ys = fromNotification [ OnNext 4; OnNext 5; OnNext 6; OnCompleted ]
                  let zs = xs |> Reactive.catch (fun _ -> ys)
                  let obv = TestObserver<int>()

                  // Act
                  let! _sub = zs.SubscribeAsync obv
                  do! obv.AwaitIgnore()

                  // Assert
                  let actual = obv.Notifications |> Seq.toList
                  let expected: Notification<int> list = [ OnNext 1; OnNext 2; OnNext 3; OnCompleted ]
                  assertThat actual (isEqualTo expected)
              }
          )

          testAsync (
              "catch error",
              async {
                  // Arrange
                  let error = MyError "error"
                  let xs = fromNotification [ OnNext 1; OnNext 2; OnNext 3; OnError error ]
                  let ys = fromNotification [ OnNext 4; OnNext 5; OnNext 6; OnCompleted ]
                  let zs = xs |> Reactive.catch (fun _ -> ys)
                  let obv = TestObserver<int>()

                  // Act
                  let! _sub = zs.SubscribeAsync obv
                  do! obv.AwaitIgnore()

                  // Assert
                  let actual = obv.Notifications |> Seq.toList

                  let expected: Notification<int> list =
                      [ OnNext 1; OnNext 2; OnNext 3; OnNext 4; OnNext 5; OnNext 6; OnCompleted ]

                  assertThat actual (isEqualTo expected)
              }
          )

          testAsync (
              "catch error exception is propagated",
              async {
                  // Arrange
                  let error = MyError "ing"
                  let xs = fromNotification [ OnNext "test"; OnError error ]

                  let zs =
                      xs
                      |> Reactive.catch (fun err ->
                          let msg =
                              match err with
                              | MyError msg -> msg
                              | _ -> "error"

                          Reactive.single msg)

                  let obv = TestObserver<string>()

                  // Act
                  let! _sub = zs.SubscribeAsync obv
                  do! obv.AwaitIgnore()

                  // Assert
                  let actual = obv.Notifications |> Seq.toList
                  let expected = [ OnNext "test"; OnNext "ing"; OnCompleted ]
                  assertThat actual (isEqualTo expected)
              }
          )

          testAsync (
              "catch error twice",
              async {
                  // Arrange
                  let error = MyError "error"
                  let xs = fromNotification [ OnNext 1; OnError error ]
                  let ys1 = fromNotification [ OnNext 2; OnError error ]
                  let ys2 = fromNotification [ OnNext 3; OnCompleted ]

                  let iter =
                      [ ys1; ys2 ]
                      |> Seq.ofList
                      |> fun x -> x.GetEnumerator()

                  let zs =
                      xs
                      |> Reactive.catch (fun _ ->
                          iter.MoveNext() |> ignore
                          iter.Current)

                  let obv = TestObserver<int>()

                  // Act
                  let! _sub = zs.SubscribeAsync obv
                  do! obv.AwaitIgnore()

                  // Assert
                  let actual = obv.Notifications |> Seq.toList
                  let expected: Notification<int> list = [ OnNext 1; OnNext 2; OnNext 3; OnCompleted ]
                  assertThat actual (isEqualTo expected)
              }
          )

          testAsync (
              "catch dispose after fallback disposes current subscription",
              async {
                  // Arrange
                  let error = MyError "error"
                  let xs = fromNotification [ OnNext 1; OnError error ]
                  let dispatch, fallback = Reactive.subject<int> ()
                  let zs = xs |> Reactive.catch (fun _ -> fallback)
                  let obv = TestObserver<int>()

                  // Act
                  let! sub = zs.SubscribeAsync obv
                  do! Async.Sleep 100 // Let error propagate and switch to fallback
                  do! dispatch.OnNextAsync 42
                  do! Async.Sleep 100

                  // Dispose should dispose the current (fallback) subscription, not the stale original
                  do! sub.DisposeAsync()
                  do! dispatch.OnNextAsync 99
                  do! Async.Sleep 100

                  // Assert - should have 1 from source and 42 from fallback, but NOT 99 after dispose
                  do! obv.Refresh()
                  let actual = obv.Notifications |> Seq.toList

                  assertThat
                      actual
                      (contain (OnNext 1) // from the source
                       >> contain (OnNext 42) // from the fallback, before dispose
                       >> notContain (OnNext 99)) // after dispose, must not arrive
              }
          ) ]
    )
