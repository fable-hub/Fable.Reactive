module Tests.Query

open Scriptorium.Quill
open Scriptorium.Nib.Assertion
open type Scriptorium.Quill.Test

open Fable.Reactive
open Tests.Utils

let tests =
    testList (
        "Builder",
        [ testAsync (
              "empty query",
              async {
                  // Arrange
                  let xs = reactive { () }
                  let obv = TestObserver<unit>()

                  // Act
                  let! _dispose = xs.SubscribeAsync obv
                  do! obv.AwaitIgnore()

                  // Assert
                  let actual = obv.Notifications |> Seq.toList
                  let expected: Notification<unit> list = [ OnCompleted ]
                  assertThat actual (isEqualTo expected)
              }
          )

          testAsync (
              "query let!",
              async {
                  // Arrange
                  let obv = TestObserver<int>()

                  let xs =
                      reactive {
                          let! a = seq [ 1; 2 ] |> Reactive.ofSeq
                          let! b = seq [ 3; 4 ] |> Reactive.ofSeq

                          yield a + b
                      }

                  // Act
                  let! _subscription = xs.SubscribeAsync obv
                  let! _latest = obv.Await()

                  // Assert - the cross product arrives in no fixed order
                  let actual = obv.Notifications |> Seq.toList

                  assertThat
                      actual
                      (hasSize 5
                       >> containAll [ OnNext 4; OnNext 5; OnNext 6; OnCompleted ])
              }
          )

          testAsync (
              "query yield!",
              async {
                  // Arrange
                  let obv = TestObserver<int>()

                  let xs = reactive { yield! Reactive.single 42 }

                  // Act
                  let! _subscription = xs.SubscribeAsync obv
                  let! _latest = obv.Await()

                  // Assert
                  let actual = obv.Notifications |> Seq.toList
                  let expected: Notification<int> list = [ OnNext 42; OnCompleted ]
                  assertThat actual (isEqualTo expected)
              }
          )

          testAsync (
              "query yield",
              async {
                  // Arrange
                  let obv = TestObserver<int>()

                  let xs = reactive { yield 42 }

                  // Act
                  let! _subscription = xs.SubscribeAsync obv
                  let! _latest = obv.Await()

                  // Assert
                  let actual = obv.Notifications |> Seq.toList
                  let expected: Notification<int> list = [ OnNext 42; OnCompleted ]
                  assertThat actual (isEqualTo expected)
              }
          )

          testAsync (
              "query combine",
              async {
                  // Arrange
                  let obv = TestObserver<int>()

                  let xs =
                      reactive {
                          yield 42
                          yield 43
                      }

                  // Act
                  let! _subscription = xs.SubscribeAsync obv
                  let! _latest = obv.Await()

                  // Assert
                  let actual = obv.Notifications |> Seq.toList
                  let expected: Notification<int> list = [ OnNext 42; OnNext 43; OnCompleted ]
                  assertThat actual (isEqualTo expected)
              }
          )

          testAsync (
              "query for in observable",
              async {
                  // Arrange
                  let obv = TestObserver<int>()

                  let xs =
                      reactive {
                          let xs = Reactive.ofSeq [ 1; 2; 3 ]

                          for x in xs do
                              yield x * 10
                      }

                  // Act
                  let! _subscription = xs.SubscribeAsync obv
                  let! _latest = obv.Await()

                  // Assert
                  let actual = obv.Notifications |> Seq.toList

                  let expected: Notification<int> list =
                      [ OnNext 10; OnNext 20; OnNext 30; OnCompleted ]

                  assertThat actual (isEqualTo expected)
              }
          )

          testAsync (
              "query for in seq",
              async {
                  // Arrange
                  let obv = TestObserver<int>()

                  let xs =
                      reactive {
                          for x in [ 1; 2; 3 ] do
                              yield x * 10
                      }

                  // Act
                  let! _subscription = xs.SubscribeAsync obv
                  let! _latest = obv.Await()

                  // Assert
                  let actual = obv.Notifications |> Seq.toList

                  let expected: Notification<int> list =
                      [ OnNext 10; OnNext 20; OnNext 30; OnCompleted ]

                  assertThat actual (isEqualTo expected)
              }
          )

          testAsync (
              "query async",
              async {
                  // Arrange
                  let obv = TestObserver<int>()

                  let xs =
                      reactive {
                          let! b = async { return 42 }
                          yield b + 2
                      }

                  // Act
                  let! _subscription = xs.SubscribeAsync obv
                  let! _latest = obv.Await()

                  // Assert
                  let actual = obv.Notifications |> Seq.toList
                  let expected: Notification<int> list = [ OnNext 44; OnCompleted ]
                  assertThat actual (isEqualTo expected)
              }
          )

          testAsync (
              "query async dispose with use!",
              async {
                  // Arrange
                  let xs = Reactive.timer 10
                  let obv = TestObserver<int>()

                  // Act
                  do!
                      async {
                          use! _ignore = xs.SubscribeAsync obv
                          ()
                      }

                  do! Async.Sleep 15

                  // Assert
                  do! obv.Refresh()
                  let actual = obv.Notifications |> Seq.toList
                  assertThat actual isEmpty
              }
          ) ]
    )
