module Tests.Bind

open Scriptorium.Quill
open Scriptorium.Nib.Assertion
open type Scriptorium.Quill.Test

open Fable.Reactive
open Tests.Utils

let tests =
    testList (
        "FlatMap",
        [ testAsync (
              "flatMap empty",
              async {
                  // Arrange
                  let xs = Reactive.empty ()
                  let zs = xs |> Reactive.flatMap id
                  let obv = TestObserver<int>()

                  // Act
                  let! _sub = zs.SubscribeAsync obv

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
              "flatMap some",
              async {
                  // Arrange
                  let xs = fromNotification [ OnNext 1; OnNext 2; OnNext 3; OnCompleted ]
                  let zs = xs |> Reactive.flatMap Reactive.single
                  let obv = TestObserver<int>()

                  // Act
                  let! _sub = zs.SubscribeAsync obv
                  do! obv.AwaitIgnore()

                  // Assert - inner subscriptions run concurrently, so order is not fixed
                  let actual = obv.Notifications |> Seq.toList

                  assertThat
                      actual
                      (hasSize 4
                       >> containAll [ OnNext 1; OnNext 2; OnNext 3; OnCompleted ])
              }
          )

          testAsync (
              // return x >>= f is the same thing as f x
              "flatMap monad law left identity",
              async {
                  // Arrange
                  let f x = Reactive.single (x * 10)
                  let xs = Reactive.single 42 |> Reactive.flatMap f
                  let ys = f 42
                  let obv1 = TestObserver<int>()
                  let obv2 = TestObserver<int>()

                  // Act
                  do! xs.RunAsync obv1
                  let! x = obv1.Await()

                  do! ys.RunAsync obv2
                  let! y = obv2.Await()

                  // Assert
                  assertThat x (isEqualTo y)
                  assertThat x (isEqualTo 420)
              }
          )

          testAsync (
              // m >>= return is no different than just m
              "flatMap monad law right identity",
              async {
                  // Arrange
                  let m = Reactive.single 42
                  let xs = m |> Reactive.flatMap Reactive.single
                  let obv1 = TestObserver<int>()
                  let obv2 = TestObserver<int>()

                  // Act
                  do! m.RunAsync obv1
                  let! x = obv1.Await()

                  do! xs.RunAsync obv2
                  let! y = obv2.Await()

                  // Assert
                  assertThat x (isEqualTo y)
                  assertThat x (isEqualTo 42)
              }
          )

          testAsync (
              // (m >>= f) >>= g is just like doing m >>= (\x -> f x >>= g)
              "flatMap monad law associativity",
              async {
                  // Arrange
                  let m = Reactive.single 42
                  let f x = Reactive.single (x * 1000)
                  let g x = Reactive.single (x * 42)

                  let xs = m |> Reactive.flatMap f |> Reactive.flatMap g

                  let ys =
                      m
                      |> Reactive.flatMap (fun x -> f x |> Reactive.flatMap g)

                  let obv1 = TestObserver<int>()
                  let obv2 = TestObserver<int>()

                  // Act
                  do! xs.RunAsync obv1
                  let! x = obv1.Await()

                  do! ys.RunAsync obv2
                  let! y = obv2.Await()

                  // Assert
                  assertThat x (isEqualTo y)
                  assertThat x (isEqualTo 1764000)
              }
          )

          testAsync (
              "flatMap expression with let!",
              async {
                  // Arrange
                  let xs = fromNotification [ OnNext 1; OnNext 2; OnNext 3; OnCompleted ]

                  let ys =
                      reactive {
                          let! x = xs
                          yield x * 2
                      }

                  let obv = TestObserver<int>()

                  // Act
                  let! _sub = ys.SubscribeAsync obv
                  do! obv.AwaitIgnore()

                  // Assert
                  let actual = obv.Notifications |> Seq.toList

                  assertThat
                      actual
                      (hasSize 4
                       >> containAll [ OnNext 2; OnNext 4; OnNext 6; OnCompleted ])
              }
          )

          testAsync (
              "flatMap expression with for",
              async {
                  // Arrange
                  let xs = fromNotification [ OnNext 1; OnNext 2; OnNext 3; OnCompleted ]

                  let ys =
                      reactive {
                          for x in xs do
                              yield x * 2
                      }

                  let obv = TestObserver<int>()

                  // Act
                  let! _sub = ys.SubscribeAsync obv
                  do! obv.AwaitIgnore()

                  // Assert
                  let actual = obv.Notifications |> Seq.toList
                  let expected: Notification<int> list = [ OnNext 2; OnNext 4; OnNext 6; OnCompleted ]
                  assertThat actual (isEqualTo expected)
              }
          )

          testAsync (
              "flatMap expression with yield!",
              async {
                  // Arrange
                  let xs = fromNotification [ OnNext 1; OnNext 2; OnNext 3; OnCompleted ]

                  let ys =
                      reactive {
                          let! x = xs
                          yield! Reactive.single (x * 2)
                      }

                  let obv = TestObserver<int>()

                  // Act
                  let! _sub = ys.SubscribeAsync obv
                  do! obv.AwaitIgnore()

                  // Assert
                  let actual = obv.Notifications |> Seq.toList

                  assertThat
                      actual
                      (hasSize 4
                       >> containAll [ OnNext 2; OnNext 4; OnNext 6; OnCompleted ])
              }
          ) ]
    )
