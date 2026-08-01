module Tests.Concat

open Scriptorium.Quill
open Scriptorium.Nib.Assertion
open type Scriptorium.Quill.Test

open Fable.Reactive
open Tests.Utils

exception MyError of string

let tests =
    testList (
        "Concat",
        [ testAsync (
              "concat empty with empty",
              async {
                  // Arrange
                  let xs = Reactive.empty ()
                  let ys = Reactive.empty ()
                  let zs = Reactive.concatSeq [ xs; ys ]
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
              "concat non empty with empty",
              async {
                  // Arrange
                  let xs = seq { 1..3 } |> Reactive.ofSeq
                  let ys = Reactive.empty ()
                  let zs = Reactive.concatSeq [ xs; ys ]
                  let obv = TestObserver<int>()

                  // Act
                  let! _sub = zs.SubscribeAsync obv
                  let! result = obv.Await()

                  // Assert
                  assertThat result (isEqualTo 3)
                  let actual = obv.Notifications |> Seq.toList
                  let expected = [ OnNext 1; OnNext 2; OnNext 3; OnCompleted ]
                  assertThat actual (isEqualTo expected)
              }
          )

          testAsync (
              "concat empty with non empty",
              async {
                  // Arrange
                  let xs = Reactive.empty ()
                  let ys = seq { 1..3 } |> Reactive.ofSeq
                  let zs = Reactive.concatSeq [ xs; ys ]
                  let obv = TestObserver<int>()

                  // Act
                  let! _sub = zs.SubscribeAsync obv
                  let! result = obv.Await()

                  // Assert
                  assertThat result (isEqualTo 3)
                  let actual = obv.Notifications |> Seq.toList
                  let expected = [ OnNext 1; OnNext 2; OnNext 3; OnCompleted ]
                  assertThat actual (isEqualTo expected)
              }
          )

          testAsync (
              "concat two",
              async {
                  // Arrange
                  let xs = seq { 1..3 } |> Reactive.ofSeq
                  let ys = seq { 4..6 } |> Reactive.ofSeq
                  let zs = Reactive.concatSeq [ xs; ys ]
                  let obv = TestObserver<int>()

                  // Act
                  let! _sub = zs.SubscribeAsync obv
                  let! result = obv.Await()

                  // Assert
                  assertThat result (isEqualTo 6)
                  let actual = obv.Notifications |> Seq.toList

                  let expected =
                      [ OnNext 1; OnNext 2; OnNext 3; OnNext 4; OnNext 5; OnNext 6; OnCompleted ]

                  assertThat actual (isEqualTo expected)
              }
          )

          testAsync (
              "concat with ++ operator",
              async {
                  // Arrange
                  let xs = seq { 1..3 } |> Reactive.ofSeq
                  let ys = seq { 4..6 } |> Reactive.ofSeq
                  let zs = xs ++ ys
                  let obv = TestObserver<int>()

                  // Act
                  let! _sub = zs.SubscribeAsync obv
                  let! result = obv.Await()

                  // Assert
                  assertThat result (isEqualTo 6)
                  let actual = obv.Notifications |> Seq.toList

                  let expected =
                      [ OnNext 1; OnNext 2; OnNext 3; OnNext 4; OnNext 5; OnNext 6; OnCompleted ]

                  assertThat actual (isEqualTo expected)
              }
          )

          testAsync (
              "concat three",
              async {
                  // Arrange
                  let a = seq { 1..2 } |> Reactive.ofSeq
                  let b = seq { 3..4 } |> Reactive.ofSeq
                  let c = seq { 5..6 } |> Reactive.ofSeq
                  let xs = Reactive.concatSeq [ a; b; c ]
                  let obv = TestObserver<int>()

                  // Act
                  let! _sub = xs.SubscribeAsync obv
                  let! result = obv.Await()

                  // Assert
                  assertThat result (isEqualTo 6)
                  let actual = obv.Notifications |> Seq.toList

                  let expected =
                      [ OnNext 1; OnNext 2; OnNext 3; OnNext 4; OnNext 5; OnNext 6; OnCompleted ]

                  assertThat actual (isEqualTo expected)
              }
          )

          testAsync (
              "concat fail with non empty",
              async {
                  // Arrange
                  let error = MyError "error"
                  let xs = Reactive.fail error
                  let ys = seq { 1..3 } |> Reactive.ofSeq
                  let zs = Reactive.concatSeq [ xs; ys ]
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
          ) ]
    )
