module Tests.SubjectTest

open Scriptorium.Quill
open Scriptorium.Nib.Assertion
open type Scriptorium.Quill.Test

open Fable.Reactive
open Tests.Utils

exception TestExn of unit

let tests =
    testList (
        "Subject",
        [ testAsync (
              "subject broadcasts completion to all observers",
              async {
                  // Arrange
                  let dispatch, stream = Reactive.subject ()
                  let obv1 = TestObserver<int>()
                  let obv2 = TestObserver<int>()

                  let! _ = stream.SubscribeAsync obv1
                  let! _ = stream.SubscribeAsync obv2

                  // Act
                  do! dispatch.OnCompletedAsync()

                  do! obv1.AwaitIgnore()
                  do! obv2.AwaitIgnore()

                  // Assert
                  let actual1 = obv1.Notifications |> Seq.toList
                  let actual2 = obv2.Notifications |> Seq.toList
                  let expected = [ OnCompleted ]

                  assertThat actual1 (isEqualTo expected)
                  assertThat actual2 (isEqualTo expected)
              }
          )

          testAsync (
              "subject broadcasts error to all observers",
              async {
                  // Arrange
                  let dispatch, stream = Reactive.subject ()
                  let obv1 = TestObserver<int>()
                  let obv2 = TestObserver<int>()

                  let! _ = stream.SubscribeAsync obv1
                  let! _ = stream.SubscribeAsync obv2

                  // Act
                  do! dispatch.OnErrorAsync(TestExn())

                  try
                      do! obv1.AwaitIgnore()
                  with _ ->
                      ()

                  try
                      do! obv2.AwaitIgnore()
                  with _ ->
                      ()

                  // Assert
                  let actual1 = obv1.Notifications |> Seq.toList
                  let actual2 = obv2.Notifications |> Seq.toList
                  let expected = [ OnError(TestExn()) ]

                  assertThat actual1 (isEqualTo expected)
                  assertThat actual2 (isEqualTo expected)
              }
          )

          testAsync (
              "subject does not broadcast error when the first observer throws",
              async {
                  // Arrange
                  let dispatch, stream = Reactive.subject ()
                  let obv1 = TestObserver<int>()
                  let obv2 = TestObserver<int>()

                  let! _ =
                      stream.SubscribeAsync (function
                          | OnNext _ -> raise (TestExn())
                          | n -> obv1.PostAsync n)

                  let! _ = stream.SubscribeAsync obv2

                  // Act
                  do! dispatch.OnNextAsync 1
                  do! dispatch.OnCompletedAsync()

                  try
                      do! obv1.AwaitIgnore()
                  with _ ->
                      ()

                  try
                      do! obv2.AwaitIgnore()
                  with _ ->
                      ()

                  // Assert
                  let actual1 = obv1.Notifications |> Seq.toList
                  let actual2 = obv2.Notifications |> Seq.toList

                  assertThat actual1 (isEqualTo [ OnError(TestExn()) ])
                  assertThat actual2 (isEqualTo [ OnNext 1; OnCompleted ])
              }
          )

          testAsync (
              "subject does not broadcast error when the second observer throws",
              async {
                  // Arrange
                  let dispatch, stream = Reactive.subject ()
                  let obv1 = TestObserver<int>()
                  let obv2 = TestObserver<int>()

                  let! _ = stream.SubscribeAsync obv1

                  let! _ =
                      stream.SubscribeAsync (function
                          | OnNext _ -> raise (TestExn())
                          | n -> obv2.PostAsync n)

                  // Act
                  do! dispatch.OnNextAsync 1
                  do! dispatch.OnCompletedAsync()

                  try
                      do! obv1.AwaitIgnore()
                  with _ ->
                      ()

                  try
                      do! obv2.AwaitIgnore()
                  with _ ->
                      ()

                  // Assert
                  let actual1 = obv1.Notifications |> Seq.toList
                  let actual2 = obv2.Notifications |> Seq.toList

                  assertThat actual1 (isEqualTo [ OnNext 1; OnCompleted ])
                  assertThat actual2 (isEqualTo [ OnError(TestExn()) ])
              }
          ) ]
    )
