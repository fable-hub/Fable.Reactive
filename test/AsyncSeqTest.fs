module Tests.AsyncSeq

open System
open System.Collections.Generic
open System.Threading
open System.Threading.Tasks

open Scriptorium.Quill
open Scriptorium.Nib.Assertion
open type Scriptorium.Quill.Test

open FSharp.Control
open Fable.Reactive
open Tests.Utils

let tests =
    testList (
        "AsyncSeq",
        [
            testAsync (
                "async seq completes and disposes its enumerator",
                async {
                    let disposed =
                        TaskCompletionSource<unit>(TaskCreationOptions.RunContinuationsAsynchronously)

                    let xs =
                        asyncSeq {
                            try
                                yield 1
                                yield 2
                            finally
                                disposed.TrySetResult() |> ignore
                        }

                    let obv = TestObserver<int>()
                    let! subscription = (Reactive.ofAsyncSeq xs).SubscribeAsync obv
                    do! obv.WaitUntil(List.exists isOnCompleted)
                    do! disposed.Task |> Async.AwaitTask
                    assertThat (List.ofSeq obv.Notifications) (isEqualTo [ OnNext 1; OnNext 2; OnCompleted ])
                    do! subscription.DisposeAsync()
                }
            )

            testAsync (
                "async seq reports source errors and disposes its enumerator",
                async {
                    let disposed =
                        TaskCompletionSource<unit>(TaskCreationOptions.RunContinuationsAsynchronously)

                    let error = InvalidOperationException("source failed")

                    let xs =
                        asyncSeq {
                            try
                                yield 1
                                raise error
                            finally
                                disposed.TrySetResult() |> ignore
                        }

                    let obv = TestObserver<int>()
                    let! subscription = (Reactive.ofAsyncSeq xs).SubscribeAsync obv
                    do! obv.WaitUntil(List.exists isOnError)
                    do! disposed.Task |> Async.AwaitTask
                    assertThat (List.ofSeq obv.Notifications) (isEqualTo [ OnNext 1; OnError error ])
                    do! subscription.DisposeAsync()
                }
            )

            testAsync (
                "disposing an async seq subscription cancels enumeration and disposes its enumerator",
                async {
                    let disposed =
                        TaskCompletionSource<unit>(TaskCreationOptions.RunContinuationsAsynchronously)

                    let xs =
                        asyncSeq {
                            try
                                yield 1
                                do! Async.Sleep Timeout.Infinite
                                yield 2
                            finally
                                disposed.TrySetResult() |> ignore
                        }

                    let obv = TestObserver<int>()
                    let! subscription = (Reactive.ofAsyncSeq xs).SubscribeAsync obv
                    do! obv.WaitUntil(List.exists isOnNext)
                    do! subscription.DisposeAsync()
                    do! disposed.Task |> Async.AwaitTask
                    do! obv.Refresh()
                    assertThat (List.ofSeq obv.Notifications) (isEqualTo [ OnNext 1 ])
                }
            )

            testAsync (
                "observable to async seq",
                async {
                    // Arrange
                    let xs =
                        seq { 1..5 }
                        |> Reactive.ofSeq
                        |> Reactive.toAsyncSeq

                    let result = List<int>()

                    let each x = async { result.Add x }

                    // Act
                    do! xs |> AsyncSeq.iterAsync each

                    // Assert
                    let actual = result |> List.ofSeq
                    assertThat actual (isEqualTo [ 1..5 ])
                }
            )

            testAsync (
                "seq to async seq to observable to async seq",
                async {
                    // Arrange
                    let xs =
                        seq { 1..5 }
                        |> AsyncSeq.ofSeq
                        |> Reactive.ofAsyncSeq
                        |> Reactive.toAsyncSeq

                    let result = List<int>()

                    let each x = async { result.Add x }

                    // Act
                    do! xs |> AsyncSeq.iterAsync each

                    // Assert
                    let actual = result |> List.ofSeq
                    assertThat actual (isEqualTo [ 1..5 ])
                }
            )
        ]
    )
