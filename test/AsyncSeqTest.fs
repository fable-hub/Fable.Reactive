module Tests.AsyncSeq

open System.Collections.Generic

open Scriptorium.Quill
open Scriptorium.Nib.Assertion
open type Scriptorium.Quill.Test

open FSharp.Control
open Fable.Reactive

let tests =
    testList (
        "AsyncSeq",
        [
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
