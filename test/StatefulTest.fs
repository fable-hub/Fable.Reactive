module Tests.Stateful

open Fable.Actor
open Fable.Reactive
open Scriptorium.Quill
open Scriptorium.Nib.Assertion
open type Scriptorium.Quill.Test
open Tests.Utils

let tests =
    testList (
        "Stateful",
        [
            testAsync (
                "deferred lazy sequences support actor subscriptions and resubscription",
                async {
                    let source =
                        Reactive.single ()
                        |> Reactive.flatMap (fun () ->
                            // TODO(upstream): remove this BEAM mitigation coverage when the pinned Fable runtime
                            // passes the direct cross-process lazy-sequence merge/concat tests without overrides.
                            Reactive.defer (fun () ->
                                seq { 1..3 }
                                |> Seq.map (fun value -> value * 2)
                                |> Reactive.ofSeq))

                    let first = TestObserver<int>()
                    let second = TestObserver<int>()
                    let! _ = source.SubscribeAsync first
                    let! _ = source.SubscribeAsync second
                    do! first.AwaitIgnore()
                    do! second.AwaitIgnore()
                    let expected = [ OnNext 2; OnNext 4; OnNext 6; OnCompleted ]
                    assertThat (first.Notifications |> Seq.toList) (isEqualTo expected)
                    assertThat (second.Notifications |> Seq.toList) (isEqualTo expected)
                }
            )
            testAsync (
                "scan emits the first value and every updated accumulator",
                async {
                    let source = Reactive.ofSeq [ 1; 2; 3 ] |> Reactive.scan (+)
                    let observer = TestObserver<int>()
                    let! _ = source.SubscribeAsync observer
                    do! observer.AwaitIgnore()

                    assertThat
                        (observer.Notifications |> Seq.toList)
                        (isEqualTo [ OnNext 1; OnNext 3; OnNext 6; OnCompleted ])
                }
            )
            testAsync (
                "reduce emits a singleton source",
                async {
                    let observer = TestObserver<int>()
                    let! _ = (Reactive.single 42 |> Reactive.min).SubscribeAsync observer
                    let! value = observer.Await()
                    assertThat value (isEqualTo 42)
                }
            )
            testAsync (
                "skip and take preserve state across producer actors",
                async {
                    let observer = TestObserver<int>()

                    let source =
                        Reactive.ofSeq [ 1; 2; 3; 4; 5 ]
                        |> Reactive.skip 1
                        |> Reactive.take 2

                    let! _ = source.SubscribeAsync observer
                    do! observer.AwaitIgnore()
                    assertThat (observer.Notifications |> Seq.toList) (isEqualTo [ OnNext 2; OnNext 3; OnCompleted ])
                }
            )
            testAsync (
                "takeLast keeps the final values in order",
                async {
                    let observer = TestObserver<int>()

                    let! _ =
                        (Reactive.ofSeq [ 1; 2; 3; 4 ]
                         |> Reactive.takeLast 2)
                            .SubscribeAsync
                            observer

                    do! observer.AwaitIgnore()
                    assertThat (observer.Notifications |> Seq.toList) (isEqualTo [ OnNext 3; OnNext 4; OnCompleted ])
                }
            )
            testAsync (
                "scan state is independent for each subscription",
                async {
                    let source =
                        Reactive.ofSeq [ 1; 2; 3 ]
                        |> Reactive.scanInit 0 (+)

                    let first = TestObserver<int>()
                    let second = TestObserver<int>()
                    let! _ = source.SubscribeAsync first
                    let! _ = source.SubscribeAsync second
                    let! one = first.Await()
                    let! two = second.Await()
                    assertThat one (isEqualTo 6)
                    assertThat two (isEqualTo 6)
                }
            )
            testAsync (
                "disposal invokes cleanup once across actor processes",
                async {
                    let cleanup = TestObserver<int>()
                    let finished = TestObserver<int>()
                    let disposable = AsyncDisposable.Create(fun () -> cleanup.OnNextAsync 1)

                    let worker () =
                        Actor.spawn (fun _ ->
                            actor {
                                do! disposable.DisposeAsync()
                                do! finished.OnNextAsync 1
                            })

                    let _ = worker ()
                    let _ = worker ()
                    do! finished.WaitUntil(fun notifications -> List.length notifications = 2)
                    do! cleanup.Refresh()
                    assertThat (cleanup.Notifications |> Seq.toList) (isEqualTo [ OnNext 1 ])
                }
            )
        ]
    )
