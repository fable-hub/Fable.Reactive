module Tests.Debounce

open System
open Scriptorium.Quill
open Scriptorium.Nib.Assertion
open type Scriptorium.Quill.Test

open Fable.Actor
open Fable.Reactive
open Tests.Utils

// decision: delivers directly to isolate debounce from unrelated subject and worker state on BEAM
let private sourceWith (worker: IAsyncObserver<int> -> Async<unit>) =
    { new IAsyncObservable<int> with
        member _.SubscribeAsync obv =
            async {
                do! worker obv

                return
                    { new IReactiveDisposable with
                        member _.DisposeAsync() = async { () }
                    }
            }
    }

let private sourceOf (notifications: Notification<int> list) =
    sourceWith (fun obv ->
        async {
            for notification in notifications do
                match notification with
                | OnNext value -> do! obv.OnNextAsync value
                | OnError error -> do! obv.OnErrorAsync error
                | OnCompleted -> do! obv.OnCompletedAsync()
        })

let tests =
    testList (
        "Debounce",
        [
            testAsync (
                "emits from a producer actor without completing the source",
                async {
                    let obv = TestObserver<int>()

                    let source =
                        sourceWith (fun upstream ->
                            async {
                                // invariant: the callback runs in a different BEAM process from subscription setup
                                Actor.spawn (fun _ -> actor { do! upstream.OnNextAsync 1 })
                                |> ignore
                            })

                    let! sub = (source |> Reactive.debounce 50).SubscribeAsync obv
                    do! obv.WaitUntil(List.contains (OnNext 1))
                    assertThat (List.ofSeq obv.Notifications) (isEqualTo [ OnNext 1 ])
                    do! sub.DisposeAsync()
                }
            )

            testAsync (
                "suppresses superseded values in a burst",
                async {
                    let obv = TestObserver<int>()

                    let source =
                        sourceOf [ OnNext 1; OnNext 2; OnNext 3 ]
                        |> Reactive.debounce 200

                    let! sub = source.SubscribeAsync obv
                    do! obv.WaitUntil(List.contains (OnNext 3))
                    assertThat (List.ofSeq obv.Notifications) (isEqualTo [ OnNext 3 ])
                    do! sub.DisposeAsync()
                }
            )

            testAsync (
                "emits across successive quiet periods",
                async {
                    let dispatch, source = Reactive.singleSubject<int>()
                    let obv = TestObserver<int>()
                    let! sub = (source |> Reactive.debounce 50).SubscribeAsync obv
                    do! dispatch.OnNextAsync 1
                    do! obv.WaitUntil(List.contains (OnNext 1))
                    do! dispatch.OnNextAsync 2
                    do! dispatch.OnNextAsync 3
                    do! obv.WaitUntil(List.contains (OnNext 3))
                    assertThat (List.ofSeq obv.Notifications) (isEqualTo [ OnNext 1; OnNext 3 ])
                    do! sub.DisposeAsync()
                }
            )

            testAsync (
                "completes after an emitted value",
                async {
                    let dispatch, source = Reactive.singleSubject<int>()
                    let obv = TestObserver<int>()
                    let! sub = (source |> Reactive.debounce 50).SubscribeAsync obv
                    do! dispatch.OnNextAsync 1
                    do! obv.WaitUntil(List.contains (OnNext 1))
                    do! dispatch.OnCompletedAsync()
                    do! obv.AwaitIgnore()
                    assertThat (List.ofSeq obv.Notifications) (isEqualTo [ OnNext 1; OnCompleted ])
                    do! sub.DisposeAsync()
                }
            )

            testAsync (
                "completion drops pending values and rejects later notifications",
                async {
                    let obv = TestObserver<int>()

                    let source =
                        sourceOf [ OnNext 1; OnCompleted; OnNext 2; OnCompleted ]
                        |> Reactive.debounce 200

                    let! sub = source.SubscribeAsync obv
                    do! obv.AwaitIgnore()
                    // A negative assertion must outlast the timer that could deliver a late value.
                    do! Async.Sleep 450
                    do! obv.Refresh()
                    assertThat (List.ofSeq obv.Notifications) (isEqualTo [ OnCompleted ])
                    do! sub.DisposeAsync()
                }
            )

            testAsync (
                "error drops pending values and forwards the original error once",
                async {
                    let error = Exception "expected debounce error"
                    let obv = TestObserver<int>()

                    let source =
                        sourceOf [ OnNext 1; OnError error; OnNext 2; OnCompleted ]
                        |> Reactive.debounce 200

                    let! sub = source.SubscribeAsync obv
                    do! obv.AwaitIgnore()
                    do! Async.Sleep 450
                    do! obv.Refresh()

                    match List.ofSeq obv.Notifications with
                    | [ OnError actual ] -> assertThat actual (isEqualTo error)
                    | actual -> failwithf "Expected one OnError notification, got %A" actual

                    do! sub.DisposeAsync()
                }
            )

            testAsync (
                "dispose suppresses pending values beyond the timer deadline",
                async {
                    let obv = TestObserver<int>()
                    let! sub = (sourceOf [ OnNext 1 ] |> Reactive.debounce 2000).SubscribeAsync obv
                    do! sub.DisposeAsync()
                    do! Async.Sleep 2300
                    do! obv.Refresh()
                    assertThat (List.ofSeq obv.Notifications) isEmpty
                }
            )

            testAsync (
                "disposing one subscription leaves another subscription active",
                async {
                    let source = sourceOf [ OnNext 1 ] |> Reactive.debounce 2000
                    let first = TestObserver<int>()
                    let second = TestObserver<int>()
                    let! sub1 = source.SubscribeAsync first
                    let! sub2 = source.SubscribeAsync second
                    do! sub1.DisposeAsync()
                    do! second.WaitUntil(List.contains (OnNext 1))
                    do! first.Refresh()
                    assertThat (List.ofSeq first.Notifications) isEmpty
                    assertThat (List.ofSeq second.Notifications) (isEqualTo [ OnNext 1 ])
                    do! sub2.DisposeAsync()
                }
            )

            testAsync (
                "can resubscribe after disposal",
                async {
                    let source = sourceOf [ OnNext 1 ] |> Reactive.debounce 50
                    let first = TestObserver<int>()
                    let! sub1 = source.SubscribeAsync first
                    do! first.WaitUntil(List.contains (OnNext 1))
                    do! sub1.DisposeAsync()

                    let second = TestObserver<int>()
                    let! sub2 = source.SubscribeAsync second
                    do! second.WaitUntil(List.contains (OnNext 1))
                    assertThat (List.ofSeq second.Notifications) (isEqualTo [ OnNext 1 ])
                    do! sub2.DisposeAsync()
                }
            )
        ]
    )
