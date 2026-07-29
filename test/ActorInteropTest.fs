module Tests.ActorInterop

open Scriptorium.Quill
open Scriptorium.Nib.Assertion
open type Scriptorium.Quill.Test

open Fable.Actor
open Fable.Actor.Types
open Fable.Reactive
open Tests.Utils

let tests =
    testList (
        "ActorInterop",
        [ testAsync (
              "subscribeActor receives all notifications",
              async {
                  let xs = fromNotification [ OnNext 1; OnNext 2; OnNext 3; OnCompleted ]
                  let received = ResizeArray<Notification<int>>()
                  let mutable completed = false

                  let actor =
                      Actor.spawn (fun inbox ->
                          let rec loop () =
                              actor {
                                  let! msg = inbox.Receive()
                                  received.Add msg

                                  match msg with
                                  | OnCompleted -> completed <- true
                                  | OnError _ -> completed <- true
                                  | _ -> ()

                                  return! loop ()
                              }

                          loop ())

                  let! _sub = Reactive.subscribeActor actor xs
                  do! waitUntil (fun () -> completed)

                  let actual = received |> Seq.toList
                  let expected = [ OnNext 1; OnNext 2; OnNext 3; OnCompleted ]
                  assertThat actual (isEqualTo expected)
                  Actor.kill actor
              }
          )

          testAsync (
              "flatMapActor emits transformed values downstream",
              async {
                  let xs = fromNotification [ OnNext 1; OnNext 2; OnNext 3; OnCompleted ]

                  let doubled =
                      xs
                      |> Reactive.flatMapActor (fun emit inbox ->
                          let rec loop () =
                              actor {
                                  let! x = inbox.Receive()
                                  emit (x * 2)
                                  return! loop ()
                              }

                          loop ())

                  let obv = TestObserver<int>()
                  let! _sub = doubled.SubscribeAsync obv
                  // Fire-and-forget emit is async, give actor time to process. The snapshot has
                  // to be re-fetched after the sleep — `Notifications` is only as fresh as the
                  // last Await/Refresh, and completion races ahead of the actor's emits.
                  do! obv.AwaitIgnore()
                  do! Async.Sleep 100
                  do! obv.Refresh()

                  let actual =
                      obv.Notifications
                      |> Seq.choose (function
                          | OnNext x -> Some x
                          | _ -> None)
                      |> Seq.toList
                      |> List.sort

                  assertThat actual (isEqualTo [ 2; 4; 6 ])
              }
          )

          testAsync (
              "mapActor applies stateful transform with backpressure",
              async {
                  let xs = fromNotification [ OnNext 10; OnNext 20; OnNext 30; OnCompleted ]

                  let withRunningSum =
                      xs
                      |> Reactive.mapActor (fun sum x -> sum + x, sum + x) 0

                  let obv = TestObserver<int>()
                  let! _sub = withRunningSum.SubscribeAsync obv
                  let! _ = obv.Await()

                  let actual =
                      obv.Notifications
                      |> Seq.choose (function
                          | OnNext x -> Some x
                          | _ -> None)
                      |> Seq.toList

                  assertThat actual (isEqualTo [ 10; 30; 60 ])
              }
          )

          testAsync (
              "flatMapActorSupervised Stop continues stream after crash",
              async {
                  let xs = fromNotification [ OnNext 1; OnNext 2; OnNext 3; OnCompleted ]

                  let result =
                      xs
                      |> Reactive.flatMapActorSupervised (fun _ -> Directive.Stop) (fun emit inbox ->
                          let rec loop () =
                              actor {
                                  let! x = inbox.Receive()

                                  if x = 2 then
                                      failwith "boom"

                                  emit (x * 10)
                                  return! loop ()
                              }

                          loop ())

                  let obv = TestObserver<int>()
                  let! _sub = result.SubscribeAsync obv
                  do! obv.AwaitIgnore()
                  do! Async.Sleep 200
                  do! obv.Refresh()

                  let actual =
                      obv.Notifications
                      |> Seq.choose (function
                          | OnNext x -> Some x
                          | _ -> None)
                      |> Seq.toList

                  // Item 2 crashes the actor (Stop = actor dies, item lost), item 3 is also lost
                  // because the actor is dead and Stop doesn't restart.
                  assertThat actual (contain 10)

                  let hasError =
                      obv.Notifications
                      |> Seq.exists (function
                          | OnError _ -> true
                          | _ -> false)

                  assertThat hasError isFalse
              }
          )

          testAsync (
              "flatMapActorSupervised Restart accepts future items after crash",
              async {
                  // Use a slow source so items arrive after restart, not all queued at once.
                  let xs =
                      Reactive.create (fun obv ->
                          async {
                              do! obv.OnNextAsync 1
                              do! Async.Sleep 50
                              do! obv.OnNextAsync 2
                              // Wait for crash + restart before sending item 3
                              do! Async.Sleep 200
                              do! obv.OnNextAsync 3
                              do! obv.OnCompletedAsync()
                              return AsyncDisposable.Empty
                          })

                  let result =
                      xs
                      |> Reactive.flatMapActorSupervised (fun _ -> Directive.Restart) (fun emit inbox ->
                          let rec loop () =
                              actor {
                                  let! x = inbox.Receive()

                                  if x = 2 then
                                      failwith "boom"

                                  emit (x * 10)
                                  return! loop ()
                              }

                          loop ())

                  let obv = TestObserver<int>()
                  let! _sub = result.SubscribeAsync obv
                  do! obv.AwaitIgnore()
                  do! Async.Sleep 500
                  do! obv.Refresh()

                  let actual =
                      obv.Notifications
                      |> Seq.choose (function
                          | OnNext x -> Some x
                          | _ -> None)
                      |> Seq.toList
                      |> List.sort

                  // Item 1 emits 10, item 2 crashes (lost), actor restarts, item 3 emits 30
                  assertThat actual (isEqualTo [ 10; 30 ])
              }
          )

          testAsync (
              "flatMapActor forwards actor crash as OnError",
              async {
                  let xs = fromNotification [ OnNext 1; OnCompleted ]
                  let error = System.InvalidOperationException("actor crash")

                  let crashing =
                      xs
                      |> Reactive.flatMapActor (fun _emit _inbox -> actor { raise error })

                  let obv = TestObserver<int>()
                  let! _sub = crashing.SubscribeAsync obv
                  do! Async.Sleep 200
                  do! obv.Refresh()

                  let hasError =
                      obv.Notifications
                      |> Seq.exists (function
                          | OnError _ -> true
                          | _ -> false)

                  assertThat hasError isTrue
              }
          )

          testAsync (
              "ofActor creates observable from actor emissions",
              async {
                  let actor, obs =
                      Reactive.ofActor (fun emit _inbox ->
                          actor {
                              emit 42
                              do! Async.Sleep 10
                              emit 43
                          })

                  let obv = TestObserver<int>()
                  let! _sub = obs.SubscribeAsync obv
                  // Give the actor time to emit
                  do! Async.Sleep 200
                  do! obv.Refresh()

                  let actual =
                      obv.Notifications
                      |> Seq.choose (function
                          | OnNext x -> Some x
                          | _ -> None)
                      |> Seq.toList

                  // `ofActor` spawns the actor eagerly and emits into a hot subject, so the
                  // pre-subscription `emit 42` is a race we cannot win from here: on .NET
                  // `Async.Start'` queues it and subscribe wins, but on the Fable targets it is
                  // `Async.StartImmediate` and runs inline before we subscribe. Only 43, emitted
                  // after the actor's sleep, is guaranteed to be observed.
                  assertThat actual (contain 43)
                  Actor.kill actor
              }
          ) ]
    )
