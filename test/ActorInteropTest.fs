module Tests.ActorInterop

open Fable.Actor
open Fable.Actor.Types
open Fable.Reactive

open Expecto
open Tests.Utils

[<Tests>]
let tests =
    testList
        "ActorInterop Tests"
        [ testAsync "subscribeActor receives all notifications" {
              let xs = fromNotification [ OnNext 1; OnNext 2; OnNext 3; OnCompleted ]
              let received = System.Collections.Generic.List<Notification<int>>()
              let tcs = System.Threading.Tasks.TaskCompletionSource<unit>()

              let actor =
                  Actor.spawn (fun inbox ->
                      let rec loop () =
                          actor {
                              let! msg = inbox.Receive()
                              received.Add msg

                              match msg with
                              | OnCompleted -> tcs.SetResult()
                              | OnError _ -> tcs.SetResult()
                              | _ -> ()

                              return! loop ()
                          }

                      loop ())

              let! _sub = Reactive.subscribeActor actor xs
              do! Async.AwaitTask tcs.Task

              let actual = received |> Seq.toList
              let expected = [ OnNext 1; OnNext 2; OnNext 3; OnCompleted ]
              Expect.equal actual expected "Actor should receive all notifications"
              Actor.kill actor
          }

          testAsync "flatMapActor emits transformed values downstream" {
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
              // Fire-and-forget emit is async, give actor time to process
              do! obv.AwaitIgnore()
              do! Async.Sleep 100

              let actual =
                  obv.Notifications
                  |> Seq.choose (function
                      | OnNext x -> Some x
                      | _ -> None)
                  |> Seq.toList

              Expect.equal (actual |> List.sort) [ 2; 4; 6 ] "Should emit doubled values"
          }

          testAsync "mapActor applies stateful transform with backpressure" {
              let xs = fromNotification [ OnNext 10; OnNext 20; OnNext 30; OnCompleted ]

              let withRunningSum =
                  xs |> Reactive.mapActor (fun sum x -> sum + x, sum + x) 0

              let obv = TestObserver<int>()
              let! _sub = withRunningSum.SubscribeAsync obv
              let! _ = obv.Await()

              let actual =
                  obv.Notifications
                  |> Seq.choose (function
                      | OnNext x -> Some x
                      | _ -> None)
                  |> Seq.toList

              Expect.equal actual [ 10; 30; 60 ] "Should emit running sums"
          }

          testAsync "flatMapActorSupervised Stop continues stream after crash" {
              let xs = fromNotification [ OnNext 1; OnNext 2; OnNext 3; OnCompleted ]

              let result =
                  xs
                  |> Reactive.flatMapActorSupervised
                      (fun _ -> Directive.Stop)
                      (fun emit inbox ->
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

              let actual =
                  obv.Notifications
                  |> Seq.choose (function
                      | OnNext x -> Some x
                      | _ -> None)
                  |> Seq.toList

              // Item 2 crashes the actor (Stop = actor dies, item lost), item 3 is also lost
              // because the actor is dead and Stop doesn't restart.
              Expect.contains actual 10 "Should emit item before crash"

              let hasError =
                  obv.Notifications
                  |> Seq.exists (function
                      | OnError _ -> true
                      | _ -> false)

              Expect.isFalse hasError "Stop should not forward error downstream"
          }

          testAsync "flatMapActorSupervised Restart accepts future items after crash" {
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
                  |> Reactive.flatMapActorSupervised
                      (fun _ -> Directive.Restart)
                      (fun emit inbox ->
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

              let actual =
                  obv.Notifications
                  |> Seq.choose (function
                      | OnNext x -> Some x
                      | _ -> None)
                  |> Seq.toList
                  |> List.sort

              // Item 1 emits 10, item 2 crashes (lost), actor restarts, item 3 emits 30
              Expect.equal actual [ 10; 30 ] "Should emit items 1 and 3, skip crashed item 2"
          }

          testAsync "flatMapActor forwards actor crash as OnError" {
              let xs = fromNotification [ OnNext 1; OnCompleted ]
              let error = System.InvalidOperationException("actor crash")

              let crashing =
                  xs
                  |> Reactive.flatMapActor (fun _emit _inbox ->
                      actor { raise error })

              let obv = TestObserver<int>()
              let! _sub = crashing.SubscribeAsync obv
              do! Async.Sleep 200

              let hasError =
                  obv.Notifications
                  |> Seq.exists (function
                      | OnError _ -> true
                      | _ -> false)

              Expect.isTrue hasError "Should forward actor crash as OnError"
          }

          testAsync "ofActor creates observable from actor emissions" {
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

              let actual =
                  obv.Notifications
                  |> Seq.choose (function
                      | OnNext x -> Some x
                      | _ -> None)
                  |> Seq.toList

              Expect.equal actual [ 42; 43 ] "Should emit actor values"
              Actor.kill actor
          } ]
