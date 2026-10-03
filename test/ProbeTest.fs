module Tests.Probe

open Scriptorium.Quill
open Scriptorium.Nib.Assertion
open type Scriptorium.Quill.Test

open Fable.Actor
open Fable.Actor.Types
open Fable.Reactive
open Tests.Utils

// A class observer whose target actor is a CONSTRUCTOR PARAMETER (not a `let`
// field that spawns internally), to test whether class field access from an
// interface method works on BEAM.
type ProbeSink(target: Actor<int * ReplyChannel<int>>) =
    interface IAsyncObserver<int> with
        member _.OnNextAsync x = async { Actor.cast target x }
        member _.OnErrorAsync _ = async { () }
        member _.OnCompletedAsync() = async { () }

// Isolated smoke tests for the actor primitives the TestObserver relies on,
// to diagnose the BEAM hang independently of the observable pipeline.
let tests =
    testList (
        "Probe",
        [
            testAsync (
                "callAsync round-trips",
                async {
                    let store =
                        Actor.spawn (fun inbox ->
                            let rec loop () =
                                actor {
                                    let! (_msg, rc: ReplyChannel<int>) = inbox.Receive()
                                    rc.Reply 42
                                    return! loop ()
                                }

                            loop ())

                    let! r = Actor.callAsync store 0
                    assertThat r (isEqualTo 42)
                }
            )

            testAsync (
                "cast then callAsync sees the state",
                async {
                    let store =
                        Actor.spawn (fun inbox ->
                            let rec loop (n: int) =
                                actor {
                                    let! (cmd, rc: ReplyChannel<int>) = inbox.Receive()
                                    let n' = if cmd >= 0 then n + cmd else n
                                    rc.Reply n'
                                    return! loop n'
                                }

                            loop 0)

                    Actor.cast store 5
                    Actor.cast store 7
                    let! total = Actor.callAsync store -1
                    assertThat total (isEqualTo 12)
                }
            )

            testAsync (
                "bridge: do! async unit inside actor body",
                async {
                    let a =
                        Actor.spawn (fun inbox ->
                            let rec loop () =
                                actor {
                                    let! (x, rc: ReplyChannel<int>) = inbox.Receive()
                                    do! async { return () }
                                    rc.Reply(x + 1)
                                    return! loop ()
                                }

                            loop ())

                    let! r = Actor.callAsync a 41
                    assertThat r (isEqualTo 42)
                }
            )

            testAsync (
                "bridge: let! value from async in actor",
                async {
                    let a =
                        Actor.spawn (fun inbox ->
                            let rec loop () =
                                actor {
                                    let! (x, rc: ReplyChannel<int>) = inbox.Receive()
                                    let! v = async { return 1 }
                                    rc.Reply(x + v)
                                    return! loop ()
                                }

                            loop ())

                    let! r = Actor.callAsync a 41
                    assertThat r (isEqualTo 42)
                }
            )

            testAsync (
                "bridge: let! from async with try/with in actor",
                async {
                    let a =
                        Actor.spawn (fun inbox ->
                            let rec loop () =
                                actor {
                                    let! (x, rc: ReplyChannel<int>) = inbox.Receive()

                                    let! v =
                                        async {
                                            try
                                                do! async { return () }
                                                return 1
                                            with _ ->
                                                return 0
                                        }

                                    rc.Reply(x + v)
                                    return! loop ()
                                }

                            loop ())

                    let! r = Actor.callAsync a 41
                    assertThat r (isEqualTo 42)
                }
            )

            testAsync (
                "inline observer casting to sink, subscribed to single",
                async {
                    let sink =
                        Actor.spawn (fun inbox ->
                            let rec loop (last: int) =
                                actor {
                                    let! (cmd, rc: ReplyChannel<int>) = inbox.Receive()
                                    let last' = if cmd >= 0 then cmd else last
                                    rc.Reply last'
                                    return! loop last'
                                }

                            loop 0)

                    let obv =
                        { new IAsyncObserver<int> with
                            member _.OnNextAsync x = async { Actor.cast sink x }
                            member _.OnErrorAsync _ = async { () }
                            member _.OnCompletedAsync() = async { () }
                        }

                    let! _sub = (Reactive.single 42).SubscribeAsync obv
                    do! Async.Sleep 100
                    let! got = Actor.callAsync sink -1
                    assertThat got (isEqualTo 42)
                }
            )

            testAsync (
                "class observer (ctor-param actor) subscribed to single",
                async {
                    let sink =
                        Actor.spawn (fun inbox ->
                            let rec loop (last: int) =
                                actor {
                                    let! (cmd, rc: ReplyChannel<int>) = inbox.Receive()
                                    let last' = if cmd >= 0 then cmd else last
                                    rc.Reply last'
                                    return! loop last'
                                }

                            loop 0)

                    let obv = ProbeSink(sink) :> IAsyncObserver<int>
                    let! _sub = (Reactive.single 42).SubscribeAsync obv
                    do! Async.Sleep 100
                    let! got = Actor.callAsync sink -1
                    assertThat got (isEqualTo 42)
                }
            )

            testAsync (
                "single delivers to observer store (no Await)",
                async {
                    let obv = TestObserver<int>()
                    let xs = Reactive.single 42
                    let! _sub = xs.SubscribeAsync obv
                    do! Async.Sleep 100
                    do! obv.Refresh()
                    let actual = obv.Notifications |> Seq.toList
                    assertThat actual (hasSize 2)
                }
            )
        ]
    )
