namespace Fable.Reactive

open System
open System.Threading
open Fable.Actor
open Fable.Actor.Types
open Fable.Reactive.Core

#if FABLE_COMPILER_BEAM
open Fable.Core
open Fable.Core.BeamInterop
#endif

/// Subscription-local lifecycle helpers for Fable.Actor workers.
module internal ActorLifetime =
    // decision: uses a shared atomic on BEAM because captured mutable values are process-local
    type Flag(initial: int) =
#if FABLE_COMPILER_BEAM
        let cell: obj =
            emitErlExpr initial "begin A = atomics:new(1, []), atomics:put(A, 1, $0), A end"

        member _.Value = emitErlExpr cell "atomics:get($0, 1)"

        member _.Set(value: int) : unit =
            emitErlExpr (cell, value) "atomics:put($0, 1, $1)"

        member _.TrySet(expected: int, value: int) : bool =
            emitErlExpr (cell, expected, value) "atomics:compare_exchange($0, 1, $1, $2) =:= ok"
#else
        let mutable value = initial

        member _.Value =
#if FABLE_COMPILER
            value
#else
            Volatile.Read(&value)
#endif
        member _.Set(next: int) =
#if FABLE_COMPILER
            value <- next
#else
            Interlocked.Exchange(&value, next) |> ignore
#endif
        member _.TrySet(expected: int, next: int) =
#if FABLE_COMPILER
            if value = expected then
                value <- next
                true
            else
                false
#else
            Interlocked.CompareExchange(&value, next, expected) = expected
#endif
#endif

    let waitUntil timeout description predicate =
        async {
            let deadline = DateTime.UtcNow.AddMilliseconds(float timeout)

            while not (predicate ()) do
                if DateTime.UtcNow >= deadline then
                    raise (TimeoutException(description))

                do! Async.Sleep 5
        }

    // decision: polls unique reply channels so disposal and actor death settle waits without a runtime call
    // invariant: a deadline abandons only the reply wait and never retries or retracts the request
    let reply<'T> () : ReplyChannel<'T> * (unit -> 'T option) =
#if FABLE_COMPILER_BEAM
        let caller: obj = emitErlExpr () "self()"
        let reference: obj = emitErlExpr () "make_ref()"

        let rc =
            {
                Reply = fun (value: 'T) -> emitErlExpr (caller, reference, value) "$0 ! {reactive_reply, $1, $2}, ok"
            }

        rc, (fun () -> emitErlExpr reference "receive {reactive_reply, $0, V} -> {some, V} after 0 -> undefined end")
#else
        let mutable result = None
        let ready = Flag(0)

        let rc =
            {
                Reply =
                    fun value ->
                        result <- Some value
                        ready.Set 1
            }

        rc, (fun () -> if ready.Value = 1 then result else None)
#endif

    type Worker<'T> =
        {
            Actor: Actor<'T>
            IsStopped: unit -> bool
            Stop: int -> Async<unit>
        }

    // decision: delegates cancellation and exit observation to the public runtime instead of constructing actor internals
    // invariant: successful Stop observes workflow exit on emulated targets and process death on BEAM
    let spawnWithToken
        (token: CancellationToken)
        (parent: Actor<obj> option)
        (onError: exn -> unit)
        (body: ActorInbox<'T> -> ActorOp<unit>)
        : Worker<'T> =
        let finished = Flag(0)

        let wrapped inbox =
            actor {
                try
                    try
                        do! body inbox
                    with ex ->
                        onError ex
                finally
                    finished.Set 1
            }

        let worker =
#if FABLE_COMPILER_BEAM
            match parent with
            | Some p -> Actor.spawnLinked (p, wrapped)
            | None -> Actor.spawn (wrapped, cancellationToken = token)
#else
            Actor.spawn (wrapped, cancellationToken = token)
#endif

        let stopped () : bool =
#if FABLE_COMPILER_BEAM
            emitErlExpr worker.Pid "not is_process_alive($0)"
#else
            finished.Value = 1
#endif

        {
            Actor = worker
            IsStopped = stopped
            Stop =
                fun timeout ->
                    async {
                        let! result = Actor.asyncStop timeout worker

                        match result with
                        | StopResult.Completed _ -> return ()
                        | StopResult.TimedOut -> return raise (TimeoutException("Actor worker did not stop"))
                    }
        }

    let spawn parent onError body =
        spawnWithToken CancellationToken.None parent onError body

    // decision: runs cleanup independently of a disposing caller's cancellation and shares its outcome
    // tradeoff: cleanup errors use stable component messages across processes rather than process-local exception objects
    let disposable timeout (close: unit -> unit) (cleanup: unit -> Async<unit>) : IReactiveDisposable =
        let phase = Flag(0)

        { new IReactiveDisposable with
            member _.DisposeAsync() =
                async {
                    if phase.TrySet(0, 1) then
                        close ()

                        Async.Start'(
                            async {
                                try
                                    do! cleanup ()
                                    phase.Set 2
                                with
                                | :? TimeoutException -> phase.Set 3
                                | _ -> phase.Set 4
                            }
                        )

                    do! waitUntil timeout "Subscription cleanup has not finished" (fun () -> phase.Value >= 2)

                    match phase.Value with
                    | 3 -> return raise (TimeoutException("Actor subscription cleanup timed out"))
                    | 4 -> return raise (InvalidOperationException("Actor subscription cleanup failed"))
                    | _ -> return ()
                }
        }

    let cleanupStep timeout description (work: Async<unit>) =
        async {
            let outcome = Flag(0)

            Async.Start'(
                async {
                    try
                        do! work
                        outcome.Set 1
                    with _ ->
                        outcome.Set 2
                }
            )

            do! waitUntil timeout description (fun () -> outcome.Value <> 0)

            if outcome.Value = 2 then
                raise (InvalidOperationException(description))
        }

    let cleanupAll (steps: (unit -> Async<unit>) list) =
        async {
            let mutable timedOut = false
            let mutable failed = false

            for step in steps do
                try
                    do! step ()
                with
                | :? TimeoutException -> timedOut <- true
                | _ -> failed <- true

            if timedOut then
                raise (TimeoutException("Actor subscription cleanup timed out"))

            if failed then
                raise (InvalidOperationException("Actor subscription cleanup failed"))
        }
