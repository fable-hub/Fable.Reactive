module Tests.Utils

open System

open Scriptorium.Nib.Assertion

open Fable.Actor
open Fable.Actor.Types
open Fable.Reactive

/// Nib assertion: the list contains every element of `expected`, order-insensitive.
/// Nib's own `haveSameElements` sorts, which `Notification<'a>` cannot do — `OnError of exn`
/// leaves the type without a comparison constraint — so equality-only containment it is.
let containAll (expected: 'a list) : Assertion<'a list> =
    assertion
        (fun xs ->
            expected
            |> List.forall (fun e -> List.contains e xs))
        (fun xs -> $"given %A{xs} should contain all of %A{expected}")

/// Notification kind predicates. Handy for `WaitUntil` conditions, where the question is
/// "have N values landed yet?" rather than "what are they?".
let isOnNext (n: Notification<'a>) =
    match n with
    | OnNext _ -> true
    | _ -> false

let isOnCompleted (n: Notification<'a>) =
    match n with
    | OnCompleted -> true
    | _ -> false

/// Immutable snapshot of the observer state, returned across the process boundary.
type private Snapshot<'a> =
    { Notifications: Notification<'a> list
      Latest: 'a option
      Error: exn option
      Completed: bool }

type private Cmd<'a> =
    | Post of Notification<'a>
    | Get

/// Test observer interface: an IAsyncObserver plus query helpers.
type ITestObserver<'a> =
    inherit IAsyncObserver<'a>
    abstract Notifications: ResizeArray<Notification<'a>>
    abstract PostAsync: Notification<'a> -> Async<unit>
    abstract Refresh: unit -> Async<unit>
    abstract WaitUntil: (Notification<'a> list -> bool) -> Async<unit>
    abstract Await: unit -> Async<'a>
    abstract AwaitIgnore: unit -> Async<unit>

/// Cross-target test observer. All state is owned by a single actor and accessed
/// only via messages (post + callAsync query) — never shared mutable memory —
/// which is required on BEAM, where `safeObserver` delivers from a spawned
/// process whose mutations another process cannot see.
///
/// Implemented as a factory returning an OBJECT EXPRESSION rather than a class:
/// on Fable BEAM a class's interface-method body cannot read its instance fields
/// (they evaluate to undefined), whereas an object expression captures `store`
/// and `cached` lexically and works on every target.
///
/// `Await`/`AwaitIgnore`/`Refresh` fetch a snapshot and cache it locally, so the
/// `Notifications` property returns the state as of the last such call.
let TestObserver<'a> () : ITestObserver<'a> =
    let store =
        Actor.spawn (fun inbox ->
            let rec loop (notifications: Notification<'a> list, latest, error, completed) =
                actor {
                    let! (cmd, rc: ReplyChannel<Snapshot<'a>>) = inbox.Receive()

                    let notifications', latest', error', completed' =
                        match cmd with
                        | Get -> notifications, latest, error, completed
                        | Post n ->
                            let ns = notifications @ [ n ]

                            match n with
                            | OnNext x -> ns, Some x, error, completed
                            | OnError e -> ns, latest, Some e, true
                            | OnCompleted -> ns, latest, error, true

                    rc.Reply
                        { Notifications = notifications'
                          Latest = latest'
                          Error = error'
                          Completed = completed' }

                    return! loop (notifications', latest', error', completed')
                }

            loop ([], None, None, false))

    let mutable cached = ResizeArray<Notification<'a>>()

    let post (n: Notification<'a>) = async { Actor.cast store (Post n) }

    let awaitSnapshot () =
        let rec loop () =
            async {
                let! snap = Actor.callAsync store Get

                if snap.Completed then
                    cached <- ResizeArray(snap.Notifications)
                    return snap
                else
                    do! Async.Sleep 5
                    return! loop ()
            }

        loop ()

    { new ITestObserver<'a> with
        member _.OnNextAsync x = post (OnNext x)
        member _.OnErrorAsync err = post (OnError err)
        member _.OnCompletedAsync() = post OnCompleted

        member _.PostAsync(n: Notification<'a>) = post n
        member _.Notifications = cached

        /// Fetch the current store snapshot and refresh the local cache. Use for
        /// streams that never complete (e.g. disposed before a terminal event).
        member _.Refresh() =
            async {
                let! snap = Actor.callAsync store Get
                cached <- ResizeArray(snap.Notifications)
            }

        /// Poll until the notifications so far satisfy the predicate, refreshing the
        /// cache as it goes. Prefer this over `Async.Sleep n` for "wait until the value
        /// shows up": a loaded CI runner can starve a timer past any fixed margin, and
        /// Quill's own timeout is what fails the test if the value never arrives.
        member _.WaitUntil(predicate) =
            let rec loop () =
                async {
                    let! snap = Actor.callAsync store Get
                    cached <- ResizeArray(snap.Notifications)

                    if predicate snap.Notifications then
                        return ()
                    else
                        do! Async.Sleep 5
                        return! loop ()
                }

            loop ()

        /// Wait until the stream completes, then return the latest OnNext value.
        /// Re-raises if the stream errored.
        member _.Await() =
            async {
                let! snap = awaitSnapshot ()

                match snap.Error with
                | Some e -> return raise e
                | None ->
                    match snap.Latest with
                    | Some x -> return x
                    | None -> return failwith "Stream completed without a value"
            }

        /// Wait until the stream completes, ignoring any error.
        member _.AwaitIgnore() =
            async {
                let! _ = awaitSnapshot ()
                return ()
            } }

/// Poll until the predicate holds. Cross-target: no Task or blocking primitives,
/// only `Async.Sleep`, which every Fable target implements.
let waitUntil (predicate: unit -> bool) : Async<unit> =
    let rec loop () =
        async {
            if predicate () then
                return ()
            else
                do! Async.Sleep 5
                return! loop ()
        }

    loop ()

let fromNotification (notifications: seq<Notification<'a>>) =
    Create.ofAsyncWorker (fun obv token ->
        async {
            for notification in notifications do
                if token.IsCancellationRequested then
                    raise (OperationCanceledException("Operation cancelled"))

                match notification with
                | OnNext x ->
                    try
                        do! obv.OnNextAsync x
                    with err ->
                        do! obv.OnErrorAsync err
                | OnError err -> do! obv.OnErrorAsync err
                | OnCompleted -> do! obv.OnCompletedAsync()
        })
