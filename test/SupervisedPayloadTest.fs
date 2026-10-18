module Tests.SupervisedPayload

open Scriptorium.Quill
open Scriptorium.Nib.Assertion
open type Scriptorium.Quill.Test

open Fable.Actor
open Fable.Actor.Types
open Fable.Reactive
open Tests.Utils

type private ExitShapedValue = { Pid: int; Reason: string }

let private verifyPayloads (values: obj list) =
    async {
        let source =
            Reactive.create (fun observer ->
                async {
                    for value in values do
                        do! observer.OnNextAsync value

                    return AsyncDisposable.Empty
                })

        let result =
            source
            |> Reactive.flatMapActorSupervised (fun _ -> Directive.Escalate) (fun emit inbox ->
                let rec loop () =
                    actor {
                        let! value = inbox.Receive()
                        emit value
                        return! loop ()
                    }

                loop ())

        let observer = TestObserver<obj>()
        let! subscription = result.SubscribeAsync observer

        do!
            observer.WaitUntil(fun notifications ->
                List.filter isOnNext notifications |> List.length = List.length values
                || List.exists isOnError notifications)

        let actual =
            observer.Notifications
            |> Seq.choose (function
                | OnNext value -> Some value
                | _ -> None)
            |> Seq.toList

        assertThat (observer.Notifications |> Seq.exists isOnError) isFalse
        assertThat actual (isEqualTo values)

        do! subscription.DisposeAsync()
    }

let tests =
    testList (
        "SupervisedPayload",
        [
            testAsync (
                "exit-shaped records reach the child as values",
                verifyPayloads [ box { Pid = 42; Reason = "payload" }; box { Pid = 43; Reason = "next" } ]
            )
            testAsync (
                "ChildExited records can be upstream values",
                verifyPayloads [ box ({ Pid = box 42; Reason = box "payload" }: ChildExited) ]
            )
            testAsync (
                "null, primitive, and tuple payloads retain order",
                verifyPayloads [ null; box 0; box false; box "payload"; box ("reactive_forwarded_value", 42) ]
            )
        ]
    )
