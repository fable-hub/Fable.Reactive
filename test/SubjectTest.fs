module Tests.SubjectTest

open Scriptorium.Quill
open Scriptorium.Nib.Assertion
open type Scriptorium.Quill.Test

open Fable.Reactive
open Tests.Utils

exception TestExn of unit

let tests =
    testList (
        "Subject",
        [
            testAsync (
                "subject broadcasts completion to all observers",
                async {
                    // Arrange
                    let dispatch, stream = Reactive.subject ()
                    let obv1 = TestObserver<int>()
                    let obv2 = TestObserver<int>()

                    let! _ = stream.SubscribeAsync obv1
                    let! _ = stream.SubscribeAsync obv2

                    // Act
                    do! dispatch.OnCompletedAsync()

                    do! obv1.AwaitIgnore()
                    do! obv2.AwaitIgnore()

                    // Assert
                    let actual1 = obv1.Notifications |> Seq.toList
                    let actual2 = obv2.Notifications |> Seq.toList
                    let expected = [ OnCompleted ]

                    assertThat actual1 (isEqualTo expected)
                    assertThat actual2 (isEqualTo expected)
                }
            )

            testAsync (
                "subject broadcasts error to all observers",
                async {
                    // Arrange
                    let dispatch, stream = Reactive.subject ()
                    let obv1 = TestObserver<int>()
                    let obv2 = TestObserver<int>()

                    let! _ = stream.SubscribeAsync obv1
                    let! _ = stream.SubscribeAsync obv2

                    // Act
                    do! dispatch.OnErrorAsync(TestExn())

                    try
                        do! obv1.AwaitIgnore()
                    with _ ->
                        ()

                    try
                        do! obv2.AwaitIgnore()
                    with _ ->
                        ()

                    // Assert
                    let actual1 = obv1.Notifications |> Seq.toList
                    let actual2 = obv2.Notifications |> Seq.toList
                    let expected = [ OnError(TestExn()) ]

                    assertThat actual1 (isEqualTo expected)
                    assertThat actual2 (isEqualTo expected)
                }
            )

            testAsync (
                "subject does not broadcast error when the first observer throws",
                async {
                    // Arrange
                    let dispatch, stream = Reactive.subject ()
                    let obv1 = TestObserver<int>()
                    let obv2 = TestObserver<int>()

                    let! _ =
                        stream.SubscribeAsync(function
                            | OnNext _ -> raise (TestExn())
                            | n -> obv1.PostAsync n)

                    let! _ = stream.SubscribeAsync obv2

                    // Act
                    do! dispatch.OnNextAsync 1
                    do! dispatch.OnCompletedAsync()

                    try
                        do! obv1.AwaitIgnore()
                    with _ ->
                        ()

                    try
                        do! obv2.AwaitIgnore()
                    with _ ->
                        ()

                    // Assert
                    let actual1 = obv1.Notifications |> Seq.toList
                    let actual2 = obv2.Notifications |> Seq.toList

                    assertThat actual1 (isEqualTo [ OnError(TestExn()) ])
                    assertThat actual2 (isEqualTo [ OnNext 1; OnCompleted ])
                }
            )

            testAsync (
                "subject does not broadcast error when the second observer throws",
                async {
                    // Arrange
                    let dispatch, stream = Reactive.subject ()
                    let obv1 = TestObserver<int>()
                    let obv2 = TestObserver<int>()

                    let! _ = stream.SubscribeAsync obv1

                    let! _ =
                        stream.SubscribeAsync(function
                            | OnNext _ -> raise (TestExn())
                            | n -> obv2.PostAsync n)

                    // Act
                    do! dispatch.OnNextAsync 1
                    do! dispatch.OnCompletedAsync()

                    try
                        do! obv1.AwaitIgnore()
                    with _ ->
                        ()

                    try
                        do! obv2.AwaitIgnore()
                    with _ ->
                        ()

                    // Assert
                    let actual1 = obv1.Notifications |> Seq.toList
                    let actual2 = obv2.Notifications |> Seq.toList

                    assertThat actual1 (isEqualTo [ OnNext 1; OnCompleted ])
                    assertThat actual2 (isEqualTo [ OnError(TestExn()) ])
                }
            )

            testAsync (
                "replay subject gives a late subscriber the last two values before live values",
                async {
                    let dispatch, stream = Reactive.replaySubject 2
                    let first = TestObserver<char>()
                    let late = TestObserver<char>()

                    let! _ = stream.SubscribeAsync first
                    do! dispatch.OnNextAsync 'a'
                    do! dispatch.OnNextAsync 'b'
                    do! dispatch.OnNextAsync 'c'
                    do! first.WaitUntil(fun ns -> List.length ns = 3)

                    let! _ = stream.SubscribeAsync late
                    do! late.WaitUntil(fun ns -> List.length ns = 2)
                    do! dispatch.OnNextAsync 'd'
                    do! first.WaitUntil(fun ns -> List.length ns = 4)
                    do! late.WaitUntil(fun ns -> List.length ns = 3)

                    assertThat
                        (List.ofSeq first.Notifications)
                        (isEqualTo [ OnNext 'a'; OnNext 'b'; OnNext 'c'; OnNext 'd' ])

                    assertThat (List.ofSeq late.Notifications) (isEqualTo [ OnNext 'b'; OnNext 'c'; OnNext 'd' ])
                }
            )

            testAsync (
                "replay subject replays values then completion to a late subscriber",
                async {
                    let dispatch, stream = Reactive.replaySubject 2
                    let first = TestObserver<char>()
                    let late = TestObserver<char>()

                    let! _ = stream.SubscribeAsync first
                    do! dispatch.OnNextAsync 'a'
                    do! dispatch.OnNextAsync 'b'
                    do! dispatch.OnNextAsync 'c'
                    do! dispatch.OnCompletedAsync()
                    do! dispatch.OnNextAsync 'd'

                    do! first.AwaitIgnore()
                    let! _ = stream.SubscribeAsync late
                    do! late.AwaitIgnore()

                    assertThat
                        (List.ofSeq first.Notifications)
                        (isEqualTo [ OnNext 'a'; OnNext 'b'; OnNext 'c'; OnCompleted ])

                    assertThat (List.ofSeq late.Notifications) (isEqualTo [ OnNext 'b'; OnNext 'c'; OnCompleted ])
                }
            )

            testAsync (
                "replay subject replays values then error to a late subscriber",
                async {
                    let dispatch, stream = Reactive.replaySubject 1
                    let error = TestExn()
                    let late = TestObserver<int>()

                    do! dispatch.OnNextAsync 1
                    do! dispatch.OnNextAsync 2
                    do! dispatch.OnErrorAsync error
                    do! dispatch.OnCompletedAsync()

                    let! _ = stream.SubscribeAsync late
                    do! late.AwaitIgnore()

                    assertThat (List.ofSeq late.Notifications) (isEqualTo [ OnNext 2; OnError error ])
                }
            )

            testAsync (
                "replay subject stops live delivery after disposal",
                async {
                    let dispatch, stream = Reactive.replaySubject 2
                    let observer = TestObserver<int>()
                    let! subscription = stream.SubscribeAsync observer

                    do! dispatch.OnNextAsync 1
                    do! observer.WaitUntil(fun ns -> List.length ns = 1)
                    do! subscription.DisposeAsync()
                    do! dispatch.OnNextAsync 2
                    do! dispatch.OnCompletedAsync()
                    do! observer.Refresh()

                    assertThat (List.ofSeq observer.Notifications) (isEqualTo [ OnNext 1 ])
                }
            )

            testAsync (
                "replay subject rejects a non-positive buffer size",
                async {
                    let rejected =
                        try
                            Reactive.replaySubject<int> 0 |> ignore
                            false
                        with _ ->
                            true

                    assertThat rejected isTrue
                }
            )
        ]
    )
