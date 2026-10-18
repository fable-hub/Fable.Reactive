# Filtering

Filtering operators keep the source value type unless they explicitly select a new type. Put the
source last so filters compose with `|>`.

```fsharp
open Fable.Reactive

let evenValues =
    Reactive.ofSeq [ 1; 2; 3; 4; 5 ]
    |> Reactive.filter (fun value -> value % 2 = 0)
    |> Reactive.take 2
```

| API | Behavior |
| --- | --- |
| `filter predicate` | Forward values satisfying a predicate |
| `filterAsync predicate` | Await an `Async<bool>` predicate for each value |
| `choose chooser` | Forward `Some value`, discarding `None` |
| `chooseAsync chooser` | Async version of `choose` |
| `distinctUntilChanged` | Suppress adjacent equal values; an earlier value may appear again |
| `skip count` | Ignore the first `count` values |
| `take count` | Forward up to `count` values, then complete |
| `takeLast count` | Buffer the final `count` values and emit them when the source completes |

For example, `Reactive.choose int.TryParse` is not directly suitable because `TryParse` uses an
out parameter. Return an option instead:

```fsharp
let parsed =
    Reactive.ofSeq [ "10"; "bad"; "20" ]
    |> Reactive.choose (fun text ->
        match System.Int32.TryParse text with
        | true, value -> Some value
        | false, _ -> None)
```

## Stop when another stream emits

`Reactive.takeUntil notifier source` completes the result when the notifier sends a value.
Notifier completion alone does not stop the source; notifier error is forwarded. It subscribes to
the notifier first, so an immediate stop signal is observed.

```fsharp
let stopped =
    Reactive.interval 0 100
    |> Reactive.takeUntil (Reactive.timer 1000)
```

Source and notifier deliver independently. A source value still in flight when the notifier is
observed can be dropped. Even awaiting a source publication before publishing to the notifier does
not establish downstream delivery order across the two streams. If that order matters, wait for a
downstream acknowledgement before sending the stop signal.

Continue with [transforming](transforming.md) or [aggregating](aggregating.md).
