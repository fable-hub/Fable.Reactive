# Aggregating

Aggregation keeps state across values in one subscription. Choose a running result when each
intermediate state matters, or a final result when only the end matters.

## Running results

`scanInit initial accumulator` starts with an explicit state. It emits the updated state after
each source value; it does not emit the initial state on its own. `scan` uses the first source value
as its initial state and emits that value. Both have async variants for an accumulator returning
`Async<'State>`.

```fsharp
open Fable.Reactive

let totals =
    Reactive.ofSeq [ 1; 2; 3 ]
    |> Reactive.scanInit 0 (+)

// Emits 1, 3, 6, then completes.
```

Use `scanInitAsync initial accumulator` or `scanAsync accumulator` when updating the state takes
asynchronous work. As with other stateful operators, each subscription has its own accumulated
state.

## Final results

`reduceAsync accumulator` emits only the final accumulated value when the source completes. It
uses the first source value as its starting state. `max` and `min` are final-result operators built
on the same reduction pattern. An empty source has no final value to emit.

```fsharp
let largest =
    Reactive.ofSeq [ 8; 3; 13; 5 ]
    |> Reactive.max

let sum =
    Reactive.ofSeq [ 1; 2; 3 ]
    |> Reactive.reduceAsync (fun total value -> async { return total + value })
```

`takeLast count` in the [filtering guide](filtering.md) is useful when you want the last values
themselves rather than a reduced result. Both kinds of final result require source completion.

## Group values

`groupBy keyMapper` emits one inner stream per distinct key. Subscribe to each inner stream as the
outer stream emits it: its first value waits for that subscription. Groups receive their source
values and terminal notification, and the outer stream completes after the groups complete.

```fsharp
let perParityTotals =
    Reactive.ofSeq [ 1; 2; 3; 4 ]
    |> Reactive.groupBy (fun value -> value % 2)
    |> Reactive.flatMap (Reactive.scanInit 0 (+))
```

The groups contain values, not a public key property. Keep the key in each value if downstream
consumers need to identify which group produced a result.
