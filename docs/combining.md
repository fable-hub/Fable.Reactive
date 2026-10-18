# Combining streams

Use `concat` when one stream must finish before the next begins. Use `merge` when both can run
together and value interleaving is acceptable.

```fsharp
open Fable.Reactive

let first = Reactive.ofSeq [ 1; 2 ]
let second = Reactive.ofSeq [ 3; 4 ]

let ordered = first |> Reactive.concat second
let concurrent = first |> Reactive.merge second
```

`ordered` emits values from `first`, then `second`. `concurrent` forwards values as they arrive;
do not rely on one source winning a race. `concatSeq` and `mergeSeq` take a sequence of streams.
`mergeInner` flattens a stream of streams with unbounded active inner subscriptions. `startWith`
prepends ordinary values before subscribing to the source.

## Pair values

| API | When it emits |
| --- | --- |
| `combineLatest other source` | After both have produced a value, then on each new value from either stream |
| `withLatestFrom other source` | When `source` emits and `other` already has a value |
| `zipSeq items source` | Pair each source value with the next item in a sequence |

The tuple order is `(sourceValue, otherValue)`. `combineLatest` and `withLatestFrom` currently
complete when either input completes; plan for that when combining finite and long-lived sources.

```fsharp
let readings = Reactive.interval 0 100
let settings = Reactive.interval 0 250

let latestPairs =
    readings |> Reactive.combineLatest settings

let annotated =
    readings |> Reactive.zipSeq [ "first"; "second"; "third" ]
```

`zipSeq` completes when the source completes or when it tries to pair a source value after the
sequence has run out. For derived values, continue with [aggregating](aggregating.md).
