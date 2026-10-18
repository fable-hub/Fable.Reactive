# Transforming

Use `map` for a synchronous one-to-one projection and `mapAsync` when the projection returns an
`Async<'T>`. `mapi` and `mapiAsync` also pass a zero-based index as part of a `(value, index)` tuple.

```fsharp
open Fable.Reactive

let labels =
    Reactive.ofSeq [ 10; 20; 30 ]
    |> Reactive.mapi (fun (value, index) -> sprintf "%d: %d" index value)

let loaded =
    Reactive.ofSeq [ 1; 2; 3 ]
    |> Reactive.mapAsync (fun id -> async { return sprintf "item-%d" id })
```

## Map each value to a stream

When each source value produces another `IAsyncObservable`, choose how to handle those inner
streams:

| API | Inner-stream behavior |
| --- | --- |
| `flatMap mapper` | Merge all mapped streams; values may interleave |
| `concatMap mapper` | Subscribe to mapped streams one at a time, in source order |
| `flatMapLatest mapper` | Switch to the newest mapped stream, disposing the previous one |
| `switchLatest` | Apply the same switching behavior to a stream of streams |

```fsharp
let ordered =
    Reactive.ofSeq [ 1; 2; 3 ]
    |> Reactive.concatMap (fun id -> Reactive.ofAsync (async { return id * 10 }))
```

`flatMapAsync` and `flatMapLatestAsync` await a mapper that returns
`Async<IAsyncObservable<'T>>`. `flatMapi` and `flatMapiAsync` also pass an index. Switching disposes
the old inner subscription before subscribing to the new one; completion of the outer stream waits
for the current inner stream.

Use [combining](combining.md) to join independent sources and [aggregating](aggregating.md) for
running state.
