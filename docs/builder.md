# The reactive builder

Open `Fable.Reactive` to use the `reactive { ... }` computation expression. It produces an
`IAsyncObservable<'T>`, which still needs a subscription to deliver values. Use `yield` for one
value and `yield!` to include an existing stream.

```fsharp
open Fable.Reactive

let values =
    reactive {
        yield 1
        yield! Reactive.ofSeq [ 2; 3 ]
    }
```

This stream emits `1`, `2`, and `3`, then completes. Consecutive `yield` and `yield!` clauses are
concatenated: the first stream finishes before the next begins. `reactive { () }` makes an empty
stream. The builder supports `yield`, not `return`.

## Bind a stream or async workflow

`let!` binds each source value to the rest of the expression. For a stream source, it uses the same
merge behavior as `Reactive.flatMap`: inner results can interleave.

```fsharp
let sums =
    reactive {
        let! left = Reactive.ofSeq [ 1; 2 ]
        let! right = Reactive.ofSeq [ 10; 20 ]
        yield left + right
    }
```

`sums` produces the four combinations `11`, `21`, `12`, and `22`; their arrival order can vary.
`let!` also accepts `Async<'T>` and turns its result into a one-value stream:

```fsharp
let loaded =
    reactive {
        let! value = async { return 42 }
        yield value * 2
    }
```

`yield!` can include an `Async<'T>` directly when its one result is the value to emit.

## Iterate in order

`for` accepts an F# sequence or an `IAsyncObservable<'T>`. It uses `concatMap`, so each iteration's
stream completes before the next iteration begins.

```fsharp
let ordered =
    reactive {
        for value in [ 1; 2; 3 ] do
            yield value * 10
    }
```

Choose `for` for ordered expansion and `let!` when merging inner streams is acceptable. See
[transforming](transforming.md#map-each-value-to-a-stream) for the corresponding operators.

## Manage the subscription outside the builder

The builder creates a stream; it does not subscribe to it or own its lifetime. Use
`SubscribeAsync` and await `DisposeAsync()` when you are done, as shown in
[getting started](getting-started.md#your-first-subscription).

The ordinary `async { ... }` builder also accepts `use!` for an `IReactiveDisposable`, including a
reactive subscription. Its cleanup starts `DisposeAsync()` without awaiting completion. When
cleanup must finish before continuing, bind the disposable with `let!` and explicitly
`do! subscription.DisposeAsync()`.
