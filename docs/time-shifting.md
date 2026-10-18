# Time shifting

The time arguments in this guide are integer milliseconds. Keep the subscription returned by
`SubscribeAsync` so an active timer or repeating source can be disposed.

## Start on a timer

`Reactive.timer dueTime` emits `0` once after `dueTime`, then completes.
`Reactive.interval firstDelay period` starts at `0` after `firstDelay` and increments on each
`period`. A positive period repeats; a non-positive period produces one value and completes.

```fsharp
open Fable.Reactive

let once = Reactive.timer 500
let ticks = Reactive.interval 500 1000
```

## Shift or reduce activity

| API | Behavior |
| --- | --- |
| `delay msecs` | Delay each notification while preserving their order |
| `debounce msecs` | Emit a value only after no newer value arrives during the quiet period |
| `sample msecs` | At periodic ticks, emit the most recently seen source value, if any |

```fsharp
let slower =
    Reactive.interval 0 100
    |> Reactive.sample 500

let settled =
    Reactive.interval 0 100
    |> Reactive.debounce 250
```

`settled` does not emit while values keep arriving every 100 ms. For a finite burst, it emits
only a value that survives the quiet period. If the source completes or errors while a debounce
timer is pending, that pending value is dropped and the terminal notification is sent.

`delay` also delays completion and error after earlier values, maintaining their order. For
stopping a stream at a deadline, combine `timer` with [`takeUntil`](filtering.md#stop-when-another-stream-emits).
