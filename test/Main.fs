module Tests.Main

open Scriptorium.Quill
open type Scriptorium.Quill.Runner

// One runner for every target. Quill knows how to end the process on each platform:
//
//   .NET / Python  Async.RunSynchronously, so the value returned here is the process exit code
//   BEAM           the run is synchronous; Quill calls halt/1, so `erl` exits non-zero on failure
//   JS / Node      nothing can block, so runTests returns 0 immediately and chains process.exit
//                  onto the resolved promise — the value returned here is ignored
//
// This is a timing-heavy suite (timers, debounce, subjects), so the "slow test" threshold is
// raised from Quill's 300ms default — several tests sleep on purpose and are not slow at all.
//
// decision: uses one Quill entry point so every target executes the same registered test lists
// assumption: Quill owns platform-specific process termination and propagates failures through the process exit code
// decision: raises the slow threshold to distinguish intentional timer waits from unexpectedly slow tests
[<EntryPoint>]
let main _argv =
    runTestsWith (
        slowThreshold 1000,
        [
            Tests.GroupBy.tests
            Tests.Observer.tests
            Tests.Subscribe.tests
            Tests.Create.tests
            Tests.Filter.tests
            Tests.Map.tests
            Tests.Merge.tests
            Tests.Concat.tests
            Tests.Bind.tests
            Tests.Query.tests
            Tests.Catch.tests
            Tests.Scan.tests
            Tests.SubjectTest.tests
            Tests.TakeUntil.tests
#if !FABLE_COMPILER
            Tests.AsyncSeq.tests
#endif
            Tests.Timeshift.tests
            Tests.Debounce.tests
            Tests.ActorInterop.tests
            Tests.Probe.tests
        ]
    )
