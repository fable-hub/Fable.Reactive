namespace Fable.Reactive

open System.Threading

module Core =
    let infinite = Seq.initInfinite id

    // decision: shares the disposal gate across BEAM processes because subscriptions cross actor boundaries
    let internal disposalGate () =
#if FABLE_COMPILER_BEAM
        let cell: obj =
            Fable.Core.BeamInterop.emitErlExpr () "begin A = atomics:new(1, []), atomics:put(A, 1, 0), A end"

        fun () -> Fable.Core.BeamInterop.emitErlExpr cell "atomics:compare_exchange($0, 1, 0, 1) =:= ok"
#else
        let mutable disposed = 0

        fun () ->
#if FABLE_COMPILER
            if disposed = 0 then
                disposed <- 1
                true
            else
                false
#else
            Interlocked.Exchange(&disposed, 1) = 0
#endif
#endif

    type Async with

        /// Starts the asynchronous computation in the thread pool, or
        /// immediately for Fable. Do not await its result. If no cancellation
        /// token is provided then the default cancellation token is used.
        ///
        /// decision: uses each target's native fire-and-forget scheduler to keep this portability boundary centralized
        /// tradeoff: Fable work may run inline before surrounding subscription setup finishes, unlike queued .NET work
        static member Start'(computation: Async<unit>, ?cancellationToken: CancellationToken) : unit =
#if FABLE_COMPILER
            Async.StartImmediate(computation, ?cancellationToken = cancellationToken)
#else
            Async.Start(computation, ?cancellationToken = cancellationToken)
#endif

    [<CompilationRepresentation(CompilationRepresentationFlags.ModuleSuffix)>]

    module Async =
        let empty = async { () }

        let noop = fun _ -> empty
