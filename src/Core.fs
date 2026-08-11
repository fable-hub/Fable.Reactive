namespace Fable.Reactive

open System.Threading

module Core =
    let infinite = Seq.initInfinite id

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
