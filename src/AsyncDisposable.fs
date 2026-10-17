namespace Fable.Reactive

open System
open System.Threading

open Fable.Reactive.Core

/// Creates asynchronous disposables whose cancellation callback runs at most once.
///
/// invariant: each AsyncDisposable instance invokes its cancellation callback no more than once
type AsyncDisposable private (cancel) =
    let tryDispose = disposalGate ()

    interface IReactiveDisposable with
        member this.DisposeAsync() =
            async {
                if tryDispose () then
                    do! cancel ()
            }

    static member Create cancel : IReactiveDisposable =
        AsyncDisposable cancel :> IReactiveDisposable

    static member Empty: IReactiveDisposable =
        let cancel () = async { return () }
        AsyncDisposable cancel :> IReactiveDisposable

    static member Composite(disposables: IReactiveDisposable seq) : IReactiveDisposable =
        let cancel () =
            async {
                for d in disposables do
                    do! d.DisposeAsync()
            }

        AsyncDisposable cancel :> IReactiveDisposable

type Disposable(cancel) =
    // invariant: each Disposable instance invokes its cancellation callback no more than once
    let tryDispose = disposalGate ()

    interface IDisposable with
        member this.Dispose() =
            if tryDispose () then
                cancel ()

    static member Create(cancel) : IDisposable = new Disposable(cancel) :> IDisposable

    static member Empty: IDisposable =
        let cancel () = ()

        new Disposable(cancel) :> IDisposable

    static member Composite(disposables: IDisposable seq) : IDisposable =
        let cancel () =
            for d in disposables do
                d.Dispose()

        new Disposable(cancel) :> IDisposable

[<AutoOpen>]
module AsyncDisposable =
    type IReactiveDisposable with

        member this.ToDisposable() =
            { new IDisposable with
                member __.Dispose() = this.DisposeAsync() |> Async.Start'
            }

    type System.IDisposable with

        member this.ToAsyncDisposable() : IReactiveDisposable =
            AsyncDisposable.Create(fun () -> async { this.Dispose() })

    let canceller () =
        let cts = new CancellationTokenSource()
        let cancel () = async { cts.Cancel() }
        AsyncDisposable.Create cancel, cts.Token
