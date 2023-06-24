open System
open System.Threading
open System.Threading.Tasks
open FSharp.Control
open FSharp.UMX
open Propulsion.Feed
open Propulsion.Internal
open Propulsion.Streams
open Serilog

Log.Logger <- LoggerConfiguration().WriteTo.Console().CreateLogger()


let memoryCheckpoints =
    { new IFeedCheckpointStore with
        member _.Start(source, tranche, origin) =
            async { return TimeSpan.FromSeconds 1000, Position.parse 0L }
        member _.Commit(source, tranche, pos) = async {return ()}}

let sinkStats =
        { new Propulsion.Streams.Stats<_>(Log.Logger, TimeSpan.FromMinutes 1, TimeSpan.FromMinutes 1) with
            override _.HandleOk(_) = ()
            override _.HandleExn(logger, x) = () }

let crawlEmptyBatch _ : AsyncSeq<TimeSpan * Internal.Batch<'b>> =
    asyncSeq { yield TimeSpan.Zero, { items = [||]; checkpoint = Position.initial; isTail = true } }
type NoopSource internal
    (   log : ILogger, statsInterval,
        tailSleepInterval,
        checkpoints : IFeedCheckpointStore, sink, tranches) =
    inherit Propulsion.Feed.Internal.TailingFeedSource(
            log, statsInterval, UMX.tag "mySource", tailSleepInterval,
            crawlEmptyBatch,
            checkpoints,
            None, sink, string)

    abstract member ListTranches : unit -> Async<Propulsion.Feed.TrancheId[]>
    default _.ListTranches() = async { return tranches }

    abstract member Pump : unit -> Async<unit>
    default x.Pump() = base.Pump(x.ListTranches)

    abstract member Start : unit -> Propulsion.Pipeline
    default x.Start() = base.Start(x.Pump())

let rec propulsion () =
    let handle _ =
        async { return () }

    let sink = Propulsion.Streams.StreamsProjector.Start(Log.Logger, 2, 2, handle, sinkStats, TimeSpan.FromSeconds 15)

    let source =
        NoopSource(
            Log.Logger,
            TimeSpan.FromSeconds 15,
            TimeSpan.FromMilliseconds 10,
            memoryCheckpoints,
            sink,
            [| TrancheId.parse "MyCategory" |]
        )

    let src = source.Start()

    src.AwaitWithStopOnCancellation() |> Async.RunSynchronously


// Giant freaking memory leak in your face here
propulsion ()
