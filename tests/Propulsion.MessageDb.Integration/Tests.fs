module Propulsion.MessageDb.Integration.Tests

open FSharp.Control
open Npgsql
open NpgsqlTypes
open Propulsion.MessageDb
open Propulsion.Internal
open Serilog
open Serilog.Core
open Swensen.Unquote
open System
open System.Collections.Generic
open System.Threading.Tasks
open Xunit

module Simple =
    type Hello = { name : string}
    type Event =
        | Hello of Hello
        interface TypeShape.UnionContract.IUnionContract
    let codec = FsCodec.SystemTextJson.Codec.Create<Event>()

let createStreamMessage streamName =
    let cmd = NpgsqlBatchCommand()
    cmd.CommandText <- "select 1 from write_message(@Id::text, @StreamName, @EventType, @Data, null, null)"
    cmd.Parameters.AddWithValue("Id", NpgsqlDbType.Uuid, Guid.NewGuid()) |> ignore
    cmd.Parameters.AddWithValue("StreamName", NpgsqlDbType.Text, streamName) |> ignore
    cmd.Parameters.AddWithValue("EventType", NpgsqlDbType.Text, "Hello") |> ignore
    cmd.Parameters.AddWithValue("Data", NpgsqlDbType.Jsonb, """{"name": "world"}""") |> ignore
    cmd

let writeMessagesToStream (conn: NpgsqlConnection) streamName = task {
    let batch = conn.CreateBatch()
    for _ in 1..20 do
        let cmd = createStreamMessage streamName
        batch.BatchCommands.Add(cmd)
    do! batch.ExecuteNonQueryAsync() :> Task }

let writeMessagesToCategory category = task {
    use conn = new NpgsqlConnection("Host=localhost; Port=5433; Username=message_store; Password=;")
    do! conn.OpenAsync()
    for _ in 1..50 do
        let streamName = $"{category}-{Guid.NewGuid():N}"
        do! writeMessagesToStream conn streamName }

[<Fact>]
let ``It processes events for a category`` () = task {
    let log = Serilog.Log.Logger
    let consumerGroup = $"{Guid.NewGuid():N}"
    let category1 = $"{Guid.NewGuid():N}"
    let category2 = $"{Guid.NewGuid():N}"
    do! writeMessagesToCategory category1
    do! writeMessagesToCategory category2
    let connString = "Host=localhost; Database=message_store; Port=5433; Username=message_store; Password=;"
    let checkpoints = ReaderCheckpoint.CheckpointStore("Host=localhost; Database=message_store; Port=5433; Username=postgres; Password=postgres", "public", $"TestGroup{consumerGroup}", TimeSpan.FromSeconds 10)
    do! checkpoints.CreateSchemaIfNotExists()
    let stats = { new Propulsion.Streams.Stats<_>(log, TimeSpan.FromMinutes 1, TimeSpan.FromMinutes 1)
                      with member _.HandleOk x = ()
                           member _.HandleExn(log, x) = () }
    let mutable stop = ignore
    let handled = HashSet<_>()
    let handle stream (events: Propulsion.Streams.Default.StreamSpan) _ct = task {
        lock handled (fun _ ->
           for evt in events do
               handled.Add((stream, evt.Index)) |> ignore)
        test <@ Array.chooseV Simple.codec.TryDecode events |> Array.forall ((=) (Simple.Hello { name = "world" })) @>
        if handled.Count >= 2000 then
            stop ()
        return struct (Propulsion.Streams.SpanResult.AllProcessed, ()) }
    use sink = Propulsion.Streams.Default.Config.Start(log, 2, 2, handle, stats, TimeSpan.FromMinutes 1)
    let source = MessageDbSource(
        log, TimeSpan.FromMinutes 1,
        connString, 1000, TimeSpan.FromMilliseconds 100,
        checkpoints, sink, [| category1; category2 |])
    use src = source.Start()
    stop <- src.Stop

    Task.Delay(TimeSpan.FromSeconds 30).ContinueWith(fun _ -> src.Stop()) |> ignore

    do! src.Await()

    // 2000 total events
    test <@ handled.Count = 2000 @>
    // 20 in each stream
    test <@ handled |> Array.ofSeq |> Array.groupBy fst |> Array.map (snd >> Array.length) |> Array.forall ((=) 20) @>
    // they were handled in order within streams
    let ordering = handled |> Seq.groupBy fst |> Seq.map (snd >> Seq.map snd >> Seq.toArray) |> Seq.toArray
    test <@ ordering |> Array.forall ((=) [| 0L..19L |]) @> }

type LogSink() =
    let mutable items = 0
    member _.Events = items
    interface ILogEventSink with
        member _.Emit(logEvent) =
            match logEvent with
            | Propulsion.Feed.Core.Log.MetricEvent (Propulsion.Feed.Core.Log.Metric.Read r) ->
                items <- items + r.items
            | _ -> ()

[<Fact>]
let ``It doesn't read the tail event again`` () = task {
    let logSink = LogSink()
    let log = LoggerConfiguration().WriteTo.Sink(logSink).CreateLogger()
    let consumerGroup = $"{Guid.NewGuid():N}"
    let category = $"{Guid.NewGuid():N}"
    let connString = "Host=localhost; Database=message_store; Port=5433; Username=message_store; Password=;"
    use conn = new NpgsqlConnection(connString)
    do! conn.OpenAsync()
    do! writeMessagesToStream conn $"{category}-1"
    let checkpoints = ReaderCheckpoint.CheckpointStore("Host=localhost; Database=message_store; Port=5433; Username=postgres; Password=postgres", "public", $"TestGroup{consumerGroup}", TimeSpan.FromSeconds 10)
    do! checkpoints.CreateSchemaIfNotExists()
    let stats = { new Propulsion.Streams.Stats<_>(log, TimeSpan.FromMinutes 1, TimeSpan.FromMinutes 1)
                      with member _.HandleOk x = ()
                           member _.HandleExn(log, x) = () }
    let handle _ _ _ = task {
        return struct (Propulsion.Streams.SpanResult.AllProcessed, ()) }
    use sink = Propulsion.Streams.Default.Config.Start(log, 2, 2, handle, stats, TimeSpan.FromMinutes 1)
    let source = MessageDbSource(
        log, TimeSpan.FromMilliseconds 100,
        connString, 1000, TimeSpan.FromMilliseconds 100,
        checkpoints, sink, [| category |])
    use src = source.Start()

    Task.Delay(TimeSpan.FromSeconds 1).ContinueWith(fun _ -> src.Stop()) |> ignore

    do! src.Await()

    test <@ logSink.Events = 20 @> }

