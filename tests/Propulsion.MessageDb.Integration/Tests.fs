module Propulsion.MessageDb.Integration.Tests

open Npgsql
open NpgsqlTypes
open Propulsion.Internal
open Propulsion.MessageDb
open Propulsion.MessageDb.ReaderCheckpoint
open Swensen.Unquote
open System
open System.Collections.Generic
open System.Diagnostics
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

let ConnectionString =
    match Environment.GetEnvironmentVariable "MSG_DB_CONNECTION_STRING" with
    | null -> "Host=localhost; Database=message_store; Port=5432; Username=message_store"
    | s -> s
let CheckpointConnectionString =
    match Environment.GetEnvironmentVariable "CHECKPOINT_CONNECTION_STRING" with
    | null -> "Host=localhost; Database=message_store; Port=5432; Username=postgres; Password=postgres"
    | s -> s

// Ensures the checkpoint table exists
CheckpointStore(CheckpointConnectionString, "public", "whatever", TimeSpan.FromMilliseconds 1).CreateSchemaIfNotExists().Wait()


let connect () = task {
    let conn = new NpgsqlConnection(ConnectionString)
    do! conn.OpenAsync()
    return conn
}

let writeMessagesToStream (conn: NpgsqlConnection) streamName = task {
    let batch = conn.CreateBatch()
    for _ in 1..20 do
        let cmd = createStreamMessage streamName
        batch.BatchCommands.Add(cmd)
    do! batch.ExecuteNonQueryAsync() :> Task }

let writeMessagesToCategory conn category = task {
    for _ in 1..50 do
        let streamName = $"{category}-{Guid.NewGuid():N}"
        do! writeMessagesToStream conn streamName
}

[<Fact>]
let ``It processes events for a category`` () = task {
    use! conn = connect ()
    let log = Serilog.Log.Logger
    let consumerGroup = $"{Guid.NewGuid():N}"
    let category1 = $"{Guid.NewGuid():N}"
    let category2 = $"{Guid.NewGuid():N}"
    do! writeMessagesToCategory conn category1
    do! writeMessagesToCategory conn category2
    let mutable stop = ignore
    let handled = HashSet<_>()
    let handle stream (events: Propulsion.Sinks.Event[]) = async {
        lock handled (fun _ ->
           for evt in events do
               handled.Add((stream, evt.Index)) |> ignore)
        test <@ Array.chooseV Simple.codec.TryDecode events |> Array.forall ((=) (Simple.Hello { name = "world" })) @>
        if handled.Count >= 2000 then
            stop ()
        return Propulsion.Sinks.StreamResult.AllProcessed, () }

    let source, sink = MessageDbSource.Configure(
        log,
        [| category1; category2 |],
        handle,
        $"TestGroup{consumerGroup}",
        storeConnectionString = ConnectionString,
        checkpointConnectionString = CheckpointConnectionString)
    use src = source.Start()
    use _ = sink
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
