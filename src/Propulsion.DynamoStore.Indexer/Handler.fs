module Propulsion.DynamoStore.Indexer.Handler

open Amazon.DynamoDBv2
open Amazon.Lambda.DynamoDBEvents
open Propulsion.DynamoStore

let private parse (log : Serilog.ILogger) (dynamoEvent : DynamoDBEvent) : AppendsEpoch.Events.StreamSpan array =
    let spans, summary = ResizeArray(), System.Text.StringBuilder()
    let mutable indexStream, noEvents = 0, 0
    try for record in dynamoEvent.Records do
            match record.Dynamodb.StreamViewType with
            | x when x = StreamViewType.NEW_IMAGE || x = StreamViewType.NEW_AND_OLD_IMAGES -> ()
            | x -> invalidOp (sprintf "Unexpected StreamViewType %O" x)

            summary.Append(record.EventName.Value[0]) |> ignore

            let updated = record.Dynamodb.NewImage
            match record.EventName with
            | ot when ot = OperationType.REMOVE -> ()
            | ot when ot = OperationType.INSERT || ot = OperationType.MODIFY ->
                let p = record.Dynamodb.Keys["p"].S
                let sn, n = IndexStreamId.ofP p, int64 updated["n"].N
                let appendedLen = int updated["a"].N
                if p.StartsWith AppendsEpoch.Category || p.StartsWith AppendsIndex.Category then indexStream <- indexStream + 1
                elif appendedLen = 0 then noEvents <- noEvents + 1
                else
                    let allBatchEventTypes = [| for x in updated["c"].L -> x.S |]
                    match allBatchEventTypes |> Array.skip (allBatchEventTypes.Length - appendedLen) with
                    | [||] -> ()
                    | appendedEts ->
                        let i = n - appendedEts.LongLength
                        spans.Add({ p = sn; i = i; c = appendedEts } : AppendsEpoch.Events.StreamSpan)
                        let et =
                            match appendedEts with
                            | [| et |] -> ":" + et
                            | xs -> sprintf ":%s+%d" xs[0] (xs.Length - 1)
                        summary.Append(p).Append(et).Append(if i = 0 then " " else sprintf "@%d " i) |> ignore
            | et -> invalidOp (sprintf "Unknown OperationType %s" et.Value)
        let spans = spans.ToArray()
        log.Information("Index {indexCount} NoEvents {noEventCount} Spans {spanCount} {summary}", indexStream, noEvents, spans.Length, summary)
        spans
    with e ->
        log.Warning(e, "Failed {summary}", summary)
        reraise ()

let handle log (service : DynamoStoreIndexer) dynamoEvent = task {
    match parse log dynamoEvent with
    | [||] -> ()
    // TOCONSIDER if there are multiple shards, they should map to individual TrancheIds in order to avoid continual concurrency violations from competing writers
    | spans -> return! service.IngestWithoutConcurrency(AppendsTrancheId.wellKnownId, spans) }

