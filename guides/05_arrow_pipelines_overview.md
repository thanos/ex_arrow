# Arrow Pipelines Overview

ExArrow v0.7.0 introduces Arrow-native pipelines for the BEAM.  This guide is
a short orientation to the pipeline modules and how they fit together; the
subsequent guides cover each in depth.

## The principle

The central architectural principle is:

  Operate on Arrow RecordBatch values.
  Not list(map()).
  Not Explorer.DataFrame.
  Not Nx.Tensor.

Explorer and Nx remain downstream consumers.  ExArrow is the Arrow layer.

## The modules

| Module              | Role                                               | Guide |
|---------------------|----------------------------------------------------|-------|
| `ExArrow.Stream`    | Open a stream of RecordBatch values from any source | [06 Arrow streams](06_arrow_streams.md) |
| `ExArrow.Batch`     | Lightweight column/row transforms on a batch       | (see module docs) |
| `ExArrow.Pipeline`  | Lazy, composable DSL for transforming and sinking   | [10 Pipeline patterns](10_arrow_pipeline_patterns.md) |
| `ExArrow.Flow`      | Parallel batch processing via Flow                  | [07 Arrow and Flow](07_arrow_and_flow.md) |
| `ExArrow.GenStage`  | Demand-driven producers with backpressure           | [08 Arrow and GenStage](08_arrow_and_genstage.md) |
| `ExArrow.Broadway`  | Ingestion pipelines (Kafka, SQS, ...)               | [09 Arrow and Broadway](09_arrow_and_broadway.md) |
| `ExArrow.Sink.*`    | Standard destinations (Parquet, Flight, DataFrame, Nx) | (see module docs) |
| `ExArrow.Telemetry` | Events for every transport and pipeline operation   | (see module docs) |

## Quick example

    ExArrow.Stream.from_parquet("/data/events.parquet")
    |> ExArrow.Pipeline.map_batches(fn batch ->
      {:ok, slim} = ExArrow.Batch.select(batch, ["id", "score"])
      slim
    end)
    |> ExArrow.Pipeline.write_parquet("/data/slim.parquet")

## Where to start

1. Read [06 Arrow streams](06_arrow_streams.md) to understand the streaming
   abstraction and the `from_*/1` constructors.
2. Read [10 Pipeline patterns](10_arrow_pipeline_patterns.md) for the
   `ExArrow.Pipeline` DSL and sinks.
3. Read the integration guides (07–09) when you need parallelism,
   backpressure, or ingestion.
