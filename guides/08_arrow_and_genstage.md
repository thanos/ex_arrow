# Arrow and GenStage

`ExArrow.GenStage` provides demand-driven producers that emit
`ExArrow.RecordBatch` values from the common ExArrow sources.  This guide
explains the architecture, the three producers, and how to wire producer,
consumer, and producer-consumer pipelines.

## Why GenStage?

[GenStage](https://github.com/elixir-lang/gen_stage) is the standard Elixir
library for demand-driven data pipelines.  A producer only emits events when
a consumer demands them, so slow consumers apply backpressure all the way to
the source — exactly what you want when reading a large Parquet file or
streaming a Flight SQL result.

ExArrow's producers emit `ExArrow.RecordBatch` handles, not row maps.  The
unit of work is the batch.

## The three producers

| Module                                | Source                                       |
|---------------------------------------|----------------------------------------------|
| `ExArrow.GenStage.ParquetProducer`    | Parquet file (`:path`) or binary (`:binary`) |
| `ExArrow.GenStage.FlightProducer`     | Flight `do_get` (`:client` + `:ticket`)      |
| `ExArrow.GenStage.ADBCProducer`       | ADBC (`:statement` or `:connection` + `:sql`)|

All three accept a pre-opened `:stream` option for testing or for sources not
covered by the dedicated options.

## Lifecycle

- **Demand-driven**: batches are only read when a consumer demands them.
- **Arrow batch delivery**: each emitted event is an `ExArrow.RecordBatch`
  handle.
- **Clean shutdown**: when the underlying stream is exhausted the producer
  sends itself a `{ExArrow.GenStage, :stop}` message and exits with reason
  `:normal`.
- **Resource cleanup**: `terminate/2` drains the stream so file/socket
  descriptors are released promptly.

## Pattern 1: producer + consumer

    defmodule Collector do
      use GenStage

      def init(pid), do: {:consumer, pid}

      def handle_events(batches, _from, pid) do
        send(pid, {:batches, batches})
        {:noreply, [], pid}
      end
    end

    {:ok, producer} =
      ExArrow.GenStage.ParquetProducer.start_link(path: "/data/events.parquet")

    {:ok, consumer} = GenStage.start_link(Collector, self())
    GenStage.sync_subscribe(consumer, to: producer, max_demand: 4)

## Pattern 2: producer-consumer

A producer-consumer transforms events between a producer and a consumer.  This
is where you plug in `ExArrow.Batch` transformations:

    defmodule MyTransformer do
      use GenStage

      def init(state), do: {:producer_consumer, state}

      def handle_events(batches, _from, state) do
        transformed =
          Enum.map(batches, fn batch ->
            {:ok, slim} = ExArrow.Batch.select(batch, ["id"])
            slim
          end)

        {:noreply, transformed, state}
      end
    end

    {:ok, producer} = ExArrow.GenStage.ParquetProducer.start_link(path: "/data/events.parquet")
    {:ok, transformer} = GenStage.start_link(MyTransformer, :ok)
    {:ok, consumer} = GenStage.start_link(Collector, self())

    GenStage.sync_subscribe(transformer, to: producer, max_demand: 1)
    GenStage.sync_subscribe(consumer, to: transformer, max_demand: 1)

## Pattern 3: producer + producer-consumer + consumer (full pipeline)

Combine the two patterns for a three-stage pipeline:

    ParquetProducer ──► Selector ──► Collector

Each stage honours demand, so the producer only reads row groups as fast as
the collector can acknowledge them.

## Telemetry

Every batch emitted by an ExArrow producer fires a
`[:ex_arrow, :stream, :batch]` telemetry event with `rows`, `columns`, and
`batch_count` measurements and `%{source: ...}` metadata.

## Error handling

If `ExArrow.Stream.next/1` returns `{:error, reason}` the producer sends
itself a stop message and exits `:normal`.  Consumers see a producer `:DOWN`
notification.  For finer-grained error propagation, wrap the producer in a
`Supervisor` with a restart strategy and monitor the producer process.

## Choosing between Flow and GenStage

| Use GenStage when                      | Use Flow when                               |
|----------------------------------------|---------------------------------------------|
| You need explicit demand/backpressure  | You want Fire-and-forget parallelism        |
| You are building a long-running pipeline | You are doing a one-shot batch job        |
| You want to wire custom consumers      | You want partition/reduce semantics         |

GenStage is the lower-level building block; Flow is built on top of it.
ExArrow exposes both so you can pick the right tool for each workload.
