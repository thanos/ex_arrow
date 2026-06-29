# Arrow and Broadway

`ExArrow.Broadway` integrates ExArrow with [Broadway](https://github.com/dashbitco/broadway),
the standard Elixir library for ingestion pipelines (Kafka, SQS, S3, ...).
This guide explains the architecture, the batch builder, and the Parquet and
Flight sinks.

## Architecture

The canonical ingestion pipeline looks like:

    Kafka / SQS / S3
        | (messages carry Arrow columnar payloads)
        v
    Broadway producer
        |
        v
    Broadway processor  ──►  ExArrow.Broadway.BatchBuilder
        |                       (groups messages into RecordBatch values)
        v
    Broadway batcher    ──►  batch_size / batch_timeout config
        |
        v
    handle_batch/4      ──►  ExArrow.Broadway.ParquetSink / FlightSink

The unit that flows through the pipeline is an `ExArrow.RecordBatch` handle,
not a row map.

## Message shape

`ExArrow.Broadway.BatchBuilder` expects each Broadway message's `data` to be
one of:

- an `ExArrow.RecordBatch.t()` handle (the common case for Arrow-aware
  producers — e.g. a Kafka deserialiser that emits Arrow IPC), or
- a `{names, binaries, dtypes, length}` tuple describing raw Arrow columns,
  which `BatchBuilder` converts to a batch via
  `ExArrow.RecordBatch.from_columns/4`.

`from_messages/1` returns `{:ok, schema, batches}` where `schema` is the
schema of the first batch and `batches` is the assembled batch list.

## BatchBuilder

    def handle_batch(:parquet, messages, _info, _ctx) do
      {:ok, schema, batches} =
        ExArrow.Broadway.BatchBuilder.from_messages(messages)

      ExArrow.Broadway.ParquetSink.write("/data/events.parquet", schema, batches)
    end

## ParquetSink

`ExArrow.Broadway.ParquetSink.write/3` writes the assembled batches to a
Parquet file in a single `ExArrow.Parquet.Writer.to_file/3` call.  It emits a
`[:ex_arrow, :parquet, :write]` telemetry event with `:rows`, `:batch_count`,
and `%{destination: path, source: :broadway}` metadata.

## FlightSink

`ExArrow.Broadway.FlightSink.write/4` uploads the assembled batches to a
Flight server via `ExArrow.Flight.Client.do_put/4`.  It emits a
`[:ex_arrow, :flight, :query]` telemetry event.

## Tuning

Batch sizing and flush intervals are controlled by the Broadway batcher
configuration (`:batch_size`, `:batch_timeout`), not by ExArrow.  Typical
settings:

    batchers: [
      parquet: [concurrency: 2, batch_size: 100, batch_timeout: 1000]
    ]

- `:batch_size` — how many messages to accumulate before flushing.  Larger
  values produce fewer, larger Arrow batches (better throughput, more memory).
- `:batch_timeout` — maximum milliseconds to wait before flushing a partial
  batch.  Lower values reduce latency at the cost of smaller batches.
- `:concurrency` — number of batch processor processes.  Increase for I/O-
  bound sinks (Parquet to a network drive, Flight to a remote server).

## Example pipeline

    defmodule MyPipeline do
      use Broadway

      def start_link(opts) do
        Broadway.start_link(__MODULE__,
          name: __MODULE__,
          producer: [module: {MyKafkaProducer, opts}],
          processors: [default: [concurrency: 4]],
          batchers: [
            parquet: [concurrency: 2, batch_size: 100, batch_timeout: 1000]
          ]
        )
      end

      def handle_message(:default, %Broadway.Message{data: batch} = msg, _ctx)
          when is_struct(batch, ExArrow.RecordBatch) do
        Broadway.Message.put_batcher(msg, :parquet)
      end

      def handle_batch(:parquet, messages, _info, _ctx) do
        {:ok, schema, batches} =
          ExArrow.Broadway.BatchBuilder.from_messages(messages)

        ExArrow.Broadway.ParquetSink.write("/data/events.parquet", schema, batches)
      end
    end

## Error handling

`BatchBuilder.from_messages/1` returns `{:error, message}` if any message has
unsupported `data`.  In `handle_batch/4` you should pattern-match on the
result and use `Broadway.Message.failed/2` to mark the messages for retry
rather than letting the batch handler crash.

## Tradeoffs

- **Batch size vs latency**: larger batches improve Arrow write throughput
  but increase end-to-end latency.  Tune `:batch_size` and `:batch_timeout`
  together.
- **Memory**: each batch processor holds up to `:batch_size` batches in
  memory.  For very large batches, lower `:batch_size` or increase
  `:concurrency`.
- **Arrow-aware producers**: for the best performance, have your Broadway
  producer emit `ExArrow.RecordBatch` handles directly (e.g. by deserialising
  Arrow IPC from Kafka).  The `{names, binaries, dtypes, length}` tuple path
  is a convenience for producers that work with raw column bytes.
