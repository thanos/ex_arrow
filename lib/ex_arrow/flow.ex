defmodule ExArrow.Flow do
  @moduledoc """
  Arrow-native Flow execution.

  Wraps `Flow` so ExArrow streams of `ExArrow.RecordBatch` values can be
  processed concurrently while staying entirely in native Arrow memory.  The
  unit of work is a **batch**, never a row map.

  Requires `{:flow, "~> 1.2"}` in your `mix.exs` dependencies.  When Flow is
  absent every function returns `{:error, "Flow is not available..."}`.

  ## Quick start

      {:ok, stream} = ExArrow.Stream.from_parquet("/data/events.parquet")

      stream
      |> ExArrow.Flow.from_batches()
      |> Flow.map(&ExArrow.RecordBatch.num_rows/1)
      |> Enum.to_list()

  ## How it works

  `from_batches/1` calls `Flow.from_enumerable/2` on the ExArrow stream.  The
  stream's `Enumerable` implementation yields one `ExArrow.RecordBatch` per
  step, so each Flow stage receives a batch handle.  Because the handle is an
  opaque reference to native memory, no column buffers are copied to the BEAM
  heap when a batch moves between stages — only the small reference term is
  sent over the mailbox.

  All standard `Flow` combinators work on the result:

  - `Flow.map/2` — transform each batch
  - `Flow.flat_map/2` — expand one batch into many
  - `Flow.partition/2` — partition batches by key for shuffled reductions
  - `Flow.reduce/3` — reduce batches within a window/partition

  ## Performance implications

  - **Parallelism**: Flow spins up a configurable number of producer and
    consumer stages (`:stages`, `:max_demand`, `:min_demand`).  Each stage
    decodes and transforms batches independently, so wall-clock time scales
    with available cores for CPU-bound work.
  - **Memory**: only batch references cross process boundaries; the Arrow
    buffers stay in native memory until a stage explicitly extracts them.
    Peak memory is roughly `stages × largest_batch` rather than the whole
    dataset.
  - **Backpressure**: GenStage demand is honoured end-to-end, so a slow
    consumer slows the producer without piling up batches.
  - **Not a row API**: converting batches to row maps inside a Flow stage
    defeats the purpose — keep transformations column-wise (e.g. via
    `ExArrow.Batch` or `ExArrow.Compute`).

  ## Telemetry

  `map_batches/2` and `each_batch/2` emit `[:ex_arrow, :pipeline, :batch]` for
  every batch processed, with `rows`, `columns`, and `batch_count`
  measurements and `%{source: :flow}` metadata.  Raw `Flow.map/2` does not
  emit telemetry (callers can attach it themselves).
  """

  @flow_available Code.ensure_loaded?(Flow)

  if @flow_available do
    @doc """
    Build a `Flow` from a stream (or list) of record batches.

    Accepts:

    - an `ExArrow.Stream.t()` or any `Enumerable.t()` of
      `ExArrow.RecordBatch.t()` values
    - `{:ok, enumerable}` — unwrapped automatically so the function composes
      with `ExArrow.Stream.from_*/1` constructors in a pipe
    - `{:error, reason}` — raises so pipeline failures surface immediately
      (use a `with` chain if you prefer explicit error handling)

    `opts` are forwarded to `Flow.from_enumerable/2` (`:stages`, `:window`,
    `:max_demand`, `:min_demand`, `:buffer_size`, ...).

    Returns a `Flow.t()`.  The flow's elements are `ExArrow.RecordBatch`
    values.

    ## Example

        {:ok, stream} = ExArrow.Stream.from_parquet("/data/events.parquet")
        flow = ExArrow.Flow.from_batches(stream, stages: 4)
    """
    @spec from_batches(Enumerable.t() | {:ok, Enumerable.t()} | {:error, term()}, keyword()) ::
            Flow.t()
    def from_batches(enumerable_or_result, opts \\ [])

    def from_batches({:ok, enumerable}, opts) when is_list(opts) do
      Flow.from_enumerable(enumerable, opts)
    end

    def from_batches({:error, reason}, _opts) do
      raise "ExArrow.Flow.from_batches/2 received an error result: #{inspect(reason)}"
    end

    def from_batches(enumerable, opts) when is_list(opts) do
      Flow.from_enumerable(enumerable, opts)
    end

    @doc """
    Map a function over each batch in `flow`, emitting a
    `[:ex_arrow, :pipeline, :batch]` telemetry event per batch.

    `fun` receives an `ExArrow.RecordBatch.t()` and returns any term.  The
    returned flow's elements are whatever `fun` returns.

    ## Example

        flow
        |> ExArrow.Flow.map_batches(fn batch ->
          {:ok, slim} = ExArrow.Batch.select(batch, ["id"])
          slim
        end)
        |> Enum.to_list()
    """
    @spec map_batches(Flow.t(), (ExArrow.RecordBatch.t() -> term())) :: Flow.t()
    def map_batches(flow, fun) when is_function(fun, 1) do
      Flow.map(flow, fn batch ->
        result = fun.(batch)
        emit_pipeline_telemetry(batch)
        result
      end)
    end

    @doc """
    Run `fun` for its side effects on each batch in `flow`, emitting a
    `[:ex_arrow, :pipeline, :batch]` telemetry event per batch.

    The flow's elements are unchanged (the original batches pass through).
    """
    @spec each_batch(Flow.t(), (ExArrow.RecordBatch.t() -> term())) :: Flow.t()
    def each_batch(flow, fun) when is_function(fun, 1) do
      Flow.map(flow, fn batch ->
        fun.(batch)
        emit_pipeline_telemetry(batch)
        batch
      end)
    end

    defp emit_pipeline_telemetry(batch) do
      if ExArrow.RecordBatch.record_batch?(batch) do
        measurements = ExArrow.Telemetry.batch_measurements(batch)
        ExArrow.Telemetry.execute([:ex_arrow, :pipeline, :batch], measurements, %{source: :flow})
      end
    end
  else
    @doc false
    @spec from_batches(term(), keyword()) :: {:error, String.t()}
    def from_batches(_enumerable, _opts \\ []) do
      {:error, "Flow is not available. Add {:flow, \"~> 1.2\"} to your mix.exs dependencies."}
    end

    @doc false
    @spec map_batches(term(), (term() -> term())) :: {:error, String.t()}
    def map_batches(_flow, _fun) do
      {:error, "Flow is not available. Add {:flow, \"~> 1.2\"} to your mix.exs dependencies."}
    end

    @doc false
    @spec each_batch(term(), (term() -> term())) :: {:error, String.t()}
    def each_batch(_flow, _fun) do
      {:error, "Flow is not available. Add {:flow, \"~> 1.2\"} to your mix.exs dependencies."}
    end
  end
end
