defmodule ExArrow.DataFrame do
  @moduledoc """
  Ergonomic conversion between Explorer DataFrames and Arrow data.

  This module provides the `from_arrow/1` and `to_arrow/1` API requested by
  users who think in DataFrame-first terms.  It delegates to
  `ExArrow.Explorer` for the actual IPC round-trip.

  Requires `{:explorer, "~> 0.11"}` in your `mix.exs` dependencies.  When
  Explorer is absent every function returns `{:error, "Explorer is not
  available..."}`.

  ## Arrow hierarchy

  An Arrow `RecordBatch` is a collection of column arrays with a shared schema
  and row count.  A `Stream` is a sequence of batches.  Both carry the same
  columnar data; `from_arrow/1` accepts either.

  ## Examples

      # DataFrame → Arrow
      df = Explorer.DataFrame.new(x: [1, 2, 3], y: ["a", "b", "c"])
      {:ok, batch} = ExArrow.DataFrame.to_arrow(df)

      # Arrow → DataFrame
      {:ok, df2} = ExArrow.DataFrame.from_arrow(batch)
      Explorer.DataFrame.n_rows(df2)  #=> 3
  """

  alias ExArrow.Explorer, as: ExArrowExplorer
  alias ExArrow.RecordBatch
  alias ExArrow.Stream

  @explorer_available Code.ensure_loaded?(Explorer.DataFrame)

  if @explorer_available do
    @doc """
    Convert Arrow data to an `Explorer.DataFrame`.

    Accepts either an `ExArrow.RecordBatch` or an `ExArrow.Stream`.  Streams
    are consumed entirely (all batches collected) before conversion.

    Returns `{:ok, dataframe}` or `{:error, message}`.

    ## Examples

        {:ok, stream} = ExArrow.IPC.Reader.from_file("/data/events.arrow")
        {:ok, df} = ExArrow.DataFrame.from_arrow(stream)
        Explorer.DataFrame.n_rows(df)  #=> 1_000_000

        {:ok, batch} = ExArrow.DataFrame.to_arrow(df)
        {:ok, df2}   = ExArrow.DataFrame.from_arrow(batch)
        Explorer.DataFrame.names(df2)  #=> ["x", "y"]
    """
    @spec from_arrow(RecordBatch.t() | Stream.t()) ::
            {:ok, Explorer.DataFrame.t()} | {:error, String.t()}
    def from_arrow(%Stream{} = stream) do
      ExArrowExplorer.from_stream(stream)
    end

    def from_arrow(%RecordBatch{} = batch) do
      ExArrowExplorer.from_record_batch(batch)
    end

    @doc """
    Convert an `Explorer.DataFrame` to an `ExArrow.RecordBatch`.

    The dataframe is serialised to Arrow IPC via
    `Explorer.DataFrame.dump_ipc_stream!/1`, then read back as a native Arrow
    batch handle.  For dataframes that produce multiple batches in the IPC
    representation, the first batch is returned (the common case for
    in-memory data is a single batch).

    Returns `{:ok, batch}` or `{:error, message}`.

    ## Examples

        df = Explorer.DataFrame.new(x: [1, 2, 3], y: ["a", "b", "c"])
        {:ok, batch} = ExArrow.DataFrame.to_arrow(df)
        ExArrow.RecordBatch.num_rows(batch)  #=> 3
    """
    @spec to_arrow(Explorer.DataFrame.t()) ::
            {:ok, RecordBatch.t()} | {:error, String.t()}
    def to_arrow(df) do
      case ExArrowExplorer.to_record_batches(df) do
        {:ok, [batch | _]} -> {:ok, batch}
        {:ok, []} -> {:error, "no batches produced from dataframe"}
        {:error, _} = err -> err
      end
    end
  else
    @doc false
    def from_arrow(_), do: {:error, explorer_missing_message()}

    @doc false
    def to_arrow(_), do: {:error, explorer_missing_message()}

    defp explorer_missing_message do
      "Explorer is not available. Add {:explorer, \"~> 0.11\"} to your mix.exs dependencies."
    end
  end
end
