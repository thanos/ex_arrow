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
    def from_arrow(arg) do
      cond do
        RecordBatch.record_batch?(arg) ->
          ExArrowExplorer.from_record_batch(arg)

        Stream.stream?(arg) ->
          ExArrowExplorer.from_stream(arg)

        true ->
          {:error, "expected an ExArrow.RecordBatch or ExArrow.Stream, got: #{inspect(arg)}"}
      end
    end

    @doc """
    Convert an `Explorer.DataFrame` to a single `ExArrow.RecordBatch`.

    The dataframe is serialised to Arrow IPC via
    `Explorer.DataFrame.dump_ipc_stream!/1`, then read back as native Arrow
    batches.  Explorer may split a large dataframe into multiple IPC batches;
    these are concatenated into a single `RecordBatch` so that the full row
    count and all values are preserved.

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
        {:ok, []} -> {:error, "no batches produced from dataframe"}
        {:ok, [batch]} -> {:ok, batch}
        {:ok, batches} -> concat_batches(batches)
        {:error, _} = err -> err
      end
    end

    defp concat_batches(batches) do
      refs = Enum.map(batches, &RecordBatch.resource_ref/1)

      case ExArrow.Native.record_batch_concat(refs) do
        {:ok, ref} -> {:ok, RecordBatch.from_ref(ref)}
        {:error, msg} -> {:error, msg}
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
