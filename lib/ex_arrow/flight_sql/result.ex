defmodule ExArrow.FlightSQL.Result do
  @moduledoc """
  Materialized result from a Flight SQL query.

  A `Result` is returned by `ExArrow.FlightSQL.Client.query/2` after all record
  batches have been collected from the server.  For large result sets, prefer
  `ExArrow.FlightSQL.Client.stream_query/2`, which returns a lazy
  `ExArrow.Stream` instead.

  ## Fields

  - `:schema` — the Arrow schema (`ExArrow.Schema.t()`) describing column names and types.
  - `:batches` — the list of `ExArrow.RecordBatch.t()` that make up the result.  Each batch
    holds data in native Arrow memory; nothing is copied to the Elixir heap.
  - `:num_rows` — total row count across all batches.

  ## Examples

      {:ok, result} = ExArrow.FlightSQL.Client.query(client, "SELECT id, name FROM users")
      result.num_rows  #=> 42
      result.schema    #=> %ExArrow.Schema{...}
      result.batches   #=> [%ExArrow.RecordBatch{...}, ...]

  ## Conversion

  Use `to_dataframe/1` to convert the result into an Explorer DataFrame (requires the
  optional `:explorer` dependency):

      {:ok, df} = ExArrow.FlightSQL.Result.to_dataframe(result)

  """

  alias ExArrow.FlightSQL.Error
  alias ExArrow.IPC.{Reader, Writer}
  alias ExArrow.{RecordBatch, Schema, Stream}

  @type t :: %__MODULE__{
          schema: Schema.t(),
          batches: [RecordBatch.t()],
          num_rows: non_neg_integer()
        }

  defstruct [:schema, :batches, :num_rows]

  @doc """
  Build a `Result` by collecting all batches from a `stream`.

  This is the implementation of `query/2` — iterating the Flight SQL stream to
  completion and assembling the result struct.  Callers should not need this directly.

  Returns `{:error, %ExArrow.FlightSQL.Error{}}` if schema inspection or any batch
  read fails mid-stream.
  """
  @spec from_stream(Stream.t()) :: {:ok, __MODULE__.t()} | {:error, Error.t()}
  def from_stream(stream) do
    with {:ok, schema} <- wrap_schema_error(Stream.schema(stream)),
         {:ok, batches} <- collect_batches(stream, []) do
      num_rows = Enum.reduce(batches, 0, fn b, acc -> acc + RecordBatch.num_rows(b) end)
      {:ok, %__MODULE__{schema: schema, batches: batches, num_rows: num_rows}}
    end
  end

  # Wrap a plain string error from Stream.schema/1 into a typed %Error{}.
  defp wrap_schema_error({:ok, _} = ok), do: ok
  defp wrap_schema_error({:error, msg}), do: {:error, Error.from_string(:protocol_error, msg)}

  # Consume the stream batch-by-batch, returning {:ok, batches} or {:error, %Error{}}.
  defp collect_batches(stream, acc) do
    case Stream.next(stream) do
      nil -> {:ok, Enum.reverse(acc)}
      {:error, msg} -> {:error, Error.from_string(:transport_error, msg)}
      batch -> collect_batches(stream, [batch | acc])
    end
  end

  @doc """
  Convert this result to an Explorer DataFrame.

  Requires the optional `:explorer` dependency.  The conversion uses an Arrow IPC
  round-trip through `ExArrow.IPC.Writer` and `ExArrow.Explorer.from_stream/1`.
  Type support is determined by Explorer/Polars; columns with types that Polars
  does not support (e.g. `decimal128`, nested `map`) may cause a conversion error.

  Returns `{:error, %ExArrow.FlightSQL.Error{code: :conversion_error}}` if Explorer
  is not available or if conversion fails.

  ## Examples

      {:ok, df} = ExArrow.FlightSQL.Result.to_dataframe(result)
  """
  @spec to_dataframe(t()) :: {:ok, term()} | {:error, Error.t()}
  def to_dataframe(%__MODULE__{} = result) do
    if Code.ensure_loaded?(ExArrow.Explorer) do
      try do
        # Serialise to IPC binary, read back as a stream, then delegate to Explorer bridge.
        with {:ok, binary} <- Writer.to_binary(result.schema, result.batches),
             {:ok, stream} <- Reader.from_binary(binary) do
          case ExArrow.Explorer.from_stream(stream) do
            {:ok, df} ->
              {:ok, df}

            {:error, msg} ->
              {:error, Error.from_string(:conversion_error, "Explorer conversion failed: #{msg}")}
          end
        else
          {:error, msg} ->
            {:error, Error.from_string(:conversion_error, "IPC round-trip failed: #{msg}")}
        end
      rescue
        e ->
          {:error,
           Error.from_string(:conversion_error, "conversion raised: #{Exception.message(e)}")}
      end
    else
      {:error,
       Error.from_string(
         :conversion_error,
         "Explorer is not available — add {:explorer, \"~> 0.11\"} to your dependencies"
       )}
    end
  end

  @doc """
  Convert a single column from this result to an Nx tensor.

  Requires the optional `:nx` dependency and a numeric column type.

  Returns `{:error, %ExArrow.FlightSQL.Error{code: :conversion_error}}` if Nx is
  not available, the column is not found, or its type is not supported.

  Only the first batch is converted.  For multi-batch results use `stream_query/2`
  and convert batch-by-batch.

  ## Examples

      {:ok, tensor} = ExArrow.FlightSQL.Result.to_tensor(result, "price")
  """
  @spec to_tensor(t(), String.t()) :: {:ok, term()} | {:error, Error.t()}
  def to_tensor(%__MODULE__{batches: []}, _column) do
    {:error, Error.from_string(:conversion_error, "result contains no batches")}
  end

  def to_tensor(%__MODULE__{batches: [batch | _]}, column) when is_binary(column) do
    if Code.ensure_loaded?(ExArrow.Nx) do
      try do
        case ExArrow.Nx.to_tensors(batch) do
          {:ok, tensors} ->
            case Map.fetch(tensors, column) do
              {:ok, tensor} ->
                {:ok, tensor}

              :error ->
                {:error,
                 Error.from_string(
                   :conversion_error,
                   "column #{inspect(column)} not found in batch"
                 )}
            end

          {:error, msg} ->
            {:error, Error.from_string(:conversion_error, "Nx conversion failed: #{msg}")}
        end
      rescue
        e ->
          {:error,
           Error.from_string(:conversion_error, "conversion raised: #{Exception.message(e)}")}
      end
    else
      {:error,
       Error.from_string(
         :conversion_error,
         "Nx is not available — add {:nx, \"~> 0.9\"} to your dependencies"
       )}
    end
  end
end
