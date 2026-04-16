defmodule ExArrow.Stream do
  @moduledoc """
  Opaque handle to a native Arrow record-batch stream.

  Provides a unified iterator interface over four backing sources:

  | Backend      | Created by                                                      |
  |--------------|-----------------------------------------------------------------|
  | `:ipc`       | `ExArrow.IPC.Reader` — Arrow IPC stream or file format          |
  | `:parquet`   | `ExArrow.Parquet.Reader` — lazy row-group Parquet reader        |
  | `:adbc`      | `ExArrow.ADBC.Statement.execute/1` — SQL result streams         |
  | `:flight_sql`| `ExArrow.FlightSQL.Client.stream_query/2` — Flight SQL streams  |

  Plain Flight `do_get` results also use the `:ipc` backend (the Flight client
  returns an IPC stream resource).

  All backends expose the same three functions:

  - `schema/1` — inspect the Arrow schema without consuming any batches
  - `next/1` — read the next batch on demand (`nil` when exhausted)
  - `to_list/1` — collect all remaining batches into a list

  Record batch data stays in native Arrow memory until consumed.  Callers
  never set the `backend` field directly; it is assigned by the function that
  opens the stream.

  ## Enumerable

  `ExArrow.Stream` implements the `Enumerable` protocol, so all `Enum` and
  `Stream` functions work directly on a stream handle:

      {:ok, stream} = ExArrow.FlightSQL.Client.stream_query(client, "SELECT * FROM t")

      # Collect all batches into a list
      batches = Enum.to_list(stream)

      # Map over batches lazily (materialises here via Enum.map)
      Enum.map(stream, fn batch -> ExArrow.RecordBatch.num_rows(batch) end)

      # Take the first N batches then stop — the rest are not fetched
      first_two = Enum.take(stream, 2)

  Each element yielded by the enumerator is an `ExArrow.RecordBatch.t()`.
  The batch count is not known up front, so `Enum.count/1` traverses the
  entire stream.  Prefer the `:num_rows` field on `ExArrow.FlightSQL.Result`
  when the result has already been materialised.

  Enumeration raises on a transport or server error.  For recoverable error
  handling iterate manually with `next/1`.

  ### Resource lifecycle

  The underlying gRPC channel and batch buffer are held in a native resource.
  The resource is released when the stream handle is garbage-collected.
  Stopping enumeration early (e.g. `Enum.take/2`) is safe — the resource will
  be released when the stream variable goes out of scope.
  """
  alias ExArrow.RecordBatch
  alias ExArrow.Schema

  @opaque t :: %__MODULE__{resource: reference(), backend: :ipc | :adbc | :parquet | :flight_sql}
  defstruct [:resource, backend: :ipc]

  @doc """
  Returns the schema of this stream (without consuming it).
  Returns `{:error, message}` if the stream is invalid (e.g. poisoned lock).
  """
  @spec schema(t()) :: {:ok, Schema.t()} | {:error, String.t()}
  def schema(%__MODULE__{resource: ref, backend: :adbc}) do
    case native().adbc_stream_schema(ref) do
      {:error, msg} -> {:error, msg}
      schema_ref -> {:ok, Schema.from_ref(schema_ref)}
    end
  end

  def schema(%__MODULE__{resource: ref, backend: :ipc}) do
    case native().ipc_stream_schema(ref) do
      {:error, msg} -> {:error, msg}
      schema_ref -> {:ok, Schema.from_ref(schema_ref)}
    end
  end

  def schema(%__MODULE__{resource: ref, backend: :parquet}) do
    schema_ref = native().parquet_stream_schema(ref)
    {:ok, Schema.from_ref(schema_ref)}
  end

  def schema(%__MODULE__{resource: ref, backend: :flight_sql}) do
    case native().flight_sql_stream_schema(ref) do
      {:error, msg} -> {:error, msg}
      {:ok, schema_ref} -> {:ok, Schema.from_ref(schema_ref)}
    end
  end

  @doc """
  Returns the next record batch from the stream, or nil when done.
  Returns `{:error, message}` on read error.

  For `:flight_sql` streams, read errors carry a structured 3-tuple so callers
  can distinguish gRPC codes:

      {:error, {code_atom, grpc_status_integer, message}} |
      {:error, string_message}

  `Enum.*` / `Stream.*` functions raise on any error shape.  For recoverable
  error handling iterate with `next/1` directly.
  """
  @spec next(t()) ::
          RecordBatch.t()
          | nil
          | {:error, String.t()}
          | {:error, {atom(), non_neg_integer(), String.t()}}
  def next(%__MODULE__{resource: ref, backend: :adbc}) do
    case native().adbc_stream_next(ref) do
      :done -> nil
      {:ok, batch_ref} -> RecordBatch.from_ref(batch_ref)
      {:error, msg} -> {:error, msg}
    end
  end

  def next(%__MODULE__{resource: ref, backend: :ipc}) do
    case native().ipc_stream_next(ref) do
      :done -> nil
      {:ok, batch_ref} -> RecordBatch.from_ref(batch_ref)
      {:error, msg} -> {:error, msg}
    end
  end

  def next(%__MODULE__{resource: ref, backend: :parquet}) do
    case native().parquet_stream_next(ref) do
      :done -> nil
      {:ok, batch_ref} -> RecordBatch.from_ref(batch_ref)
      {:error, msg} -> {:error, msg}
    end
  end

  def next(%__MODULE__{resource: ref, backend: :flight_sql}) do
    case native().flight_sql_stream_next(ref) do
      :done -> nil
      {:ok, batch_ref} -> RecordBatch.from_ref(batch_ref)
      # Pass the structured triple through so callers retain the gRPC code and status.
      {:error, {_code, _grpc_status, _msg} = triple} -> {:error, triple}
      {:error, msg} -> {:error, msg}
    end
  end

  @doc """
  Collects all remaining batches from the stream into a list.

  Stops at the first error and raises.  Returns an empty list for an
  already-exhausted stream.
  """
  @spec to_list(t()) :: [RecordBatch.t()]
  def to_list(%__MODULE__{} = stream) do
    do_collect(stream, [])
  end

  defp native, do: Application.get_env(:ex_arrow, :stream_native, ExArrow.Native)

  defp do_collect(stream, acc) do
    case next(stream) do
      nil -> Enum.reverse(acc)
      {:error, {code, _status, msg}} -> raise "ExArrow.Stream.to_list/1 failed: [#{code}] #{msg}"
      {:error, msg} -> raise "ExArrow.Stream.to_list/1 failed: #{msg}"
      batch -> do_collect(stream, [batch | acc])
    end
  end
end

defimpl Enumerable, for: ExArrow.Stream do
  @moduledoc false

  @spec reduce(ExArrow.Stream.t(), Enumerable.acc(), Enumerable.reducer()) ::
          Enumerable.result()
  # Halt — stop immediately and return the accumulator as-is.
  def reduce(_stream, {:halt, acc}, _fun), do: {:halted, acc}

  # Suspend — return a continuation for lazy/coroutine-style consumption.
  def reduce(stream, {:suspend, acc}, fun),
    do: {:suspended, acc, &reduce(stream, &1, fun)}

  # Continue — fetch the next batch and recurse.
  def reduce(stream, {:cont, acc}, fun) do
    case ExArrow.Stream.next(stream) do
      nil ->
        {:done, acc}

      {:error, {code, _status, msg}} ->
        raise "ExArrow.Stream enumeration error: [#{code}] #{msg}"

      {:error, msg} ->
        raise "ExArrow.Stream enumeration error: #{msg}"

      batch ->
        reduce(stream, fun.(batch, acc), fun)
    end
  end

  # Batch count is not known ahead of time; fall back to full enumeration.
  @spec count(ExArrow.Stream.t()) :: {:error, module()}
  def count(_stream), do: {:error, __MODULE__}

  # Membership test is not meaningful for record-batch streams.
  @spec member?(ExArrow.Stream.t(), term()) :: {:error, module()}
  def member?(_stream, _element), do: {:error, __MODULE__}

  # Random-access slicing requires knowing the total size up front.
  @spec slice(ExArrow.Stream.t()) :: {:error, module()}
  def slice(_stream), do: {:error, __MODULE__}
end
