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
  alias ExArrow.ADBC.Connection, as: ADBCConnection
  alias ExArrow.ADBC.Statement, as: ADBCStatement
  alias ExArrow.RecordBatch
  alias ExArrow.Schema

  @opaque t :: %__MODULE__{
            resource: reference(),
            backend: :ipc | :adbc | :parquet | :flight_sql,
            source: term()
          }
  defstruct [:resource, :source, backend: :ipc]

  @doc """
  Returns the origin metadata attached to this stream by the `from_*/` 
  constructors.  The value is backend-specific (e.g. `{:parquet, path}` or
  `{:flight_sql, sql}`) and is forwarded to telemetry events as `:source`.
  """
  @spec source(t()) :: term()
  def source(%__MODULE__{source: source}), do: source

  @doc false
  @spec stream?(term()) :: boolean()
  def stream?(%__MODULE__{}), do: true
  def stream?(_), do: false

  # ── Source constructors (Milestone 1) ───────────────────────────────────────

  @doc """
  Open an Arrow IPC stream from an in-memory `binary`.

  Delegates to `ExArrow.IPC.Reader.from_binary/1` and tags the resulting
  stream with `source: {:ipc, :binary}` for telemetry.  Returns
  `{:ok, stream}` or `{:error, message}`.

  ## Example

      {:ok, stream} = ExArrow.Stream.from_ipc(ipc_bytes)
  """
  @spec from_ipc(binary()) :: {:ok, t()} | {:error, String.t()}
  def from_ipc(binary) when is_binary(binary) do
    with {:ok, stream} <- ExArrow.IPC.Reader.from_binary(binary) do
      {:ok, %{stream | source: {:ipc, :binary}}}
    end
  end

  @doc """
  Open an Arrow IPC stream from a file at `path`.

  Delegates to `ExArrow.IPC.Reader.from_file/1` and tags the stream with
  `source: {:ipc, path}`.  Returns `{:ok, stream}` or `{:error, message}`.
  """
  @spec from_ipc_file(Path.t()) :: {:ok, t()} | {:error, String.t()}
  def from_ipc_file(path) when is_binary(path) do
    with {:ok, stream} <- ExArrow.IPC.Reader.from_file(path) do
      {:ok, %{stream | source: {:ipc, path}}}
    end
  end

  @doc """
  Open a Parquet file at `path` for lazy row-group streaming.

  Delegates to `ExArrow.Parquet.Reader.from_file/1`, tags the stream with
  `source: {:parquet, path}`, and emits a `[:ex_arrow, :parquet, :read]`
  telemetry event.  Returns `{:ok, stream}` or `{:error, message}`.

  ## Example

      {:ok, stream} = ExArrow.Stream.from_parquet("/data/events.parquet")
  """
  @spec from_parquet(Path.t()) :: {:ok, t()} | {:error, String.t()}
  def from_parquet(path) when is_binary(path) do
    ExArrow.Telemetry.execute([:ex_arrow, :parquet, :read], %{}, %{source: path})

    with {:ok, stream} <- ExArrow.Parquet.Reader.from_file(path) do
      {:ok, %{stream | source: {:parquet, path}}}
    end
  end

  @doc """
  Open a Parquet stream from an in-memory `binary`.

  Delegates to `ExArrow.Parquet.Reader.from_binary/1` and emits a
  `[:ex_arrow, :parquet, :read]` telemetry event with `source: :binary`.
  """
  @spec from_parquet_binary(binary()) :: {:ok, t()} | {:error, String.t()}
  def from_parquet_binary(binary) when is_binary(binary) do
    ExArrow.Telemetry.execute([:ex_arrow, :parquet, :read], %{}, %{source: :binary})

    with {:ok, stream} <- ExArrow.Parquet.Reader.from_binary(binary) do
      {:ok, %{stream | source: {:parquet, :binary}}}
    end
  end

  @doc """
  Retrieve data for `ticket` from a Flight server as a stream of record
  batches.

  Delegates to `ExArrow.Flight.Client.do_get/2` and emits a
  `[:ex_arrow, :flight, :query]` telemetry event.  The stream is tagged with
  `source: {:flight, ticket}`.

  ## Example

      {:ok, stream} = ExArrow.Stream.from_flight(client, "sales_2024")
  """
  @spec from_flight(ExArrow.Flight.Client.t(), term()) ::
          {:ok, t()} | {:error, term()}
  def from_flight(client, ticket) do
    ExArrow.Telemetry.execute([:ex_arrow, :flight, :query], %{}, %{source: ticket})

    with {:ok, stream} <- ExArrow.Flight.Client.do_get(client, ticket) do
      {:ok, %{stream | source: {:flight, ticket}}}
    end
  end

  @doc """
  Execute a SQL query against a Flight SQL server and return a lazy stream of
  record batches.

  Delegates to `ExArrow.FlightSQL.Client.stream_query/2`, emits a
  `[:ex_arrow, :flight_sql, :query]` telemetry event, and tags the stream with
  `source: {:flight_sql, sql}`.

  ## Example

      {:ok, stream} = ExArrow.Stream.from_flight_sql(client, "SELECT * FROM events")
  """
  @spec from_flight_sql(ExArrow.FlightSQL.Client.t(), String.t()) ::
          {:ok, t()} | {:error, term()}
  # sobelow_skip ["SQL.Query"]
  # False positive: SQL is forwarded to a remote Flight SQL server over gRPC.
  def from_flight_sql(client, sql) when is_binary(sql) do
    ExArrow.Telemetry.execute([:ex_arrow, :flight_sql, :query], %{}, %{source: sql})

    with {:ok, stream} <- ExArrow.FlightSQL.Client.stream_query(client, sql) do
      {:ok, %{stream | source: {:flight_sql, sql}}}
    end
  end

  @doc """
  Execute an ADBC statement and return its result as a stream of record
  batches.

  Accepts either a prepared `ExArrow.ADBC.Statement.t()` (built with
  `ExArrow.ADBC.Statement.new/2` or `new/3`) or a `{connection, sql}` pair,
  in which case a one-shot statement is created, executed, and discarded.

  The stream is tagged with `source: {:adbc, sql}` (or `:statement` when a
  pre-built statement is passed).

  ## Examples

      {:ok, stream} = ExArrow.Stream.from_adbc(stmt)

      {:ok, stream} = ExArrow.Stream.from_adbc(conn, "SELECT * FROM events")
  """
  @spec from_adbc(ADBCStatement.t()) ::
          {:ok, t()} | {:error, term()}
  def from_adbc(statement) do
    with {:ok, stream} <- ADBCStatement.execute(statement) do
      {:ok, %{stream | source: {:adbc, :statement}}}
    end
  end

  @spec from_adbc(ADBCConnection.t(), String.t()) ::
          {:ok, t()} | {:error, term()}
  # sobelow_skip ["SQL.Query"]
  # False positive: SQL is forwarded to an ADBC driver and never executed
  # locally in this process.
  def from_adbc(connection, sql) when is_binary(sql) do
    with {:ok, stmt} <- ADBCStatement.new(connection, sql),
         {:ok, stream} <- ADBCStatement.execute(stmt) do
      {:ok, %{stream | source: {:adbc, sql}}}
    end
  end

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
  def next(%__MODULE__{resource: ref, backend: :adbc} = stream) do
    case native().adbc_stream_next(ref) do
      :done -> nil
      {:ok, batch_ref} -> emit_batch(stream, RecordBatch.from_ref(batch_ref))
      {:error, msg} -> {:error, msg}
    end
  end

  def next(%__MODULE__{resource: ref, backend: :ipc} = stream) do
    case native().ipc_stream_next(ref) do
      :done -> nil
      {:ok, batch_ref} -> emit_batch(stream, RecordBatch.from_ref(batch_ref))
      {:error, msg} -> {:error, msg}
    end
  end

  def next(%__MODULE__{resource: ref, backend: :parquet} = stream) do
    case native().parquet_stream_next(ref) do
      :done -> nil
      {:ok, batch_ref} -> emit_batch(stream, RecordBatch.from_ref(batch_ref))
      {:error, msg} -> {:error, msg}
    end
  end

  def next(%__MODULE__{resource: ref, backend: :flight_sql} = stream) do
    case native().flight_sql_stream_next(ref) do
      :done -> nil
      {:ok, batch_ref} -> emit_batch(stream, RecordBatch.from_ref(batch_ref))
      # Pass the structured triple through so callers retain the gRPC code and status.
      {:error, {_code, _grpc_status, _msg} = triple} -> {:error, triple}
      {:error, msg} -> {:error, msg}
    end
  end

  defp emit_batch(%__MODULE__{source: source}, batch) do
    measurements = ExArrow.Telemetry.batch_measurements(batch)
    metadata = %{source: source, schema: nil}
    ExArrow.Telemetry.execute([:ex_arrow, :stream, :batch], measurements, metadata)
    batch
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
