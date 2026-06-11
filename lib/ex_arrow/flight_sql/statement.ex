defmodule ExArrow.FlightSQL.Statement do
  @moduledoc """
  An opaque handle to a server-side prepared statement.

  A `Statement` is created by `ExArrow.FlightSQL.Client.prepare/2` and
  represents a query that has been parsed and planned on the server.

  ## Lifecycle

      {:ok, stmt} =
        ExArrow.FlightSQL.Client.prepare(client, "SELECT * FROM users WHERE id = ?")

      # Bind parameters
      {:ok, params} =
        ExArrow.RecordBatch.from_columns(["id"], [<<123::little-signed-64>>], ["s64"], 1)

      :ok = ExArrow.FlightSQL.Statement.bind(stmt, params)

      # Execute
      {:ok, stream} = ExArrow.FlightSQL.Statement.execute(stmt)
      batches = Enum.to_list(stream)

      # Re-bind and re-execute
      :ok = ExArrow.FlightSQL.Statement.bind(stmt, other_params)
      {:ok, stream2} = ExArrow.FlightSQL.Statement.execute(stmt)

      # Close when done
      :ok = ExArrow.FlightSQL.Statement.close(stmt)

  ## Parameter binding

  Parameters are bound as Arrow `RecordBatch` values using `bind/2`.  The
  batch schema must be compatible with the parameter schema returned by the
  server during `CreatePreparedStatement`.  Column names must match parameter
  names; column types must be compatible.

  Use `parameter_schema/1` to inspect the expected parameter schema before
  binding.  See `ExArrow.RecordBatch` for the full set of dtype strings
  supported by `ExArrow.RecordBatch.from_columns/4`, including primitives,
  date/timestamp/duration, and `utf8`/`binary` variants.

  ## Close semantics

  `close/1` sends `ActionClosePreparedStatement` to the server, releasing
  server-side resources.  Closed-state is tracked inside the underlying NIF
  resource; after `close/1` returns `:ok`, any subsequent call to `bind/2`,
  `execute/1`, `execute_update/1`, or `parameter_schema/1` returns
  `{:error, %Error{code: :protocol_error, message: "statement is closed"}}`.

  Close is idempotent: calling `close/1` on an already-closed statement
  returns `:ok`.

  If `close/1` returns `{:error, ...}` (for example a transport error mid-
  call) the statement handle is still consumed locally — retrying `close/1`
  is a no-op and returns `:ok`, but the server-side resource may not have
  been released and can leak until the underlying connection is closed.
  See `close/1` for details.

  ## Compatibility

  Prepared statement support is optional in the Arrow Flight SQL
  specification.  Servers that do not implement `CreatePreparedStatement`
  will cause `Client.prepare/2` to return
  `{:error, %ExArrow.FlightSQL.Error{code: :unimplemented}}`.
  """

  alias ExArrow.FlightSQL.Error
  alias ExArrow.{RecordBatch, Schema, Stream}

  @opaque t :: %__MODULE__{resource: reference()}

  defstruct [:resource]

  @doc """
  Bind a `RecordBatch` of parameters to the prepared statement.

  The batch must match the parameter schema returned by the server.  Column
  names must correspond to the `?` placeholders in the SQL query; column
  types must be compatible Arrow types.  Use `parameter_schema/1` to inspect
  the expected schema.

  Binding replaces any previously bound parameters.  After binding, call
  `execute/1` or `execute_update/1` to run the statement with the parameters.

  Returns `:ok` on success or `{:error, %Error{}}` on failure (including
  `:protocol_error` if the statement has been closed).

  ## Examples

      {:ok, params} =
        ExArrow.RecordBatch.from_columns(["id"], [<<42::little-signed-64>>], ["s64"], 1)

      :ok = ExArrow.FlightSQL.Statement.bind(stmt, params)
  """
  @spec bind(t(), RecordBatch.t()) :: :ok | {:error, Error.t()}
  def bind(%__MODULE__{resource: ref}, %RecordBatch{resource: batch_ref}) do
    case native().flight_sql_prepared_bind(ref, batch_ref) do
      :ok -> :ok
      {:error, nif_err} -> {:error, wrap_nif_error(nif_err)}
    end
  end

  @doc """
  Return the parameter schema of the prepared statement.

  The schema describes the column names and Arrow types that `bind/2`
  expects.  An empty schema means the statement takes no parameters.

  Returns `{:ok, %ExArrow.Schema{}}` or `{:error, %Error{}}` (including
  `:protocol_error` if the statement has been closed).

  ## Examples

      {:ok, schema} = ExArrow.FlightSQL.Statement.parameter_schema(stmt)
      [%ExArrow.Field{name: "id", dtype: "int64"}] = ExArrow.Schema.fields(schema)
  """
  @spec parameter_schema(t()) :: {:ok, Schema.t()} | {:error, Error.t()}
  def parameter_schema(%__MODULE__{resource: ref}) do
    case native().flight_sql_prepared_parameter_schema(ref) do
      {:ok, schema_ref} -> {:ok, Schema.from_ref(schema_ref)}
      {:error, nif_err} -> {:error, wrap_nif_error(nif_err)}
    end
  end

  @doc """
  Execute the prepared statement and return a lazy record-batch stream.

  Sends `ExecutePreparedStatement` to the server and opens a `DoGet` stream
  on the returned endpoint.  If parameters were bound with `bind/2`, they
  are sent to the server as part of the execution.

  Returns `{:ok, %ExArrow.Stream{}}` or `{:error, %ExArrow.FlightSQL.Error{}}`
  (including `:protocol_error` if the statement has been closed).

  ## Examples

      {:ok, stream} = ExArrow.FlightSQL.Statement.execute(stmt)
      batches = Enum.to_list(stream)
  """
  @spec execute(t()) :: {:ok, Stream.t()} | {:error, Error.t()}
  def execute(%__MODULE__{resource: ref}) do
    case native().flight_sql_prepared_execute(ref) do
      {:ok, stream_ref} -> {:ok, %Stream{resource: stream_ref, backend: :flight_sql}}
      {:error, nif_err} -> {:error, wrap_nif_error(nif_err)}
    end
  end

  @doc """
  Execute the prepared statement as a DML or DDL operation.

  Returns `{:ok, n}` where `n` is the number of affected rows, or
  `{:ok, :unknown}` when the server does not report a count.

  Returns `{:error, %ExArrow.FlightSQL.Error{}}` on failure (including
  `:protocol_error` if the statement has been closed).

  ## Examples

      {:ok, stmt} = ExArrow.FlightSQL.Client.prepare(client, "DELETE FROM t WHERE id = 42")
      {:ok, 1} = ExArrow.FlightSQL.Statement.execute_update(stmt)
  """
  @spec execute_update(t()) :: {:ok, non_neg_integer() | :unknown} | {:error, Error.t()}
  def execute_update(%__MODULE__{resource: ref}) do
    case native().flight_sql_prepared_execute_update(ref) do
      {:ok, :unknown} -> {:ok, :unknown}
      {:ok, n} when is_integer(n) -> {:ok, n}
      {:error, nif_err} -> {:error, wrap_nif_error(nif_err)}
    end
  end

  @doc """
  Close the prepared statement and release server-side resources.

  Sends `ActionClosePreparedStatement` to the server.  Closed-state is
  tracked inside the underlying NIF resource: after `close/1` returns,
  any subsequent call to `bind/2`, `execute/1`, `execute_update/1`, or
  `parameter_schema/1` returns `{:error, %Error{code: :protocol_error}}`.

  Close is idempotent: calling `close/1` on an already-closed statement
  returns `:ok` without contacting the server.

  ## Return values

  - `:ok` — the server acknowledged `ActionClosePreparedStatement` and
    released the resource.
  - `{:error, %Error{}}` — a transport, protocol, or server error
    occurred while closing.

  ## Behaviour on error

  The underlying `arrow-flight` `PreparedStatement::close(self)` consumes
  the statement value, so retrying after a failure is not possible.  When
  `close/1` returns `{:error, ...}`:

  - The statement handle is consumed locally regardless of outcome.
  - All subsequent operations on the handle (including a retry of
    `close/1`) return `:ok` for `close/1` and `:protocol_error` for
    everything else.
  - The server-side resource may or may not have been freed.  If the
    failure was transient (for example a transport error mid-call) the
    server-side prepared statement may leak until the connection is
    closed.

  Callers that need a guarantee of server-side cleanup should drop the
  whole client connection on a `close/1` error.

  ## Examples

      :ok = ExArrow.FlightSQL.Statement.close(stmt)

      # Defensive cleanup pattern
      try do
        # ... use stmt ...
      after
        _ = ExArrow.FlightSQL.Statement.close(stmt)
      end
  """
  @spec close(t()) :: :ok | {:error, Error.t()}
  def close(%__MODULE__{resource: ref}) do
    case native().flight_sql_prepared_close(ref) do
      :ok -> :ok
      {:error, nif_err} -> {:error, wrap_nif_error(nif_err)}
    end
  end

  # ── Helpers ───────────────────────────────────────────────────────────────────

  defp native,
    do: Application.get_env(:ex_arrow, :flight_sql_statement_native, ExArrow.Native)

  defp wrap_nif_error({code, grpc_status, msg})
       when is_atom(code) and is_integer(grpc_status) and is_binary(msg) do
    Error.from_nif({code, grpc_status, msg})
  end

  defp wrap_nif_error(msg) when is_binary(msg) do
    Error.from_string(:transport_error, msg)
  end

  defp wrap_nif_error(other) do
    Error.from_string(:transport_error, inspect(other))
  end
end
