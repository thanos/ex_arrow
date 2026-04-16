defmodule ExArrow.FlightSQL.Statement do
  @moduledoc """
  An opaque handle to a server-side prepared statement.

  A `Statement` is created by `ExArrow.FlightSQL.Client.prepare/2` and
  represents a query that has been parsed and planned on the server.  The
  same statement can be executed multiple times.

  ## Usage

      {:ok, stmt} = ExArrow.FlightSQL.Client.prepare(client, "SELECT * FROM t WHERE ts > '2024-01-01'")

      # Execute as a streaming query
      {:ok, stream} = ExArrow.FlightSQL.Statement.execute(stmt)
      batches = Enum.to_list(stream)

      # Execute again (reuses the server-side plan)
      {:ok, stream2} = ExArrow.FlightSQL.Statement.execute(stmt)

  ## Lifecycle

  The underlying server-side handle is released when the `Statement` struct
  is garbage-collected.  There is no explicit `close/1` in v0.5.0; dropping
  the struct is sufficient.

  ## Parameter binding

  Parameter binding is not supported in v0.5.0.  Statements are executed
  with no bound parameters.  Parameterized queries (`SELECT * FROM t WHERE id = ?`)
  can be prepared and executed, but the parameter values cannot be set from
  Elixir in this release.

  ## Compatibility

  Prepared statement support is optional in the Arrow Flight SQL
  specification.  Servers that do not implement `CreatePreparedStatement`
  will cause `Client.prepare/2` to return
  `{:error, %ExArrow.FlightSQL.Error{code: :unimplemented}}`.
  """

  alias ExArrow.FlightSQL.Error
  alias ExArrow.Stream

  @opaque t :: %__MODULE__{resource: reference()}
  defstruct [:resource]

  @doc """
  Execute the prepared statement and return a lazy record-batch stream.

  Sends `ExecutePreparedStatement` to the server and opens a `DoGet` stream
  on the returned endpoint.

  Returns `{:ok, %ExArrow.Stream{}}` or `{:error, %ExArrow.FlightSQL.Error{}}`.

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

  Returns `{:error, %ExArrow.FlightSQL.Error{}}` on failure.

  ## Examples

      {:ok, stmt}    = ExArrow.FlightSQL.Client.prepare(client, "DELETE FROM t WHERE id = 42")
      {:ok, 1}       = ExArrow.FlightSQL.Statement.execute_update(stmt)
  """
  @spec execute_update(t()) :: {:ok, non_neg_integer() | :unknown} | {:error, Error.t()}
  def execute_update(%__MODULE__{resource: ref}) do
    case native().flight_sql_prepared_execute_update(ref) do
      {:ok, :unknown} -> {:ok, :unknown}
      {:ok, n} when is_integer(n) -> {:ok, n}
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
