# Test-only native stubs for ExArrow.FlightSQL.Statement.
# Inject via:
#   Application.put_env(:ex_arrow, :flight_sql_statement_native, ExArrow.FlightSQL.StmtNative*)

# Returns a successful stream ref for execute/1 -- used to test the happy path
# without a live NIF resource.
defmodule ExArrow.FlightSQL.StmtNativeOk do
  @moduledoc false
  def flight_sql_prepared_execute(_ref), do: {:ok, :fake_stream_ref}
  def flight_sql_prepared_execute_update(_ref), do: {:ok, 5}
  def flight_sql_prepared_bind(_ref, _batch_ref), do: :ok
  def flight_sql_prepared_parameter_schema(_ref), do: {:ok, :fake_schema_ref}
  def flight_sql_prepared_close(_ref), do: :ok
end

# Returns {:ok, :unknown} for execute_update -- server did not report a row count.
defmodule ExArrow.FlightSQL.StmtNativeUnknown do
  @moduledoc false
  def flight_sql_prepared_execute_update(_ref), do: {:ok, :unknown}
end

# Returns a gRPC-style error for both execute paths.
defmodule ExArrow.FlightSQL.StmtNativeError do
  @moduledoc false
  def flight_sql_prepared_execute(_ref),
    do: {:error, {:server_error, 13, "internal error"}}

  def flight_sql_prepared_execute_update(_ref),
    do: {:error, {:permission_denied, 7, "read-only"}}

  def flight_sql_prepared_bind(_ref, _batch_ref),
    do: {:error, {:invalid_argument, 3, "schema mismatch"}}

  def flight_sql_prepared_parameter_schema(_ref),
    do: {:error, {:unimplemented, 12, "parameter schema not available"}}

  def flight_sql_prepared_close(_ref),
    do: {:error, {:server_error, 13, "close failed"}}
end

# Models the NIF behaviour for an already-closed statement: every operation
# (other than close, which is idempotent) returns :protocol_error.
defmodule ExArrow.FlightSQL.StmtNativeClosed do
  @moduledoc false
  def flight_sql_prepared_execute(_ref),
    do: {:error, {:protocol_error, 0, "statement is closed"}}

  def flight_sql_prepared_execute_update(_ref),
    do: {:error, {:protocol_error, 0, "statement is closed"}}

  def flight_sql_prepared_bind(_ref, _batch_ref),
    do: {:error, {:protocol_error, 0, "statement is closed"}}

  def flight_sql_prepared_parameter_schema(_ref),
    do: {:error, {:protocol_error, 0, "statement is closed"}}

  # Idempotent: real NIF returns :ok when the inner Option has already been taken.
  def flight_sql_prepared_close(_ref), do: :ok
end

# Stateful stub that models the real NIF's Mutex<Option<PreparedStatement>>
# closed-state behaviour: the first close call succeeds and flips the
# resource into the closed state; subsequent close calls are idempotent and
# also return :ok; every other operation on a closed resource returns
# :protocol_error.
#
# State is held in a process dictionary keyed by the calling process so
# tests using this stub remain isolated when run async: false.
defmodule ExArrow.FlightSQL.StmtNativeStatefulClose do
  @moduledoc false
  @key :__ex_arrow_stmt_stub_state__

  @doc "Reset the stub state for the current process."
  def reset, do: Process.put(@key, %{closed?: false, calls: 0})

  @doc "Whether close has been observed at least once."
  def closed?, do: state().closed?

  @doc "Total number of close calls observed."
  def call_count, do: state().calls

  defp state do
    Process.get(@key) || %{closed?: false, calls: 0}
  end

  defp put_state(s), do: Process.put(@key, s)

  defp ensure_state do
    case Process.get(@key) do
      nil ->
        s = %{closed?: false, calls: 0}
        Process.put(@key, s)
        s

      s ->
        s
    end
  end

  def flight_sql_prepared_close(_ref) do
    s = ensure_state()
    put_state(%{s | closed?: true, calls: s.calls + 1})
    :ok
  end

  def flight_sql_prepared_execute(_ref), do: closed_or_ok({:ok, :fake_stream_ref})

  def flight_sql_prepared_execute_update(_ref), do: closed_or_ok({:ok, 5})

  def flight_sql_prepared_bind(_ref, _batch_ref), do: closed_or_ok(:ok)

  def flight_sql_prepared_parameter_schema(_ref),
    do: closed_or_ok({:ok, :fake_schema_ref})

  defp closed_or_ok(default) do
    if state().closed? do
      {:error, {:protocol_error, 0, "statement is closed"}}
    else
      default
    end
  end
end

# Returns a binary (non-tuple) error -- exercises the binary clause of wrap_nif_error.
defmodule ExArrow.FlightSQL.StmtNativeBinaryError do
  @moduledoc false
  def flight_sql_prepared_execute(_ref), do: {:error, "stream failed"}
  def flight_sql_prepared_execute_update(_ref), do: {:error, "dml failed"}
  def flight_sql_prepared_bind(_ref, _batch_ref), do: {:error, "bind failed"}
  def flight_sql_prepared_parameter_schema(_ref), do: {:error, "schema failed"}
  def flight_sql_prepared_close(_ref), do: :ok
end

# Also add a stub for flight_sql_prepare in ClientImpl tests.
# Returns {:ok, fake_stmt_ref} for prepare, simulating a successful server response.
defmodule ExArrow.FlightSQL.TestNativePrepareOk do
  @moduledoc false
  def flight_sql_prepare(_client_ref, _sql), do: {:ok, :fake_stmt_ref}
end

# Returns unimplemented error for prepare.
defmodule ExArrow.FlightSQL.TestNativePrepareUnimplemented do
  @moduledoc false
  def flight_sql_prepare(_client_ref, _sql),
    do: {:error, {:unimplemented, 12, "prepared statements not supported"}}
end
