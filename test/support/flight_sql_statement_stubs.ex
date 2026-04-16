# Test-only native stubs for ExArrow.FlightSQL.Statement.
# Inject via:
#   Application.put_env(:ex_arrow, :flight_sql_statement_native, ExArrow.FlightSQL.StmtNative*)

# Returns a successful stream ref for execute/1 — used to test the happy path
# without a live NIF resource.
defmodule ExArrow.FlightSQL.StmtNativeOk do
  @moduledoc false
  def flight_sql_prepared_execute(_ref), do: {:ok, :fake_stream_ref}
  def flight_sql_prepared_execute_update(_ref), do: {:ok, 5}
end

# Returns {:ok, :unknown} for execute_update — server did not report a row count.
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
end

# Returns a binary (non-tuple) error — exercises the binary clause of wrap_nif_error.
defmodule ExArrow.FlightSQL.StmtNativeBinaryError do
  @moduledoc false
  def flight_sql_prepared_execute(_ref), do: {:error, "stream failed"}
  def flight_sql_prepared_execute_update(_ref), do: {:error, "dml failed"}
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
