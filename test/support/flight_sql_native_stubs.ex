# Test-only native stubs for ExArrow.FlightSQL.ClientImpl.
# Inject via:
#   Application.put_env(:ex_arrow, :flight_sql_client_native, ExArrow.FlightSQL.TestNative*)

# Returns {:error, binary} — exercises the `is_binary(msg)` clause of wrap_nif_error/1.
defmodule ExArrow.FlightSQL.TestNativeBinaryError do
  @moduledoc false
  def flight_sql_connect(_host, _port, _tls_mode, _headers),
    do: {:error, "connection refused"}
end

# Returns {:error, {code, grpc_status, msg}} — exercises the 3-tuple clause of wrap_nif_error/1.
defmodule ExArrow.FlightSQL.TestNativeTupleError do
  @moduledoc false
  def flight_sql_connect(_host, _port, _tls_mode, _headers),
    do: {:error, {:unauthenticated, 16, "missing token"}}
end

# Returns {:error, atom} — exercises the fallback clause of wrap_nif_error/1 (neither
# binary nor 3-tuple atom/int/binary), which calls inspect/1 on the term.
defmodule ExArrow.FlightSQL.TestNativeFallbackError do
  @moduledoc false
  def flight_sql_connect(_host, _port, _tls_mode, _headers),
    do: {:error, :unexpected_atom}
end
