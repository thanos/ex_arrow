# Test-only native stubs for ExArrow.Stream.
# Set Application.put_env(:ex_arrow, :stream_native, ExArrow.Stream.TestNativeError) etc.

defmodule ExArrow.Stream.TestNativeDone do
  @moduledoc false
  # Returns :done for all stream-next calls so Stream.next/1 returns nil.
  def adbc_stream_next(_ref), do: :done
  def ipc_stream_next(_ref), do: :done
  def parquet_stream_next(_ref), do: :done
  def flight_sql_stream_next(_ref), do: :done
end

defmodule ExArrow.Stream.TestNativeError do
  @moduledoc false
  # Returns {:error, msg} for schema and next so error branches are exercised.
  @spec adbc_stream_schema(reference()) :: {:error, String.t()}
  def adbc_stream_schema(_ref), do: {:error, "adbc stream schema error"}
  @spec ipc_stream_schema(reference()) :: {:error, String.t()}
  def ipc_stream_schema(_ref), do: {:error, "ipc stream schema error"}
  @spec adbc_stream_next(reference()) :: {:error, String.t()}
  def adbc_stream_next(_ref), do: {:error, "adbc stream next error"}
  @spec ipc_stream_next(reference()) :: {:error, String.t()}
  def ipc_stream_next(_ref), do: {:error, "ipc stream next error"}
  @spec parquet_stream_next(reference()) :: {:error, String.t()}
  def parquet_stream_next(_ref), do: {:error, "parquet stream next error"}
  # Flight SQL error branches
  def flight_sql_stream_schema(_ref), do: {:error, "flight_sql stream schema error"}
  def flight_sql_stream_next(_ref), do: {:error, "flight_sql stream next error"}
end

# Returns a gRPC-style 3-tuple error from flight_sql_stream_next so the
# "[code] msg" formatting branch in Stream.next/1 can be tested.
defmodule ExArrow.Stream.TestNativeFlightSqlTriple do
  @moduledoc false
  def flight_sql_stream_schema(_ref), do: {:ok, :fake_schema_ref}
  def flight_sql_stream_next(_ref), do: {:error, {:unavailable, 14, "server gone"}}
end

# Returns a successful schema and :done for next — used to test from_stream/1
# with an empty (but valid) stream without requiring a real NIF.
defmodule ExArrow.Stream.TestNativeFlightSqlOk do
  @moduledoc false
  def flight_sql_stream_schema(_ref), do: {:ok, :fake_schema_ref}
  def flight_sql_stream_next(_ref), do: :done
end
