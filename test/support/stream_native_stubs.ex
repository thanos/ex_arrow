# Test-only native stubs for ExArrow.Stream.
# Set Application.put_env(:ex_arrow, :stream_native, ExArrow.Stream.TestNativeError) etc.

defmodule ExArrow.Stream.TestNativeDone do
  @moduledoc false
  # Returns :done for all stream-next calls so Stream.next/1 returns nil.
  def adbc_stream_next(_ref), do: :done
  def ipc_stream_next(_ref), do: :done
  def parquet_stream_next(_ref), do: :done
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
end
