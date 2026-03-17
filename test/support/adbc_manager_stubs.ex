# Lightweight stubs for the external modules used by AdbcPackageManager.
# Inject them via Application.put_env to exercise private code paths without
# a real ADBC driver or Explorer installation.

defmodule ExArrow.ADBC.AdbcDbStub do
  @moduledoc false
  @spec start_link(list()) :: {:ok, pid()} | {:error, term()}
  def start_link(_opts), do: {:ok, spawn(fn -> Process.sleep(:infinity) end)}
end

defmodule ExArrow.ADBC.AdbcDbErrStub do
  @moduledoc false
  @spec start_link(list()) :: {:ok, pid()} | {:error, term()}
  def start_link(_opts), do: {:error, :stub_db_failed}
end

defmodule ExArrow.ADBC.AdbcConnStub do
  @moduledoc false
  @spec start_link(list()) :: {:ok, pid()} | {:error, term()}
  def start_link(_opts), do: {:ok, spawn(fn -> Process.sleep(:infinity) end)}
  @spec query(pid(), binary()) :: {:ok, term()} | {:error, term()}
  def query(_conn_pid, _sql), do: {:ok, :stub_query_result}
end

defmodule ExArrow.ADBC.AdbcConnErrStub do
  @moduledoc false
  @spec start_link(list()) :: {:ok, pid()} | {:error, term()}
  def start_link(_opts), do: {:error, :stub_conn_failed}
end

defmodule ExArrow.ADBC.AdbcConnQueryErrStub do
  @moduledoc false
  @spec start_link(list()) :: {:ok, pid()} | {:error, term()}
  def start_link(_opts), do: {:ok, spawn(fn -> Process.sleep(:infinity) end)}
  @spec query(pid(), binary()) :: {:ok, term()} | {:error, term()}
  def query(_conn_pid, _sql), do: {:error, :stub_query_failed}
end

defmodule ExArrow.ADBC.AdbcResultStub do
  @moduledoc false
  @spec materialize(map()) :: map()
  def materialize(result), do: result
  @spec to_map(map()) :: map()
  def to_map(_result), do: %{"n" => [1, 2, 3]}
end

# Returns a valid IPC stream binary so Reader.from_binary/1 succeeds.
defmodule ExArrow.ADBC.ExplorerDfStub do
  @moduledoc false
  @spec new(map()) :: map()
  def new(map), do: map

  @spec dump_ipc_stream!(DataFrame.t()) :: binary()
  def dump_ipc_stream!(_df) do
    {:ok, binary} = ExArrow.Native.ipc_test_fixture_binary()
    binary
  end
end
