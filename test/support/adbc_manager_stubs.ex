# Lightweight stubs for the external modules used by AdbcPackageManager.
# Inject them via Application.put_env to exercise private code paths without
# a real ADBC driver or Explorer installation.
#
# start_link/1 stubs use spawn_link/1 so the spawned process is linked to the
# caller (the AdbcPackageManager GenServer).  When the manager exits the link
# kills the stub process automatically, matching real start_link semantics and
# preventing sleeping processes from accumulating in the test VM.

defmodule ExArrow.ADBC.AdbcDbStub do
  @moduledoc false
  @spec start_link(list()) :: {:ok, pid()} | {:error, term()}
  def start_link(_opts), do: {:ok, spawn_link(fn -> Process.sleep(:infinity) end)}
end

defmodule ExArrow.ADBC.AdbcDbErrStub do
  @moduledoc false
  @spec start_link(list()) :: {:ok, pid()} | {:error, term()}
  def start_link(_opts), do: {:error, :stub_db_failed}
end

defmodule ExArrow.ADBC.AdbcConnStub do
  @moduledoc false
  @spec start_link(list()) :: {:ok, pid()} | {:error, term()}
  def start_link(_opts), do: {:ok, spawn_link(fn -> Process.sleep(:infinity) end)}
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
  def start_link(_opts), do: {:ok, spawn_link(fn -> Process.sleep(:infinity) end)}
  @spec query(pid(), binary()) :: {:ok, term()} | {:error, term()}
  def query(_conn_pid, _sql), do: {:error, :stub_query_failed}
end

# Returns {:ok, {:ok, inner}} so AdbcPackageManager.adbc_result_to_stream/1 hits the
# unwrap clause for nested {:ok, result} from some drivers.
defmodule ExArrow.ADBC.AdbcConnNestedOkStub do
  @moduledoc false
  @spec start_link(list()) :: {:ok, pid()} | {:error, term()}
  def start_link(_opts), do: {:ok, spawn_link(fn -> Process.sleep(:infinity) end)}
  @spec query(pid(), binary()) :: {:ok, term()} | {:error, term()}
  def query(_conn_pid, _sql), do: {:ok, {:ok, %{}}}
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

  @spec dump_ipc_stream!(term()) :: binary()
  def dump_ipc_stream!(_df) do
    {:ok, binary} = ExArrow.Native.ipc_test_fixture_binary()
    binary
  end
end

# Explorer stub that produces invalid IPC bytes so Reader.from_binary/1 fails.
defmodule ExArrow.ADBC.ExplorerDfBadIpcStub do
  @moduledoc false
  def new(map), do: map

  @spec dump_ipc_stream!(term()) :: binary()
  def dump_ipc_stream!(_df), do: <<"not_valid_arrow_ipc">>
end
