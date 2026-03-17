defmodule ExArrow.ADBC.AdbcPackageManager do
  @moduledoc false
  use GenServer

  alias ExArrow.ADBC.AdbcPackagePool
  alias ExArrow.IPC.Reader

  @pool_name ExArrow.ADBC.AdbcPackagePool

  @doc false
  @spec start_link(keyword()) :: {:ok, pid()} | {:error, term()}
  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @doc false
  @spec get_pids() :: {:ok, {pid(), pid()}} | {:error, term()}
  def get_pids do
    GenServer.call(__MODULE__, :get_pids, 5_000)
  end

  @doc false
  @spec create_statement(pid() | nil) :: {:ok, reference()} | {:error, term()}
  def create_statement(_conn_pid \\ nil) do
    GenServer.call(__MODULE__, :create_statement, 5_000)
  end

  @doc false
  @spec set_statement_sql(reference(), String.t()) :: :ok | {:error, term()}
  def set_statement_sql(ref, sql) when is_reference(ref) and is_binary(sql) do
    GenServer.call(__MODULE__, {:set_statement_sql, ref, sql}, 5_000)
  end

  @doc false
  @spec execute_statement(reference()) :: {:ok, term()} | {:error, term()}
  def execute_statement(ref) when is_reference(ref) do
    GenServer.call(__MODULE__, {:execute_statement, ref}, 30_000)
  end

  @impl true
  @spec init(keyword()) :: {:ok, term()} | {:error, term()}
  def init(_opts) do
    table = :ets.new(__MODULE__, [:set, :private, :named_table])
    state = start_if_configured(%{table: table})

    {:ok, state}
  end

  @impl true
  @spec handle_call(term(), term(), term()) ::
          {:reply, term(), term()}
          | {:noreply, term()}
          | {:stop, term(), term()}
          | {:error, term()}
  def handle_call(:get_pids, _from, nil) do
    {:reply, {:error, :not_configured}, nil}
  end

  def handle_call(:get_pids, _from, %{db: _, conn: _} = state) do
    {:reply, {:ok, {state.db, state.conn}}, state}
  end

  def handle_call(:get_pids, _from, %{table: _} = state) do
    case ensure_started(state) do
      {:ok, new_state} -> {:reply, {:ok, {new_state.db, Map.get(new_state, :conn)}}, new_state}
      {:error, reason, new_state} -> {:reply, {:error, reason}, new_state}
    end
  end

  def handle_call(:get_pids, _from, {:error, reason}) do
    {:reply, {:error, reason}, {:error, reason}}
  end

  def handle_call(:create_statement, _from, %{table: table} = state) do
    ref = make_ref()
    :ets.insert(table, {ref, nil})
    {:reply, {:ok, ref}, state}
  end

  # sobelow_skip ["Sobelow.SQL.Query"]
  def handle_call({:set_statement_sql, _ref, _sql}, _from, state) when not is_map(state) do
    {:reply, {:error, :not_configured}, state}
  end

  def(handle_call({:set_statement_sql, ref, sql}, _from, state)) do
    table = Map.get(state, :table)

    if table do
      try do
        [{^ref, _}] = :ets.lookup(table, ref)
        :ets.insert(table, {ref, sql})
        {:reply, :ok, state}
      catch
        :error, _ -> {:reply, {:error, "statement not found"}, state}
      end
    else
      {:reply, {:error, :not_configured}, state}
    end
  end

  @spec handle_call({:execute_statement, reference()}, term(), term()) ::
          {:reply, term(), term()}
          | {:noreply, term()}
          | {:stop, term(), term()}
          | {:error, term()}
  # sobelow_skip ["Sobelow.SQL.Query"]
  def handle_call({:execute_statement, ref}, _from, state) do
    table = Map.get(state, :table)

    case table && :ets.lookup(table, ref) do
      [{^ref, sql}] when is_binary(sql) ->
        case ensure_started(state) do
          {:ok, new_state} ->
            case query(sql, new_state) do
              {:ok, result} ->
                {:reply, adbc_result_to_stream(result), new_state}

              {:error, _} = err ->
                {:reply, err, new_state}
            end

          {:error, reason, new_state} ->
            {:reply, {:error, reason}, new_state}
        end

      [{^ref, nil}] ->
        {:reply, {:error, "set_sql was not called"}, state}

      _ ->
        {:reply, {:error, "statement not found"}, state}
    end
  end

  # sobelow_skip ["Sobelow.SQL.Query"]
  defp query(sql, state) do
    if pool = Map.get(state, :pool) do
      pool_module = Module.safe_concat(["Elixir", "ExArrow", "ADBC", "AdbcPackagePool"])
      pool_module.query(pool, sql)
    else
      conn_pid = Map.get(state, :conn)
      apply(adbc_conn_module(), :query, [conn_pid, sql])
    end
  end

  defp ensure_started(%{db: _} = state), do: {:ok, state}

  defp ensure_started(%{table: _} = state) do
    case start_if_configured(state) do
      %{db: _} = new_state -> {:ok, new_state}
      {:error, reason} -> {:error, reason, state}
      other -> {:error, :not_configured, other}
    end
  end

  defp start_if_configured(%{table: _} = state) do
    opts = Application.get_env(:ex_arrow, :adbc_package)

    with true <- is_list(opts) and opts != [],
         {:ok, db_pid} <- start_database(opts),
         {:ok, new_state} <- start_pool_or_connection(db_pid, state) do
      new_state
    else
      {:error, _} = err -> err
      _ -> state
    end
  end

  defp start_database(opts) do
    apply(adbc_db_module(), :start_link, [opts])
  end

  defp use_pool? do
    pool_size = Application.get_env(:ex_arrow, :adbc_package_pool_size, 1)

    pool_size > 1 and Code.ensure_loaded?(NimblePool) and
      Code.ensure_loaded?(AdbcPackagePool)
  end

  defp start_pool_or_connection(db_pid, state) do
    result = if use_pool?(), do: start_pool(db_pid, state), else: start_connection(db_pid, state)

    case result do
      {:ok, _} = ok ->
        ok

      {:error, reason} ->
        Process.exit(db_pid, :kill)
        {:error, reason}
    end
  end

  defp start_pool(db_pid, state) do
    pool_size = Application.get_env(:ex_arrow, :adbc_package_pool_size, 1)

    case AdbcPackagePool.start_link(
           database: db_pid,
           name: @pool_name,
           pool_size: pool_size
         ) do
      {:ok, _pid} -> {:ok, Map.merge(state, %{db: db_pid, pool: @pool_name})}
      {:error, reason} -> {:error, reason}
    end
  end

  defp start_connection(db_pid, state) do
    case apply(adbc_conn_module(), :start_link, [[database: db_pid]]) do
      {:ok, conn_pid} -> {:ok, Map.merge(state, %{db: db_pid, conn: conn_pid})}
      {:error, reason} -> {:error, reason}
    end
  end

  defp adbc_db_module,
    do:
      Application.get_env(
        :ex_arrow,
        :adbc_db_module,
        Module.safe_concat(["Elixir", "Adbc", "Database"])
      )

  defp adbc_conn_module,
    do:
      Application.get_env(
        :ex_arrow,
        :adbc_conn_module,
        Module.safe_concat(["Elixir", "Adbc", "Connection"])
      )

  defp adbc_result_module,
    do:
      Application.get_env(
        :ex_arrow,
        :adbc_result_module,
        Module.safe_concat(["Elixir", "Adbc", "Result"])
      )

  defp explorer_df_module,
    do:
      Application.get_env(
        :ex_arrow,
        :explorer_df_module,
        Module.safe_concat(["Elixir", "Explorer", "DataFrame"])
      )

  defp adbc_result_to_stream({:ok, result}) do
    adbc_result_to_stream(result)
  end

  defp adbc_result_to_stream(result) do
    result_module = adbc_result_module()
    materialized = apply(result_module, :materialize, [result])
    map = apply(result_module, :to_map, [materialized])

    explorer_df = explorer_df_module()

    if Code.ensure_loaded?(explorer_df) do
      df = apply(explorer_df, :new, [map])
      # ExArrow.IPC.Reader expects stream format; dump_ipc! writes file format (footer).
      binary = apply(explorer_df, :dump_ipc_stream!, [df])

      case Reader.from_binary(binary) do
        {:ok, stream} -> {:ok, stream}
        {:error, _} = err -> err
      end
    else
      {:error,
       "adbc_package backend requires the :explorer dependency to convert query results to ExArrow.Stream. Add {:explorer, \"~> 0.8\"} to your deps."}
    end
  end
end
