if Code.ensure_loaded?(NimblePool) do
  defmodule ExArrow.ADBC.AdbcPackagePool do
    @moduledoc false
    @behaviour NimblePool

    @doc false
    @spec start_link(keyword()) :: {:ok, pid()} | {:error, term()}
    def start_link(opts) when is_list(opts) do
      db_pid = Keyword.fetch!(opts, :database)
      name = Keyword.get(opts, :name, __MODULE__)
      pool_size = Keyword.get(opts, :pool_size, System.schedulers_online())

      nimble_pool_mod().start_link(worker: {__MODULE__, db_pid}, name: name, pool_size: pool_size)
    end

    @doc false
    @spec query(term(), String.t(), keyword()) :: term()
    def query(pool, sql, opts \\ []) when is_binary(sql) do
      pool_timeout = Keyword.get(opts, :pool_timeout, 5_000)
      conn_module = Module.safe_concat(["Elixir", "Adbc", "Connection"])

      nimble_pool_mod().checkout!(
        pool,
        sql,
        fn _from, conn_pid ->
          {apply(conn_module, :query, [conn_pid, sql]), :ok}
        end,
        pool_timeout
      )
    end

    defp nimble_pool_mod do
      Application.get_env(:ex_arrow, :nimble_pool_mod, NimblePool)
    end

    @impl NimblePool
    def init_worker(db_pid = pool_state) when is_pid(db_pid) do
      conn_module = Module.safe_concat(["Elixir", "Adbc", "Connection"])

      case apply(conn_module, :start_link, [[database: db_pid]]) do
        {:ok, conn_pid} -> {:ok, conn_pid, pool_state}
        {:error, reason} -> {:error, reason}
      end
    end

    @impl NimblePool
    def handle_checkout(_arg, _from, conn_pid, pool_state) do
      {:ok, conn_pid, conn_pid, pool_state}
    end

    @impl NimblePool
    def handle_checkin(:ok, _from, conn_pid, pool_state) do
      {:ok, conn_pid, pool_state}
    end

    def handle_checkin({:remove, reason}, _from, _conn_pid, pool_state) do
      {:remove, reason, pool_state}
    end

    @impl NimblePool
    def terminate_worker(_reason, conn_pid, pool_state) do
      if is_pid(conn_pid) do
        Process.exit(conn_pid, :kill)
      end

      {:ok, pool_state}
    end
  end
end
