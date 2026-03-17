if Code.ensure_loaded?(NimblePool) do
  defmodule ExArrow.ADBC.ConnectionPool do
    @moduledoc """
    NimblePool-backed ADBC connection pool for ExArrow.

    Opens a pool of `ExArrow.ADBC.Connection` handles against a single
    `ExArrow.ADBC.Database`, recycling them across callers.  This is the
    recommended approach for applications that issue many concurrent or
    frequent ADBC queries.

    ## Usage

    ### Supervised pool (recommended)

    Add the pool to your supervision tree:

        children = [
          # 1. Open the database (once)
          {ExArrow.ADBC.DatabaseServer, name: :mydb, driver_path: "/path/to/libadbc_driver_duckdb.so"},
          # 2. Start a pool of connections against it
          {ExArrow.ADBC.ConnectionPool, name: :mypool, database: :mydb, pool_size: 4}
        ]
        Supervisor.start_link(children, strategy: :one_for_one)

    Then call the pool:

        {:ok, stream} = ExArrow.ADBC.ConnectionPool.query(:mypool, "SELECT 1 AS n")

    ### Ad-hoc pool

        {:ok, db}   = ExArrow.ADBC.Database.open(driver_path: "/path/to/driver.so")
        {:ok, pool} = ExArrow.ADBC.ConnectionPool.start_link(database: db, pool_size: 4)

        {:ok, stream} = ExArrow.ADBC.ConnectionPool.query(pool, "SELECT 42 AS answer")

    ## Options

    * `:database`    — `ExArrow.ADBC.Database.t()` or a registered name (`atom`).
      Required.
    * `:pool_size`   — number of connections to keep open (default: `System.schedulers_online()`).
    * `:name`        — optional `GenServer`-style name for the pool process.
    * `:lazy`        — if `true`, connections are opened on first checkout rather than at
      pool start (default: `false`).

    ## Checkout options (passed as the final keyword list to `query/3`)

    * `:pool_timeout` — milliseconds to wait for an available connection (default: `5_000`).
    * `:timeout`      — milliseconds for the SQL statement to execute (default: `15_000`).
    """

    @behaviour NimblePool

    alias ExArrow.ADBC.{Connection, DatabaseServer, Statement}
    alias ExArrow.Stream

    # Pool worker state: the open Database handle (shared) and the per-worker Connection.
    defmodule Worker do
      @moduledoc false
      defstruct [:db, :conn]
    end

    @doc """
    Starts a connection pool linked to the current process.

    See the module doc for accepted options.
    """
    @spec start_link(keyword()) :: {:ok, pid()} | {:error, term()}
    def start_link(opts) when is_list(opts) do
      {pool_size, opts} = Keyword.pop(opts, :pool_size, System.schedulers_online())
      {lazy, opts} = Keyword.pop(opts, :lazy, false)
      {name, opts} = Keyword.pop(opts, :name)

      db_or_name = Keyword.fetch!(opts, :database)

      nimble_opts = [
        worker: {__MODULE__, db_or_name},
        pool_size: pool_size,
        lazy: lazy
      ]

      nimble_opts = if name, do: [{:name, name} | nimble_opts], else: nimble_opts
      nimble_pool_mod().start_link(nimble_opts)
    end

    @doc """
    Executes `sql` using a checked-out connection from `pool`.

    Returns `{:ok, ExArrow.Stream.t()}` where the stream has already been
    fully collected into memory so the connection can be returned to the pool
    immediately.  Use `ExArrow.Stream.to_list/1` to decode the batches.

    ## Options

    * `:pool_timeout` — ms to wait for a connection (default `5_000`).
    * `:timeout`      — ms allowed for statement execution (default `15_000`).
    """
    @spec query(term(), String.t(), keyword()) ::
            {:ok, Stream.t()} | {:error, term()}
    def query(pool, sql, opts \\ []) when is_binary(sql) do
      pool_timeout = Keyword.get(opts, :pool_timeout, 5_000)

      nimble_pool_mod().checkout!(
        pool,
        :checkout,
        fn _from, %Worker{conn: conn} = worker ->
          result = run_statement(conn, sql)
          checkin = if match?({:error, _}, result), do: {:remove, :error}, else: :ok
          {result, checkin, worker}
        end,
        pool_timeout
      )
    end

    @doc """
    Runs `fun.(conn)` with a checked-out `ExArrow.ADBC.Connection.t()`.

    Use this for multi-statement operations or metadata calls that do not fit
    the single-SQL `query/3` API.  `fun` must return `{result, checkin_tag}`
    where `checkin_tag` is `:ok` (reuse the connection) or `{:remove, reason}`
    (discard and replace it).

    ## Example

        ExArrow.ADBC.ConnectionPool.with_connection(pool, fn conn ->
          {:ok, stmt} = ExArrow.ADBC.Statement.new(conn)
          ExArrow.ADBC.Statement.set_sql(stmt, "SELECT 1")
          {ExArrow.ADBC.Statement.execute(stmt), :ok}
        end)
    """
    @spec with_connection(
            term(),
            (Connection.t() -> {term(), :ok | {:remove, term()}}),
            keyword()
          ) ::
            term()
    def with_connection(pool, fun, opts \\ []) when is_function(fun, 1) do
      pool_timeout = Keyword.get(opts, :pool_timeout, 5_000)

      nimble_pool_mod().checkout!(
        pool,
        :checkout,
        fn _from, %Worker{conn: conn} = worker ->
          {result, checkin} = fun.(conn)
          {result, checkin, worker}
        end,
        pool_timeout
      )
    end

    # ── NimblePool callbacks ────────────────────────────────────────────────────

    @impl NimblePool
    def init_worker(db_or_name) do
      # Resolve any GenServer name (atom, {:global, term}, {:via, mod, term}) to a
      # Database handle.  We identify a "name" by checking it is NOT an already-open
      # Database struct — this avoids pattern-matching on the opaque struct internals
      # and accepts all valid GenServer.name() forms (not just atoms).
      db =
        if is_struct(db_or_name, ExArrow.ADBC.Database),
          do: db_or_name,
          else: DatabaseServer.get(db_or_name)

      case Connection.open(db) do
        {:ok, conn} -> {:ok, %Worker{db: db, conn: conn}, db_or_name}
        {:error, reason} -> {:error, reason}
      end
    end

    @impl NimblePool
    def handle_checkout(:checkout, _from, worker, pool_state) do
      {:ok, worker, worker, pool_state}
    end

    @impl NimblePool
    def handle_checkin(:ok, _from, worker, pool_state) do
      {:ok, worker, pool_state}
    end

    def handle_checkin({:remove, _reason}, _from, _worker, pool_state) do
      {:remove, :closed, pool_state}
    end

    @impl NimblePool
    def terminate_worker(_reason, %Worker{conn: conn}, pool_state) do
      Connection.close(conn)
      {:ok, pool_state}
    end

    # ── Private helpers ─────────────────────────────────────────────────────────

    defp nimble_pool_mod do
      Application.get_env(:ex_arrow, :nimble_pool_mod, NimblePool)
    end

    defp run_statement(conn, sql) do
      with {:ok, stmt} <- Statement.new(conn),
           :ok <- Statement.set_sql(stmt, sql),
           {:ok, stream} <- Statement.execute(stmt) do
        {:ok, stream}
      end
    end
  end
end
