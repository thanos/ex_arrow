defmodule ExArrow.ADBC.DatabaseServer do
  @moduledoc """
  A supervised wrapper around `ExArrow.ADBC.Database` that holds a database
  handle open for the lifetime of the process and makes it reachable by name.

  This is used in conjunction with `ExArrow.ADBC.ConnectionPool` so that the
  pool can look up the database handle by its registered name when creating
  new worker connections.

  ## Usage

      children = [
        {ExArrow.ADBC.DatabaseServer,
          name: :mydb,
          driver_path: "/path/to/libadbc_driver_duckdb.so"},
        {ExArrow.ADBC.ConnectionPool,
          name: :mypool, database: :mydb, pool_size: 4}
      ]
      Supervisor.start_link(children, strategy: :one_for_one)

  ## Options

  All options accepted by `ExArrow.ADBC.Database.open/1` are forwarded.
  Additionally:

  * `:name` — optional registered name for the server (defaults to `__MODULE__`).
  """

  use GenServer

  alias ExArrow.ADBC.Database

  # ── Public API ──────────────────────────────────────────────────────────────

  @doc """
  Starts a DatabaseServer that opens and holds a database handle.
  """
  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts) when is_list(opts) do
    {name, db_opts} = Keyword.pop(opts, :name, __MODULE__)
    GenServer.start_link(__MODULE__, db_opts, name: name)
  end

  @doc """
  Returns the `ExArrow.ADBC.Database.t()` held by the named server.

  `name` can be any valid GenServer name: a local atom, a
  `{:global, term}` tuple, or a `{:via, module, term}` tuple.
  """
  @spec get(GenServer.name()) :: Database.t()
  def get(name) do
    GenServer.call(name, :get)
  end

  # ── GenServer callbacks ─────────────────────────────────────────────────────

  @impl GenServer
  def init(db_opts) do
    case Database.open(db_opts) do
      {:ok, db} -> {:ok, db}
      {:error, reason} -> {:stop, reason}
    end
  end

  @impl GenServer
  def handle_call(:get, _from, db) do
    {:reply, db, db}
  end

  @impl GenServer
  def terminate(_reason, db) do
    Database.close(db)
    :ok
  end
end
