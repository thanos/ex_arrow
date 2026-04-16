defmodule ExArrow.FlightSQL.Client do
  @moduledoc """
  Arrow Flight SQL client for executing SQL queries against a remote server.

  Delegates to the configured implementation module (see `:flight_sql_client_impl` in
  application config). The default implementation is backed by NIFs using the
  `arrow-flight` + tonic Rust crate stack.

  ## Quick start

      {:ok, client} = ExArrow.FlightSQL.Client.connect("localhost:32010")
      {:ok, result} = ExArrow.FlightSQL.Client.query(client, "SELECT id, name FROM users")
      result.num_rows  #=> 42

  ## Connection

  `connect/1` accepts a `"host:port"` URI string. `connect/2` accepts the same
  string plus a keyword options list.

  ### TLS

  | `:tls` value | Behaviour |
  |---|---|
  | not set, loopback host | plaintext (auto) |
  | not set, remote host | TLS with native OS certificate store (auto, secure default) |
  | `false` | plaintext regardless of host |
  | `true` | TLS with native OS certificate store |
  | `[ca_cert_pem: pem]` | TLS with a custom PEM-encoded CA certificate |

  ### Authentication

  Pass credentials as gRPC metadata via the `:headers` option:

      {:ok, client} = ExArrow.FlightSQL.Client.connect("dremio.example.com:32010",
        tls: true,
        headers: [{"authorization", "Bearer my-pat-token"}]
      )

  ## Queries

  `query/2` collects all result batches and returns a materialized
  `ExArrow.FlightSQL.Result`.  Use `stream_query/2` for large result sets that
  must be consumed lazily.

  `execute/2` runs DML (INSERT / UPDATE / DELETE / DDL) and returns the affected
  row count or `:unknown` when the server does not report one.

  ## Compatibility

  Designed for DuckDB Flight SQL server (v0.10+), DataFusion, Dremio, and other
  servers that implement the Arrow Flight SQL specification.  End-to-end
  validation requires a live server; see the `flight_sql_integration` test tag.

  Multi-endpoint `FlightInfo` responses (distributed queries) are not supported in
  v0.5.0 — `query/2` returns `{:error, %Error{code: :multi_endpoint}}` in that case.

  Transaction operations (`BEGIN`, `COMMIT`, `ROLLBACK`) are deferred to v0.6.0.
  """

  alias ExArrow.FlightSQL.{Error, Result, Statement}
  alias ExArrow.Stream

  @opaque t :: %__MODULE__{resource: reference()}
  defstruct [:resource]

  @compile {:inline, [impl: 0]}
  defp impl do
    Application.get_env(:ex_arrow, :flight_sql_client_impl, ExArrow.FlightSQL.ClientImpl)
  end

  # ── Connection ────────────────────────────────────────────────────────────────

  @doc """
  Connect to a Flight SQL server at the given URI.

  `uri` must be a `"host:port"` string, e.g. `"localhost:32010"`.
  An explicit port is strongly recommended; a bare `"host"` string is accepted as
  a convenience and defaults to port `32010` (the Arrow Flight SQL convention).

  ## Examples

      {:ok, client} = ExArrow.FlightSQL.Client.connect("localhost:32010")

      {:ok, client} = ExArrow.FlightSQL.Client.connect("dremio.example.com:32010",
        tls: true,
        headers: [{"authorization", "Bearer token"}]
      )
  """
  @spec connect(String.t(), keyword()) :: {:ok, t()} | {:error, Error.t()}
  def connect(uri, opts \\ []) when is_binary(uri) do
    impl().connect(uri, opts)
  end

  @doc """
  Close the connection and release native resources.

  In v0.5.0 the underlying gRPC channel is released when the client handle is
  garbage-collected. Calling `close/1` explicitly is safe and idempotent, but
  does not eagerly close the channel.
  """
  @spec close(t()) :: :ok
  def close(%__MODULE__{} = client) do
    impl().close(client)
  end

  # ── Queries ───────────────────────────────────────────────────────────────────

  @doc """
  Execute a SQL query and return a materialized result.

  Collects all record batches from the server before returning.  For large result
  sets, prefer `stream_query/2`.

  Returns `{:ok, %ExArrow.FlightSQL.Result{}}` or `{:error, %ExArrow.FlightSQL.Error{}}`.

  > #### Concurrency {: .warning}
  > Concurrent calls on the **same** client handle are serialised — the underlying
  > gRPC client requires exclusive access per call.  For parallel queries, create
  > separate client handles with `connect/2`.

  ## Examples

      {:ok, result} = ExArrow.FlightSQL.Client.query(client, "SELECT * FROM t")
      result.num_rows   #=> 100
      result.schema     #=> %ExArrow.Schema{...}
  """
  @spec query(t(), String.t()) :: {:ok, Result.t()} | {:error, Error.t()}
  # sobelow_skip ["SQL.Query"]
  # False positive: SQL is forwarded to a remote Flight SQL server over gRPC and
  # is never executed locally in this process.
  def query(%__MODULE__{} = client, sql) when is_binary(sql) do
    with {:ok, stream} <- impl().query(client, sql, []) do
      Result.from_stream(stream)
    end
  end

  @doc """
  Execute a SQL query and return a materialized result, raising on failure.

  Raises `ExArrow.FlightSQL.Error` if the query fails.

  ## Examples

      result = ExArrow.FlightSQL.Client.query!(client, "SELECT * FROM t")
  """
  @spec query!(t(), String.t()) :: Result.t()
  # sobelow_skip ["SQL.Query"]
  # False positive: delegates to query/2, which forwards SQL to a remote Flight SQL server over gRPC.
  def query!(%__MODULE__{} = client, sql) when is_binary(sql) do
    case query(client, sql) do
      {:ok, result} -> result
      {:error, err} -> raise err
    end
  end

  @doc """
  Execute a SQL query and return a lazy stream of record batches.

  Returns `{:ok, %ExArrow.Stream{}}` where the stream is consumed one batch at
  a time via `ExArrow.Stream.next/1`.  The gRPC connection remains open until the
  stream is exhausted or the stream resource is garbage-collected.

  Prefer this over `query/2` for large result sets.

  > #### Concurrency {: .warning}
  > Concurrent calls on the **same** client handle are serialised — the underlying
  > gRPC client requires exclusive access per call.  For parallel queries, create
  > separate client handles with `connect/2`.

  ## Examples

      {:ok, stream} = ExArrow.FlightSQL.Client.stream_query(client, "SELECT * FROM large_table")
      schema = ExArrow.Stream.schema(stream)
      ExArrow.Stream.to_list(stream)  # collect all — or iterate lazily with next/1
  """
  @spec stream_query(t(), String.t()) :: {:ok, Stream.t()} | {:error, Error.t()}
  # sobelow_skip ["SQL.Query"]
  # False positive: SQL is forwarded to a remote Flight SQL server over gRPC and
  # is never executed locally in this process.
  def stream_query(%__MODULE__{} = client, sql) when is_binary(sql) do
    impl().query(client, sql, [])
  end

  # ── DML ───────────────────────────────────────────────────────────────────────

  @doc """
  Execute a DML or DDL statement.

  Returns `{:ok, n}` where `n` is the number of affected rows (non-negative integer),
  or `{:ok, :unknown}` when the server does not report a row count.

  Returns `{:error, %ExArrow.FlightSQL.Error{}}` on failure.

  > #### Concurrency {: .warning}
  > Concurrent calls on the **same** client handle are serialised.
  > Create separate handles with `connect/2` for parallel workloads.

  ## Examples

      {:ok, 3}        = ExArrow.FlightSQL.Client.execute(client, "DELETE FROM t WHERE id < 4")
      {:ok, :unknown} = ExArrow.FlightSQL.Client.execute(client, "CREATE TABLE t (id INT)")
  """
  @spec execute(t(), String.t()) ::
          {:ok, non_neg_integer() | :unknown} | {:error, Error.t()}
  def execute(%__MODULE__{} = client, sql) when is_binary(sql) do
    impl().execute(client, sql, [])
  end

  # ── Prepared statements ───────────────────────────────────────────────────────

  @doc """
  Prepare a SQL query on the server and return a reusable statement handle.

  Sends `CreatePreparedStatement` to the server, which parses and plans the
  query and returns an opaque handle.  The handle can be executed one or more
  times with `ExArrow.FlightSQL.Statement.execute/1` (for SELECT-like queries)
  or `ExArrow.FlightSQL.Statement.execute_update/1` (for DML/DDL).

  Returns `{:ok, %ExArrow.FlightSQL.Statement{}}` or
  `{:error, %ExArrow.FlightSQL.Error{}}`.

  > #### Concurrency {: .warning}
  > Concurrent calls on the **same** client handle are serialised.
  > Create separate handles with `connect/2` for parallel workloads.

  ## Compatibility

  Prepared statement support is optional in the Flight SQL specification.
  Servers that do not implement `CreatePreparedStatement` return
  `{:error, %Error{code: :unimplemented}}`.

  Parameter binding (passing `?` placeholders with Arrow data) is not
  supported in v0.5.0.

  ## Examples

      {:ok, stmt} = ExArrow.FlightSQL.Client.prepare(client, "SELECT * FROM t")
      {:ok, stream} = ExArrow.FlightSQL.Statement.execute(stmt)
      batches = Enum.to_list(stream)

      # Re-execute the same statement without re-preparing
      {:ok, stream2} = ExArrow.FlightSQL.Statement.execute(stmt)
  """
  @spec prepare(t(), String.t()) :: {:ok, Statement.t()} | {:error, Error.t()}
  def prepare(%__MODULE__{} = client, sql) when is_binary(sql) do
    impl().prepare(client, sql, [])
  end

  # ── Metadata ─────────────────────────────────────────────────────────────────

  @doc """
  List tables visible to the connected user.

  Returns a lazy `ExArrow.Stream` of record batches.  The result schema
  follows the Arrow Flight SQL specification:

  | Column | Type | Description |
  |--------|------|-------------|
  | `catalog_name` | `utf8` | Catalog name (nullable) |
  | `db_schema_name` | `utf8` | Schema name (nullable) |
  | `table_name` | `utf8` | Table name |
  | `table_type` | `utf8` | Table type, e.g. `"TABLE"`, `"VIEW"` |

  When `:include_schema` is `true`, an additional `table_schema` column
  containing the IPC-encoded Arrow schema of each table is also included.

  ## Options

  - `:catalog` — filter by exact catalog name (default: no filter)
  - `:db_schema_filter` — SQL `LIKE` pattern for schema names (default: no filter)
  - `:table_name_filter` — SQL `LIKE` pattern for table names (default: no filter)
  - `:table_types` — list of type strings to include, e.g. `["TABLE", "VIEW"]`
    (default: all types)
  - `:include_schema` — `true` to include IPC-encoded column schema in results
    (default: `false`)

  ## Server compatibility

  Server support for filter parameters is optional.  A server that does not
  implement a particular filter may ignore it and return unfiltered results
  or return `{:error, %Error{code: :unimplemented}}`.

  ## Examples

      {:ok, stream} = ExArrow.FlightSQL.Client.get_tables(client)
      batches = Enum.to_list(stream)

      {:ok, stream} = ExArrow.FlightSQL.Client.get_tables(client,
        db_schema_filter: "public",
        table_types: ["TABLE"]
      )
  """
  @spec get_tables(t(), keyword()) :: {:ok, ExArrow.Stream.t()} | {:error, Error.t()}
  def get_tables(%__MODULE__{} = client, opts \\ []) do
    impl().get_tables(client, opts)
  end

  @doc """
  List database schemas visible to the connected user.

  Returns a lazy `ExArrow.Stream` of record batches.  The result schema
  follows the Arrow Flight SQL specification:

  | Column | Type | Description |
  |--------|------|-------------|
  | `catalog_name` | `utf8` | Catalog name (nullable) |
  | `db_schema_name` | `utf8` | Schema name |

  ## Options

  - `:catalog` — filter by exact catalog name (default: no filter)
  - `:db_schema_filter` — SQL `LIKE` pattern for schema names (default: no filter)

  ## Server compatibility

  Server support for filter parameters is optional.  A server that does not
  implement `GetDbSchemas` will return
  `{:error, %Error{code: :unimplemented}}`.

  ## Examples

      {:ok, stream} = ExArrow.FlightSQL.Client.get_db_schemas(client)
      batches = Enum.to_list(stream)

      {:ok, stream} = ExArrow.FlightSQL.Client.get_db_schemas(client, catalog: "main")
  """
  @spec get_db_schemas(t(), keyword()) :: {:ok, ExArrow.Stream.t()} | {:error, Error.t()}
  def get_db_schemas(%__MODULE__{} = client, opts \\ []) do
    impl().get_db_schemas(client, opts)
  end

  @doc """
  Retrieve server capability and SQL dialect information.

  Returns a lazy `ExArrow.Stream` of record batches.  Each row encodes a
  single `SqlInfo` entry as defined by the Arrow Flight SQL specification.
  The result schema has two columns:

  | Column | Type | Description |
  |--------|------|-------------|
  | `info_name` | `uint32` | Numeric `SqlInfo` code |
  | `value` | `dense_union(...)` | Value — type depends on the info code |

  All available `SqlInfo` entries are returned.  The exact set depends on the
  server; not all servers expose all codes.

  ## Server compatibility

  A server that does not implement `GetSqlInfo` will return
  `{:error, %Error{code: :unimplemented}}`.

  ## Examples

      {:ok, stream} = ExArrow.FlightSQL.Client.get_sql_info(client)
      batches = Enum.to_list(stream)
  """
  @spec get_sql_info(t()) :: {:ok, ExArrow.Stream.t()} | {:error, Error.t()}
  def get_sql_info(%__MODULE__{} = client) do
    impl().get_sql_info(client, [])
  end
end
