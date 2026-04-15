defmodule ExArrow.FlightSQL.Client do
  @moduledoc """
  Arrow Flight SQL client for executing SQL queries against a remote server.

  Delegates to the configured implementation module (see `:flight_sql_client_impl` in
  application config). The default is `ExArrow.FlightSQL.ClientImpl`, which is backed by
  NIFs using the `arrow-flight` + tonic Rust crate stack.

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

  Tested against DuckDB Flight SQL server (v0.10+) and DataFusion.

  Multi-endpoint `FlightInfo` responses (distributed queries) are not supported in
  v0.5.0 — `query/2` returns `{:error, %Error{code: :multi_endpoint}}` in that case.

  Transaction operations (`BEGIN`, `COMMIT`, `ROLLBACK`) are deferred to v0.6.0.
  """

  alias ExArrow.FlightSQL.{ClientBehaviour, Error, Result}
  alias ExArrow.Stream

  @opaque t :: %__MODULE__{resource: reference()}
  defstruct [:resource]

  defp impl do
    Application.get_env(:ex_arrow, :flight_sql_client_impl, ExArrow.FlightSQL.ClientImpl)
  end

  # ── Connection ────────────────────────────────────────────────────────────────

  @doc """
  Connect to a Flight SQL server at the given URI.

  `uri` must be a `"host:port"` string, e.g. `"localhost:32010"`. A bare
  `"host"` string is accepted and defaults to port `31337`.

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

  ## Examples

      {:ok, result} = ExArrow.FlightSQL.Client.query(client, "SELECT * FROM t")
      result.num_rows   #=> 100
      result.schema     #=> %ExArrow.Schema{...}
  """
  @spec query(t(), String.t()) :: {:ok, Result.t()} | {:error, Error.t()}
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
  def stream_query(%__MODULE__{} = client, sql) when is_binary(sql) do
    impl().query(client, sql, [])
  end

  # ── DML ───────────────────────────────────────────────────────────────────────

  @doc """
  Execute a DML or DDL statement.

  Returns `{:ok, n}` where `n` is the number of affected rows (non-negative integer),
  or `{:ok, :unknown}` when the server does not report a row count.

  Returns `{:error, %ExArrow.FlightSQL.Error{}}` on failure.

  ## Examples

      {:ok, 3}        = ExArrow.FlightSQL.Client.execute(client, "DELETE FROM t WHERE id < 4")
      {:ok, :unknown} = ExArrow.FlightSQL.Client.execute(client, "CREATE TABLE t (id INT)")
  """
  @spec execute(t(), String.t()) ::
          {:ok, non_neg_integer() | :unknown} | {:error, Error.t()}
  def execute(%__MODULE__{} = client, sql) when is_binary(sql) do
    impl().execute(client, sql, [])
  end

  # ── Behaviour delegation check ────────────────────────────────────────────────

  @doc false
  @spec __behaviour__ :: module()
  def __behaviour__, do: ClientBehaviour
end
