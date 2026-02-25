# Arrow Flight

Arrow Flight is a gRPC-based protocol for exchanging large Arrow datasets at high throughput.

This guide covers ExArrow's complete Flight API: client, echo server, and operational concerns.

## Overview

| Component | Module |
|-----------|--------|
| Client | `ExArrow.Flight.Client` |
| Server | `ExArrow.Flight.Server` |
| Metadata | `ExArrow.Flight.FlightInfo`, `ExArrow.Flight.ActionType` |

Connections are **plaintext HTTP/2** only. TLS is deferred to a future release.

---

## Server

The built-in echo server stores the last `do_put` stream and serves it on `do_get` with ticket `"echo"`. It supports the full Flight RPC surface.

```elixir
{:ok, server} = ExArrow.Flight.Server.start_link(9999, [])
{:ok, port}   = ExArrow.Flight.Server.port(server)
{:ok, host}   = ExArrow.Flight.Server.host(server)
:ok           = ExArrow.Flight.Server.stop(server)
```

Options for `start_link/2`:
- `:host` — bind address (default `"127.0.0.1"`). Use `"0.0.0.0"` to listen on all interfaces.

The server will not return from `start_link` until its TCP port is accepting connections, so no sleep or health-poll is needed before calling the client.

---

## Client

### Connect

```elixir
{:ok, client} = ExArrow.Flight.Client.connect("localhost", port, [])
# With a connection timeout:
{:ok, client} = ExArrow.Flight.Client.connect("localhost", port, connect_timeout_ms: 5_000)
```

Passing `tls: true` returns `{:error, :tls_not_supported}`.

### do_put / do_get

```elixir
:ok              = ExArrow.Flight.Client.do_put(client, schema, [batch1, batch2])
{:ok, stream}    = ExArrow.Flight.Client.do_get(client, "echo")
```

### list_flights

Enumerate all available flights (empty `criteria` = list all):

```elixir
{:ok, flights} = ExArrow.Flight.Client.list_flights(client, <<>>)
# flights :: [%ExArrow.Flight.FlightInfo{...}]
```

Each `%FlightInfo{}` contains:
- `schema_bytes` — raw IPC-encoded schema.
- `descriptor` — `{:cmd, "echo"}` or `{:path, ["..."]}`.
- `endpoints` — list of `%{ticket: binary(), locations: [String.t()]}`.
- `total_records` / `total_bytes` — row/byte counts, or `-1` if unknown.

### get_flight_info

Retrieve metadata for a specific flight by descriptor:

```elixir
{:ok, info} = ExArrow.Flight.Client.get_flight_info(client, {:cmd, "echo"})
```

### get_schema

Retrieve the Arrow schema for a flight descriptor directly as an `ExArrow.Schema.t()` handle:

```elixir
{:ok, schema} = ExArrow.Flight.Client.get_schema(client, {:cmd, "echo"})
fields = ExArrow.Schema.fields(schema)
```

### list_actions / do_action

```elixir
{:ok, action_types} = ExArrow.Flight.Client.list_actions(client)
# action_types :: [%ExArrow.Flight.ActionType{type: "clear", description: "..."}]

{:ok, ["pong"]} = ExArrow.Flight.Client.do_action(client, "ping", <<>>)
{:ok, []}       = ExArrow.Flight.Client.do_action(client, "clear", <<>>)
```

Supported built-in actions on the echo server:

| Action | Effect | Returns |
|--------|--------|---------|
| `"ping"` | None | `["pong"]` |
| `"clear"` | Clears stored echo data | `[]` |

---

## Timeouts

**Connection timeout**: set `:connect_timeout_ms` in `connect/3` options.

**Per-call timeouts**: not yet exposed through the public API. Implement at the call site using standard `Task` + timeout patterns:

```elixir
task = Task.async(fn -> ExArrow.Flight.Client.do_get(client, "echo") end)
case Task.yield(task, 5_000) || Task.shutdown(task) do
  {:ok, result} -> result
  nil           -> {:error, :timeout}
end
```

---

## Cancellation

Flight streams are lazy: to cancel a `do_get`, stop consuming the returned stream. The underlying gRPC connection will detect the dropped side on the next poll.

There is no explicit cancel NIF. A future release will add `do_get` with per-call deadlines via tonic's `Request::set_timeout`.

---

## Retry policy

ExArrow does not implement retries internally. Apply retry logic at the call site:

```elixir
defp with_retry(fun, attempts \\ 3) do
  case fun.() do
    {:error, _} = err when attempts > 1 ->
      Process.sleep(100)
      with_retry(fun, attempts - 1)
    result ->
      result
  end
end
```

---

## Security

All data (schemas, record batches, action bodies) travels **unencrypted** over HTTP/2. Only use ExArrow Flight for:
- Loopback / localhost communication.
- Trusted private networks.

Do not use for cross-datacenter or internet-facing communication until TLS is added.

---

## Examples

```bash
# Terminal 1 — start echo server
mix run examples/flight_echo/server.exs

# Terminal 2 — run client
mix run examples/flight_echo/client.exs
```

## Integration tests

```bash
mix test test/ex_arrow/flight_integration_test.exs --include flight
```

Flight tests are excluded from `mix test` by default (`:flight` tag).
