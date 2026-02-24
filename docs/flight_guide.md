# Arrow Flight (MVP)

Minimal Arrow Flight client and echo server: **do_put** and **do_get** by ticket.

## Overview

- **Server**: `ExArrow.Flight.Server` – echo server that stores the last `do_put` stream and serves it on `do_get` with ticket `"echo"`.
- **Client**: `ExArrow.Flight.Client` – connect, `do_put` (schema + batches), `do_get` (ticket to stream).

TLS and full Flight API (list_flights, get_flight_info, actions) are left for later milestones.

## Server

```elixir
{:ok, server} = ExArrow.Flight.Server.start_link(9999, [])
{:ok, port} = ExArrow.Flight.Server.port(server)
:ok = ExArrow.Flight.Server.stop(server)
```

## Client

```elixir
{:ok, client} = ExArrow.Flight.Client.connect("localhost", port, [])
:ok = ExArrow.Flight.Client.do_put(client, schema, [batch])
{:ok, get_stream} = ExArrow.Flight.Client.do_get(client, "echo")
```

## Examples

Run server: `mix run examples/flight_echo/server.exs`  
Run client (other terminal): `mix run examples/flight_echo/client.exs`

## Integration test

```bash
mix test test/ex_arrow/flight_integration_test.exs --include flight
```

By default `mix test` excludes `:flight`.
