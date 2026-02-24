# ExArrow

Apache Arrow support for the BEAM: IPC (stream and file), Arrow Flight (client and server), and ADBC bindings.

## Requirements

- Elixir ~> 1.18
- Rust (for compiling the NIF)
- OTP 26+

## Installation

Add to your dependencies:

```elixir
def deps do
  [{:ex_arrow, "~> 0.1.0"}]
end
```

## Usage

See the [Overview](docs/overview.md) and [Memory model](docs/memory_model.md) in `docs/`. API reference is available via `mix docs`.

## Development

- `mix compile` – builds the Rust NIF from `native/ex_arrow_native`
- `mix test` – runs tests
- `mix docs` – generates ExDoc
- `mix run examples/ipc_roundtrip.exs` – run IPC example (stub)

## License

Apache 2.0
