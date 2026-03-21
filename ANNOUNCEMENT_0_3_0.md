# ExArrow 0.3.0 — Parquet, Compute kernels, Explorer bridge, Nx bridge

Hi everyone 👋

ExArrow 0.3.0 is out. This release ships the four features that were on the
v0.3 roadmap: Arrow compute kernels that stay entirely in native memory,
Parquet read/write, a one-call Explorer bridge, and an Nx bridge that shares
raw byte buffers with tensors.

---

## What is ExArrow?

ExArrow gives Elixir and Erlang applications first-class Apache Arrow support:
IPC streaming, Arrow Flight (gRPC), and ADBC database connectivity.  Column
data lives in Rust buffers; the BEAM holds lightweight opaque handles.
Precompiled NIFs for Linux x86-64/aarch64, macOS arm64/x86-64, and Windows —
no Rust required.

```elixir
{:ex_arrow, "~> 0.3.0"}
```

---

## What is new in 0.3.0

### Parquet read/write

Read and write Parquet files with the same `ExArrow.Stream` interface you
already use for IPC and ADBC:

```elixir
# Read
{:ok, stream} = ExArrow.Parquet.Reader.from_file("/data/events.parquet")
batches       = ExArrow.Stream.to_list(stream)

# Write
{:ok, binary} = ExArrow.Parquet.Writer.to_binary(schema, batches)
:ok           = ExArrow.Parquet.Writer.to_file("/out/result.parquet", schema, batches)
```

In-memory binaries are also accepted by the reader, so files downloaded from
S3 or received over HTTP can be parsed without touching the filesystem.

---

### Arrow compute kernels

`ExArrow.Compute` wraps the `arrow-select` and `arrow-ord` Rust crates.
Everything runs in native Arrow memory — no data is ever copied into BEAM
terms, and the results are new `ExArrow.RecordBatch` handles you can chain
directly into further operations, IPC writes, or Flight uploads:

```elixir
{:ok, slim}   = ExArrow.Compute.project(batch, ["id", "score", "region"])
{:ok, sorted} = ExArrow.Compute.sort(slim, "score", ascending: false)
{:ok, active} = ExArrow.Compute.filter(sorted, predicate_batch)
```

---

### Explorer bridge

`ExArrow.Explorer` converts between `ExArrow.Stream` / `ExArrow.RecordBatch`
and `Explorer.DataFrame` in a single call.  The bridge uses Arrow IPC
internally — no CSV, no row-by-row conversion.

Requires `{:explorer, "~> 0.8"}` in your `mix.exs` (optional).

```elixir
# Stream from a Flight server → DataFrame
{:ok, stream} = ExArrow.Flight.Client.do_get(client, "sales_2024")
{:ok, df}     = ExArrow.Explorer.from_stream(stream)
Explorer.DataFrame.filter(df, score > 0.9)

# DataFrame → Parquet file
{:ok, stream} = ExArrow.Explorer.to_stream(df)
:ok = ExArrow.Parquet.Writer.to_file("/out/result.parquet",
        ExArrow.Stream.schema(stream) |> elem(1),
        ExArrow.Stream.to_list(stream))
```

---

### Nx bridge

`ExArrow.Nx` converts Arrow numeric columns to `Nx.Tensor` values and back
by copying the raw byte buffer once.  No intermediate list, no extra heap
allocation.

Requires `{:nx, "~> 0.7"}` in your `mix.exs` (optional).

```elixir
# Column to tensor
{:ok, tensor} = ExArrow.Nx.column_to_tensor(batch, "price")
mean_price    = tensor |> Nx.mean() |> Nx.to_number()

# All numeric columns at once
{:ok, tensors} = ExArrow.Nx.to_tensors(batch)

# Tensor back to a record batch
weights      = Nx.tensor([0.1, 0.2, 0.7], type: {:f, 64})
{:ok, batch} = ExArrow.Nx.from_tensor(weights, "weights")
```

---

## No breaking changes

Update the version pin, run `mix deps.get`, and you are done:

```elixir
{:ex_arrow, "~> 0.3.0"}
```

---

## Links

- **Hex:** https://hex.pm/packages/ex_arrow
- **Docs:** https://hexdocs.pm/ex_arrow
- **Changelog:** https://github.com/thanos/ex_arrow/blob/main/CHANGELOG.md
- **Release notes:** https://github.com/thanos/ex_arrow/blob/main/RELEASE_NOTES_0_3_0.md
- **Source:** https://github.com/thanos/ex_arrow

Feedback, issues, and PRs very welcome. Thanks!
