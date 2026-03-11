# ExArrow 0.3.0 — Release Notes

**Released:** 2026-03-10

ExArrow 0.3.0 ships the four features promised on the v0.3 roadmap: Arrow
compute kernels, Parquet read/write, an Explorer bridge, and an Nx bridge.
All changes are backward compatible; upgrading from 0.2.0 requires only a
version bump in `mix.exs`.

---

## What is new

### Arrow compute kernels

`ExArrow.Compute` exposes three operations that run entirely inside native
Arrow memory via the `arrow-select` and `arrow-ord` Rust crates.  No column
data is ever copied into BEAM terms; every function returns a new
`ExArrow.RecordBatch` handle.

```elixir
# Select only the columns you need
{:ok, slim} = ExArrow.Compute.project(batch, ["user_id", "score", "region"])

# Sort by score descending
{:ok, sorted} = ExArrow.Compute.sort(slim, "score", ascending: false)

# Filter to rows where is_active == true
# (build a boolean batch from a boolean column or a second query)
{:ok, active} = ExArrow.Compute.filter(sorted, predicate_batch)
```

Operations can be chained without any intermediate serialisation:

```elixir
{:ok, result} =
  ExArrow.Compute.project(batch, ["id", "score"])
  |> then(fn {:ok, b} -> ExArrow.Compute.sort(b, "score", ascending: false) end)
  |> then(fn {:ok, b} -> ExArrow.Compute.filter(b, top_n_predicate) end)
```

---

### Parquet support

`ExArrow.Parquet.Reader` and `ExArrow.Parquet.Writer` wrap the `parquet` Rust
crate and expose the same `ExArrow.Stream` interface as IPC and ADBC.

**Read from a file or binary:**

```elixir
{:ok, stream} = ExArrow.Parquet.Reader.from_file("/data/events.parquet")
{:ok, schema} = ExArrow.Stream.schema(stream)
batches       = ExArrow.Stream.to_list(stream)

# Or from a binary downloaded from S3 / object storage
{:ok, stream} = ExArrow.Parquet.Reader.from_binary(parquet_bytes)
```

**Write to a file or binary:**

```elixir
:ok = ExArrow.Parquet.Writer.to_file("/out/result.parquet", schema, batches)

# Or capture as an in-memory binary (e.g. to upload to S3)
{:ok, parquet_bytes} = ExArrow.Parquet.Writer.to_binary(schema, batches)
```

**Round-trip with compute kernels:**

```elixir
{:ok, stream}   = ExArrow.Parquet.Reader.from_file("/data/raw.parquet")
batches         = ExArrow.Stream.to_list(stream)
{:ok, schema}   = ExArrow.Parquet.Reader.from_file("/data/raw.parquet")
                  |> then(fn {:ok, s} -> ExArrow.Stream.schema(s) end)

# Project and sort before writing
{:ok, processed} = ExArrow.Compute.project(hd(batches), ["id", "score"])
{:ok, sorted}    = ExArrow.Compute.sort(processed, "score")
:ok = ExArrow.Parquet.Writer.to_file("/data/sorted.parquet", schema, [sorted])
```

---

### Explorer bridge module

`ExArrow.Explorer` converts between `ExArrow.Stream` / `ExArrow.RecordBatch`
and `Explorer.DataFrame` with a single function call.  The bridge uses Arrow
IPC internally; no CSV or row-by-row conversion is performed.

Requires `{:explorer, "~> 0.8"}` in your `mix.exs`.

**From a stream or batch to a DataFrame:**

```elixir
{:ok, stream} = ExArrow.IPC.Reader.from_file("/data/events.arrow")
{:ok, df}     = ExArrow.Explorer.from_stream(stream)
Explorer.DataFrame.filter(df, score > 0.9)

# Single batch
batch     = ExArrow.Stream.next(stream)
{:ok, df} = ExArrow.Explorer.from_record_batch(batch)
```

**From a DataFrame back to an ExArrow stream:**

```elixir
df = Explorer.DataFrame.new(x: [1, 2, 3], y: ["a", "b", "c"])

{:ok, stream}  = ExArrow.Explorer.to_stream(df)
{:ok, batches} = ExArrow.Explorer.to_record_batches(df)
```

**Full pipeline — query, compute, analyse:**

```elixir
{:ok, stream}  = ExArrow.ADBC.Statement.execute(stmt)
{:ok, slim}    = ExArrow.Compute.project(ExArrow.Stream.next(stream),
                   ["user_id", "score"])
{:ok, df}      = ExArrow.Explorer.from_record_batch(slim)
Explorer.DataFrame.describe(df)
```

When Explorer is not present, every function returns
`{:error, "Explorer is not available …"}`.

---

### Nx bridge module

`ExArrow.Nx` converts Arrow numeric columns to `Nx.Tensor` values (and back)
by copying the raw byte buffer once — no list materialisation.

Requires `{:nx, "~> 0.7"}` in your `mix.exs`.

**Column to tensor:**

```elixir
{:ok, stream}  = ExArrow.Parquet.Reader.from_file("/data/trades.parquet")
batch          = ExArrow.Stream.next(stream)
{:ok, prices}  = ExArrow.Nx.column_to_tensor(batch, "price")
mean_price     = prices |> Nx.mean() |> Nx.to_number()
```

**All numeric columns at once:**

```elixir
{:ok, tensors} = ExArrow.Nx.to_tensors(batch)
# %{"price" => #Nx.Tensor<f64[1000]>, "qty" => #Nx.Tensor<s64[1000]>}
sorted_prices = tensors["price"] |> Nx.sort()
```

**Tensor back to a record batch:**

```elixir
weights = Nx.tensor([0.1, 0.2, 0.7], type: {:f, 64})
{:ok, batch} = ExArrow.Nx.from_tensor(weights, "weights")
```

Supported Arrow → Nx type mappings:

| Arrow type | Nx dtype   |
|------------|------------|
| Int8       | `{:s, 8}`  |
| Int16      | `{:s, 16}` |
| Int32      | `{:s, 32}` |
| Int64      | `{:s, 64}` |
| UInt8      | `{:u, 8}`  |
| UInt16     | `{:u, 16}` |
| UInt32     | `{:u, 32}` |
| UInt64     | `{:u, 64}` |
| Float32    | `{:f, 32}` |
| Float64    | `{:f, 64}` |

Non-numeric columns return `{:error, "unsupported column type…"}` from
`column_to_tensor/2` and are silently skipped by `to_tensors/1`.  When Nx
is not present, every function returns `{:error, "Nx is not available …"}`.

---

## New public API

| Module | Function | Description |
|--------|----------|-------------|
| `ExArrow.Compute` | `filter/2` | Filter rows using a boolean-typed predicate batch |
| `ExArrow.Compute` | `project/2` | Select and reorder columns by name |
| `ExArrow.Compute` | `sort/3` | Sort batch by a named column |
| `ExArrow.Parquet.Reader` | `from_file/1` | Open a Parquet file as an `ExArrow.Stream` |
| `ExArrow.Parquet.Reader` | `from_binary/1` | Open a Parquet binary as an `ExArrow.Stream` |
| `ExArrow.Parquet.Writer` | `to_file/3` | Write schema + batches to a Parquet file |
| `ExArrow.Parquet.Writer` | `to_binary/2` | Serialize schema + batches to a Parquet binary |
| `ExArrow.Explorer` | `from_stream/1` | Convert `ExArrow.Stream` → `Explorer.DataFrame` |
| `ExArrow.Explorer` | `from_record_batch/1` | Convert `ExArrow.RecordBatch` → `Explorer.DataFrame` |
| `ExArrow.Explorer` | `to_stream/1` | Convert `Explorer.DataFrame` → `ExArrow.Stream` |
| `ExArrow.Explorer` | `to_record_batches/1` | Convert `Explorer.DataFrame` → `[ExArrow.RecordBatch]` |
| `ExArrow.Nx` | `column_to_tensor/2` | Arrow column → `Nx.Tensor` |
| `ExArrow.Nx` | `to_tensors/1` | All numeric columns → `%{name => Nx.Tensor}` |
| `ExArrow.Nx` | `from_tensor/2` | `Nx.Tensor` → single-column `ExArrow.RecordBatch` |

---

## Optional dependencies

No new required dependencies.  The two new bridge modules each unlock when
their optional dep is present:

```elixir
# Explorer bridge (ExArrow.Explorer)
{:explorer, "~> 0.8"}

# Nx bridge (ExArrow.Nx)
{:nx, "~> 0.7"}
```

The existing optional deps (ADBC, NimblePool) are unchanged.

---

## Upgrade guide

No breaking changes.  Update your version pin:

```elixir
# Before
{:ex_arrow, "~> 0.2.0"}

# After
{:ex_arrow, "~> 0.3.0"}
```

Then run `mix deps.get` and `mix compile`.

---

## Full changelog

See [CHANGELOG.md](CHANGELOG.md) for the complete list of changes.
