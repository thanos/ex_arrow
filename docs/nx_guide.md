# Nx Tensor Bridge

`ExArrow.Nx` converts between Apache Arrow numeric columns and `Nx.Tensor`
values by sharing the raw byte buffer — no intermediate list materialisation.
It also builds multi-column `ExArrow.RecordBatch` values directly from a map
of tensors in a single NIF call.

Requires `{:nx, "~> 0.9"}` in your `mix.exs`.  When Nx is absent every
function returns `{:error, "Nx is not available…"}`.

## Supported column types

| Arrow type | Nx dtype    |
|------------|-------------|
| Int8       | `{:s, 8}`   |
| Int16      | `{:s, 16}`  |
| Int32      | `{:s, 32}`  |
| Int64      | `{:s, 64}`  |
| UInt8      | `{:u, 8}`   |
| UInt16     | `{:u, 16}`  |
| UInt32     | `{:u, 32}`  |
| UInt64     | `{:u, 64}`  |
| Float32    | `{:f, 32}`  |
| Float64    | `{:f, 64}`  |

Columns of other types (Utf8, Boolean, Timestamp, …) return
`{:error, "unsupported column type…"}` from `column_to_tensor/2` and are
silently skipped by `to_tensors/1`.

## Reading: Arrow columns → tensors

### Single column

```elixir
{:ok, stream} = ExArrow.Parquet.Reader.from_file("/data/trades.parquet")
batch = ExArrow.Stream.next(stream)

{:ok, prices} = ExArrow.Nx.column_to_tensor(batch, "price")
# #Nx.Tensor<f64[1000]>

mean_price = prices |> Nx.mean() |> Nx.to_number()
```

### All numeric columns at once

```elixir
{:ok, tensors} = ExArrow.Nx.to_tensors(batch)
# %{"price" => #Nx.Tensor<f64[1000]>, "qty" => #Nx.Tensor<s64[1000]>}

sorted = tensors["price"] |> Nx.sort()
```

Non-numeric columns are silently omitted from the result map.

## Writing: tensors → Arrow

### Single tensor → single-column batch

```elixir
weights = Nx.tensor([0.1, 0.2, 0.7], type: {:f, 64})
{:ok, batch} = ExArrow.Nx.from_tensor(weights, "weights")
# %ExArrow.RecordBatch{} with one column "weights", 3 rows
```

### Multiple tensors → multi-column batch (v0.4+)

`from_tensors/1` builds a multi-column `RecordBatch` from a
`%{col_name => Nx.Tensor}` map in a **single NIF call**:

```elixir
tensors = %{
  "price"  => Nx.tensor([1.0, 2.0, 3.0], type: {:f, 64}),
  "volume" => Nx.tensor([10, 20, 30],     type: {:s, 64}),
  "symbol" => Nx.tensor([1, 2, 3],        type: {:u, 8})
}

{:ok, batch} = ExArrow.Nx.from_tensors(tensors)
# %ExArrow.RecordBatch{} with three columns, 3 rows each
```

Column order in the resulting batch follows `Map.to_list/1` (sorted by key
name).  All tensors must have the same number of elements; mismatched sizes
return `{:error, "all tensors must have the same size…"}`.

## Null handling

Arrow null positions are treated as zero bytes in the extracted buffer.  If
your column contains nulls and you need to distinguish them, inspect the null
bitmap of the original `ExArrow.RecordBatch` directly.  Explicit null support
in the tensor bridge may be added in a future release.

## Full pipeline example

Read from Parquet, filter and compute in Nx, write back to Arrow:

```elixir
# 1. Read
{:ok, stream} = ExArrow.Parquet.Reader.from_file("/data/features.parquet")
batch = ExArrow.Stream.next(stream)

# 2. Extract features as tensors
{:ok, tensors} = ExArrow.Nx.to_tensors(batch)

# 3. Compute (e.g. normalise)
normalised = Map.new(tensors, fn {k, v} ->
  {k, Nx.divide(v, Nx.reduce_max(v))}
end)

# 4. Write back to a RecordBatch
{:ok, result_batch} = ExArrow.Nx.from_tensors(normalised)

# 5. Persist
{:ok, schema} = ExArrow.Stream.schema(stream)
:ok = ExArrow.Parquet.Writer.to_file("/data/normalised.parquet", schema, [result_batch])
```

## See also

- [Memory model](memory_model.md) — how buffer sharing between Arrow and Nx avoids copying
- [Parquet guide](parquet_guide.md) — reading data into batches for tensor extraction
- [CDI guide](cdi_guide.md) — zero-copy interop with other runtimes
