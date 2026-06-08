# Nx Integration

ExArrow provides first-class interchange between Nx tensors and Arrow
RecordBatches.  The conversion path uses raw byte buffer extraction — no
intermediate list materialisation occurs.

## Top-level API

```elixir
# Tensor → Arrow
tensor = Nx.tensor([1, 2, 3], type: {:s, 64})
{:ok, batch} = ExArrow.from_nx(tensor)

# Arrow → Tensor
{:ok, recovered} = ExArrow.to_nx(batch)
Nx.to_list(recovered)  #=> [1, 2, 3]
```

## Supported dtypes

| Nx dtype     | Arrow type |
|--------------|------------|
| `{:u, 8}`    | UInt8      |
| `{:s, 64}`   | Int64      |
| `{:f, 32}`   | Float32    |
| `{:f, 64}`   | Float64    |

All other integer and float dtypes (`{:s, 8}`, `{:s, 16}`, `{:s, 32}`,
`{:u, 16}`, `{:u, 32}`, `{:u, 64}`) are also supported.  Bfloat16, complex,
and other Nx dtypes are not supported.

## Boolean tensors

Nx does not have a dedicated boolean dtype.  Booleans are represented as
`{:u, 8}` tensors with values 0 and 1.  Use the `as: :boolean` option to
create an Arrow Boolean column:

```elixir
flags = Nx.tensor([1, 0, 1, 0], type: {:u, 8})
{:ok, batch} = ExArrow.from_nx(flags, as: :boolean)
```

When reading back, Arrow Boolean columns are returned as `{:u, 8}` tensors:

```elixir
{:ok, tensor} = ExArrow.to_nx(batch)
Nx.type(tensor)  #=> {:u, 8}
Nx.to_list(tensor)  #=> [1, 0, 1, 0]
```

## Rank-1 tensors (single column)

A rank-1 tensor produces a single-column RecordBatch.  The column is named
`"value"` by default; use the `:name` option to customise:

```elixir
{:ok, batch} = ExArrow.from_nx(tensor, name: "prices")
```

## Rank-2 tensors (multi-column)

A rank-2 tensor with shape `{rows, cols}` produces a multi-column RecordBatch
with `cols` columns named `"c0"`, `"c1"`, ..., `"c{cols-1}"` and `rows` rows:

```elixir
tensor = Nx.tensor([[1, 2, 3], [4, 5, 6]], type: {:s, 64})
{:ok, batch} = ExArrow.from_nx(tensor)
# batch has columns: c0=[1,4], c1=[2,5], c2=[3,6]
```

The reverse path (multi-column batch → rank-2 tensor) works when all numeric
columns have the same dtype:

```elixir
{:ok, recovered} = ExArrow.to_nx(batch)
Nx.shape(recovered)  #=> {2, 3}
```

If columns have mixed dtypes, `to_nx/1` returns an error.  Use
`ExArrow.Nx.to_tensors/1` for per-column access.

## Rank > 2

Tensors of rank > 2 are not supported.  Flatten or reshape before conversion.

## Null handling

Arrow null positions are treated as zero bytes in the extracted buffer.  If
your column contains nulls and you need to distinguish them, inspect the
original batch.  Null-aware Nx conversion may be added in a future release.

## Lower-level API

The `ExArrow.Nx` module provides column-level operations:

- `column_to_tensor/2` — extract one named column as a tensor
- `to_tensors/1` — extract all numeric/boolean columns as a map
- `from_tensor/2` — single tensor → single-column batch
- `from_tensors/1` — map of tensors → multi-column batch
