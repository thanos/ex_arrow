# Explorer Integration

ExArrow provides first-class interchange between Explorer DataFrames and Arrow
RecordBatches.  The conversion path is always columnar and binary (Arrow IPC
round-trip) — no CSV, no row-by-row materialisation.

## Top-level API

```elixir
# DataFrame → Arrow
df = Explorer.DataFrame.new(x: [1, 2, 3], y: ["a", "b", "c"])
{:ok, batch} = ExArrow.from_dataframe(df)

# Arrow → DataFrame
{:ok, df2} = ExArrow.to_dataframe(batch)
```

## DataFrame-oriented API

```elixir
# DataFrame → Arrow (same as from_dataframe, different naming convention)
{:ok, batch} = ExArrow.DataFrame.to_arrow(df)

# Arrow → DataFrame (accepts RecordBatch or Stream)
{:ok, df} = ExArrow.DataFrame.from_arrow(batch)
```

## What is preserved

- **Column names**: the same names appear on both sides.
- **Row count**: the number of rows is unchanged.
- **Values**: integer, float, boolean, and string values are preserved exactly.
- **Nullability**: null positions in columns survive the round-trip.

## What is not preserved

Explorer does not distinguish between nullable and non-nullable columns in its
dtype system.  When an Explorer DataFrame is serialised to Arrow IPC, all
columns may appear as nullable regardless of whether the source data contained
nulls.  The actual data values (including nils) are always correct.

## Type mapping

| Explorer dtype | Arrow type |
|----------------|------------|
| `:integer`     | Int64      |
| `:float`       | Float64    |
| `:boolean`     | Boolean    |
| `:string`      | Utf8       |

Date, datetime, and duration dtypes are not yet mapped.  These will be added
as the NIF layer gains support for the corresponding Arrow types.

The authoritative mapping table lives in `ExArrow.Schema.Mapper`.

## When to prefer Arrow-native consumption

Converting to a DataFrame materialises all data into Explorer's native memory
format.  If you only need to stream Arrow data to another Arrow consumer (e.g.
a Flight do_put, an IPC file write, or a Parquet write), keep it as an
`ExArrow.Stream` or `ExArrow.RecordBatch` and avoid the conversion overhead.

## Lower-level API

The `ExArrow.Explorer` module provides finer-grained functions:

- `from_stream/1` — convert an `ExArrow.Stream` to a DataFrame
- `from_record_batch/1` — convert a single batch to a DataFrame
- `to_stream/1` — convert a DataFrame to an `ExArrow.Stream`
- `to_record_batches/1` — convert a DataFrame to a list of batches
