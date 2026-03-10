# Arrow compute guide

`ExArrow.Compute` provides three kernels that operate directly on native Arrow
buffers:

| Function | Description |
|----------|-------------|
| `filter/2` | Keep rows where a boolean column is `true` |
| `project/2` | Select a subset of columns by name |
| `sort/3` | Sort all rows by a named column |

All operations happen entirely in native Rust memory.  The BEAM scheduler is
never stalled on column-level data — only the small `ExArrow.RecordBatch` handle
struct is created on the BEAM heap.

---

## filter/2

Keep only rows where the **first column** of a predicate batch is `true`.

```elixir
# Assume `batch` has columns [id, score, is_active]
# Extract the boolean column into its own batch
{:ok, mask}     = ExArrow.Compute.project(batch, ["is_active"])
{:ok, filtered} = ExArrow.Compute.filter(batch, mask)
# filtered has the same schema as batch but only the rows where is_active = true
```

### Building a predicate from a query

The most natural source of a boolean column is a query result:

```elixir
{:ok, db}    = ExArrow.ADBC.Database.open(driver_name: "adbc_driver_sqlite",
                 uri: "file:app.db")
{:ok, conn}  = ExArrow.ADBC.Connection.open(db)
{:ok, stmt}  = ExArrow.ADBC.Statement.new(conn, """
  SELECT id, score, score > 0.9 AS is_top FROM users
""")
{:ok, stream}  = ExArrow.ADBC.Statement.execute(stmt)
batch          = ExArrow.Stream.next(stream)

{:ok, mask}     = ExArrow.Compute.project(batch, ["is_top"])
{:ok, top_rows} = ExArrow.Compute.filter(batch, mask)
```

### Error cases

```elixir
# Predicate batch must have at least one column
{:error, "predicate batch must have at least one column"} =
  ExArrow.Compute.filter(batch, empty_batch)

# First column must be boolean
{:ok, int_col}  = ExArrow.Compute.project(batch, ["id"])
{:error, msg}   = ExArrow.Compute.filter(batch, int_col)
# msg contains "boolean"
```

---

## project/2

Select a subset of columns and optionally reorder them.

```elixir
# Select two columns
{:ok, slim} = ExArrow.Compute.project(batch, ["user_id", "score"])

# Reorder: result schema is [score, user_id]
{:ok, swapped} = ExArrow.Compute.project(batch, ["score", "user_id"])

# Unknown column
{:error, "column 'missing' not found"} =
  ExArrow.Compute.project(batch, ["missing"])
```

`project/2` is useful before writing to Parquet or sending via Flight when
you only want to transmit the columns the consumer needs:

```elixir
{:ok, slim}        = ExArrow.Compute.project(batch, ["id", "score"])
{:ok, schema}      = ExArrow.Stream.schema(stream)    # keep original schema
{:ok, slim_schema} = {:ok, ExArrow.RecordBatch.schema(slim)}
:ok = ExArrow.Parquet.Writer.to_file("/out/scores.parquet", slim_schema, [slim])
```

---

## sort/3

Sort the entire batch by a named column.  All columns travel with their rows.

```elixir
# Ascending (default)
{:ok, asc}  = ExArrow.Compute.sort(batch, "score")

# Descending
{:ok, desc} = ExArrow.Compute.sort(batch, "score", ascending: false)

# String column — alphabetical order
{:ok, sorted} = ExArrow.Compute.sort(batch, "name")
```

Nulls are always placed **first** regardless of sort direction.  This matches
the SQL `NULLS FIRST` behaviour.

---

## Chaining operations

Compute functions return a new `ExArrow.RecordBatch` handle, so they can be
chained:

```elixir
{:ok, slim}     = ExArrow.Compute.project(batch, ["id", "score", "is_active"])
{:ok, mask}     = ExArrow.Compute.project(slim, ["is_active"])
{:ok, filtered} = ExArrow.Compute.filter(slim, mask)
{:ok, sorted}   = ExArrow.Compute.sort(filtered, "score", ascending: false)

# Write the result to Parquet
schema = ExArrow.RecordBatch.schema(sorted)
:ok = ExArrow.Parquet.Writer.to_file("/out/top.parquet", schema, [sorted])
```

---

## Performance notes

- All Arrow buffers remain in native Rust memory throughout.  Calling
  `filter`, `project`, or `sort` does not copy column data into the BEAM heap.
- The result is a new `ExArrow.RecordBatch` NIF resource that shares no memory
  with the original; the original batch is retained until all Elixir references
  to it are garbage-collected.
- For very large batches (hundreds of millions of rows), consider using
  multiple batches / streaming so that each operation works on a manageable
  chunk.
