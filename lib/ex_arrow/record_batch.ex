defmodule ExArrow.RecordBatch do
  @moduledoc """
  Arrow record batch handle (opaque reference to native record batch).

  A batch is a collection of column arrays with a shared schema and row count.
  It sits between `ExArrow.Array` (one column) and `ExArrow.Table` or
  `ExArrow.Stream` (multiple batches).  Data stays in native memory; accessors
  return handles or small metadata.

  ## Position in the hierarchy

      Schema ── Field (metadata)
                  │
      RecordBatch ── Array (one per column)
                        │
      Table / Stream ── RecordBatch (one or more)

  ## Supported dtype strings (`from_columns/4`)

  The `from_columns/4` constructor accepts a per-column dtype string.  The
  full set of supported strings, the corresponding Arrow logical type, and
  the wire format expected for each column binary are listed below.

  ### Fixed-width primitives

  Each column binary is exactly `length × element_size` bytes, in
  little-endian byte order for multi-byte types.

  | dtype  | Arrow type         | element size |
  |--------|--------------------|--------------|
  | `"s8"`  | `Int8`             | 1 byte       |
  | `"s16"` | `Int16`            | 2 bytes      |
  | `"s32"` | `Int32`            | 4 bytes      |
  | `"s64"` | `Int64`            | 8 bytes      |
  | `"u8"`  | `UInt8`            | 1 byte       |
  | `"u16"` | `UInt16`           | 2 bytes      |
  | `"u32"` | `UInt32`           | 4 bytes      |
  | `"u64"` | `UInt64`           | 8 bytes      |
  | `"f32"` | `Float32`          | 4 bytes      |
  | `"f64"` | `Float64`          | 8 bytes      |

  ### Boolean

  `"bool"`: exactly `length` bytes, one byte per element (0 = false,
  non-zero = true).

  ### Date and time

  Dates are days or milliseconds since 1970-01-01.  Timestamps are ticks
  since the Unix epoch in UTC.  Durations are tick counts.  All temporal
  types are little-endian.

  | dtype  | Arrow type | Rust scalar | element size |
  |--------|------------|-------------|--------------|
  | `"date32"`              | `Date32`                            | i32 days   | 4 bytes |
  | `"date64"`              | `Date64`                            | i64 millis | 8 bytes |
  | `"timestamp_seconds"`   | `Timestamp(Second, None)`           | i64 sec    | 8 bytes |
  | `"timestamp_millis"`    | `Timestamp(Millisecond, None)`      | i64 ms     | 8 bytes |
  | `"timestamp_micros"`    | `Timestamp(Microsecond, None)`      | i64 µs     | 8 bytes |
  | `"timestamp_nanos"`     | `Timestamp(Nanosecond, None)`       | i64 ns     | 8 bytes |
  | `"duration_seconds"`    | `Duration(Second)`                  | i64 sec    | 8 bytes |
  | `"duration_millis"`     | `Duration(Millisecond)`             | i64 ms     | 8 bytes |
  | `"duration_micros"`     | `Duration(Microsecond)`             | i64 µs     | 8 bytes |
  | `"duration_nanos"`      | `Duration(Nanosecond)`              | i64 ns     | 8 bytes |

  Timestamps are emitted with no timezone (`None`).  The caller is
  responsible for ensuring the i64 ticks are in UTC if the consuming
  server treats the column as zoned.

  ### Variable-length string and binary

  Variable-length columns use a length-prefixed wire format.  The column
  binary is the concatenation of `length` records, each of the form:

      <<elem_len::unsigned-little-32, elem_bytes::binary-size(elem_len)>>

  | dtype           | Arrow type     |
  |-----------------|----------------|
  | `"utf8"`        | `Utf8`         |
  | `"large_utf8"`  | `LargeUtf8`    |
  | `"binary"`      | `Binary`       |
  | `"large_binary"`| `LargeBinary`  |

  `"utf8"` and `"large_utf8"` validate UTF-8 on the entire payload and
  return `{:error, msg}` if any element is invalid.  `"binary"` and
  `"large_binary"` accept arbitrary bytes.

  ## Nullability

  `from_columns/4` produces non-nullable columns (`Field.nullable = false`).
  Pass nulls by binding a separate column or by using a parameter schema
  that accepts non-null values only.
  """
  alias ExArrow.Native
  alias ExArrow.Schema

  @opaque t :: %__MODULE__{resource: reference()}
  defstruct [:resource]

  @doc false
  @spec record_batch?(term()) :: boolean()
  def record_batch?(%__MODULE__{}), do: true
  def record_batch?(_), do: false

  @doc false
  @spec from_ref(reference()) :: t()
  def from_ref(ref), do: %__MODULE__{resource: ref}

  @doc false
  @spec resource_ref(t()) :: reference()
  def resource_ref(%__MODULE__{resource: ref}), do: ref

  @doc """
  Returns the schema of this record batch.
  """
  @spec schema(t()) :: Schema.t()
  def schema(%__MODULE__{resource: ref}) do
    ref |> Native.record_batch_schema() |> Schema.from_ref()
  end

  @doc """
  Returns the number of rows in this batch.
  """
  @spec num_rows(t()) :: non_neg_integer()
  def num_rows(%__MODULE__{resource: ref}) do
    Native.record_batch_num_rows(ref)
  end

  @doc """
  Returns the number of columns in this batch.

  Derived from the batch's schema; no separate NIF call is needed.

  ## Examples

      {:ok, ipc_bin} = ExArrow.Native.ipc_test_fixture_binary()
      {:ok, stream}  = ExArrow.IPC.Reader.from_binary(ipc_bin)
      batch = ExArrow.Stream.next(stream)
      ExArrow.RecordBatch.num_columns(batch)  #=> 2
  """
  @spec num_columns(t()) :: non_neg_integer()
  def num_columns(%__MODULE__{} = batch) do
    batch |> schema() |> Schema.fields() |> length()
  end

  @doc """
  Returns the column names of this batch.

  Derived from the batch's schema.  Equivalent to
  `ExArrow.Schema.field_names(ExArrow.RecordBatch.schema(batch))`.

  ## Examples

      {:ok, ipc_bin} = ExArrow.Native.ipc_test_fixture_binary()
      {:ok, stream}  = ExArrow.IPC.Reader.from_binary(ipc_bin)
      batch = ExArrow.Stream.next(stream)
  """
  @spec column_names(t()) :: [String.t()]
  def column_names(%__MODULE__{} = batch) do
    batch |> schema() |> Schema.field_names()
  end

  @doc """
  Create a `RecordBatch` from column-oriented binary data.

  Each column is provided as a raw binary paired with an Arrow dtype
  string and a shared row count.  This constructor builds parameter batches for Flight SQL prepared statement binding.

  ## Parameters

  - `names`: list of column name strings
  - `binaries`: list of column data binaries (one per column).  See the
    [supported dtypes](#module-supported-dtype-strings-from_columns-4)
    table in the moduledoc for the wire format of each dtype.
  - `dtypes`: list of Arrow dtype strings, one per column
  - `length`: number of rows (must be the same for every column)

  All four lists must have the same length and at least one entry.

  ## Returns

  - `{:ok, %ExArrow.RecordBatch{}}` on success
  - `{:error, message}` if the inputs are inconsistent (mismatched
    list lengths, malformed binary, unknown dtype, invalid UTF-8 in a
    `"utf8"`/`"large_utf8"` column, etc.)

  ## Examples

      # Single int64 column with one row
      {:ok, batch} = ExArrow.RecordBatch.from_columns(
        ["id"],
        [<<42::little-signed-64>>],
        ["s64"],
        1
      )

      # Mixed primitives
      {:ok, batch} = ExArrow.RecordBatch.from_columns(
        ["id", "score"],
        [<<1::little-signed-64>>, <<3.14::little-float-64>>],
        ["s64", "f64"],
        1
      )

      # utf8 column with two rows ("hello", "world") using length-prefixed
      # records: <<len::little-32, bytes::binary-size(len)>>
      utf8 = <<5::little-32, "hello", 5::little-32, "world">>
      {:ok, batch} = ExArrow.RecordBatch.from_columns(["s"], [utf8], ["utf8"], 2)

      # timestamp_micros column
      ts = <<1_700_000_000_000_000::little-signed-64>>
      {:ok, batch} =
        ExArrow.RecordBatch.from_columns(["t"], [ts], ["timestamp_micros"], 1)
  """
  @spec from_columns([String.t()], [binary()], [String.t()], non_neg_integer()) ::
          {:ok, t()} | {:error, String.t()}
  def from_columns(names, binaries, dtypes, length)
      when is_list(names) and is_list(binaries) and is_list(dtypes) and
             is_integer(length) and length >= 0 do
    case Native.record_batch_from_column_binaries(names, binaries, dtypes, length) do
      {:ok, ref} -> {:ok, %__MODULE__{resource: ref}}
      {:error, _} = err -> err
    end
  end
end
