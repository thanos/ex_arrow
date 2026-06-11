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

  Derived from the batch's schema — no separate NIF call is needed.

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

  Each column is provided as a raw little-endian binary, paired with an
  Arrow dtype string and shared row count.  This is the primary constructor
  for building parameter batches for Flight SQL prepared statement binding.

  ## Parameters

  - `names` — list of column name strings
  - `binaries` — list of raw column data binaries (little-endian for
    integers and floats; one byte per element for `"bool"`)
  - `dtypes` — list of Arrow dtype strings (see below)
  - `length` — number of rows (must be the same for every column)

  ## Supported dtype strings

  | dtype  | Arrow type | element size |
  |--------|------------|--------------|
  | `"s8"`  | Int8       | 1 byte       |
  | `"s16"` | Int16      | 2 bytes      |
  | `"s32"` | Int32      | 4 bytes      |
  | `"s64"` | Int64      | 8 bytes      |
  | `"u8"`  | UInt8      | 1 byte       |
  | `"u16"` | UInt16     | 2 bytes      |
  | `"u32"` | UInt32     | 4 bytes      |
  | `"u64"` | UInt64     | 8 bytes      |
  | `"f32"` | Float32    | 4 bytes      |
  | `"f64"` | Float64    | 8 bytes      |
  | `"bool"` | Boolean   | 1 byte (0 = false, non-zero = true) |

  String, binary, date, timestamp, and duration types are not yet supported
  by this constructor.

  ## Returns

  - `{:ok, %ExArrow.RecordBatch{}}` on success
  - `{:error, message}` if the inputs are inconsistent (mismatched lengths,
    binary size doesn't match `length × element_size`, unknown dtype, etc.)

  ## Examples

      # Single int64 column with one row
      {:ok, batch} = ExArrow.RecordBatch.from_columns(
        ["id"],
        [<<42::little-signed-64>>],
        ["s64"],
        1
      )

      # Multiple columns: int64 and float64
      {:ok, batch} = ExArrow.RecordBatch.from_columns(
        ["id", "score"],
        [<<1::little-signed-64>>, <<3.14::little-float-64>>],
        ["s64", "f64"],
        1
      )
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
