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

  This is the primary way to construct a `RecordBatch` for parameter binding
  in Flight SQL prepared statements.  Each column is provided as a raw binary
  in native Arrow IPC format, with a corresponding Arrow type string.

  ## Parameters

  - `names`  - list of column name strings
  - `binaries` - list of raw column data binaries (little-endian for integers/floats)
  - `dtypes` - list of Arrow type strings (e.g. `["int64", "utf8", "float64"]`)
  - `length` - number of rows (must be consistent across all columns)

  ## Supported Arrow type strings

  Integer types: `"int8"`, `"int16"`, `"int32"`, `"int64"`,
  `"uint8"`, `"uint16"`, `"uint32"`, `"uint64"`

  Float types: `"float16"`, `"float32"`, `"float64"`

  String types: `"utf8"`, `"large_utf8"`

  Boolean: `"bool"`

  Date/Time: `"date32"`, `"timestamp_seconds"`, `"timestamp_millis"`,
  `"timestamp_micros"`, `"timestamp_nanos"`

  Duration: `"duration_seconds"`, `"duration_millis"`,
  `"duration_micros"`, `"duration_nanos"`

  Binary: `"binary"`, `"large_binary"`

  ## Examples

      # Single int64 column with one row
      batch = ExArrow.RecordBatch.from_columns(
        ["id"],
        [<<42::little-signed-64>>],
        ["int64"],
        1
      )

      # Multiple columns
      batch = ExArrow.RecordBatch.from_columns(
        ["id", "name"],
        [<<1::little-signed-64>>, "Alice"],
        ["int64", "utf8"],
        1
      )
  """
  @spec from_columns([String.t()], [binary()], [String.t()], non_neg_integer()) ::
          t()
  def from_columns(names, binaries, dtypes, length)
      when is_list(names) and is_list(binaries) and is_list(dtypes) and
             is_integer(length) and length >= 0 do
    ref =
      Native.record_batch_from_column_binaries(
        names,
        binaries,
        dtypes,
        length
      )

    %__MODULE__{resource: ref}
  end
end
