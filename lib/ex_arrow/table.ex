defmodule ExArrow.Table do
  @moduledoc """
  An Arrow table: a collection of record batches with a shared schema.

  A `Table` is a logical view over one or more `ExArrow.RecordBatch` instances
  that share the same schema.  Unlike `ExArrow.Stream` (which is lazy and
  backed by a native resource), `Table` is an Elixir-side aggregation of
  already-materialised batches.

  ## Position in the hierarchy

      Schema ── Field (metadata)
                  │
      RecordBatch ── Array (one per column)
                        │
      Table ── RecordBatch (one or more, shared schema)
                        │
      Stream ── RecordBatch (lazy sequence)

  ## When to use Table vs Stream

  - Use `Table` when you have all batches in hand (e.g. after collecting a
    stream) and want a convenient container with `schema/1`, `num_rows/1`,
    and `batches/1`.
  - Use `Stream` for lazy consumption from IPC, Flight, ADBC, or Parquet
    sources.
  """

  alias ExArrow.RecordBatch
  alias ExArrow.Schema

  @type t :: %__MODULE__{
          schema: Schema.t(),
          batches: [RecordBatch.t()]
        }

  defstruct [:schema, :batches]

  @doc """
  Create a Table from a list of record batches.

  All batches must share the same schema.  The schema is taken from the first
  batch.  If the list is empty, returns `{:error, message}`.

  Returns `{:ok, table}` or `{:error, message}`.

  ## Examples

      {:ok, ipc_bin} = ExArrow.Native.ipc_test_fixture_binary()
      {:ok, stream}  = ExArrow.IPC.Reader.from_binary(ipc_bin)
      batches = ExArrow.Stream.to_list(stream)
      {:ok, table} = ExArrow.Table.from_batches(batches)
      ExArrow.Table.num_rows(table)  #=> 2
  """
  @spec from_batches([RecordBatch.t()]) :: {:ok, t()} | {:error, String.t()}
  def from_batches([]), do: {:error, "cannot create Table from empty batch list"}

  def from_batches([first | _] = batches) do
    schema = RecordBatch.schema(first)
    {:ok, %__MODULE__{schema: schema, batches: batches}}
  end

  @doc """
  Returns the schema of this table.
  """
  @spec schema(t()) :: Schema.t()
  def schema(%__MODULE__{schema: s}), do: s

  @doc """
  Returns the list of record batches in this table.
  """
  @spec batches(t()) :: [RecordBatch.t()]
  def batches(%__MODULE__{batches: b}), do: b

  @doc """
  Returns the total number of rows across all batches in this table.

  ## Examples

      {:ok, table} = ExArrow.Table.from_batches(batches)
      ExArrow.Table.num_rows(table)  #=> sum of all batch row counts
  """
  @spec num_rows(t()) :: non_neg_integer()
  def num_rows(%__MODULE__{batches: batches}) do
    Enum.sum(Enum.map(batches, &RecordBatch.num_rows/1))
  end
end
