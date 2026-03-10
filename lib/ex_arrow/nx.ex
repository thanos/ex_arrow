defmodule ExArrow.Nx do
  @moduledoc """
  Bridge between ExArrow and Nx tensors.

  Converts numeric Arrow columns to `Nx.Tensor` values (and back) by sharing
  the raw byte buffer.  The buffer is copied once from native Arrow memory into
  an Elixir binary, then handed directly to `Nx.from_binary/2` — no row-by-row
  conversion or intermediate list materialisation.

  Supported Arrow column types: `int8`, `int16`, `int32`, `int64`, `uint8`,
  `uint16`, `uint32`, `uint64`, `float32`, `float64`.

  Requires `{:nx, "~> 0.7"}` in your `mix.exs` dependencies.

  ## Examples

      # Arrow column → Nx tensor
      {:ok, tensor} = ExArrow.Nx.column_to_tensor(batch, "price")
      tensor |> Nx.mean() |> Nx.to_number()

      # All numeric columns → map of tensors
      {:ok, tensors} = ExArrow.Nx.to_tensors(batch)
      tensors["score"] |> Nx.sort()

      # Nx tensor → single-column RecordBatch
      tensor = Nx.tensor([1.0, 2.0, 3.0], type: {:f, 64})
      {:ok, batch} = ExArrow.Nx.from_tensor(tensor, "values")
  """

  alias ExArrow.Native
  alias ExArrow.RecordBatch
  alias ExArrow.Schema

  @nx_available Code.ensure_loaded?(Nx)

  if @nx_available do
    @doc """
    Convert a named numeric column from a `RecordBatch` to an `Nx.Tensor`.

    The column buffer is copied once from native Arrow memory to an Elixir
    binary, then passed to `Nx.from_binary/2`.  No list materialisation occurs.

    Returns `{:ok, tensor}` or `{:error, message}`.
    """
    @spec column_to_tensor(RecordBatch.t(), String.t()) ::
            {:ok, Nx.Tensor.t()} | {:error, String.t()}
    def column_to_tensor(%RecordBatch{resource: ref}, col_name) when is_binary(col_name) do
      case Native.record_batch_column_buffer(ref, col_name) do
        {:ok, {binary, dtype_str, _length}} ->
          case parse_nx_dtype(dtype_str) do
            {:ok, nx_dtype} -> {:ok, Nx.from_binary(binary, nx_dtype)}
            {:error, msg} -> {:error, msg}
          end

        {:error, msg} ->
          {:error, msg}
      end
    end

    @doc """
    Convert all numeric columns from a `RecordBatch` to a map of `Nx.Tensor` values.

    Non-numeric columns (strings, booleans, timestamps, etc.) are silently
    skipped.  Returns `{:ok, %{column_name => tensor}}` or `{:error, message}`.
    """
    @spec to_tensors(RecordBatch.t()) ::
            {:ok, %{String.t() => Nx.Tensor.t()}} | {:error, String.t()}
    def to_tensors(%RecordBatch{} = batch) do
      schema = RecordBatch.schema(batch)
      fields = Schema.fields(schema)

      result =
        Enum.reduce_while(fields, {:ok, %{}}, fn field, {:ok, acc} ->
          case column_to_tensor(batch, field.name) do
            {:ok, tensor} ->
              {:cont, {:ok, Map.put(acc, field.name, tensor)}}

            {:error, "unsupported column type" <> _} ->
              # Skip non-numeric columns silently
              {:cont, {:ok, acc}}

            {:error, msg} ->
              {:halt, {:error, msg}}
          end
        end)

      result
    end

    @doc """
    Convert an `Nx.Tensor` (rank-1 or rank-2) to an `ExArrow.RecordBatch` with
    a single column named `column_name`.

    The tensor's raw bytes are extracted via `Nx.to_binary/1` and passed to the
    Arrow NIF.  For rank-2 tensors, all elements are written into a single flat
    column (rows × columns flattened).

    Returns `{:ok, batch}` or `{:error, message}`.
    """
    @spec from_tensor(Nx.Tensor.t(), String.t()) ::
            {:ok, RecordBatch.t()} | {:error, String.t()}
    def from_tensor(tensor, col_name) when is_binary(col_name) do
      nx_dtype = Nx.type(tensor)
      binary = Nx.to_binary(tensor)
      length = Nx.size(tensor)

      case nx_dtype_to_arrow(nx_dtype) do
        {:ok, dtype_str} ->
          case Native.record_batch_from_column_binary(col_name, binary, dtype_str, length) do
            {:ok, ref} -> {:ok, RecordBatch.from_ref(ref)}
            {:error, msg} -> {:error, msg}
          end

        {:error, msg} ->
          {:error, msg}
      end
    end

    # ── dtype helpers ────────────────────────────────────────────────────────

    defp parse_nx_dtype("s8"), do: {:ok, {:s, 8}}
    defp parse_nx_dtype("s16"), do: {:ok, {:s, 16}}
    defp parse_nx_dtype("s32"), do: {:ok, {:s, 32}}
    defp parse_nx_dtype("s64"), do: {:ok, {:s, 64}}
    defp parse_nx_dtype("u8"), do: {:ok, {:u, 8}}
    defp parse_nx_dtype("u16"), do: {:ok, {:u, 16}}
    defp parse_nx_dtype("u32"), do: {:ok, {:u, 32}}
    defp parse_nx_dtype("u64"), do: {:ok, {:u, 64}}
    defp parse_nx_dtype("f32"), do: {:ok, {:f, 32}}
    defp parse_nx_dtype("f64"), do: {:ok, {:f, 64}}
    defp parse_nx_dtype(other), do: {:error, "unknown Arrow dtype string: #{other}"}

    defp nx_dtype_to_arrow({:s, 8}), do: {:ok, "s8"}
    defp nx_dtype_to_arrow({:s, 16}), do: {:ok, "s16"}
    defp nx_dtype_to_arrow({:s, 32}), do: {:ok, "s32"}
    defp nx_dtype_to_arrow({:s, 64}), do: {:ok, "s64"}
    defp nx_dtype_to_arrow({:u, 8}), do: {:ok, "u8"}
    defp nx_dtype_to_arrow({:u, 16}), do: {:ok, "u16"}
    defp nx_dtype_to_arrow({:u, 32}), do: {:ok, "u32"}
    defp nx_dtype_to_arrow({:u, 64}), do: {:ok, "u64"}
    defp nx_dtype_to_arrow({:f, 32}), do: {:ok, "f32"}
    defp nx_dtype_to_arrow({:f, 64}), do: {:ok, "f64"}

    defp nx_dtype_to_arrow(dt),
      do: {:error, "unsupported Nx dtype for Arrow conversion: #{inspect(dt)}"}
  else
    @doc false
    def column_to_tensor(_batch, _col_name), do: {:error, nx_missing_message()}

    @doc false
    def to_tensors(_batch), do: {:error, nx_missing_message()}

    @doc false
    def from_tensor(_tensor, _col_name), do: {:error, nx_missing_message()}

    defp nx_missing_message do
      "Nx is not available. Add {:nx, \"~> 0.7\"} to your mix.exs dependencies."
    end
  end
end
