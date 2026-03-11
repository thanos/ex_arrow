defmodule ExArrow.Nx do
  @moduledoc """
  Bridge between ExArrow and Nx tensors.

  Converts numeric Arrow columns to `Nx.Tensor` values (and back) by copying
  the raw byte buffer once from native Arrow memory into an Elixir binary, then
  handing it directly to `Nx.from_binary/2`.  No intermediate list
  materialisation occurs.

  Requires `{:nx, "~> 0.7"}` in your `mix.exs` dependencies.  When Nx is
  absent every function returns `{:error, "Nx is not available..."}`.

  ### Supported column types

  | Arrow type        | Nx dtype     |
  |-------------------|--------------|
  | Int8              | `{:s, 8}`    |
  | Int16             | `{:s, 16}`   |
  | Int32             | `{:s, 32}`   |
  | Int64             | `{:s, 64}`   |
  | UInt8             | `{:u, 8}`    |
  | UInt16            | `{:u, 16}`   |
  | UInt32            | `{:u, 32}`   |
  | UInt64            | `{:u, 64}`   |
  | Float32           | `{:f, 32}`   |
  | Float64           | `{:f, 64}`   |

  Columns of other types (Utf8, Boolean, Timestamp, …) are not supported for
  direct buffer extraction and return `{:error, "unsupported column type…"}`.
  `to_tensors/1` silently skips non-numeric columns.

  ### Null handling

  Arrow null positions are treated as zero bytes in the extracted buffer.  If
  your column contains nulls and you need to distinguish them, inspect the
  original batch (null support may be added in a future release).

  ## Quick example

      # Read a batch, extract the "price" column as a float64 tensor
      {:ok, stream}  = ExArrow.Parquet.Reader.from_file("/data/trades.parquet")
      batch          = ExArrow.Stream.next(stream)
      {:ok, tensor}  = ExArrow.Nx.column_to_tensor(batch, "price")
      mean_price     = tensor |> Nx.mean() |> Nx.to_number()
  """

  alias ExArrow.Native
  alias ExArrow.RecordBatch
  alias ExArrow.Schema

  @nx_available Code.ensure_loaded?(Nx)

  if @nx_available do
    @doc """
    Convert a named numeric column from `batch` to an `Nx.Tensor`.

    The column's raw byte buffer is copied once from native Arrow memory into an
    Elixir binary, then passed to `Nx.from_binary/2`.  No list materialisation
    occurs.

    Returns `{:ok, tensor}` or `{:error, message}`.

    ## Examples

        # Extract an int64 column
        {:ok, ids} = ExArrow.Nx.column_to_tensor(batch, "id")
        Nx.type(ids)   #=> {:s, 64}
        Nx.shape(ids)  #=> {1000}

        # Extract a float64 column and compute the mean
        {:ok, prices} = ExArrow.Nx.column_to_tensor(batch, "price")
        Nx.mean(prices) |> Nx.to_number()

        # Non-numeric column returns an error
        {:error, msg} = ExArrow.Nx.column_to_tensor(batch, "name")
        msg #=> "unsupported column type for Nx: Utf8"

        # Unknown column returns an error
        {:error, msg} = ExArrow.Nx.column_to_tensor(batch, "no_such_col")
    """
    @spec column_to_tensor(RecordBatch.t(), String.t()) ::
            {:ok, Nx.Tensor.t()} | {:error, String.t()}
    def column_to_tensor(batch, col_name) when is_binary(col_name) do
      ref = RecordBatch.resource_ref(batch)

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
    Convert all numeric columns from `batch` to a map of `Nx.Tensor` values.

    Non-numeric columns (Utf8, Boolean, Timestamp, etc.) are silently skipped.

    Returns `{:ok, %{column_name => tensor}}` or `{:error, message}`.

    ## Example

        {:ok, tensors} = ExArrow.Nx.to_tensors(batch)
        # tensors is a map: %{"price" => #Nx.Tensor<...>, "qty" => #Nx.Tensor<...>}
        tensors["price"] |> Nx.sort()
        Map.keys(tensors)  # only numeric columns are present
    """
    @spec to_tensors(RecordBatch.t()) ::
            {:ok, %{String.t() => Nx.Tensor.t()}} | {:error, String.t()}
    def to_tensors(batch) do
      schema = RecordBatch.schema(batch)
      fields = Schema.fields(schema)

      Enum.reduce_while(fields, {:ok, %{}}, fn field, {:ok, acc} ->
        case column_to_tensor(batch, field.name) do
          {:ok, tensor} ->
            {:cont, {:ok, Map.put(acc, field.name, tensor)}}

          {:error, "unsupported column type" <> _} ->
            {:cont, {:ok, acc}}

          {:error, msg} ->
            {:halt, {:error, msg}}
        end
      end)
    end

    @doc """
    Convert an `Nx.Tensor` to a single-column `ExArrow.RecordBatch`.

    The tensor's raw bytes are extracted via `Nx.to_binary/1` and written into
    a native Arrow array.  For rank-2 or higher-rank tensors, all elements are
    flattened into a single 1-D column (`Nx.size(tensor)` elements).

    Supported Nx dtypes: `{:s, 8|16|32|64}`, `{:u, 8|16|32|64}`,
    `{:f, 32|64}`.  Other dtypes (e.g. `{:bf, 16}`, `{:c, 64}`) return
    `{:error, "unsupported Nx dtype…"}`.

    Returns `{:ok, batch}` or `{:error, message}`.

    ## Examples

        # Float64 tensor → RecordBatch
        tensor = Nx.tensor([1.0, 2.0, 3.0], type: {:f, 64})
        {:ok, batch} = ExArrow.Nx.from_tensor(tensor, "weights")
        ExArrow.RecordBatch.num_rows(batch)  #=> 3

        # Round-trip: tensor → batch → tensor
        original = Nx.tensor([10, 20, 30], type: {:s, 64})
        {:ok, batch}     = ExArrow.Nx.from_tensor(original, "vals")
        {:ok, recovered} = ExArrow.Nx.column_to_tensor(batch, "vals")
        Nx.to_list(recovered)  #=> [10, 20, 30]

        # Unsupported dtype
        {:error, msg} = ExArrow.Nx.from_tensor(Nx.tensor([1, 2], type: {:bf, 16}), "x")
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

    @doc """
    Convert a map of `{column_name => Nx.Tensor}` to a multi-column
    `ExArrow.RecordBatch` in a single call.

    All tensors must have the same number of elements (`Nx.size/1`).  For
    rank-2 or higher-rank tensors the elements are flattened into a 1-D column.

    Column order in the resulting batch follows `Map.to_list/1` ordering (i.e.
    sorted by key).  Supported dtypes are the same as `from_tensor/2`.

    Returns `{:ok, batch}` or `{:error, message}`.

    ## Examples

        tensors = %{
          "price" => Nx.tensor([1.5, 2.5, 3.5], type: {:f, 64}),
          "qty"   => Nx.tensor([10, 20, 30],     type: {:s, 32})
        }
        {:ok, batch} = ExArrow.Nx.from_tensors(tensors)
        ExArrow.RecordBatch.num_rows(batch)  #=> 3

        # Round-trip: all columns
        {:ok, recovered} = ExArrow.Nx.to_tensors(batch)
        Nx.to_list(recovered["price"])  #=> [1.5, 2.5, 3.5]

        # Mismatched sizes return an error
        bad = %{"a" => Nx.tensor([1, 2]), "b" => Nx.tensor([1, 2, 3])}
        {:error, _} = ExArrow.Nx.from_tensors(bad)
    """
    @spec from_tensors(%{String.t() => Nx.Tensor.t()}) ::
            {:ok, RecordBatch.t()} | {:error, String.t()}
    def from_tensors(tensors) when is_map(tensors) do
      case collect_tensor_columns(Map.to_list(tensors)) do
        {:error, _} = err ->
          err

        {:ok, [], [], [], []} ->
          {:error, "from_tensors requires at least one column"}

        {:ok, names_rev, dtypes_rev, binaries_rev, lengths_rev} ->
          build_from_columns(names_rev, dtypes_rev, binaries_rev, lengths_rev)
      end
    end

    defp collect_tensor_columns(entries) do
      Enum.reduce_while(entries, {:ok, [], [], [], []}, fn
        {name, tensor}, {:ok, ns, ds, bs, ls} ->
          collect_one_column(name, tensor, ns, ds, bs, ls)
      end)
    end

    defp collect_one_column(name, tensor, ns, ds, bs, ls) do
      if is_binary(name) do
        case nx_dtype_to_arrow(Nx.type(tensor)) do
          {:ok, dtype_str} ->
            {:cont,
             {:ok, [name | ns], [dtype_str | ds], [Nx.to_binary(tensor) | bs],
              [Nx.size(tensor) | ls]}}

          {:error, _} = err ->
            {:halt, err}
        end
      else
        {:halt, {:error, "column name must be a string, got: #{inspect(name)}"}}
      end
    end

    defp build_from_columns(names_rev, dtypes_rev, binaries_rev, lengths_rev) do
      unique_lengths = Enum.uniq(lengths_rev)

      if length(unique_lengths) > 1 do
        {:error,
         "all tensors must have the same size; got sizes #{inspect(Enum.reverse(lengths_rev))}"}
      else
        [len | _] = lengths_rev

        case Native.record_batch_from_column_binaries(
               Enum.reverse(names_rev),
               Enum.reverse(binaries_rev),
               Enum.reverse(dtypes_rev),
               len
             ) do
          {:ok, ref} -> {:ok, RecordBatch.from_ref(ref)}
          {:error, _} = err -> err
        end
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

    @doc false
    def from_tensors(_tensors), do: {:error, nx_missing_message()}

    defp nx_missing_message do
      "Nx is not available. Add {:nx, \"~> 0.7\"} to your mix.exs dependencies."
    end
  end
end
