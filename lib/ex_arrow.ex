defmodule ExArrow do
  @moduledoc """
  Apache Arrow support for the BEAM: IPC, Flight, ADBC, and data interchange.

  ExArrow keeps Arrow data in native (Rust) memory and exposes opaque handles
  on the Elixir side. Copying to the BEAM heap happens only when explicitly
  requested.

  ## Arrow hierarchy

  Arrow organises columnar data in a strict hierarchy:

  - **Array** — a single column of typed values (the leaf node).
  - **RecordBatch** — a collection of Arrays sharing a row count and schema.
  - **Table** — a logical table backed by one or more RecordBatches.
  - **Stream** — a lazy sequence of RecordBatches (used by IPC, Flight, ADBC,
    and Parquet).
  - **Schema** — metadata describing field names, types, and nullability.
  - **Field** — one column's metadata within a Schema.

  Data flows through these levels: a Stream yields Batches, a Batch exposes
  its Schema and row count, and the Schema lists its Fields.

  ## Data interchange (v0.6+)

  ExArrow serves as a universal data interchange layer between Arrow and the
  wider Elixir ecosystem:

  - `from_dataframe/1` / `to_dataframe/1` — Explorer DataFrame <-> Arrow
  - `from_nx/1` / `to_nx/1` — Nx Tensor <-> Arrow

  These top-level functions delegate to focused bridge modules:
  `ExArrow.DataFrame`, `ExArrow.Explorer`, and `ExArrow.Nx`.

  ## Public API outline

  ### Data interchange
  - `ExArrow.from_dataframe/1`, `to_dataframe/1` — Explorer <-> Arrow
  - `ExArrow.from_nx/1`, `to_nx/1` — Nx <-> Arrow
  - `ExArrow.DataFrame.from_arrow/1`, `to_arrow/1` — DataFrame-oriented API

  ### Core handles (opaque references)
  - `ExArrow.Schema` – schema metadata (fields)
  - `ExArrow.Field` – field name, type, and nullability
  - `ExArrow.Array` – column array handle
  - `ExArrow.RecordBatch` – batch of columns with shared row count
  - `ExArrow.Table` – table with schema and batches
  - `ExArrow.Stream` – stream of record batches (IPC/Flight)

  ### IPC (`ExArrow.IPC`)
  - `ExArrow.IPC.Reader.from_binary/1`, `from_file/1` – read stream from binary or file
  - `ExArrow.IPC.Writer.to_binary/2`, `to_file/3` – write batches to binary or file

  ### Flight (`ExArrow.Flight`)
  - `ExArrow.Flight.Client` – connect, do_get, do_put
  - `ExArrow.Flight.Server` – minimal server (e.g. echo)

  ### ADBC (`ExArrow.ADBC`)
  - `ExArrow.ADBC.Database.open/1` – open database (driver path or opts)
  - `ExArrow.ADBC.Connection.open/1` – open connection from database
  - `ExArrow.ADBC.Statement` – new(conn, sql) or new(conn, sql, bind: batch), execute (returns stream); set_sql/bind for reuse/rebind

  ### Schema mapping
  - `ExArrow.Schema.Mapper` – bidirectional Arrow <-> Explorer/Nx type mapping

  ### Errors
  - `ExArrow.Error` – structured exception with code, message, details
  """

  @explorer_available Code.ensure_loaded?(Explorer.DataFrame)
  @nx_available Code.ensure_loaded?(Nx)

  @doc """
  Returns the native NIF crate version. Used to verify the NIF loads.
  """
  @spec native_version() :: String.t()
  def native_version do
    ExArrow.Native.nif_version()
  end

  if @explorer_available do
    @doc """
    Convert an `Explorer.DataFrame` to an `ExArrow.RecordBatch`.

    The dataframe is serialised to Arrow IPC and read back as a native batch
    handle.  Schema, nullability, row count, and values are preserved.

    Returns `{:ok, batch}` or `{:error, message}`.

    ## Examples

        df = Explorer.DataFrame.new(x: [1, 2, 3], y: ["a", "b", "c"])
        {:ok, batch} = ExArrow.from_dataframe(df)
        ExArrow.RecordBatch.num_rows(batch)  #=> 3
    """
    @spec from_dataframe(Explorer.DataFrame.t()) ::
            {:ok, ExArrow.RecordBatch.t()} | {:error, String.t()}
    def from_dataframe(df), do: ExArrow.DataFrame.to_arrow(df)

    @doc """
    Convert an `ExArrow.RecordBatch` or `ExArrow.Stream` to an
    `Explorer.DataFrame`.

    Schema, nullability, row count, and values are preserved.

    Returns `{:ok, dataframe}` or `{:error, message}`.

    ## Examples

        {:ok, batch} = ExArrow.from_dataframe(df)
        {:ok, df2} = ExArrow.to_dataframe(batch)
        Explorer.DataFrame.n_rows(df2)  #=> 3
    """
    @spec to_dataframe(ExArrow.RecordBatch.t() | ExArrow.Stream.t()) ::
            {:ok, Explorer.DataFrame.t()} | {:error, String.t()}
    def to_dataframe(batch_or_stream), do: ExArrow.DataFrame.from_arrow(batch_or_stream)
  else
    @doc false
    def from_dataframe(_), do: {:error, "Explorer is not available. Add {:explorer, \"~> 0.11\"} to your mix.exs dependencies."}

    @doc false
    def to_dataframe(_), do: {:error, "Explorer is not available. Add {:explorer, \"~> 0.11\"} to your mix.exs dependencies."}
  end

  if @nx_available do
    @doc """
    Convert an `Nx.Tensor` to an `ExArrow.RecordBatch`.

    Supported dtypes: `{:u, 8}`, `{:s, 64}`, `{:f, 32}`, `{:f, 64}`, and
    all other integer/float dtypes supported by `ExArrow.Schema.Mapper`.

    Rank-1 tensors produce a single-column batch named `"value"`.  Rank-2
    tensors produce an N-column batch with columns named `"c0"`, `"c1"`, ...
    `"c{N-1}"`, where N is the size of the second axis.  Tensors of rank > 2
    are not supported.

    ## Options

    - `:as` — when set to `:boolean`, the column is created as an Arrow
      Boolean array.  Only valid for `{:u, 8}` tensors.  Default: `:numeric`.
    - `:name` — column name for rank-1 tensors.  Default: `"value"`.

    Returns `{:ok, batch}` or `{:error, message}`.

    ## Examples

        # Rank-1 s64 tensor
        {:ok, batch} = ExArrow.from_nx(Nx.tensor([1, 2, 3], type: {:s, 64}))
        ExArrow.RecordBatch.num_rows(batch)  #=> 3

        # Rank-2 f64 tensor → 3 columns (c0, c1, c2), 2 rows
        {:ok, batch} = ExArrow.from_nx(Nx.tensor([[1.0, 2.0, 3.0], [4.0, 5.0, 6.0]], type: {:f, 64}))

        # Boolean tensor
        {:ok, batch} = ExArrow.from_nx(Nx.tensor([1, 0, 1], type: {:u, 8}), as: :boolean)
    """
    @spec from_nx(Nx.Tensor.t(), keyword()) ::
            {:ok, ExArrow.RecordBatch.t()} | {:error, String.t()}
    def from_nx(tensor, opts \\ []) do
      shape = Nx.shape(tensor)

      cond do
        tuple_size(shape) > 2 ->
          {:error, "tensors of rank > 2 are not supported for Arrow conversion"}

        tuple_size(shape) == 2 ->
          from_nx_rank2(tensor)

        true ->
          name = Keyword.get(opts, :name, "value")
          ExArrow.Nx.from_tensor(tensor, name, Keyword.take(opts, [:as]))
      end
    end

    defp from_nx_rank2(tensor) do
      {_rows, cols} = Nx.shape(tensor)
      nx_dtype = Nx.type(tensor)

      column_tensors =
        for c <- 0..(cols - 1) do
          slice = Nx.tensor(Nx.to_list(tensor[[.., c]]), type: nx_dtype)
          {"c#{c}", slice}
        end

      ExArrow.Nx.from_tensors(Map.new(column_tensors))
    end

    @doc """
    Convert an `ExArrow.RecordBatch` to an `Nx.Tensor`.

    For a single-column numeric/boolean batch, returns a rank-1 tensor.
    For a multi-column batch with uniform numeric dtype, returns a rank-2
    tensor where columns become the second axis.

    Returns `{:ok, tensor}` or `{:error, message}`.

    ## Examples

        # Single column → rank-1
        {:ok, batch} = ExArrow.from_nx(Nx.tensor([1, 2, 3], type: {:s, 64}))
        {:ok, tensor} = ExArrow.to_nx(batch)
        Nx.shape(tensor)  #=> {3}

        # Multi-column uniform → rank-2
        {:ok, batch} = ExArrow.from_nx(Nx.tensor([[1, 2], [3, 4]], type: {:s, 64}))
        {:ok, tensor} = ExArrow.to_nx(batch)
        Nx.shape(tensor)  #=> {2, 2}
    """
    @spec to_nx(ExArrow.RecordBatch.t()) ::
            {:ok, Nx.Tensor.t()} | {:error, String.t()}
    def to_nx(batch) do
      schema = ExArrow.RecordBatch.schema(batch)
      fields = ExArrow.Schema.fields(schema)

      case extract_numeric_fields(fields) do
        {:error, _} = err -> err
        {:ok, []} -> {:error, "no numeric columns available for Nx conversion"}
        {:ok, numeric_fields} ->
          columns = Enum.map(numeric_fields, & &1.name)
          to_nx_from_columns(batch, columns)
      end
    end

    defp extract_numeric_fields(fields) do
      mapper = ExArrow.Schema.Mapper

      Enum.reduce_while(fields, {:ok, []}, fn field, {:ok, acc} ->
        case mapper.arrow_type_atom_to_dtype(field.type) do
          {:ok, dtype} ->
            if mapper.numeric?(dtype) do
              {:cont, {:ok, acc ++ [field]}}
            else
              {:cont, {:ok, acc}}
            end

          {:error, _} ->
            {:cont, {:ok, acc}}
        end
      end)
    end

    defp to_nx_from_columns(batch, [single_col]) do
      ExArrow.Nx.column_to_tensor(batch, single_col)
    end

    defp to_nx_from_columns(batch, columns) do
      case ExArrow.Nx.to_tensors(batch) do
        {:ok, tensors} ->
          col_tensors = Enum.map(columns, &Map.fetch!(tensors, &1))
          first_dtype = Nx.type(hd(col_tensors))

          if Enum.all?(col_tensors, &(Nx.type(&1) == first_dtype)) do
            rows = Nx.size(hd(col_tensors))
            stacked = Nx.stack(col_tensors, axis: 1)
            {:ok, Nx.reshape(stacked, {rows, length(columns)})}
          else
            {:error,
             "cannot create rank-2 tensor: columns have non-uniform dtypes. Use ExArrow.Nx.to_tensors/1 for per-column access."}
          end

        {:error, _} = err ->
          err
      end
    end
  else
    @doc false
    def from_nx(_tensor, _opts \\ []), do: {:error, "Nx is not available. Add {:nx, \"~> 0.9\"} to your mix.exs dependencies."}

    @doc false
    def to_nx(_batch), do: {:error, "Nx is not available. Add {:nx, \"~> 0.9\"} to your mix.exs dependencies."}
  end
end
