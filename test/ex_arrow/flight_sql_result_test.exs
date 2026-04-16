defmodule ExArrow.FlightSQL.ResultTest do
  use ExUnit.Case, async: false

  alias ExArrow.FlightSQL.{Error, Result}

  setup do
    prev = Application.get_env(:ex_arrow, :stream_native)

    on_exit(fn ->
      case prev do
        nil -> Application.delete_env(:ex_arrow, :stream_native)
        val -> Application.put_env(:ex_arrow, :stream_native, val)
      end
    end)

    :ok
  end

  describe "struct" do
    test "holds schema, batches, and num_rows" do
      schema = %ExArrow.Schema{resource: make_ref()}
      batch = %ExArrow.RecordBatch{resource: make_ref()}

      result = %Result{schema: schema, batches: [batch], num_rows: 5}

      assert result.schema == schema
      assert result.batches == [batch]
      assert result.num_rows == 5
    end

    test "allows empty batches" do
      schema = %ExArrow.Schema{resource: make_ref()}
      result = %Result{schema: schema, batches: [], num_rows: 0}
      assert result.num_rows == 0
    end
  end

  describe "to_dataframe/1 — with fake schema ref" do
    # Explorer is available in the test environment. Passing a fake (non-NIF)
    # schema ref causes the IPC serialisation NIF to raise, which is caught by
    # the rescue block and returned as {:error, %Error{code: :conversion_error}}.
    test "returns conversion_error when IPC serialisation raises on fake ref" do
      result = %Result{
        schema: %ExArrow.Schema{resource: make_ref()},
        batches: [],
        num_rows: 0
      }

      assert {:error, %Error{code: :conversion_error}} = Result.to_dataframe(result)
    end
  end

  describe "to_tensor/2 — column missing" do
    test "returns conversion_error for empty batches" do
      result = %Result{
        schema: %ExArrow.Schema{resource: make_ref()},
        batches: [],
        num_rows: 0
      }

      assert {:error, %Error{code: :conversion_error, message: msg}} =
               Result.to_tensor(result, "price")

      assert msg =~ "no batches"
    end
  end

  describe "to_tensor/2 — with fake batch ref" do
    # Nx is available in the test environment. Passing a fake (non-NIF) batch ref
    # causes the tensor conversion NIF to raise, which is caught by the rescue
    # block and returned as {:error, %Error{code: :conversion_error}}.
    test "returns conversion_error when tensor conversion raises on fake ref" do
      schema = %ExArrow.Schema{resource: make_ref()}
      batch = %ExArrow.RecordBatch{resource: make_ref()}
      result = %Result{schema: schema, batches: [batch], num_rows: 1}

      assert {:error, %Error{code: :conversion_error}} = Result.to_tensor(result, "price")
    end
  end

  # ── from_stream/1 ────────────────────────────────────────────────────────────

  describe "from_stream/1 — empty stream (stub native)" do
    test "returns {:ok, result} with empty batches and num_rows 0" do
      Application.put_env(:ex_arrow, :stream_native, ExArrow.Stream.TestNativeFlightSqlOk)
      stream = %ExArrow.Stream{resource: make_ref(), backend: :flight_sql}

      assert {:ok, result} = Result.from_stream(stream)
      assert result.batches == []
      assert result.num_rows == 0
      assert %ExArrow.Schema{} = result.schema
    end
  end

  describe "from_stream/1 — schema error" do
    test "returns {:error, %Error{code: :protocol_error}} when schema fetch fails" do
      Application.put_env(:ex_arrow, :stream_native, ExArrow.Stream.TestNativeError)
      stream = %ExArrow.Stream{resource: make_ref(), backend: :flight_sql}

      assert {:error, %Error{code: :protocol_error, message: msg}} = Result.from_stream(stream)
      assert msg =~ "flight_sql stream schema error"
    end
  end

  describe "from_stream/1 — batch read error" do
    test "returns {:error, %Error{code: :transport_error}} when a batch read fails" do
      # TestNativeFlightSqlTriple returns {:ok, schema} then {:error, {triple}} for next.
      Application.put_env(:ex_arrow, :stream_native, ExArrow.Stream.TestNativeFlightSqlTriple)
      stream = %ExArrow.Stream{resource: make_ref(), backend: :flight_sql}

      assert {:error, %Error{code: :transport_error, message: msg}} = Result.from_stream(stream)
      assert msg =~ "unavailable"
      assert msg =~ "server gone"
    end
  end

  describe "from_stream/1 — real IPC stream" do
    @tag :nif
    test "collects all batches and counts rows from a real stream" do
      {:ok, ipc_bin} = ExArrow.Native.ipc_test_fixture_binary()
      {:ok, stream_ref} = ExArrow.Native.ipc_reader_from_binary(ipc_bin)
      stream = %ExArrow.Stream{resource: stream_ref, backend: :ipc}

      assert {:ok, result} = Result.from_stream(stream)
      assert result.num_rows > 0
      assert length(result.batches) > 0
      assert %ExArrow.Schema{} = result.schema
    end
  end

  # ── Explorer integration ─────────────────────────────────────────────────────

  if Code.ensure_loaded?(Explorer.DataFrame) do
    # Build a real Result from the IPC test fixture (id: int64, name: utf8, 2 rows).
    defp real_result do
      {:ok, ipc_bin} = ExArrow.Native.ipc_test_fixture_binary()
      {:ok, stream_ref} = ExArrow.Native.ipc_reader_from_binary(ipc_bin)
      stream = %ExArrow.Stream{resource: stream_ref, backend: :ipc}
      {:ok, result} = Result.from_stream(stream)
      result
    end

    describe "to_dataframe/1 — real NIF round-trip" do
      @tag :nif
      @tag :explorer
      test "returns {:ok, Explorer.DataFrame} from a real result" do
        result = real_result()
        assert {:ok, df} = Result.to_dataframe(result)
        assert is_struct(df, Explorer.DataFrame)
      end

      @tag :nif
      @tag :explorer
      test "DataFrame has the correct row count" do
        result = real_result()
        {:ok, df} = Result.to_dataframe(result)
        assert Explorer.DataFrame.n_rows(df) == result.num_rows
      end

      @tag :nif
      @tag :explorer
      test "DataFrame has the correct column names" do
        result = real_result()
        schema_names = ExArrow.Schema.field_names(result.schema)
        {:ok, df} = Result.to_dataframe(result)
        assert Enum.sort(Explorer.DataFrame.names(df)) == Enum.sort(schema_names)
      end
    end
  end

  # ── Nx integration ───────────────────────────────────────────────────────────

  if Code.ensure_loaded?(Nx) do
    defp real_result_nx do
      {:ok, ipc_bin} = ExArrow.Native.ipc_test_fixture_binary()
      {:ok, stream_ref} = ExArrow.Native.ipc_reader_from_binary(ipc_bin)
      stream = %ExArrow.Stream{resource: stream_ref, backend: :ipc}
      {:ok, result} = Result.from_stream(stream)
      result
    end

    describe "to_tensor/2 — real NIF round-trip" do
      @tag :nif
      @tag :nx
      test "returns {:ok, Nx.Tensor} for a numeric column" do
        result = real_result_nx()
        assert {:ok, tensor} = Result.to_tensor(result, "id")
        assert Nx.type(tensor) == {:s, 64}
        assert Nx.size(tensor) == result.num_rows
      end

      @tag :nif
      @tag :nx
      test "returns conversion_error for a non-numeric column" do
        result = real_result_nx()

        assert {:error, %Error{code: :conversion_error, message: msg}} =
                 Result.to_tensor(result, "name")

        assert msg =~ "not found in batch"
      end

      @tag :nif
      @tag :nx
      test "returns conversion_error for an unknown column" do
        result = real_result_nx()

        assert {:error, %Error{code: :conversion_error, message: msg}} =
                 Result.to_tensor(result, "no_such_col")

        assert msg =~ "not found in batch"
      end
    end
  end
end
