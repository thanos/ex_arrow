defmodule ExArrow.FlightSQL.MetadataTest do
  use ExUnit.Case, async: false

  alias ExArrow.FlightSQL.{Client, Error}
  alias ExArrow.Stream

  # ── Helpers ───────────────────────────────────────────────────────────────────

  setup do
    prev_impl = Application.get_env(:ex_arrow, :flight_sql_client_impl)
    prev_native = Application.get_env(:ex_arrow, :flight_sql_client_native)

    on_exit(fn ->
      restore(:flight_sql_client_impl, prev_impl)
      restore(:flight_sql_client_native, prev_native)
    end)

    :ok
  end

  defp restore(key, nil), do: Application.delete_env(:ex_arrow, key)
  defp restore(key, val), do: Application.put_env(:ex_arrow, key, val)

  defp use_ok_native do
    Application.put_env(:ex_arrow, :flight_sql_client_native, ExArrow.FlightSQL.TestNativeMetadataOk)
  end

  defp use_unimplemented_native do
    Application.put_env(
      :ex_arrow,
      :flight_sql_client_native,
      ExArrow.FlightSQL.TestNativeMetadataUnimplemented
    )
  end

  defp fake_client, do: %Client{resource: make_ref()}

  # ── get_tables/2 — via ClientImpl ─────────────────────────────────────────────

  describe "get_tables/2 — success" do
    test "returns {:ok, %Stream{}} with default options" do
      use_ok_native()
      assert {:ok, %Stream{backend: :flight_sql}} = Client.get_tables(fake_client())
    end

    test "returns {:ok, %Stream{}} with all filter options" do
      use_ok_native()

      assert {:ok, %Stream{backend: :flight_sql}} =
               Client.get_tables(fake_client(),
                 catalog: "main",
                 db_schema_filter: "pub%",
                 table_name_filter: "orders%",
                 table_types: ["TABLE", "VIEW"],
                 include_schema: true
               )
    end

    test "nil catalog and db_schema_filter are passed through" do
      use_ok_native()
      # No catalog or filter — should still succeed
      assert {:ok, %Stream{}} = Client.get_tables(fake_client(), table_types: [])
    end
  end

  describe "get_tables/2 — unimplemented" do
    test "returns {:error, %Error{code: :unimplemented}} when server rejects" do
      use_unimplemented_native()

      assert {:error, %Error{code: :unimplemented, message: msg}} =
               Client.get_tables(fake_client())

      assert msg =~ "not supported"
    end
  end

  # ── get_db_schemas/2 — via ClientImpl ────────────────────────────────────────

  describe "get_db_schemas/2 — success" do
    test "returns {:ok, %Stream{}} with no options" do
      use_ok_native()
      assert {:ok, %Stream{backend: :flight_sql}} = Client.get_db_schemas(fake_client())
    end

    test "accepts catalog and db_schema_filter options" do
      use_ok_native()

      assert {:ok, %Stream{backend: :flight_sql}} =
               Client.get_db_schemas(fake_client(),
                 catalog: "main",
                 db_schema_filter: "pub%"
               )
    end
  end

  describe "get_db_schemas/2 — unimplemented" do
    test "returns {:error, %Error{code: :unimplemented}} when server rejects" do
      use_unimplemented_native()

      assert {:error, %Error{code: :unimplemented}} = Client.get_db_schemas(fake_client())
    end
  end

  # ── get_sql_info/1 — via ClientImpl ──────────────────────────────────────────

  describe "get_sql_info/1 — success" do
    test "returns {:ok, %Stream{}} when server responds" do
      use_ok_native()
      assert {:ok, %Stream{backend: :flight_sql}} = Client.get_sql_info(fake_client())
    end
  end

  describe "get_sql_info/1 — unimplemented" do
    test "returns {:error, %Error{code: :unimplemented}} when server rejects" do
      use_unimplemented_native()

      assert {:error, %Error{code: :unimplemented}} = Client.get_sql_info(fake_client())
    end
  end

  # ── Mox — delegation ─────────────────────────────────────────────────────────

  describe "with mock — get_tables/2" do
    setup context do
      Mox.set_mox_from_context(context)
      Application.put_env(:ex_arrow, :flight_sql_client_impl, ExArrow.FlightSQL.ClientMock)
      :ok
    end

    test "delegates to configured impl" do
      client = fake_client()
      fake_stream = %Stream{resource: make_ref(), backend: :flight_sql}

      Mox.expect(ExArrow.FlightSQL.ClientMock, :get_tables, fn ^client, [catalog: "main"] ->
        {:ok, fake_stream}
      end)

      assert {:ok, ^fake_stream} = Client.get_tables(client, catalog: "main")
    end

    test "propagates error from impl" do
      client = fake_client()

      Mox.expect(ExArrow.FlightSQL.ClientMock, :get_tables, fn _client, [] ->
        {:error, %Error{code: :transport_error, message: "timeout"}}
      end)

      assert {:error, %Error{code: :transport_error}} = Client.get_tables(client)
    end
  end

  describe "with mock — get_db_schemas/2" do
    setup context do
      Mox.set_mox_from_context(context)
      Application.put_env(:ex_arrow, :flight_sql_client_impl, ExArrow.FlightSQL.ClientMock)
      :ok
    end

    test "delegates to configured impl" do
      client = fake_client()
      fake_stream = %Stream{resource: make_ref(), backend: :flight_sql}

      Mox.expect(ExArrow.FlightSQL.ClientMock, :get_db_schemas, fn ^client, [] ->
        {:ok, fake_stream}
      end)

      assert {:ok, ^fake_stream} = Client.get_db_schemas(client)
    end

    test "propagates error from impl" do
      client = fake_client()

      Mox.expect(ExArrow.FlightSQL.ClientMock, :get_db_schemas, fn _client, _ ->
        {:error, %Error{code: :permission_denied, message: "forbidden"}}
      end)

      assert {:error, %Error{code: :permission_denied}} = Client.get_db_schemas(client)
    end
  end

  describe "with mock — get_sql_info/1" do
    setup context do
      Mox.set_mox_from_context(context)
      Application.put_env(:ex_arrow, :flight_sql_client_impl, ExArrow.FlightSQL.ClientMock)
      :ok
    end

    test "delegates to configured impl" do
      client = fake_client()
      fake_stream = %Stream{resource: make_ref(), backend: :flight_sql}

      Mox.expect(ExArrow.FlightSQL.ClientMock, :get_sql_info, fn ^client, [] ->
        {:ok, fake_stream}
      end)

      assert {:ok, ^fake_stream} = Client.get_sql_info(client)
    end

    test "propagates error from impl" do
      client = fake_client()

      Mox.expect(ExArrow.FlightSQL.ClientMock, :get_sql_info, fn _client, _ ->
        {:error, %Error{code: :unimplemented, message: "not supported"}}
      end)

      assert {:error, %Error{code: :unimplemented}} = Client.get_sql_info(client)
    end
  end

end
