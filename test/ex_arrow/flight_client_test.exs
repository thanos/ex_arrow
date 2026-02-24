defmodule ExArrow.Flight.ClientTest do
  use ExUnit.Case, async: true

  setup context do
    Mox.set_mox_from_context(context)
    :ok
  end

  describe "real implementation (default)" do
    test "connect/3 to non-existent server returns error" do
      assert {:error, _msg} = ExArrow.Flight.Client.connect("localhost", 39_281, [])
    end

    test "connect/3 with opts to non-existent server returns error" do
      assert {:error, _msg} = ExArrow.Flight.Client.connect("host", 39_282, tls: true)
    end
  end

  describe "with Mox mock" do
    test "connect/3 uses mock when configured and returns success" do
      Application.put_env(:ex_arrow, :flight_client_impl, ExArrow.Flight.ClientMock)
      on_exit(fn -> Application.delete_env(:ex_arrow, :flight_client_impl) end)

      fake_client = %ExArrow.Flight.Client{resource: make_ref()}

      ExArrow.Flight.ClientMock
      |> Mox.expect(:connect, fn "myhost", 9090, [] ->
        {:ok, fake_client}
      end)

      assert {:ok, ^fake_client} = ExArrow.Flight.Client.connect("myhost", 9090)
    end

    test "do_get/2 uses mock when configured" do
      Application.put_env(:ex_arrow, :flight_client_impl, ExArrow.Flight.ClientMock)
      on_exit(fn -> Application.delete_env(:ex_arrow, :flight_client_impl) end)

      fake_client = %ExArrow.Flight.Client{resource: make_ref()}
      fake_stream = %ExArrow.Stream{resource: make_ref()}

      ExArrow.Flight.ClientMock
      |> Mox.expect(:do_get, fn ^fake_client, "ticket" -> {:ok, fake_stream} end)

      assert {:ok, ^fake_stream} = ExArrow.Flight.Client.do_get(fake_client, "ticket")
    end

    test "do_put/3 uses mock when configured" do
      Application.put_env(:ex_arrow, :flight_client_impl, ExArrow.Flight.ClientMock)
      on_exit(fn -> Application.delete_env(:ex_arrow, :flight_client_impl) end)

      fake_client = %ExArrow.Flight.Client{resource: make_ref()}
      schema = %ExArrow.Schema{resource: make_ref()}

      ExArrow.Flight.ClientMock
      |> Mox.expect(:do_put, fn ^fake_client, ^schema, [] -> :ok end)

      assert :ok = ExArrow.Flight.Client.do_put(fake_client, schema, [])
    end
  end
end
