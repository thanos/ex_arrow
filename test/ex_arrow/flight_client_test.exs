defmodule ExArrow.Flight.ClientTest do
  use ExUnit.Case, async: true

  setup context do
    Mox.set_mox_from_context(context)
    :ok
  end

  describe "stub implementation (default)" do
    test "connect/3 returns not_implemented" do
      assert ExArrow.Flight.Client.connect("localhost", 9999) == {:error, :not_implemented}
    end

    test "connect/3 with opts returns not_implemented" do
      assert ExArrow.Flight.Client.connect("host", 8080, tls: true) == {:error, :not_implemented}
    end

    test "do_get/2 returns not_implemented" do
      client = %ExArrow.Flight.Client{resource: make_ref()}
      assert ExArrow.Flight.Client.do_get(client, "ticket") == {:error, :not_implemented}
    end

    test "do_put/3 returns not_implemented" do
      client = %ExArrow.Flight.Client{resource: make_ref()}
      schema = %ExArrow.Schema{resource: make_ref()}
      assert ExArrow.Flight.Client.do_put(client, schema, []) == {:error, :not_implemented}
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
  end
end
