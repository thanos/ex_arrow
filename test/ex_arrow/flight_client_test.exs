defmodule ExArrow.Flight.ClientTest do
  use ExUnit.Case, async: true

  alias ExArrow.Flight.{ActionType, Client, FlightInfo}

  setup context do
    Mox.set_mox_from_context(context)
    :ok
  end

  # ── real implementation (no server) ─────────────────────────────────────────

  describe "real implementation (default)" do
    test "connect/3 to non-existent loopback server returns error" do
      assert {:error, _msg} = Client.connect("localhost", 39_281, [])
    end

    test "connect/3 with tls: true attempts TLS and returns connection error" do
      # tls: true is now supported; connecting to a non-existent server returns
      # a connection error rather than :tls_not_supported.
      assert {:error, _msg} = Client.connect("localhost", 39_282, tls: true)
    end

    test "connect/3 with tls: false uses plaintext" do
      assert {:error, _msg} = Client.connect("localhost", 39_283, tls: false)
    end

    test "connect/3 with invalid tls list opt returns structured error" do
      assert {:error, {:invalid_tls_opt, _}} =
               Client.connect("localhost", 39_284, tls: [no_cert: true])
    end

    test "connect/3 to non-loopback host auto-selects TLS and returns connection error" do
      # Non-loopback hosts default to TLS; connection to a non-existent server
      # still fails, but with a TLS/transport error rather than :tls_not_supported.
      assert {:error, _msg} = Client.connect("flight.example.invalid", 9999, [])
    end
  end

  # ── Mox mock: Milestone 3 callbacks ─────────────────────────────────────────

  describe "with Mox mock — connect / do_get / do_put" do
    test "connect/3 uses mock when configured" do
      Application.put_env(:ex_arrow, :flight_client_impl, ExArrow.Flight.ClientMock)
      on_exit(fn -> Application.delete_env(:ex_arrow, :flight_client_impl) end)

      fake_client = %Client{resource: make_ref()}

      Mox.expect(ExArrow.Flight.ClientMock, :connect, fn "myhost", 9090, [] ->
        {:ok, fake_client}
      end)

      assert {:ok, ^fake_client} = Client.connect("myhost", 9090)
    end

    test "do_get/2 uses mock when configured" do
      Application.put_env(:ex_arrow, :flight_client_impl, ExArrow.Flight.ClientMock)
      on_exit(fn -> Application.delete_env(:ex_arrow, :flight_client_impl) end)

      fake_client = %Client{resource: make_ref()}
      fake_stream = %ExArrow.Stream{resource: make_ref()}

      Mox.expect(ExArrow.Flight.ClientMock, :do_get, fn ^fake_client, "ticket" ->
        {:ok, fake_stream}
      end)

      assert {:ok, ^fake_stream} = Client.do_get(fake_client, "ticket")
    end

    test "do_put/3 uses mock when configured" do
      Application.put_env(:ex_arrow, :flight_client_impl, ExArrow.Flight.ClientMock)
      on_exit(fn -> Application.delete_env(:ex_arrow, :flight_client_impl) end)

      fake_client = %Client{resource: make_ref()}
      schema = %ExArrow.Schema{resource: make_ref()}

      Mox.expect(ExArrow.Flight.ClientMock, :do_put, fn ^fake_client, ^schema, [] -> :ok end)

      assert :ok = Client.do_put(fake_client, schema, [])
    end
  end

  # ── Mox mock: Milestone 4 callbacks ─────────────────────────────────────────

  describe "with Mox mock — list_flights" do
    test "list_flights/2 delegates to impl and returns FlightInfo list" do
      Application.put_env(:ex_arrow, :flight_client_impl, ExArrow.Flight.ClientMock)
      on_exit(fn -> Application.delete_env(:ex_arrow, :flight_client_impl) end)

      fake_client = %Client{resource: make_ref()}

      fake_info = %FlightInfo{
        schema_bytes: <<>>,
        descriptor: {:cmd, "x"},
        endpoints: [],
        total_records: 0,
        total_bytes: -1
      }

      Mox.expect(ExArrow.Flight.ClientMock, :list_flights, fn ^fake_client, <<>> ->
        {:ok, [fake_info]}
      end)

      assert {:ok, [^fake_info]} = Client.list_flights(fake_client)
    end

    test "list_flights/2 passes criteria bytes through" do
      Application.put_env(:ex_arrow, :flight_client_impl, ExArrow.Flight.ClientMock)
      on_exit(fn -> Application.delete_env(:ex_arrow, :flight_client_impl) end)

      fake_client = %Client{resource: make_ref()}

      Mox.expect(ExArrow.Flight.ClientMock, :list_flights, fn ^fake_client, "filter" ->
        {:ok, []}
      end)

      assert {:ok, []} = Client.list_flights(fake_client, "filter")
    end

    test "list_flights/2 propagates error from impl" do
      Application.put_env(:ex_arrow, :flight_client_impl, ExArrow.Flight.ClientMock)
      on_exit(fn -> Application.delete_env(:ex_arrow, :flight_client_impl) end)

      fake_client = %Client{resource: make_ref()}

      Mox.expect(ExArrow.Flight.ClientMock, :list_flights, fn _, _ ->
        {:error, "network failure"}
      end)

      assert {:error, "network failure"} = Client.list_flights(fake_client)
    end
  end

  describe "with Mox mock — get_flight_info" do
    test "get_flight_info/2 delegates to impl" do
      Application.put_env(:ex_arrow, :flight_client_impl, ExArrow.Flight.ClientMock)
      on_exit(fn -> Application.delete_env(:ex_arrow, :flight_client_impl) end)

      fake_client = %Client{resource: make_ref()}

      fake_info = %FlightInfo{
        schema_bytes: <<>>,
        descriptor: {:cmd, "echo"},
        endpoints: [],
        total_records: 5,
        total_bytes: -1
      }

      Mox.expect(ExArrow.Flight.ClientMock, :get_flight_info, fn ^fake_client, {:cmd, "echo"} ->
        {:ok, fake_info}
      end)

      assert {:ok, ^fake_info} = Client.get_flight_info(fake_client, {:cmd, "echo"})
    end

    test "get_flight_info/2 with path descriptor" do
      Application.put_env(:ex_arrow, :flight_client_impl, ExArrow.Flight.ClientMock)
      on_exit(fn -> Application.delete_env(:ex_arrow, :flight_client_impl) end)

      fake_client = %Client{resource: make_ref()}

      Mox.expect(ExArrow.Flight.ClientMock, :get_flight_info, fn ^fake_client,
                                                                 {:path, ["a", "b"]} ->
        {:error, "not found"}
      end)

      assert {:error, "not found"} =
               Client.get_flight_info(fake_client, {:path, ["a", "b"]})
    end
  end

  describe "with Mox mock — get_schema" do
    test "get_schema/2 delegates to impl and returns schema" do
      Application.put_env(:ex_arrow, :flight_client_impl, ExArrow.Flight.ClientMock)
      on_exit(fn -> Application.delete_env(:ex_arrow, :flight_client_impl) end)

      fake_client = %Client{resource: make_ref()}
      fake_schema = %ExArrow.Schema{resource: make_ref()}

      Mox.expect(ExArrow.Flight.ClientMock, :get_schema, fn ^fake_client, {:cmd, "echo"} ->
        {:ok, fake_schema}
      end)

      assert {:ok, ^fake_schema} = Client.get_schema(fake_client, {:cmd, "echo"})
    end

    test "get_schema/2 propagates not-found error" do
      Application.put_env(:ex_arrow, :flight_client_impl, ExArrow.Flight.ClientMock)
      on_exit(fn -> Application.delete_env(:ex_arrow, :flight_client_impl) end)

      fake_client = %Client{resource: make_ref()}

      Mox.expect(ExArrow.Flight.ClientMock, :get_schema, fn _, _ ->
        {:error, "not found"}
      end)

      assert {:error, "not found"} = Client.get_schema(fake_client, {:cmd, "x"})
    end
  end

  describe "with Mox mock — list_actions" do
    test "list_actions/1 delegates to impl and returns ActionType list" do
      Application.put_env(:ex_arrow, :flight_client_impl, ExArrow.Flight.ClientMock)
      on_exit(fn -> Application.delete_env(:ex_arrow, :flight_client_impl) end)

      fake_client = %Client{resource: make_ref()}

      actions = [
        %ActionType{type: "clear", description: "Clear echo data."},
        %ActionType{type: "ping", description: "Ping the server."}
      ]

      Mox.expect(ExArrow.Flight.ClientMock, :list_actions, fn ^fake_client ->
        {:ok, actions}
      end)

      assert {:ok, ^actions} = Client.list_actions(fake_client)
    end

    test "list_actions/1 propagates error" do
      Application.put_env(:ex_arrow, :flight_client_impl, ExArrow.Flight.ClientMock)
      on_exit(fn -> Application.delete_env(:ex_arrow, :flight_client_impl) end)

      fake_client = %Client{resource: make_ref()}

      Mox.expect(ExArrow.Flight.ClientMock, :list_actions, fn _ ->
        {:error, "timeout"}
      end)

      assert {:error, "timeout"} = Client.list_actions(fake_client)
    end
  end

  describe "with Mox mock — do_action" do
    test "do_action/3 delegates to impl" do
      Application.put_env(:ex_arrow, :flight_client_impl, ExArrow.Flight.ClientMock)
      on_exit(fn -> Application.delete_env(:ex_arrow, :flight_client_impl) end)

      fake_client = %Client{resource: make_ref()}

      Mox.expect(ExArrow.Flight.ClientMock, :do_action, fn ^fake_client, "ping", <<>> ->
        {:ok, ["pong"]}
      end)

      assert {:ok, ["pong"]} = Client.do_action(fake_client, "ping", <<>>)
    end

    test "do_action/3 default body is <<>>" do
      Application.put_env(:ex_arrow, :flight_client_impl, ExArrow.Flight.ClientMock)
      on_exit(fn -> Application.delete_env(:ex_arrow, :flight_client_impl) end)

      fake_client = %Client{resource: make_ref()}

      Mox.expect(ExArrow.Flight.ClientMock, :do_action, fn ^fake_client, "ping", <<>> ->
        {:ok, ["pong"]}
      end)

      # Two-arg form uses the default body
      assert {:ok, ["pong"]} = Client.do_action(fake_client, "ping")
    end

    test "do_action/3 with non-empty body" do
      Application.put_env(:ex_arrow, :flight_client_impl, ExArrow.Flight.ClientMock)
      on_exit(fn -> Application.delete_env(:ex_arrow, :flight_client_impl) end)

      fake_client = %Client{resource: make_ref()}

      Mox.expect(ExArrow.Flight.ClientMock, :do_action, fn ^fake_client, "custom", <<1, 2, 3>> ->
        {:ok, [<<4, 5>>]}
      end)

      assert {:ok, [<<4, 5>>]} = Client.do_action(fake_client, "custom", <<1, 2, 3>>)
    end

    test "do_action/3 propagates error" do
      Application.put_env(:ex_arrow, :flight_client_impl, ExArrow.Flight.ClientMock)
      on_exit(fn -> Application.delete_env(:ex_arrow, :flight_client_impl) end)

      fake_client = %Client{resource: make_ref()}

      Mox.expect(ExArrow.Flight.ClientMock, :do_action, fn _, _, _ ->
        {:error, "unknown action"}
      end)

      assert {:error, "unknown action"} = Client.do_action(fake_client, "nope", <<>>)
    end
  end
end
