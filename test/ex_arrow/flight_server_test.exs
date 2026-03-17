defmodule ExArrow.Flight.ServerTest do
  use ExUnit.Case, async: false

  describe "start_link/2" do
    test "start_link(0) returns server, port is positive, stop/1 stops it" do
      assert {:ok, server} = ExArrow.Flight.Server.start_link(0, [])
      assert {:ok, port} = ExArrow.Flight.Server.port(server)
      assert is_integer(port) and port > 0
      assert :ok = ExArrow.Flight.Server.stop(server)
    end

    test "default host is 127.0.0.1" do
      assert {:ok, server} = ExArrow.Flight.Server.start_link(0)
      assert {:ok, "127.0.0.1"} = ExArrow.Flight.Server.host(server)
      assert :ok = ExArrow.Flight.Server.stop(server)
    end

    @tag :flight
    test "host: 0.0.0.0 binds to all interfaces" do
      assert {:ok, server} = ExArrow.Flight.Server.start_link(0, host: "0.0.0.0")
      assert {:ok, "0.0.0.0"} = ExArrow.Flight.Server.host(server)
      assert {:ok, port} = ExArrow.Flight.Server.port(server)
      assert port > 0

      # A client can connect via loopback even when the server listens on all interfaces
      assert {:ok, client} = ExArrow.Flight.Client.connect("localhost", port, [])
      assert %ExArrow.Flight.Client{} = client

      assert :ok = ExArrow.Flight.Server.stop(server)
    end

    test "accepts opts without host (backward-compatible)" do
      assert {:ok, server} = ExArrow.Flight.Server.start_link(0, [])
      assert :ok = ExArrow.Flight.Server.stop(server)
    end

    # ── TLS option validation (pure-Elixir, no network required) ──────────────

    test "returns error when :tls is not a keyword list" do
      assert {:error, msg} = ExArrow.Flight.Server.start_link(0, tls: "bad")
      assert msg =~ "keyword list"
    end

    test "returns error when :tls is missing :cert_pem" do
      assert {:error, msg} = ExArrow.Flight.Server.start_link(0, tls: [key_pem: "k"])
      assert msg =~ "cert_pem"
    end

    test "returns error when :tls is missing :key_pem" do
      assert {:error, msg} = ExArrow.Flight.Server.start_link(0, tls: [cert_pem: "c"])
      assert msg =~ "key_pem"
    end
  end
end
