defmodule ExArrow.FlightSQL.OptionsTest do
  use ExUnit.Case, async: true

  alias ExArrow.FlightSQL.{Error, Options}

  # ── URI parsing ───────────────────────────────────────────────────────────────

  describe "URI parsing — host:port" do
    test "standard host:port" do
      assert {:ok, %{host: "localhost", port: 32_010}} = Options.parse("localhost:32010", [])
    end

    test "numeric host (IPv4)" do
      assert {:ok, %{host: "127.0.0.1", port: 8_815}} = Options.parse("127.0.0.1:8815", [])
    end

    test "host only uses default port 32010 (Arrow Flight SQL convention)" do
      assert {:ok, %{host: "myserver", port: 32_010}} = Options.parse("myserver", [])
    end

    test "port 1 is the minimum accepted" do
      assert {:ok, %{port: 1}} = Options.parse("host:1", [])
    end

    test "port 65535 is the maximum accepted" do
      assert {:ok, %{port: 65_535}} = Options.parse("host:65535", [])
    end

    test "port 0 is rejected" do
      assert {:error, %Error{code: :invalid_option}} = Options.parse("host:0", [])
    end

    test "port 65536 is rejected" do
      assert {:error, %Error{code: :invalid_option}} = Options.parse("host:65536", [])
    end

    test "non-numeric port is rejected" do
      assert {:error, %Error{code: :invalid_option}} = Options.parse("host:abc", [])
    end

    test "extra colons in port segment are rejected" do
      # "host:80:extra" → port_str = "80:extra" → {80, ":extra"} → remainder non-empty
      assert {:error, %Error{code: :invalid_option}} = Options.parse("host:80:extra", [])
    end
  end

  describe "URI parsing — bracketed IPv6" do
    test "[::1]:port parses loopback IPv6" do
      assert {:ok, %{host: "::1", port: 9_000}} = Options.parse("[::1]:9000", [])
    end

    test "[fe80::1]:port parses link-local IPv6" do
      assert {:ok, %{host: "fe80::1", port: 5_005}} = Options.parse("[fe80::1]:5005", [])
    end

    test "[::1] without port is rejected" do
      assert {:error, %Error{code: :invalid_option}} = Options.parse("[::1]", [])
    end

    test "unclosed bracket is rejected" do
      assert {:error, %Error{code: :invalid_option}} = Options.parse("[::1", [])
    end

    test "bracket without port is rejected" do
      assert {:error, %Error{code: :invalid_option}} = Options.parse("[::1]:abc", [])
    end
  end

  # ── TLS auto-detection ────────────────────────────────────────────────────────

  describe "TLS auto-detection (nil opt)" do
    test "localhost → :plaintext" do
      assert {:ok, %{tls_mode: :plaintext}} = Options.parse("localhost:32010", [])
    end

    test "127.0.0.1 → :plaintext" do
      assert {:ok, %{tls_mode: :plaintext}} = Options.parse("127.0.0.1:32010", [])
    end

    test "[::1] → :plaintext (IPv6 loopback)" do
      assert {:ok, %{tls_mode: :plaintext}} = Options.parse("[::1]:32010", [])
    end

    test "ip6-localhost → :plaintext" do
      assert {:ok, %{tls_mode: :plaintext}} = Options.parse("ip6-localhost:32010", [])
    end

    test "remote hostname → :system_certs" do
      assert {:ok, %{tls_mode: :system_certs}} = Options.parse("remote.server:32010", [])
    end

    test "remote IP → :system_certs" do
      assert {:ok, %{tls_mode: :system_certs}} = Options.parse("10.0.0.1:32010", [])
    end
  end

  describe "TLS explicit option" do
    test "tls: false → :plaintext regardless of host" do
      assert {:ok, %{tls_mode: :plaintext}} =
               Options.parse("remote.server:32010", tls: false)
    end

    test "tls: true → :system_certs" do
      assert {:ok, %{tls_mode: :system_certs}} = Options.parse("localhost:32010", tls: true)
    end

    test "tls: [ca_cert_pem: pem] → {:custom_ca, pem}" do
      pem = "-----BEGIN CERTIFICATE-----\nZg==\n-----END CERTIFICATE-----"

      assert {:ok, %{tls_mode: {:custom_ca, ^pem}}} =
               Options.parse("localhost:32010", tls: [ca_cert_pem: pem])
    end

    test "tls: :bad_atom → invalid_option" do
      assert {:error, %Error{code: :invalid_option}} =
               Options.parse("localhost:32010", tls: :bad_atom)
    end

    test "tls: unknown keyword → invalid_option" do
      assert {:error, %Error{code: :invalid_option}} =
               Options.parse("localhost:32010", tls: [unknown_key: true])
    end

    test "tls: [ca_cert_pem: non_binary] → invalid_option" do
      assert {:error, %Error{code: :invalid_option}} =
               Options.parse("localhost:32010", tls: [ca_cert_pem: 12_345])
    end

    test "tls: [ca_cert_pem: nil] → invalid_option" do
      assert {:error, %Error{code: :invalid_option}} =
               Options.parse("localhost:32010", tls: [ca_cert_pem: nil])
    end
  end

  # ── Headers validation ────────────────────────────────────────────────────────

  describe "headers validation" do
    test "empty list is accepted" do
      assert {:ok, %{headers: []}} = Options.parse("localhost:32010", headers: [])
    end

    test "single valid tuple is accepted" do
      assert {:ok, %{headers: [{"authorization", "Bearer tok"}]}} =
               Options.parse("localhost:32010", headers: [{"authorization", "Bearer tok"}])
    end

    test "multiple headers preserve insertion order" do
      hdrs = [{"x-a", "1"}, {"x-b", "2"}, {"x-c", "3"}]
      assert {:ok, %{headers: ^hdrs}} = Options.parse("localhost:32010", headers: hdrs)
    end

    test "non-tuple entry → invalid_option" do
      assert {:error, %Error{code: :invalid_option}} =
               Options.parse("localhost:32010", headers: ["not-a-tuple"])
    end

    test "atom key → invalid_option" do
      assert {:error, %Error{code: :invalid_option}} =
               Options.parse("localhost:32010", headers: [{:atom_key, "val"}])
    end

    test "non-string value → invalid_option" do
      assert {:error, %Error{code: :invalid_option}} =
               Options.parse("localhost:32010", headers: [{"key", 123}])
    end

    test "non-list headers → invalid_option" do
      assert {:error, %Error{code: :invalid_option}} =
               Options.parse("localhost:32010", headers: :bad)
    end

    test "missing headers key defaults to empty list" do
      assert {:ok, %{headers: []}} = Options.parse("localhost:32010", [])
    end
  end

  # ── Output map keys ───────────────────────────────────────────────────────────

  describe "result map" do
    test "contains all required keys" do
      assert {:ok, opts} = Options.parse("localhost:32010", [])
      assert is_binary(opts.host)
      assert is_integer(opts.port)

      assert opts.tls_mode in [:plaintext, :system_certs] or
               match?({:custom_ca, _}, opts.tls_mode)

      assert is_list(opts.headers)
    end
  end
end
