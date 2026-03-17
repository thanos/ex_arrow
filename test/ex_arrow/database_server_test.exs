defmodule ExArrow.ADBC.DatabaseServerTest do
  # async: false — mutates :adbc_database_impl application env.
  # set_mox_global — DatabaseServer.init/1 runs in a spawned GenServer process,
  # not the test process, so Mox must be in global mode to allow expectations
  # set on the test process to be honoured by the GenServer process.
  use ExUnit.Case, async: false

  import Mox

  alias ExArrow.ADBC.{Database, DatabaseServer}

  setup :set_mox_global

  setup do
    prev = Application.get_env(:ex_arrow, :adbc_database_impl)
    Application.put_env(:ex_arrow, :adbc_database_impl, ExArrow.ADBC.DatabaseMock)

    on_exit(fn ->
      if prev,
        do: Application.put_env(:ex_arrow, :adbc_database_impl, prev),
        else: Application.delete_env(:ex_arrow, :adbc_database_impl)
    end)

    :ok
  end

  # Use {:global, ref} instead of a dynamic atom — atoms are never GC'd, so
  # generating one per test run would slowly leak them across the test VM.
  # GenServer, GenServer.stop, and DatabaseServer.get/1 all accept {:global, term}.
  defp unique_name, do: {:global, make_ref()}

  # ── start_link/1 ────────────────────────────────────────────────────────────

  describe "start_link/1" do
    test "starts the server when Database.open succeeds" do
      fake_db = %Database{resource: make_ref()}
      stub(ExArrow.ADBC.DatabaseMock, :open, fn _opts -> {:ok, fake_db} end)

      assert {:ok, pid} = DatabaseServer.start_link(name: unique_name(), driver_path: "fake.so")
      assert is_pid(pid)
      assert Process.alive?(pid)

      GenServer.stop(pid)
    end

    test "returns error when Database.open fails" do
      stub(ExArrow.ADBC.DatabaseMock, :open, fn _opts -> {:error, "driver not found"} end)

      # proc_lib:start_link (used internally by GenServer) may deliver an EXIT
      # signal to the calling process before it can unlink, so we trap exits
      # for this test to prevent ExUnit from treating the signal as a crash.
      Process.flag(:trap_exit, true)

      assert {:error, "driver not found"} =
               DatabaseServer.start_link(name: unique_name(), driver_path: "bad.so")

      # Drain any lingering EXIT messages so they don't affect later tests.
      receive do
        {:EXIT, _, _} -> :ok
      after
        0 -> :ok
      end
    end

    test "registers the server under the given name" do
      fake_db = %Database{resource: make_ref()}
      stub(ExArrow.ADBC.DatabaseMock, :open, fn _opts -> {:ok, fake_db} end)

      {:global, key} = name = unique_name()
      {:ok, pid} = DatabaseServer.start_link(name: name, driver_path: "fake.so")

      assert :global.whereis_name(key) == pid

      GenServer.stop(pid)
    end

    test "uses __MODULE__ as default name when :name is omitted" do
      fake_db = %Database{resource: make_ref()}
      stub(ExArrow.ADBC.DatabaseMock, :open, fn _opts -> {:ok, fake_db} end)

      # May already be running; stop it first if so
      case Process.whereis(DatabaseServer) do
        nil -> :ok
        existing -> GenServer.stop(existing)
      end

      {:ok, pid} = DatabaseServer.start_link(driver_path: "fake.so")
      assert Process.whereis(DatabaseServer) == pid

      GenServer.stop(pid)
    end
  end

  # ── get/1 ────────────────────────────────────────────────────────────────────

  describe "get/1" do
    test "returns the Database struct held by the server" do
      fake_db = %Database{resource: make_ref()}
      stub(ExArrow.ADBC.DatabaseMock, :open, fn _opts -> {:ok, fake_db} end)

      name = unique_name()
      {:ok, pid} = DatabaseServer.start_link(name: name, driver_path: "fake.so")

      assert DatabaseServer.get(name) == fake_db

      GenServer.stop(pid)
    end
  end

  # ── terminate/2 ─────────────────────────────────────────────────────────────

  describe "terminate/2" do
    test "server stops cleanly (Database.close is a struct no-op)" do
      fake_db = %Database{resource: make_ref()}
      stub(ExArrow.ADBC.DatabaseMock, :open, fn _opts -> {:ok, fake_db} end)

      name = unique_name()
      {:ok, pid} = DatabaseServer.start_link(name: name, driver_path: "fake.so")

      # GenServer.stop/1 triggers terminate/2 → Database.close/1 (always :ok)
      assert :ok = GenServer.stop(pid)
      refute Process.alive?(pid)
    end
  end
end
