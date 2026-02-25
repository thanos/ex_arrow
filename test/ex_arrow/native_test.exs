defmodule ExArrow.NativeTest do
  use ExUnit.Case, async: true

  describe "nif_loaded?/0" do
    @tag :nif
    test "returns true when NIF is loaded (e.g. in test env)" do
      assert ExArrow.Native.nif_loaded?() == true
    end
  end

  describe "nif_version/0" do
    @tag :nif
    test "returns a non-empty string when NIF is loaded" do
      version = ExArrow.Native.nif_version()
      assert is_binary(version)
      assert byte_size(version) >= 1
    end
  end

  # Stub functions are exercised indirectly via IPC/Flight/ADBC tests.
  # When NIF is loaded, these dispatch to Rust; coverage of the Elixir stub
  # lines is achieved when the NIF is not loaded (stub is the fallback).
  # We test one stub path when loaded: ipc_test_fixture_binary returns binary.
  describe "IPC NIFs (when loaded)" do
    @tag :nif
    test "ipc_test_fixture_binary returns {:ok, binary}" do
      assert {:ok, bin} = ExArrow.Native.ipc_test_fixture_binary()
      assert is_binary(bin)
      assert byte_size(bin) > 0
    end

    @tag :nif
    test "ipc_reader_from_binary with fixture returns {:ok, stream_ref}" do
      {:ok, bin} = ExArrow.Native.ipc_test_fixture_binary()
      assert {:ok, ref} = ExArrow.Native.ipc_reader_from_binary(bin)
      assert is_reference(ref)
    end

    @tag :nif
    test "ipc_stream_schema with stream ref returns schema ref" do
      {:ok, bin} = ExArrow.Native.ipc_test_fixture_binary()
      {:ok, stream_ref} = ExArrow.Native.ipc_reader_from_binary(bin)
      result = ExArrow.Native.ipc_stream_schema(stream_ref)
      assert is_reference(result)
    end

    @tag :nif
    test "ipc_stream_next with stream ref returns batch ref then :done" do
      {:ok, bin} = ExArrow.Native.ipc_test_fixture_binary()
      {:ok, stream_ref} = ExArrow.Native.ipc_reader_from_binary(bin)
      first = ExArrow.Native.ipc_stream_next(stream_ref)
      assert {:ok, batch_ref} = first
      assert is_reference(batch_ref)
      assert ExArrow.Native.ipc_stream_next(stream_ref) == :done
    end
  end
end
