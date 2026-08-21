defmodule ExArrow.Parquet.OptsTest do
  use ExUnit.Case, async: true

  alias ExArrow.Parquet.Opts

  describe "validate_read/1" do
    test "accepts empty opts" do
      assert {:ok, []} = Opts.validate_read([])
    end

    test "rejects non-keyword and unknown keys" do
      assert {:error, msg} = Opts.validate_read(%{columns: ["a"]})
      assert msg =~ "keyword"

      assert {:error, msg} = Opts.validate_read(foo: 1)
      assert msg =~ "unknown"
    end

    test "validates columns" do
      assert {:ok, [columns: ["a"]]} = Opts.validate_read(columns: ["a"])
      assert {:error, _} = Opts.validate_read(columns: [])
      assert {:error, _} = Opts.validate_read(columns: [:a])
      assert {:error, _} = Opts.validate_read(columns: "a")
    end

    test "validates row_groups" do
      assert {:ok, [row_groups: [0, 2]]} = Opts.validate_read(row_groups: [0, 2])
      assert {:error, _} = Opts.validate_read(row_groups: [-1])
      assert {:error, _} = Opts.validate_read(row_groups: 0)
    end

    test "validates filter AST including and/or" do
      assert {:ok, _} = Opts.validate_read(filters: {:gt, "score", 0.9})
      assert {:ok, _} = Opts.validate_read(filters: {:and, [{:gt, "x", 1}, {:lt, "x", 10}]})
      assert {:ok, _} = Opts.validate_read(filters: {:or, [{:eq, "n", "a"}, {:eq, "n", "b"}]})

      assert {:error, _} = Opts.validate_read(filters: {:and, []})
      assert {:error, _} = Opts.validate_read(filters: {:or, []})
      assert {:error, _} = Opts.validate_read(filters: {:and, [{:gt, "x", :atom}]})
      assert {:error, _} = Opts.validate_read(filters: {:eq, :col, 1})
      assert {:error, _} = Opts.validate_read(filters: {:eq, "col", ~D[2024-01-01]})
      assert {:error, _} = Opts.validate_read(filters: {:nope, "x", 1})
    end
  end

  describe "validate_write/1" do
    test "accepts compression aliases and zstd levels" do
      assert {:ok, [compression: :none]} = Opts.validate_write(compression: :none)
      assert {:ok, [compression: :uncompressed]} = Opts.validate_write(compression: :uncompressed)
      assert {:ok, [compression: {:zstd, 3}]} = Opts.validate_write(compression: {:zstd, 3})
      assert {:error, _} = Opts.validate_write(compression: {:zstd, 0})
      assert {:error, _} = Opts.validate_write(compression: {:zstd, 23})
      assert {:error, _} = Opts.validate_write(compression: :brotli)
    end

    test "validates row_group_size and dictionary" do
      assert {:ok, [row_group_size: 64]} = Opts.validate_write(row_group_size: 64)
      assert {:error, _} = Opts.validate_write(row_group_size: 0)
      assert {:error, _} = Opts.validate_write(row_group_size: -1)
      assert {:error, _} = Opts.validate_write(row_group_size: 1.5)

      assert {:ok, [dictionary: false]} = Opts.validate_write(dictionary: false)
      assert {:error, _} = Opts.validate_write(dictionary: "yes")
    end

    test "rejects unknown write keys and non-keyword" do
      assert {:error, _} = Opts.validate_write(columns: ["a"])
      assert {:error, _} = Opts.validate_write(%{})
    end
  end
end
