defmodule ExArrow.Schema.MapperTest do
  use ExUnit.Case, async: true

  alias ExArrow.Schema.Mapper

  describe "nx_dtype_to_arrow/1" do
    for {nx_dtype, arrow_str} <- [
          {{:s, 8}, "s8"},
          {{:s, 16}, "s16"},
          {{:s, 32}, "s32"},
          {{:s, 64}, "s64"},
          {{:u, 8}, "u8"},
          {{:u, 16}, "u16"},
          {{:u, 32}, "u32"},
          {{:u, 64}, "u64"},
          {{:f, 32}, "f32"},
          {{:f, 64}, "f64"}
        ] do
      test "maps #{inspect(nx_dtype)} to #{inspect(arrow_str)}" do
        assert {:ok, unquote(arrow_str)} = Mapper.nx_dtype_to_arrow(unquote(nx_dtype))
      end
    end

    test "returns error for unsupported dtype" do
      assert {:error, msg} = Mapper.nx_dtype_to_arrow({:bf, 16})
      assert msg =~ "unsupported"
    end
  end

  describe "arrow_dtype_to_nx/1" do
    for {arrow_str, nx_dtype} <- [
          {"s8", {:s, 8}},
          {"s16", {:s, 16}},
          {"s32", {:s, 32}},
          {"s64", {:s, 64}},
          {"u8", {:u, 8}},
          {"u16", {:u, 16}},
          {"u32", {:u, 32}},
          {"u64", {:u, 64}},
          {"f32", {:f, 32}},
          {"f64", {:f, 64}}
        ] do
      test "maps #{inspect(arrow_str)} to #{inspect(nx_dtype)}" do
        assert {:ok, unquote(nx_dtype)} = Mapper.arrow_dtype_to_nx(unquote(arrow_str))
      end
    end

    test "maps bool to {:u, 8}" do
      assert {:ok, {:u, 8}} = Mapper.arrow_dtype_to_nx("bool")
    end

    test "returns error for utf8" do
      assert {:error, msg} = Mapper.arrow_dtype_to_nx("utf8")
      assert msg =~ "unsupported"
    end
  end

  describe "explorer_dtype_to_arrow/1" do
    test "maps :integer to s64" do
      assert {:ok, "s64"} = Mapper.explorer_dtype_to_arrow(:integer)
    end

    test "maps :float to f64" do
      assert {:ok, "f64"} = Mapper.explorer_dtype_to_arrow(:float)
    end

    test "maps :boolean to bool" do
      assert {:ok, "bool"} = Mapper.explorer_dtype_to_arrow(:boolean)
    end

    test "maps :string to utf8" do
      assert {:ok, "utf8"} = Mapper.explorer_dtype_to_arrow(:string)
    end

    test "returns error for :date" do
      assert {:error, msg} = Mapper.explorer_dtype_to_arrow(:date)
      assert msg =~ "unsupported"
    end

    test "returns error for :datetime" do
      assert {:error, msg} = Mapper.explorer_dtype_to_arrow(:datetime)
      assert msg =~ "unsupported"
    end

    test "returns error for :nil" do
      assert {:error, msg} = Mapper.explorer_dtype_to_arrow(nil)
      assert msg =~ "unsupported"
    end
  end

  describe "arrow_dtype_to_explorer/1" do
    for s <- ["s8", "s16", "s32", "s64", "u8", "u16", "u32", "u64"] do
      test "maps #{inspect(s)} to :integer" do
        assert {:ok, :integer} = Mapper.arrow_dtype_to_explorer(unquote(s))
      end
    end

    for s <- ["f32", "f64"] do
      test "maps #{inspect(s)} to :float" do
        assert {:ok, :float} = Mapper.arrow_dtype_to_explorer(unquote(s))
      end
    end

    test "maps bool to :boolean" do
      assert {:ok, :boolean} = Mapper.arrow_dtype_to_explorer("bool")
    end

    test "maps utf8 to :string" do
      assert {:ok, :string} = Mapper.arrow_dtype_to_explorer("utf8")
    end

    test "returns error for unknown dtype" do
      assert {:error, msg} = Mapper.arrow_dtype_to_explorer("timestamp")
      assert msg =~ "unsupported"
    end
  end

  describe "arrow_type_atom_to_dtype/1" do
    for {atom, str} <- [
          {:boolean, "bool"},
          {:int8, "s8"},
          {:int16, "s16"},
          {:int32, "s32"},
          {:int64, "s64"},
          {:uint8, "u8"},
          {:uint16, "u16"},
          {:uint32, "u32"},
          {:uint64, "u64"},
          {:float32, "f32"},
          {:float64, "f64"},
          {:utf8, "utf8"},
          {:large_utf8, "utf8"}
        ] do
      test "maps #{atom} to #{inspect(str)}" do
        assert {:ok, unquote(str)} = Mapper.arrow_type_atom_to_dtype(unquote(atom))
      end
    end

    test "returns error for :timestamp" do
      assert {:error, msg} = Mapper.arrow_type_atom_to_dtype(:timestamp)
      assert msg =~ "unsupported"
    end

    test "returns error for :null" do
      assert {:error, msg} = Mapper.arrow_type_atom_to_dtype(:null)
      assert msg =~ "unsupported"
    end
  end

  describe "arrow_dtype_to_type_atom/1" do
    for {str, atom} <- [
          {"s8", :int8},
          {"s16", :int16},
          {"s32", :int32},
          {"s64", :int64},
          {"u8", :uint8},
          {"u16", :uint16},
          {"u32", :uint32},
          {"u64", :uint64},
          {"f32", :float32},
          {"f64", :float64},
          {"bool", :boolean},
          {"utf8", :utf8}
        ] do
      test "maps #{inspect(str)} to #{atom}" do
        assert {:ok, unquote(atom)} = Mapper.arrow_dtype_to_type_atom(unquote(str))
      end
    end

    test "returns error for unknown dtype" do
      assert {:error, msg} = Mapper.arrow_dtype_to_type_atom("date32")
      assert msg =~ "unsupported"
    end
  end

  describe "numeric?/1" do
    for s <- ["s8", "s16", "s32", "s64", "u8", "u16", "u32", "u64", "f32", "f64", "bool"] do
      test "returns true for #{inspect(s)}" do
        assert Mapper.numeric?(unquote(s))
      end
    end

    test "returns false for utf8" do
      refute Mapper.numeric?("utf8")
    end

    test "returns false for unknown dtype" do
      refute Mapper.numeric?("timestamp")
    end
  end

  describe "round-trip: Nx <-> Arrow dtype string" do
    for {nx_dtype, _arrow_str} <- [
          {{:s, 8}, "s8"},
          {{:s, 16}, "s16"},
          {{:s, 32}, "s32"},
          {{:s, 64}, "s64"},
          {{:u, 8}, "u8"},
          {{:u, 16}, "u16"},
          {{:u, 32}, "u32"},
          {{:u, 64}, "u64"},
          {{:f, 32}, "f32"},
          {{:f, 64}, "f64"}
        ] do
      test "nx -> arrow -> nx identity for #{inspect(nx_dtype)}" do
        {:ok, arrow} = Mapper.nx_dtype_to_arrow(unquote(nx_dtype))
        {:ok, nx_back} = Mapper.arrow_dtype_to_nx(arrow)
        assert nx_back == unquote(nx_dtype)
      end
    end
  end

  describe "round-trip: Arrow type atom <-> dtype string" do
    for {atom, _str} <- [
          {:boolean, "bool"},
          {:int8, "s8"},
          {:int16, "s16"},
          {:int32, "s32"},
          {:int64, "s64"},
          {:uint8, "u8"},
          {:uint16, "u16"},
          {:uint32, "u32"},
          {:uint64, "u64"},
          {:float32, "f32"},
          {:float64, "f64"},
          {:utf8, "utf8"}
        ] do
      test "atom -> dtype -> atom identity for #{atom}" do
        {:ok, dtype} = Mapper.arrow_type_atom_to_dtype(unquote(atom))
        {:ok, atom_back} = Mapper.arrow_dtype_to_type_atom(dtype)
        assert atom_back == unquote(atom)
      end
    end
  end
end
