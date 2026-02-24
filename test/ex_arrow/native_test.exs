defmodule ExArrow.NativeTest do
  use ExUnit.Case, async: true

  describe "nif_loaded?/0" do
    @tag :nif
    test "returns true when NIF is loaded (e.g. in test env)" do
      assert ExArrow.Native.nif_loaded?() == true
    end
  end
end
