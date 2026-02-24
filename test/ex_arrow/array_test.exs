defmodule ExArrow.ArrayTest do
  use ExUnit.Case, async: true

  describe "Array struct" do
    test "is an opaque handle with resource field" do
      ref = make_ref()
      array = %ExArrow.Array{resource: ref}
      assert array.resource == ref
      assert %ExArrow.Array{} = array
    end

    test "new/1 returns struct with given resource" do
      ref = make_ref()
      assert %ExArrow.Array{resource: ^ref} = ExArrow.Array.new(ref)
    end
  end
end
