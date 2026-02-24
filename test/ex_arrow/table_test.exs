defmodule ExArrow.TableTest do
  use ExUnit.Case, async: true

  describe "stub API" do
    test "schema/1 returns nil until NIF is implemented" do
      table = %ExArrow.Table{resource: make_ref()}
      assert ExArrow.Table.schema(table) == nil
    end

    test "num_rows/1 returns 0 until NIF is implemented" do
      table = %ExArrow.Table{resource: make_ref()}
      assert ExArrow.Table.num_rows(table) == 0
    end
  end
end
