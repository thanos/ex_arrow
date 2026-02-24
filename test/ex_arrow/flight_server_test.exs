defmodule ExArrow.Flight.ServerTest do
  use ExUnit.Case, async: true

  describe "start_link/2" do
    test "returns not_implemented (stub until NIF)" do
      assert ExArrow.Flight.Server.start_link(9090) == {:error, :not_implemented}
    end

    test "accepts opts" do
      assert ExArrow.Flight.Server.start_link(9090, []) == {:error, :not_implemented}
    end
  end
end
