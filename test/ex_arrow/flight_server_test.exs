defmodule ExArrow.Flight.ServerTest do
  use ExUnit.Case, async: false

  describe "start_link/2" do
    test "start_link(0) returns server and port, stop/1 stops it" do
      assert {:ok, server} = ExArrow.Flight.Server.start_link(0, [])
      assert {:ok, port} = ExArrow.Flight.Server.port(server)
      assert is_integer(port) and port > 0
      assert :ok = ExArrow.Flight.Server.stop(server)
    end

    test "accepts opts" do
      assert {:ok, server} = ExArrow.Flight.Server.start_link(0, [])
      assert :ok = ExArrow.Flight.Server.stop(server)
    end
  end
end
