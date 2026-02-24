defmodule ExArrow.Flight.ActionTypeTest do
  use ExUnit.Case, async: true

  alias ExArrow.Flight.ActionType

  describe "from_native/1" do
    test "constructs struct from a {type, description} tuple" do
      result = ActionType.from_native({"clear", "Clear the stored echo data."})
      assert %ActionType{type: "clear", description: "Clear the stored echo data."} = result
    end

    test "constructs struct for ping action" do
      result = ActionType.from_native({"ping", "Responds with 'pong'."})
      assert result.type == "ping"
      assert result.description == "Responds with 'pong'."
    end

    test "preserves empty description" do
      result = ActionType.from_native({"custom", ""})
      assert result.type == "custom"
      assert result.description == ""
    end

    test "preserves unicode in type and description" do
      result = ActionType.from_native({"éàü", "Ünïcödé description"})
      assert result.type == "éàü"
      assert result.description == "Ünïcödé description"
    end

    test "produces an %ActionType{} struct" do
      assert %ActionType{} = ActionType.from_native({"x", "y"})
    end
  end

  describe "struct" do
    test "fields default to nil when not set" do
      at = %ActionType{}
      assert at.type == nil
      assert at.description == nil
    end

    test "can be pattern-matched on fields" do
      at = %ActionType{type: "clear", description: "desc"}
      assert %ActionType{type: "clear"} = at
    end
  end
end
