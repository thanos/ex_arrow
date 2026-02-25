defmodule ExArrow.ADBC.ErrorTest do
  use ExUnit.Case, async: true

  alias ExArrow.ADBC.Error

  describe "from_message/1" do
    test "wraps a string in an Error struct with message set" do
      err = Error.from_message("driver load failed")
      assert %Error{message: "driver load failed", sqlstate: nil, vendor_code: nil} = err
    end

    test "leaves sqlstate and vendor_code as nil" do
      err = Error.from_message("any message")
      assert err.sqlstate == nil
      assert err.vendor_code == nil
    end

    test "accepts empty string" do
      err = Error.from_message("")
      assert err.message == ""
    end
  end

  describe "message/1" do
    test "returns message from an Error struct" do
      err = Error.from_message("connection refused")
      assert Error.message(err) == "connection refused"
    end

    test "returns the string when given a raw string" do
      assert Error.message("raw error string") == "raw error string"
    end

    test "pass-through for empty string" do
      assert Error.message("") == ""
    end
  end
end
