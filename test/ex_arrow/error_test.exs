defmodule ExArrow.ErrorTest do
  use ExUnit.Case, async: true

  describe "Exception.message/1" do
    test "formats code and message when details is nil" do
      err = ExArrow.Error.exception(code: :invalid_schema, message: "bad")
      assert Exception.message(err) == "[invalid_schema] bad"
    end

    test "formats code, message and details when details is present" do
      err =
        ExArrow.Error.exception(
          code: :io_error,
          message: "read failed",
          details: %{path: "/tmp/x"}
        )

      assert Exception.message(err) =~ "[io_error]"
      assert Exception.message(err) =~ "read failed"
      assert Exception.message(err) =~ "path"
      assert Exception.message(err) =~ "/tmp/x"
    end
  end
end
