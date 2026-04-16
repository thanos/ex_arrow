defmodule ExArrow.FlightSQL.ErrorTest do
  use ExUnit.Case, async: true

  alias ExArrow.FlightSQL.Error

  describe "message/1" do
    test "formats code and message without details" do
      err = %Error{code: :invalid_argument, message: "syntax error near FROM"}
      assert Error.message(err) == "[invalid_argument] syntax error near FROM"
    end

    test "appends details when present" do
      err = %Error{code: :transport_error, message: "connection refused", details: %{port: 9999}}
      msg = Error.message(err)
      assert msg =~ "[transport_error]"
      assert msg =~ "connection refused"
      assert msg =~ "9999"
    end
  end

  describe "from_nif/1" do
    test "builds error from NIF 3-tuple" do
      err = Error.from_nif({:invalid_argument, 3, "syntax error"})
      assert err.code == :invalid_argument
      assert err.message == "syntax error"
      assert err.grpc_status == 3
    end

    test "sets grpc_status to nil when status integer is 0" do
      err = Error.from_nif({:transport_error, 0, "connection refused"})
      assert err.grpc_status == nil
    end

    test "preserves non-zero grpc_status" do
      err = Error.from_nif({:unauthenticated, 16, "missing token"})
      assert err.grpc_status == 16
    end
  end

  describe "from_string/2" do
    test "builds error with code and message" do
      err = Error.from_string(:invalid_option, "bad tls value")
      assert err.code == :invalid_option
      assert err.message == "bad tls value"
      assert err.grpc_status == nil
      assert err.details == nil
    end
  end

  describe "Exception protocol" do
    test "is a valid exception (raise/rescue)" do
      err = %Error{code: :server_error, message: "internal error"}

      result =
        try do
          raise err
        rescue
          e in Error -> {:caught, e.code}
        end

      assert result == {:caught, :server_error}
    end
  end

  describe "error codes" do
    test "all documented codes are valid atoms" do
      codes = [
        :transport_error,
        :server_error,
        :unimplemented,
        :unauthenticated,
        :permission_denied,
        :not_found,
        :invalid_argument,
        :protocol_error,
        :multi_endpoint,
        :invalid_option,
        :conversion_error
      ]

      for code <- codes do
        err = %Error{code: code, message: "test"}
        assert is_binary(Error.message(err))
      end
    end
  end

  describe "message/1 edge cases" do
    test "nil details branch is not reached when details is nil (struct default)" do
      err = %Error{code: :transport_error, message: "oops"}
      assert Error.message(err) == "[transport_error] oops"
    end

    test "details branch renders when details is a map" do
      err = %Error{code: :server_error, message: "boom", details: %{code: 500}}
      msg = Error.message(err)
      assert msg =~ "[server_error]"
      assert msg =~ "boom"
      assert msg =~ "500"
    end

    test "details branch renders when details is a string" do
      err = %Error{code: :not_found, message: "table missing", details: "extra context"}
      msg = Error.message(err)
      assert msg =~ "extra context"
    end
  end

  describe "from_nif/1 grpc_status handling" do
    test "grpc_status 0 maps to nil" do
      err = Error.from_nif({:transport_error, 0, "channel closed"})
      assert err.grpc_status == nil
    end

    test "non-zero grpc_status is preserved" do
      err = Error.from_nif({:invalid_argument, 3, "bad sql"})
      assert err.grpc_status == 3
    end
  end
end
