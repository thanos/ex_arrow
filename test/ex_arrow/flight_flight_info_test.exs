defmodule ExArrow.Flight.FlightInfoTest do
  use ExUnit.Case, async: true

  alias ExArrow.Flight.FlightInfo

  @schema_bytes <<1, 2, 3, 4>>

  defp native(opts \\ []) do
    schema = Keyword.get(opts, :schema, @schema_bytes)
    descriptor = Keyword.get(opts, :descriptor, {:cmd, "echo"})
    endpoints = Keyword.get(opts, :endpoints, [{"echo", []}])
    total_records = Keyword.get(opts, :total_records, 10)
    total_bytes = Keyword.get(opts, :total_bytes, -1)
    {schema, descriptor, endpoints, total_records, total_bytes}
  end

  describe "from_native/1" do
    test "constructs struct from a 5-tuple with CMD descriptor" do
      result = FlightInfo.from_native(native())

      assert %FlightInfo{} = result
      assert result.schema_bytes == @schema_bytes
      assert result.descriptor == {:cmd, "echo"}
      assert result.total_records == 10
      assert result.total_bytes == -1
    end

    test "preserves PATH descriptor unchanged" do
      result = FlightInfo.from_native(native(descriptor: {:path, ["flights", "echo"]}))
      assert result.descriptor == {:path, ["flights", "echo"]}
    end

    test "preserves nil descriptor" do
      result = FlightInfo.from_native(native(descriptor: nil))
      assert result.descriptor == nil
    end

    test "decodes endpoints into maps with :ticket and :locations keys" do
      endpoints = [
        {"echo", ["grpc://host1:9999", "grpc://host2:9999"]},
        {"other", []}
      ]

      result = FlightInfo.from_native(native(endpoints: endpoints))

      assert [ep1, ep2] = result.endpoints
      assert ep1.ticket == "echo"
      assert ep1.locations == ["grpc://host1:9999", "grpc://host2:9999"]
      assert ep2.ticket == "other"
      assert ep2.locations == []
    end

    test "returns empty endpoints list when there are no endpoints" do
      result = FlightInfo.from_native(native(endpoints: []))
      assert result.endpoints == []
    end

    test "single endpoint with no locations" do
      result = FlightInfo.from_native(native(endpoints: [{"ticket_a", []}]))
      assert [%{ticket: "ticket_a", locations: []}] = result.endpoints
    end

    test "total_records and total_bytes are preserved" do
      result = FlightInfo.from_native(native(total_records: 42, total_bytes: 1024))
      assert result.total_records == 42
      assert result.total_bytes == 1024
    end

    test "total_records -1 signals unknown" do
      result = FlightInfo.from_native(native(total_records: -1, total_bytes: -1))
      assert result.total_records == -1
      assert result.total_bytes == -1
    end

    test "schema_bytes is preserved as-is" do
      bytes = <<255, 0, 128, 64>>
      result = FlightInfo.from_native(native(schema: bytes))
      assert result.schema_bytes == bytes
    end

    test "empty schema_bytes is allowed" do
      result = FlightInfo.from_native(native(schema: <<>>))
      assert result.schema_bytes == <<>>
    end
  end

  describe "struct" do
    test "fields default to nil when not set" do
      fi = %FlightInfo{}
      assert fi.schema_bytes == nil
      assert fi.descriptor == nil
      assert fi.endpoints == nil
      assert fi.total_records == nil
      assert fi.total_bytes == nil
    end
  end
end
