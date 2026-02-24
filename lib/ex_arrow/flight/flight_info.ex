defmodule ExArrow.Flight.FlightInfo do
  @moduledoc """
  Metadata describing a named Arrow dataset served by a Flight server.

  Returned by `ExArrow.Flight.Client.list_flights/2` and
  `ExArrow.Flight.Client.get_flight_info/2`.

  Fields:
  - `schema_bytes` — IPC-encoded Arrow schema (raw binary).
  - `descriptor` — how the dataset is identified: `{:cmd, binary()}` or
    `{:path, [String.t()]}`.
  - `endpoints` — one or more `%{ticket: binary(), locations: [String.t()]}`
    maps describing where and how to retrieve the data.
  - `total_records` — total row count across all endpoints, or `-1` if unknown.
  - `total_bytes` — total byte size, or `-1` if unknown.
  """

  @type descriptor :: {:cmd, binary()} | {:path, [String.t()]} | nil
  @type endpoint :: %{ticket: binary(), locations: [String.t()]}

  @type t :: %__MODULE__{
          schema_bytes: binary() | nil,
          descriptor: descriptor(),
          endpoints: [endpoint()] | nil,
          total_records: integer() | nil,
          total_bytes: integer() | nil
        }

  defstruct [:schema_bytes, :descriptor, :endpoints, :total_records, :total_bytes]

  @spec from_native({any(), any(), any(), any(), any()}) :: ExArrow.Flight.FlightInfo.t()
  @doc false
  def from_native({schema_bytes, descriptor, endpoints, total_records, total_bytes}) do
    %__MODULE__{
      schema_bytes: schema_bytes,
      descriptor: descriptor,
      endpoints: Enum.map(endpoints, fn {ticket, locs} -> %{ticket: ticket, locations: locs} end),
      total_records: total_records,
      total_bytes: total_bytes
    }
  end
end
