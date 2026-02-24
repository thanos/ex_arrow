defmodule ExArrow do
  @moduledoc """
  Apache Arrow support for the BEAM: IPC, Flight, and ADBC.

  ExArrow keeps Arrow data in native (Rust) memory and exposes opaque handles
  on the Elixir side. Copying to the BEAM heap happens only when explicitly
  requested.

  ## Public API outline

  ### Core handles (opaque references)
  - `ExArrow.Schema` – schema metadata (fields)
  - `ExArrow.Field` – field name and type
  - `ExArrow.Array` – column array handle
  - `ExArrow.RecordBatch` – batch of columns with shared row count
  - `ExArrow.Table` – table with schema and batches
  - `ExArrow.Stream` – stream of record batches (IPC/Flight)

  ### IPC (`ExArrow.IPC`)
  - `ExArrow.IPC.Reader.from_binary/1`, `from_file/1` – read stream from binary or file
  - `ExArrow.IPC.Writer.to_binary/2`, `to_file/3` – write batches to binary or file

  ### Flight (`ExArrow.Flight`)
  - `ExArrow.Flight.Client` – connect, do_get, do_put
  - `ExArrow.Flight.Server` – minimal server (e.g. echo)

  ### ADBC (`ExArrow.ADBC`)
  - `ExArrow.ADBC.Database.open/1` – open database (driver path or opts)
  - `ExArrow.ADBC.Connection.open/1` – open connection from database
  - `ExArrow.ADBC.Statement` – new, set_sql, execute (returns stream)

  ### Errors
  - `ExArrow.Error` – structured exception with code, message, details

  ## Version
  """

  @doc """
  Returns the native NIF crate version. Used to verify the NIF loads.
  """
  @spec native_version() :: String.t()
  def native_version do
    ExArrow.Native.nif_version()
  end
end
