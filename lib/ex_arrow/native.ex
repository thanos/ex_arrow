defmodule ExArrow.Native do
  @moduledoc false
  use Rustler,
    otp_app: :ex_arrow,
    crate: :ex_arrow_native,
    path: "native/ex_arrow_native",
    mode: if(Mix.env() == :prod, do: :release, else: :debug)

  @doc """
  Returns the native crate version. Used to verify the NIF loads.
  """
  def nif_version, do: :erlang.nif_error(:nif_not_loaded)
end
