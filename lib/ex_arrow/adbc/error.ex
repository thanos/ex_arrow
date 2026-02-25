defmodule ExArrow.ADBC.Error do
  @moduledoc """
  ADBC error or diagnostic message.

  Drivers return errors as strings; ExArrow passes them through as `{:error, message}`.
  When available, `message` may include driver-specific details (e.g. SQLSTATE, vendor code).
  Use this module to normalize or inspect errors if you need structured handling.

  Future versions may parse `message` into `sqlstate` and `vendor_code` where the driver
  provides them in a known format.
  """
  defstruct [:message, :sqlstate, :vendor_code]

  @type t :: %__MODULE__{
          message: String.t(),
          sqlstate: String.t() | nil,
          vendor_code: integer() | nil
        }

  @doc """
  Wraps a raw error (string from NIF/driver) into an Error struct.
  `sqlstate` and `vendor_code` are left as nil unless parsed from the message.
  """
  @spec from_message(String.t()) :: t()
  def from_message(message) when is_binary(message) do
    %__MODULE__{message: message, sqlstate: nil, vendor_code: nil}
  end

  @doc """
  Returns the error message (string).
  Accepts either an Error struct or a raw string for convenience.
  """
  @spec message(t() | String.t()) :: String.t()
  def message(%__MODULE__{message: msg}), do: msg
  def message(msg) when is_binary(msg), do: msg
end
