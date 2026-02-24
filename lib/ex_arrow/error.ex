defmodule ExArrow.Error do
  @moduledoc """
  Structured error type for ExArrow operations.

  All native (NIF) and API errors are mapped to this exception with
  a code, message, and optional details for debugging or logging.
  """
  defexception [:code, :message, :details]

  @type t :: %__MODULE__{
          code: atom() | String.t(),
          message: String.t(),
          details: term() | nil
        }

  @spec exception(list() | String.t()) :: t()
  @doc false
  def exception(args) when is_list(args) do
    struct(__MODULE__, args)
  end

  def exception(message) when is_binary(message) do
    %__MODULE__{code: :unknown, message: message, details: nil}
  end

  @spec message(t()) :: String.t()
  @doc false
  def message(%__MODULE__{code: code, message: msg, details: nil}) do
    "[#{code}] #{msg}"
  end

  def message(%__MODULE__{code: code, message: msg, details: details}) do
    "[#{code}] #{msg} #{inspect(details)}"
  end
end
