defmodule ExArrow.Stream do
  @moduledoc """
  Arrow stream handle (opaque reference to native stream).

  Used for IPC streaming and Flight result streams. Yields record batches
  via an Elixir iterator; data stays in native memory until consumed.
  """
  alias ExArrow.RecordBatch

  @opaque t :: %__MODULE__{resource: reference()}
  defstruct [:resource]

  @doc """
  Returns the next record batch from the stream, or nil when done.
  Stub: returns nil until NIF is implemented.
  """
  @spec next(t()) :: RecordBatch.t() | nil
  def next(_stream) do
    nil
  end
end
