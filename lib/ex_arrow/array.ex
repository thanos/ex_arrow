defmodule ExArrow.Array do
  @moduledoc """
  Arrow array handle (opaque reference to native array).

  Column data lives in native memory. This module provides a stable handle;
  copying to the BEAM heap is done only when explicitly requested (future API).
  """
  @opaque t :: %__MODULE__{resource: reference()}
  defstruct [:resource]
end
