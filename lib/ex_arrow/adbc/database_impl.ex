defmodule ExArrow.ADBC.DatabaseImpl do
  @moduledoc false
  @behaviour ExArrow.ADBC.DatabaseBehaviour

  alias ExArrow.ADBC.Database
  alias ExArrow.Native

  @impl true
  def open(driver_path_or_opts) do
    spec = normalize_driver_spec(driver_path_or_opts)

    case Native.adbc_database_open(spec) do
      {:ok, ref} -> {:ok, %Database{resource: ref}}
      {:error, msg} -> {:error, msg}
    end
  end

  defp normalize_driver_spec(path) when is_binary(path), do: path
  defp normalize_driver_spec(opts) when is_list(opts), do: opts
  defp normalize_driver_spec(_), do: []
end
