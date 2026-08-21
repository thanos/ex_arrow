defmodule ExArrow.Application do
  @moduledoc false
  use Application

  @impl true
  def start(_type, _args) do
    children =
      if adbc_package_configured?() do
        [ExArrow.ADBC.AdbcPackageManager]
      else
        []
      end

    opts = [strategy: :one_for_one, name: ExArrow.Supervisor]
    Supervisor.start_link(children, opts)
  end

  defp adbc_package_configured? do
    case Application.get_env(:ex_arrow, :adbc_package) do
      opts when is_list(opts) and opts != [] ->
        adbc_database_loaded?()

      _ ->
        false
    end
  end

  defp adbc_database_loaded? do
    Code.ensure_loaded?(Module.safe_concat(["Elixir", "Adbc", "Database"]))
  rescue
    # Optional `:adbc` dep — atom may not exist yet; avoid Module.concat/2.
    ArgumentError -> false
  end
end
