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
    adbc_module = Module.safe_concat(["Elixir", "Adbc", "Database"])

    Code.ensure_loaded?(adbc_module) &&
      case Application.get_env(:ex_arrow, :adbc_package) do
        opts when is_list(opts) and opts != [] -> true
        _ -> false
      end
  end
end
