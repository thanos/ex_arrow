defmodule ExArrow.ADBC.DatabaseImpl do
  @moduledoc false
  @behaviour ExArrow.ADBC.DatabaseBehaviour

  alias ExArrow.ADBC.Database

  @impl true
  def open(driver_path_or_opts) do
    case validate_driver_spec(driver_path_or_opts) do
      {:ok, spec} ->
        case native().adbc_database_open(spec) do
          {:ok, ref} -> {:ok, %Database{resource: ref}}
          {:error, msg} -> {:error, msg}
        end

      {:error, _} = err ->
        err
    end
  end

  defp native do
    Application.get_env(:ex_arrow, :adbc_native, ExArrow.Native)
  end

  defp validate_driver_spec(path) when is_binary(path) do
    if File.exists?(path) do
      {:ok, path}
    else
      {:error, "driver file not found: " <> path}
    end
  end

  defp validate_driver_spec(opts) when is_list(opts) do
    if Keyword.keyword?(opts) do
      case validate_driver_opts_values(opts) do
        :ok ->
          if has_driver_key?(opts) do
            {:ok, opts}
          else
            {:error,
             "options must include :driver_path or :driver_name, got keys: #{inspect(Keyword.keys(opts))}"}
          end

        {:error, _} = err ->
          err
      end
    else
      {:error,
       "expected options as keyword list (e.g. [driver_path: path] or [driver_name: name]), got: #{inspect(opts, limit: 80)}"}
    end
  end

  defp validate_driver_spec(other) do
    {:error,
     "expected driver path (string) or options (keyword list with :driver_path or :driver_name), got: #{inspect(other, limit: 80)}"}
  end

  defp has_driver_key?(opts) do
    path = Keyword.get(opts, :driver_path)
    name = Keyword.get(opts, :driver_name)
    (is_binary(path) and path != "") or (is_binary(name) and name != "")
  end

  defp validate_driver_opts_values(opts) do
    with :ok <- validate_opt_string(opts, :driver_path, false),
         :ok <- validate_opt_string(opts, :driver_name, false),
         :ok <- validate_opt_string(opts, :uri, true) do
      :ok
    end
  end

  defp validate_opt_string(opts, key, optional) do
    case Keyword.fetch(opts, key) do
      :error when optional ->
        :ok

      :error ->
        :ok

      {:ok, nil} when optional ->
        :ok

      {:ok, nil} ->
        {:error, "option :#{key} must be a non-empty string, got: nil"}

      {:ok, v} when not is_binary(v) ->
        {:error, "option :#{key} must be a string, got: #{inspect(v, limit: 40)}"}

      {:ok, ""} when key in [:driver_path, :driver_name] ->
        {:error, "option :#{key} must be a non-empty string"}

      _ ->
        :ok
    end
  end
end
