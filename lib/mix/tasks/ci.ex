defmodule Mix.Tasks.Ci do
  @shortdoc "Run CI steps locally (script/ci); same as GitHub Actions without push"
  @moduledoc """
  Runs the same steps as the CI workflow: deps.get, compile --warnings-as-errors, test, docs.

  Delegates to `script/ci` so you can also run `./script/ci` from the project root.
  """
  use Mix.Task

  @impl true
  def run(_args) do
    script = Path.join(File.cwd!(), "script/ci")

    unless File.exists?(script) do
      Mix.raise("CI script not found: #{script}")
    end

    {output, status} = System.cmd("bash", [script], cd: File.cwd!())
    IO.write(output)
    if status != 0, do: Mix.raise("CI script exited with code #{status}")
  end
end
