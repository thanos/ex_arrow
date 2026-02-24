defmodule ExArrow.MixProject do
  use Mix.Project

  def project do
    [
      app: :ex_arrow,
      version: "0.1.0",
      elixir: "~> 1.18",
      start_permanent: Mix.env() == :prod,
      deps: deps(),
      docs: docs(),
      aliases: aliases(),
      test_coverage: [tool: ExCoveralls],
      preferred_cli_env: [
        coveralls: :test,
        "coveralls.html": :test,
        "coveralls.github": :test
      ]
    ]
  end

  defp aliases do
    [ci: "ci"]
  end

  def application do
    [
      extra_applications: [:logger]
    ]
  end

  defp deps do
    [
      {:rustler, "~> 0.32"},
      {:ex_doc, "~> 0.34", only: :dev},
      {:credo, "~> 1.7", only: [:dev, :test], runtime: false},
      {:excoveralls, "~> 0.18", only: :test},
      {:stream_data, "~> 0.6", only: :test}
    ]
  end

  defp docs do
    [
      main: "overview",
      formatters: ["html"],
      source_url: "https://github.com/your-org/ex_arrow",
      extras: ["docs/overview.md", "docs/memory_model.md", "docs/ipc_guide.md"]
    ]
  end
end
