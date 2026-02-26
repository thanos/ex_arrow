defmodule ExArrow.MixProject do
  use Mix.Project

  def project do
    [
      app: :ex_arrow,
      version: "0.1.0",
      # Same as Explorer: support OTP 25 (NIF 2.15) and OTP 26 (NIF 2.16) users
      elixir: "~> 1.14",
      start_permanent: Mix.env() == :prod,
      elixirc_paths: elixirc_paths(Mix.env()),
      deps: deps(),
      docs: docs(),
      aliases: aliases(),
      package: package(),
      test_coverage: [tool: ExCoveralls],
      dialyzer: [
        plt_file: {:no_warn, "priv/plts/project.plt"},
        plt_add_apps: [:mix]
      ],
      preferred_cli_env: [
        coveralls: :test,
        "coveralls.html": :test,
        "coveralls.github": :test
      ]
    ]
  end

  defp package do
    [
      description: "Apache Arrow support for the BEAM: IPC, Flight, ADBC bindings",
      licenses: ["Apache-2.0"],
      maintainers: ["Thanos Vassilakis"],
      links: %{
        "GitHub" => "https://github.com/thanos/ex_arrow",
        "Docs" => "https://hexdocs.pm/ex_arrow"
      },
      files: [
        "lib",
        "native/ex_arrow_native/.cargo",
        "native/ex_arrow_native/src",
        "native/ex_arrow_native/Cargo.toml",
        "native/ex_arrow_native/Cargo.lock",
        "checksum-*.exs",
        "mix.exs",
        "README.md",
        "CHANGELOG.md"
      ]
    ]
  end

  defp elixirc_paths(:test), do: ["lib", "test/support"]
  defp elixirc_paths(_), do: ["lib"]

  defp aliases do
    [ci: "ci"]
  end

  def application do
    [
      extra_applications: [:logger],
      mod: {ExArrow.Application, []}
    ]
  end

  defp deps do
    [
      {:rustler_precompiled, "~> 0.8"},
      {:adbc, "~> 0.7", optional: true},
      {:explorer, "~> 0.8", optional: true},
      {:nimble_pool, "~> 1.1", optional: true},
      {:rustler, "~>  0.32.0", optional: true},
      {:ex_doc, "~> 0.34", only: :dev},
      {:credo, "~> 1.7", only: [:dev, :test], runtime: false},
      {:dialyxir, "~> 1.4", only: [:dev, :test], runtime: false},
      {:sobelow, "~> 0.13", only: [:dev, :test], runtime: false},
      {:excoveralls, "~> 0.18", only: :test},
      {:mox, "~> 1.0", only: :test},
      {:stream_data, "~> 1.2.0", only: :test}
    ]
  end

  defp docs do
    [
      main: "overview",
      formatters: ["html"],
      source_url: "https://github.com/thanos/ex_arrow",
      source_ref: "main",
      extras: [
        "docs/overview.md",
        "docs/memory_model.md",
        "docs/ipc_guide.md",
        "docs/flight_guide.md",
        "docs/adbc_guide.md"
      ]
    ]
  end
end
