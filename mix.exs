defmodule FinanctialDataPreparation.MixProject do
  use Mix.Project

  def project do
    [
      app: :financial_data_preparation,
      version: "0.1.0",
      elixir: "~> 1.17",
      start_permanent: Mix.env() == :prod,
      deps: deps()
    ]
  end

  # Run "mix help compile.app" to learn about applications.
  def application do
    [
      extra_applications: [:logger]
    ]
  end

  # Run "mix help deps" to learn about dependencies.
  defp deps do
    [
      {:flow, "~> 1.2.4"},
      {:explorer, "~> 0.10.0"},
      {:nx, "~> 0.9.1"},
      {:benchee, "~> 1.3.1"},
      {:csv, "~> 3.2"},
      {:vega_lite, "~> 0.1.9"},
      {:jason, "~> 1.4.4"}
    ]
  end
end
