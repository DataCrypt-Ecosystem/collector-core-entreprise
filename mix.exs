defmodule DatacryptRfb.MixProject do
  use Mix.Project

  def project do
    [
      app: :datacrypt_rfb,
      version: "0.1.0",
      elixir: "~> 1.20",
      start_permanent: Mix.env() == :prod,
      deps: deps()
    ]
  end

  # Run "mix help compile.app" to learn about applications.
  def application do
    [
      extra_applications: [:logger],
      mod: {DatacryptRfb.Application, []}
    ]
  end

  # Run "mix help deps" to learn about dependencies.
  defp deps do
    [
      {:explorer, "~> 0.8"},
      {:req, "~> 0.5"}
    ]
  end
end
