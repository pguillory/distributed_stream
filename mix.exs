defmodule DistributedStream.MixProject do
  use Mix.Project

  def project do
    [
      package: package(),
      app: :distributed_stream,
      version: "0.1.2",
      elixir: "~> 1.16",
      start_permanent: Mix.env() == :prod,
      deps: deps(),
      aliases: [
        test: "test --no-start"
      ]
    ]
  end

  defp package do
    [
      description: "Library for distributing stream processing across a cluster",
      licenses: ["MIT"],
      maintainers: ["pguillory@gmail.com"],
      links: %{
        "GitHub" => "https://github.com/pguillory/distributed_stream"
      }
    ]
  end

  def application do
    [
      extra_applications: [:logger]
    ]
  end

  defp deps do
    [
      {:ex_doc, ">= 0.0.0", only: :dev, runtime: false},
      # {:local_cluster, ">= 0.0.0", only: [:test]}
    ]
  end
end
