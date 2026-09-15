defmodule Mix.Tasks.Rfb.Baixar do
  @moduledoc """
  Mix Task para iniciar o pipeline de extração e processamento da Receita Federal.
  Uso: mix rfb.baixar
  """
  use Mix.Task

  @impl Mix.Task
  def run(args) do
    Mix.Task.run("app.start")

    DatacryptRfb.Cli.process(args)
  end
end
