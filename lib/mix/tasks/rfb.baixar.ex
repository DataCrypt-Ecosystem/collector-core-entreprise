defmodule Mix.Tasks.Rfb.Baixar do
  @moduledoc """
  Mix Task para iniciar o pipeline de extração e processamento da Receita Federal.
  Uso: mix rfb.baixar
  """
  use Mix.Task

  @impl Mix.Task
  def run(args) do
    # Inicializa a aplicação (supervision tree) se necessário
    Mix.Task.run("app.start")
    
    # Chama o ponto de entrada da CLI
    DatacryptRfb.Cli.process(args)
  end
end
