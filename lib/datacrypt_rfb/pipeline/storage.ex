defmodule DatacryptRfb.Pipeline.Storage do
  @moduledoc """
  Lógica I/O para leitura/escrita Parquet no disco.
  """
  alias Explorer.DataFrame, as: DF
  require Logger

  @base_dir "datalake"

  @doc """
  Constrói o caminho absoluto ou relativo para um arquivo Parquet dentro do nosso Data Lake local.
  Ex: build_path("receita_federal", "empresas", "2023_10")
  """
  def build_path(source, entity, partition \\ "latest") do
    Path.join([@base_dir, source, entity, "#{partition}.parquet"])
  end

  @doc """
  Converte o LazyFrame (executando as operações pendentes do Polars) e 
  escreve os dados em um arquivo Apache Parquet usando compressão `zstd` para economia de disco.
  """
  def write_parquet(lazy_df, path) do
    Logger.info("Coletando LazyFrame e escrevendo em Parquet: #{path} (Compressão: zstd)")
    
    path |> Path.dirname() |> File.mkdir_p!()

    try do
      eager_df = DF.collect(lazy_df)
      DF.to_parquet!(eager_df, path, compression: :zstd)
      
      Logger.info("Escrita Parquet concluída com sucesso!")
      {:ok, path}
    rescue
      e ->
        Logger.error("Falha ao escrever Parquet em #{path}: #{inspect(e)}")
        {:error, e}
    end
  end
end
