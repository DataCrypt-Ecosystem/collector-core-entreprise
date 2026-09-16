defmodule DatacryptRfb.Pipeline.Storage do
  @moduledoc """
  Lógica I/O para leitura/escrita Parquet no disco.
  """
  alias Explorer.DataFrame, as: DF
  require Logger

  @base_dir "datalake"

  @doc """
  Retorna o diretório de uma partição do Data Lake.

  As partições usam o padrão Hive `competencia=YYYY-MM`, que permite que
  ferramentas de consulta filtrem a competência sem varrer todo o Data Lake.
  """
  def partition_path(source, entity, partition) do
    Path.join([@base_dir, source, entity, "competencia=#{partition}"])
  end

  @doc "Constrói o caminho de uma parte Parquet dentro de uma competência."
  def build_path(source, entity, partition, part) do
    Path.join(partition_path(source, entity, partition), "part-#{part}.parquet")
  end

  @doc "Mantém acesso explícito ao layout antigo durante a transição."
  def legacy_path(source, entity, partition) do
    Path.join([@base_dir, source, entity, "#{partition}.parquet"])
  end

  @doc """
  Converte o LazyFrame (executando as operações pendentes do Polars) e 
  escreve os dados em um arquivo Apache Parquet usando compressão `zstd` para economia de disco.
  """
  def write_parquet(lazy_df, path) do
    Logger.info("Coletando LazyFrame e escrevendo em Parquet: #{path} (Compressão: zstd)")

    path |> Path.dirname() |> File.mkdir_p!()
    temporary_path = "#{path}.tmp-#{System.unique_integer([:positive])}"

    try do
      eager_df = DF.collect(lazy_df)
      DF.to_parquet!(eager_df, temporary_path, compression: :zstd)
      File.rename!(temporary_path, path)

      Logger.info("Escrita Parquet concluída com sucesso!")
      {:ok, path}
    rescue
      e ->
        File.rm(temporary_path)
        Logger.error("Falha ao escrever Parquet em #{path}: #{inspect(e)}")
        {:error, e}
    end
  end
end
