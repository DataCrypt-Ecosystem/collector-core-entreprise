defmodule DatacryptRfb.Pipeline.Processor do
  @moduledoc """
  Lógica com Explorer/Polars (Limpeza, Tipagem, Delta).
  """
  require Explorer.DataFrame, as: DF
  # Importa macros de query para transformação (Q.cast, Q.coalesce, etc)
  require Explorer.Query, as: Q
  require Logger

  @doc """
  Lê um arquivo CSV fragmentado da Receita Federal como um LazyFrame.
  
  Geralmente os dados da RFB não possuem cabeçalho e usam ';' como separador.
  Como usamos lazy: true, o arquivo só será processado em disco quando houver uma ação de collect/write.
  """
  def lazy_read_csv(csv_path, column_mapping \\ []) do
    Logger.info("Carregando LazyFrame para o CSV: #{csv_path}")
    
    # Ex: column_mapping = [{"column_1", "cnpj_basico"}, {"column_2", "razao_social"}]
    DF.from_csv!(csv_path, delimiter: ";", header: false, lazy: true)
    |> DF.rename(column_mapping)
  end

  @doc """
  Aplica regras de negócio de limpeza e tipagem no LazyFrame.
  O Rust processará essas regras de forma otimizada.
  """
  def clean_and_cast(lazy_df) do
    # TODO: Inserir regras específicas da RFB.
    # Exemplo de mutação e cast de tipos:
    # lazy_df |> DF.mutate(data_situacao: Q.cast(data_situacao, :date))
    lazy_df
  end

  @doc """
  Aplica a Estratégia de Delta (Upsert Lógico).
  
  Lê o Parquet do mês anterior, compara com o novo lote, e devolve
  um LazyFrame contendo apenas as inserções ou atualizações (Delta).
  """
  def compute_delta(new_lazy_df, old_parquet_path, join_keys) do
    if File.exists?(old_parquet_path) do
      Logger.info("Computando Delta contra Parquet anterior: #{old_parquet_path}")
      
      old_lazy_df = DF.from_parquet!(old_parquet_path, lazy: true)
      
      # O Anti-Join encontra os registros do `new_lazy_df` que não estão no `old_lazy_df`
      # (ou que mudaram, se incluirmos a coluna de data de atualização no join_keys)
      DF.join(new_lazy_df, old_lazy_df, how: :anti, on: join_keys)
    else
      Logger.info("Parquet anterior não encontrado. Todo o lote atual será considerado como Delta novo.")
      new_lazy_df
    end
  end
end
