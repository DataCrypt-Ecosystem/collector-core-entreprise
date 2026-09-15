defmodule DatacryptRfb.Pipeline.Processor do
  @moduledoc """
  Lógica com Explorer/Polars (Limpeza, Tipagem, Delta).
  """
  alias Explorer.DataFrame, as: DF
  alias Explorer.Series, as: S
  alias DatacryptRfb.Pipeline.Schema
  require Logger

  @doc """
  Lê um arquivo CSV fragmentado da Receita Federal como um LazyFrame.

  Geralmente os dados da RFB não possuem cabeçalho e usam ';' como separador.
  Como usamos lazy: true, o arquivo só será processado em disco quando houver uma ação de collect/write.
  """
  def lazy_read_csv(csv_path, entity) when is_binary(entity) do
    Logger.info("Carregando LazyFrame para o CSV: #{csv_path}")

    columns = Schema.columns(entity)

    if is_nil(columns) do
      raise ArgumentError, "Schema não definido para a entidade #{inspect(entity)}"
    end

    DF.from_csv!(csv_path,
      delimiter: ";",
      header: false,
      lazy: true,
      encoding: "utf8-lossy",
      infer_schema_length: 0
    )
    |> DF.rename(columns)
  end

  @doc """
  Aplica regras de negócio de limpeza e tipagem no LazyFrame.
  O Rust processará essas regras de forma otimizada.
  """
  def clean_and_cast(lazy_df) do
    DF.mutate_with(lazy_df, fn query ->
      Enum.map(lazy_df.names, fn
        "capital_social" ->
          capital_social =
            query["capital_social"]
            |> S.strip()
            |> S.replace(",", ".")
            |> S.cast(:float)

          {"capital_social", capital_social}

        name ->
          {name, S.strip(query[name])}
      end)
    end)
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

      DF.join(new_lazy_df, old_lazy_df, how: :anti, on: join_keys)
    else
      Logger.info(
        "Parquet anterior não encontrado. Todo o lote atual será considerado como Delta novo."
      )

      new_lazy_df
    end
  end
end
