defmodule DatacryptRfb.Cli do
  @moduledoc """
  Ponto de entrada das Mix Tasks e parse de argumentos.
  """

  require Logger

  alias DatacryptRfb.Workers.{Downloader, Extractor}
  alias DatacryptRfb.Pipeline.{Processor, Storage}
  require Logger

  @doc """
  Orquestra o fluxo completo de ETL: Download -> Unzip -> Chunking -> Processing -> Parquet
  """
  def process(args \\ []) do
    Logger.info("Iniciando orquestração da Receita Federal...")

    # Simulação de parse de argumentos (na prática viria do OptionParser)
    url = Keyword.get(args, :url, "https://url-do-nextcloud/webdav/rfb/Empresas.zip")
    entity = Keyword.get(args, :entity, "empresas")
    partition_id = Keyword.get(args, :partition, "atual")

    # Caminho isolado no HD para os arquivos brutos e extraídos
    temp_dir = Path.join(System.tmp_dir!(), "datacrypt_rfb_#{entity}_#{partition_id}")
    zip_path = Path.join(temp_dir, "raw_data.zip")

    # Pipeline Principal
    with {:ok, ^zip_path} <- Downloader.download_file(url, zip_path),
         {:ok, csv_files} <- Extractor.unzip(zip_path, temp_dir) do
      
      # Processar iterativamente cada CSV descompactado (no caso de haver mais de um no ZIP)
      Enum.each(csv_files, fn csv_path ->
        # 1. Divide o CSV gigante em partes de 500k linhas cada
        {:ok, chunks} = Extractor.chunk_csv(csv_path, 500_000)
        
        # 2. Orquestra a execução concorrente de cada pedaço limitando a RAM
        Extractor.process_files(chunks, fn chunk_file ->
          process_chunk(chunk_file, entity, partition_id)
        end, 4) # Concorrência máxima = 4
      end)

      # Cleanup do lixo temporário (arquivos brutos)
      File.rm_rf!(temp_dir)
      Logger.info("Processamento da entidade '#{entity}' 100% finalizado!")
      :ok
    else
      error ->
        Logger.error("Fluxo interrompido por falha: #{inspect(error)}")
        File.rm_rf!(temp_dir)
        {:error, error}
    end
  end

  defp process_chunk(chunk_csv_path, entity, partition_id) do
    # Identificador único para este chunk, ex: "atual_raw_data_chunk_1"
    chunk_id = Path.basename(chunk_csv_path, ".csv")
    parquet_path = Storage.build_path("receita_federal", entity, "#{partition_id}_#{chunk_id}")
    
    # Caminho do mês passado usado para o Upsert (Delta)
    old_parquet = Storage.build_path("receita_federal", entity, "mes_anterior")

    # Fluxo do Polars/Explorer no Worker:
    chunk_csv_path
    |> Processor.lazy_read_csv()
    |> Processor.clean_and_cast()
    |> Processor.compute_delta(old_parquet, ["cnpj_basico"]) # Exemplo de chave primária
    |> Storage.write_parquet(parquet_path)

    # Limpar o pedaço em CSV já consumido para desocupar o disco imediatamente
    File.rm(chunk_csv_path)
    
    {:ok, parquet_path}
  end
end
