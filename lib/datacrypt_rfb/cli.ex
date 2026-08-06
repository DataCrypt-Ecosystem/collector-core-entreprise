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

    {parsed_args, _, _} = OptionParser.parse(args, strict: [partition: :string])

    partition_id = Keyword.get(parsed_args, :partition, "2024-01")
    folder_url = "https://arquivos.receitafederal.gov.br/public.php/webdav/Dados/Cadastros/CNPJ/#{partition_id}/"

    Logger.info("Buscando arquivos na partição: #{folder_url}")

    case Downloader.list_zip_files(folder_url) do
      {:ok, urls} ->
        Logger.info("Encontrados #{length(urls)} arquivos ZIP para processar.")
        
        Enum.each(urls, fn url ->
          file_name = URI.parse(url).path |> Path.basename()
          # Remove números do final (ex: "Empresas0.zip" -> "empresas")
          entity = Regex.replace(~r/\d+$/, Path.basename(file_name, ".zip"), "") |> String.downcase()
          
          temp_dir = Path.join(System.tmp_dir!(), "datacrypt_rfb_#{entity}_#{partition_id}")
          zip_path = Path.join(temp_dir, file_name)
          
          Logger.info("Processando #{file_name} (Entidade: #{entity})...")
          
          with {:ok, ^zip_path} <- Downloader.download_file(url, zip_path),
               {:ok, csv_files} <- Extractor.unzip(zip_path, temp_dir) do
            
            Enum.each(csv_files, fn csv_path ->
              {:ok, chunks} = Extractor.chunk_csv(csv_path, 500_000)
              
              Extractor.process_files(chunks, fn chunk_file ->
                process_chunk(chunk_file, entity, partition_id)
              end, 4)
            end)

            File.rm_rf!(temp_dir)
            Logger.info("Processamento de '#{file_name}' finalizado com sucesso!")
          else
            error ->
              Logger.error("Falha ao processar #{file_name}: #{inspect(error)}")
              File.rm_rf!(temp_dir)
          end
        end)
        
        Logger.info("Processamento da partição #{partition_id} 100% finalizado!")
        :ok
        
      {:error, reason} ->
        Logger.error("Falha ao listar arquivos: #{inspect(reason)}")
        {:error, reason}
    end
  end

  defp process_chunk(chunk_csv_path, entity, partition_id) do
    chunk_id = Path.basename(chunk_csv_path, ".csv")
    parquet_path = Storage.build_path("receita_federal", entity, "#{partition_id}_#{chunk_id}")
    
    old_parquet = Storage.build_path("receita_federal", entity, "mes_anterior")

    chunk_csv_path
    |> Processor.lazy_read_csv()
    |> Processor.clean_and_cast()
    |> Processor.compute_delta(old_parquet, ["cnpj_basico"])
    |> Storage.write_parquet(parquet_path)

    File.rm(chunk_csv_path)
    
    {:ok, parquet_path}
  end
end
