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

  `--partition YYYY` processa os doze meses do ano. `--partition YYYY-MM`
  processa somente a competência informada.
  """
  def process(args \\ []) do
    {parsed_args, _, _} = OptionParser.parse(args, strict: [partition: :string])

    partition = Keyword.get(parsed_args, :partition, "2024-01")

    partition
    |> expand_partitions()
    |> Enum.reduce_while(:ok, fn partition_id, :ok ->
      case process_partition(partition_id) do
        :ok -> {:cont, :ok}
        {:error, reason} -> {:halt, {:error, {partition_id, reason}}}
      end
    end)
  end

  @doc "Retorna o diretório raiz usado para arquivos temporários do pipeline."
  def temporary_root do
    System.get_env("RFB_TMP_DIR", Path.join([File.cwd!(), "tmp", "rfb"]))
  end

  @doc "Expande um ano em suas doze competências ou mantém um mês específico."
  def expand_partitions(<<year::binary-size(4)>> = partition) do
    if Regex.match?(~r/^\d{4}$/, partition) do
      for month <- 1..12, do: "#{year}-#{String.pad_leading(Integer.to_string(month), 2, "0")}"
    else
      validate_month_partition!(partition)
    end
  end

  def expand_partitions(partition), do: validate_month_partition!(partition)

  defp validate_month_partition!(partition) do
    if Regex.match?(~r/^\d{4}-(0[1-9]|1[0-2])$/, partition) do
      [partition]
    else
      raise ArgumentError,
            "partição inválida #{inspect(partition)}; use YYYY ou YYYY-MM"
    end
  end

  defp process_partition(partition_id) do
    Logger.info("Iniciando orquestração da Receita Federal para #{partition_id}...")

    folder_url = Downloader.folder_url(partition_id)

    Logger.info("Buscando arquivos na partição: #{folder_url}")

    case Downloader.list_zip_files(folder_url) do
      {:ok, urls} ->
        Logger.info("Encontrados #{length(urls)} arquivos ZIP para processar.")

        Enum.each(urls, fn url ->
          file_name = URI.parse(url).path |> Path.basename()
          # Remove números do final (ex: "Empresas0.zip" -> "empresas")
          entity =
            Regex.replace(~r/\d+$/, Path.basename(file_name, ".zip"), "") |> String.downcase()

          temp_dir = Path.join(temporary_root(), "datacrypt_rfb_#{entity}_#{partition_id}")
          zip_path = Path.join(temp_dir, file_name)

          Logger.info("Processando #{file_name} (Entidade: #{entity})...")

          # Remove resíduos de uma execução interrompida antes de reutilizar
          # o diretório da mesma entidade e competência.
          File.rm_rf!(temp_dir)
          File.mkdir_p!(temp_dir)

          with {:ok, ^zip_path} <- Downloader.download_file(url, zip_path),
               {:ok, csv_files} <- Extractor.unzip(zip_path, temp_dir) do
            Enum.each(csv_files, fn csv_path ->
              Extractor.process_csv(
                csv_path,
                fn chunk_file ->
                  process_chunk(chunk_file, entity, partition_id)
                end,
                500_000,
                4
              )
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
    parquet_path = Storage.build_path("receita_federal", entity, partition_id, chunk_id)

    old_parquet = Storage.legacy_path("receita_federal", entity, "mes_anterior")

    chunk_csv_path
    |> Processor.lazy_read_csv(entity)
    |> Processor.clean_and_cast()
    |> Processor.compute_delta(old_parquet, ["cnpj_basico"])
    |> Storage.write_parquet(parquet_path)

    File.rm(chunk_csv_path)

    {:ok, parquet_path}
  end
end
