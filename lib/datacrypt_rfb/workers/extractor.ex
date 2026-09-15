defmodule DatacryptRfb.Workers.Extractor do
  @moduledoc """
  GenServer para descompactação e conversão em chunks via Task.async_stream.
  """
  use GenServer
  require Logger

  def start_link(opts) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @doc """
  Descompacta o arquivo ZIP de forma eficiente para o diretório de destino.
  """
  def unzip(zip_path, dest_dir) do
    Logger.info("Descompactando #{zip_path} para #{dest_dir}...")

    File.mkdir_p!(dest_dir)

    case :zip.unzip(String.to_charlist(zip_path), cwd: String.to_charlist(dest_dir)) do
      {:ok, files} ->
        extracted = Enum.map(files, &List.to_string/1)
        Logger.info("Descompactação concluída. #{length(extracted)} arquivo(s) extraído(s).")
        {:ok, extracted}

      {:error, reason} ->
        Logger.error("Falha ao descompactar #{zip_path}: #{inspect(reason)}")
        {:error, reason}
    end
  end

  @doc """
  Divide um ficheiro CSV gigante em múltiplos arquivos menores (chunks) para controle granular de RAM.
  Utiliza File.stream! para nunca carregar o arquivo inteiro na memória.
  """
  def chunk_csv(csv_path, lines_per_chunk \\ 500_000) do
    Logger.info("Dividindo #{csv_path} em chunks de #{lines_per_chunk} linhas...")

    dir = Path.dirname(csv_path)
    base_name = Path.basename(csv_path, ".csv")

    chunk_paths =
      File.stream!(csv_path, [:read])
      |> Stream.chunk_every(lines_per_chunk)
      |> Stream.with_index(1)
      |> Enum.map(fn {lines_batch, index} ->
        chunk_file = Path.join(dir, "#{base_name}_chunk_#{index}.csv")
        File.write!(chunk_file, lines_batch, [:write])

        # Progresso no terminal
        IO.write(
          "\r    => Chunking: Lote #{index} gerado (até #{index * lines_per_chunk} linhas)..."
        )

        chunk_file
      end)

    IO.write("\r                                                                      \r")
    Logger.info("Conversão finalizada. #{length(chunk_paths)} chunks gerados.")
    {:ok, chunk_paths}
  end

  @doc """
  Processa um CSV em fluxo, mantendo somente os chunks em execução na memória
  e no disco temporário. Cada chunk é removido assim que termina.
  """
  def process_csv(csv_path, process_func, lines_per_chunk \\ 500_000, max_concurrency \\ 4) do
    Logger.info(
      "Processando #{csv_path} em chunks de #{lines_per_chunk} linhas " <>
        "(Max Concurrency: #{max_concurrency})..."
    )

    dir = Path.dirname(csv_path)
    base_name = Path.basename(csv_path, ".csv")

    File.stream!(csv_path, [:read])
    |> Stream.chunk_every(lines_per_chunk)
    |> Stream.with_index(1)
    |> Task.async_stream(
      fn {lines_batch, index} ->
        chunk_file = Path.join(dir, "#{base_name}_chunk_#{index}.csv")
        File.write!(chunk_file, lines_batch, [:write])

        try do
          process_func.(chunk_file)
        after
          File.rm(chunk_file)
        end
      end,
      max_concurrency: max_concurrency,
      timeout: :infinity
    )
    |> Enum.reduce({0, 0}, fn
      {:ok, _result}, {success, errors} ->
        {success + 1, errors}

      {:error, reason}, {success, errors} ->
        Logger.error("Falha no processamento de um chunk: #{inspect(reason)}")
        {success, errors + 1}
    end)
    |> then(fn {success, errors} = result ->
      Logger.info("Processamento finalizado. Sucesso: #{success} | Falhas: #{errors}")
      result
    end)
  end

  @doc """
  Orquestra o processamento paralelo limitando a concorrência via Task.async_stream.
  """
  def process_files(files, process_func, max_concurrency \\ 4) do
    total = length(files)

    Logger.info(
      "Iniciando processamento paralelo de #{total} arquivos (Max Concurrency: #{max_concurrency})..."
    )

    files
    |> Task.async_stream(
      fn file ->
        process_func.(file)
      end,
      max_concurrency: max_concurrency,
      timeout: :infinity
    )
    |> Enum.reduce({0, 0}, fn
      {:ok, _result}, {success, error} ->
        IO.write("\r    => Processando Dataframes: #{success + error + 1}/#{total} concluídos...")
        {success + 1, error}

      {:error, _reason}, {success, error} ->
        IO.write("\r    => Processando Dataframes: #{success + error + 1}/#{total} concluídos...")
        {success, error + 1}

      error_msg, {success, error_count} ->
        Logger.error("Crash no processamento de um chunk: #{inspect(error_msg)}")

        IO.write(
          "\r    => Processando Dataframes: #{success + error_count + 1}/#{total} concluídos..."
        )

        {success, error_count + 1}
    end)
    |> case do
      {s, e} ->
        IO.write("\r                                                                      \r")
        Logger.info("Processamento finalizado. Sucesso: #{s} | Falhas: #{e}")
        {s, e}
    end
  end

  @impl true
  def init(state) do
    {:ok, state}
  end
end
