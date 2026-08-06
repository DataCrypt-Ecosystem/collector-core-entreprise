defmodule DatacryptRfb.Workers.Extractor do
  @moduledoc """
  GenServer para descompactação e conversão em chunks via Task.async_stream.
  """
  use GenServer
  require Logger

  # -- Client API --

  def start_link(opts) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @doc """
  Descompacta o arquivo ZIP de forma eficiente para o diretório de destino.
  """
  def unzip(zip_path, dest_dir) do
    Logger.info("Descompactando #{zip_path} para #{dest_dir}...")
    
    File.mkdir_p!(dest_dir)
    
    # :zip.unzip do Erlang extrai direto pro disco (poupa memória)
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
    
    # Lê em stream, divide em lotes e escreve para novos arquivos
    chunk_paths =
      File.stream!(csv_path, [:read, :utf8])
      |> Stream.chunk_every(lines_per_chunk)
      |> Stream.with_index(1)
      |> Enum.map(fn {lines_batch, index} ->
        chunk_file = Path.join(dir, "#{base_name}_chunk_#{index}.csv")
        File.write!(chunk_file, lines_batch, [:write, :utf8])
        chunk_file
      end)

    Logger.info("Conversão finalizada. #{length(chunk_paths)} chunks gerados.")
    {:ok, chunk_paths}
  end

  @doc """
  Orquestra o processamento paralelo limitando a concorrência via Task.async_stream.
  """
  def process_files(files, process_func, max_concurrency \\ 4) do
    Logger.info("Iniciando processamento paralelo de #{length(files)} arquivos (Max Concurrency: #{max_concurrency})...")

    files
    |> Task.async_stream(fn file ->
      process_func.(file)
    end, max_concurrency: max_concurrency, timeout: :infinity)
    |> Enum.reduce({0, 0}, fn 
      {:ok, _result}, {success, error} -> {success + 1, error}
      {:error, _reason}, {success, error} -> {success, error + 1}
      # Captura de crash na Task
      error, {success, error_count} -> 
        Logger.error("Crash no processamento de um chunk: #{inspect(error)}")
        {success, error_count + 1}
    end)
  end

  # -- Server Callbacks --

  @impl true
  def init(state) do
    {:ok, state}
  end
end
