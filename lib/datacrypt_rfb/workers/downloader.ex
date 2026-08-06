defmodule DatacryptRfb.Workers.Downloader do
  @moduledoc """
  GenServer para gerir estado do download via WebDAV (com Req + File.stream!).
  """
  use GenServer
  require Logger

  def start_link(opts) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @doc """
  Lista todos os arquivos .zip num diretório WebDAV usando PROPFIND.
  """
  def list_zip_files(folder_url) do
    opts = GenServer.call(__MODULE__, :get_opts)
    
    folder_url = if String.ends_with?(folder_url, "/"), do: folder_url, else: folder_url <> "/"
    
    req_opts = 
      opts
      |> Keyword.put(:method, "PROPFIND")
      |> Keyword.put(:headers, [{"depth", "1"}])

    case Req.request(folder_url, req_opts) do
      {:ok, %Req.Response{status: status, body: xml}} when status in 200..299 ->
        hrefs = 
          Regex.scan(~r/<(?:d:)??href>(.*?)<\/(?:d:)??href>/i, xml)
          |> Enum.map(fn [_, path] -> path end)
          |> Enum.filter(&String.ends_with?(String.downcase(&1), ".zip"))
          |> Enum.map(fn path -> URI.merge(folder_url, path) |> URI.to_string() end)
        
        {:ok, hrefs}
      {:ok, response} ->
        {:error, {:bad_status, response.status}}
      {:error, reason} ->
        {:error, reason}
    end
  end

  @doc """
  Realiza o download de um ficheiro via WebDAV para o disco local em modo stream.
  Para paralelismo e controle de concorrência, use Task.async_stream na camada orquestradora
  e chame esta função de dentro da Task.
  """
  def download_file(url, dest_path) do
    opts = GenServer.call(__MODULE__, :get_opts)
    
    Logger.info("Iniciando download de #{url} para #{dest_path}...")
    
    dest_path |> Path.dirname() |> File.mkdir_p!()

    {:ok, counter} = Agent.start_link(fn -> 0 end)
    file = File.open!(dest_path, [:write, :binary])

    req_opts = 
      opts
      |> Keyword.put(:into, fn {:data, data}, {req, res} ->
        downloaded = Agent.get_and_update(counter, fn acc -> 
          new = acc + byte_size(data)
          {new, new}
        end)
        
        prev = downloaded - byte_size(data)
        # Atualiza o terminal a cada ~5 MB
        if div(downloaded, 5_242_880) > div(prev, 5_242_880) do
          IO.write("\r    => Progresso: #{Float.round(downloaded / 1048576, 1)} MB baixados...")
        end
        
        IO.binwrite(file, data)
        {:cont, {req, res}}
      end)
      |> Keyword.put_new(:receive_timeout, 3_600_000)

    result = Req.get(url, req_opts)
    
    File.close(file)
    final_mb = Float.round(Agent.get(counter, & &1) / 1048576, 1)
    Agent.stop(counter)
    
    # Limpa a linha de progresso
    IO.write("\r                                                                      \r")

    case result do
      {:ok, %Req.Response{status: status}} when status in 200..299 ->
        Logger.info("Download concluído com sucesso: #{dest_path} (#{final_mb} MB)")
        {:ok, dest_path}
        
      {:ok, response} ->
        Logger.error("Falha no download (Status HTTP #{response.status}): #{url}")
        File.rm(dest_path)
        {:error, {:bad_status, response.status}}
        
      {:error, reason} ->
        Logger.error("Erro na requisição WebDAV para #{url}: #{inspect(reason)}")
        File.rm(dest_path)
        {:error, reason}
    end
  end

  @impl true
  def init(_opts) do
    webdav_opts = [auth: {:basic, "gn672Ad4CF8N6TK:"}]
    {:ok, webdav_opts}
  end

  @impl true
  def handle_call(:get_opts, _from, state) do
    {:reply, state, state}
  end
end
