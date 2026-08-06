defmodule DatacryptRfb.Workers.Downloader do
  @moduledoc """
  GenServer para gerir estado do download via WebDAV (com Req + File.stream!).
  """
  use GenServer
  require Logger

  # -- Client API --

  def start_link(opts) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @doc """
  Realiza o download de um ficheiro via WebDAV para o disco local em modo stream.
  Para paralelismo e controle de concorrência, use Task.async_stream na camada orquestradora
  e chame esta função de dentro da Task.
  """
  def download_file(url, dest_path) do
    # Obtém configurações de auth ou headers guardados no state do GenServer
    opts = GenServer.call(__MODULE__, :get_opts)
    
    Logger.info("Iniciando download de #{url} para #{dest_path}...")
    
    # Criar diretório de destino se não existir
    dest_path |> Path.dirname() |> File.mkdir_p!()

    # Usamos o into: File.stream! para gravar direto no disco sem sobrecarregar a RAM
    req_opts = 
      opts
      |> Keyword.put(:into, File.stream!(dest_path, [:write, :binary]))
      |> Keyword.put_new(:receive_timeout, 3_600_000) # Timeout longo (1 hora) para GBs

    case Req.get(url, req_opts) do
      {:ok, %Req.Response{status: status}} when status in 200..299 ->
        Logger.info("Download concluído com sucesso: #{dest_path}")
        {:ok, dest_path}
        
      {:ok, response} ->
        Logger.error("Falha no download (Status HTTP #{response.status}): #{url}")
        File.rm(dest_path) # Limpar lixo
        {:error, {:bad_status, response.status}}
        
      {:error, reason} ->
        Logger.error("Erro na requisição WebDAV para #{url}: #{inspect(reason)}")
        File.rm(dest_path) # Limpar lixo
        {:error, reason}
    end
  end

  # -- Server Callbacks --

  @impl true
  def init(opts) do
    # Carregaria variáveis de ambiente como auth do Nextcloud: 
    # auth: {:basic, System.get_env("WEBDAV_USER"), System.get_env("WEBDAV_PASS")}
    webdav_opts = Keyword.get(opts, :webdav_opts, [])
    {:ok, webdav_opts}
  end

  @impl true
  def handle_call(:get_opts, _from, state) do
    {:reply, state, state}
  end
end
