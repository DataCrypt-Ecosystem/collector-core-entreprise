defmodule DatacryptRfb.Workers.Downloader do
  @moduledoc """
  GenServer para gerir estado do download via WebDAV (com Req + File.stream!).
  """
  use GenServer
  require Logger

  @host "https://arquivos.receitafederal.gov.br"
  @default_share_token "gn672Ad4CF8N6TK"

  @doc "Monta a URL WebDAV pública de uma partição da Receita Federal."
  def folder_url(partition_id) do
    token = System.get_env("RFB_PUBLIC_SHARE_TOKEN", @default_share_token)
    "#{@host}/public.php/dav/files/#{token}/Dados/Cadastros/CNPJ/#{partition_id}/"
  end

  def start_link(opts) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @doc """
  Lista todos os arquivos .zip num diretório WebDAV usando PROPFIND.
  """
  def list_zip_files(folder_url) do
    case GenServer.call(__MODULE__, :get_opts) do
      {:error, reason} ->
        {:error, reason}

      opts ->
        folder_url =
          if String.ends_with?(folder_url, "/"), do: folder_url, else: folder_url <> "/"

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
  end

  @doc """
  Realiza o download de um ficheiro via WebDAV para o disco local em modo stream.
  Para paralelismo e controle de concorrência, use Task.async_stream na camada orquestradora
  e chame esta função de dentro da Task.
  """
  def download_file(url, dest_path) do
    case GenServer.call(__MODULE__, :get_opts) do
      {:error, reason} ->
        {:error, reason}

      opts ->
        Logger.info("Iniciando download de #{url} para #{dest_path}...")

        dest_path |> Path.dirname() |> File.mkdir_p!()

        {:ok, counter} = Agent.start_link(fn -> 0 end)
        file = File.open!(dest_path, [:write, :binary])

        req_opts =
          opts
          |> Keyword.put(:into, fn {:data, data}, {req, res} ->
            downloaded =
              Agent.get_and_update(counter, fn acc ->
                new = acc + byte_size(data)
                {new, new}
              end)

            prev = downloaded - byte_size(data)

            if div(downloaded, 5_242_880) > div(prev, 5_242_880) do
              IO.write(
                "\r    => Progresso: #{Float.round(downloaded / 1_048_576, 1)} MB baixados..."
              )
            end

            IO.binwrite(file, data)
            {:cont, {req, res}}
          end)
          |> Keyword.put_new(:receive_timeout, 3_600_000)
          # Cada tentativa precisa começar com um arquivo vazio. O retry
          # automático do Req reutiliza o callback de streaming e pode
          # concatenar uma resposta parcial a outra, corrompendo o ZIP.
          |> Keyword.put(:retry, false)

        result = Req.get(url, req_opts)

        File.close(file)
        final_mb = Float.round(Agent.get(counter, & &1) / 1_048_576, 1)
        Agent.stop(counter)
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
  end

  @impl true
  def init(_opts) do
    token = System.get_env("RFB_PUBLIC_SHARE_TOKEN", @default_share_token)
    auth = System.get_env("RFB_WEBDAV_AUTH", "#{token}:")
    {:ok, auth}
  end

  @impl true
  def handle_call(:get_opts, _from, state) do
    reply =
      case state do
        value when is_binary(value) and value != "" -> [auth: {:basic, value}]
        _ -> []
      end

    {:reply, reply, state}
  end
end
