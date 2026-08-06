defmodule DatacryptRfb.Application do
  @moduledoc false

  use Application

  @impl true
  def start(_type, _args) do
    children = [
      {DatacryptRfb.Workers.Downloader, []},
      {DatacryptRfb.Workers.Extractor, []}
    ]

    opts = [strategy: :one_for_one, name: DatacryptRfb.Supervisor]
    Supervisor.start_link(children, opts)
  end
end
