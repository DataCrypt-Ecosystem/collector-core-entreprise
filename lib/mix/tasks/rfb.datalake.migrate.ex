defmodule Mix.Tasks.Rfb.Datalake.Migrate do
  @moduledoc """
  Migra Parquets do layout legado para partições Hive.

      mix rfb.datalake.migrate
      mix rfb.datalake.migrate --dry-run
  """

  use Mix.Task

  alias DatacryptRfb.Pipeline.Storage

  @source "receita_federal"
  @base_dir "datalake"

  @impl Mix.Task
  def run(args) do
    {opts, _, _} = OptionParser.parse(args, strict: [dry_run: :boolean])
    dry_run? = Keyword.get(opts, :dry_run, false)

    files = Path.wildcard(Path.join([@base_dir, @source, "*", "*.parquet"]))

    {migrated, skipped, errors} =
      Enum.reduce(files, {0, 0, 0}, fn file, counts ->
        migrate_file(file, counts, dry_run?)
      end)

    Mix.shell().info(
      "Migração concluída. Movidos: #{migrated} | Ignorados: #{skipped} | Erros: #{errors}"
    )

    if errors > 0, do: Mix.raise("A migração terminou com erros")
  end

  defp migrate_file(file, {migrated, skipped, errors}, dry_run?) do
    entity = file |> Path.dirname() |> Path.basename()
    filename = Path.basename(file)

    case Regex.run(~r/^(\d{4}-\d{2})_(.+)\.parquet$/, filename, capture: :all_but_first) do
      [partition, part] ->
        target = Storage.build_path(@source, entity, partition, part)

        cond do
          File.exists?(target) ->
            Mix.shell().info("Ignorado, destino já existe: #{target}")
            {migrated, skipped + 1, errors}

          dry_run? ->
            Mix.shell().info("Moveria: #{file} -> #{target}")
            {migrated + 1, skipped, errors}

          true ->
            File.mkdir_p!(Path.dirname(target))

            case File.rename(file, target) do
              :ok ->
                Mix.shell().info("Movido: #{target}")
                {migrated + 1, skipped, errors}

              {:error, reason} ->
                Mix.shell().error("Falha ao mover #{file}: #{inspect(reason)}")
                {migrated, skipped, errors + 1}
            end
        end

      _ ->
        Mix.shell().info("Ignorado, nome fora do layout legado: #{file}")
        {migrated, skipped + 1, errors}
    end
  end
end
