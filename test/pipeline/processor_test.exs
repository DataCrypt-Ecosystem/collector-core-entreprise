defmodule DatacryptRfb.Pipeline.ProcessorTest do
  use ExUnit.Case, async: true

  alias DatacryptRfb.Pipeline.Processor
  alias DatacryptRfb.Pipeline.Schema
  alias DatacryptRfb.Workers.Extractor
  alias Explorer.DataFrame, as: DF

  test "lê empresas sem cabeçalho, aplica schema e tipa capital social" do
    path =
      Path.join(System.tmp_dir!(), "rfb_processor_test_#{System.unique_integer([:positive])}.csv")

    File.write!(
      path,
      " 12345678 ; ACME LTDA ;2062;49;1234,56;01;\n"
    )

    on_exit(fn -> File.rm(path) end)

    dataframe =
      path
      |> Processor.lazy_read_csv("empresas")
      |> Processor.clean_and_cast()
      |> DF.collect()

    assert dataframe.names == [
             "cnpj_basico",
             "razao_social",
             "natureza_juridica",
             "qualificacao_responsavel",
             "capital_social",
             "porte_empresa",
             "ente_federativo_responsavel"
           ]

    assert dataframe["cnpj_basico"] |> Explorer.Series.to_list() == ["12345678"]
    assert dataframe["razao_social"] |> Explorer.Series.to_list() == ["ACME LTDA"]
    assert dataframe["capital_social"] |> Explorer.Series.to_list() == [1234.56]
  end

  test "processa chunks em fluxo e remove cada arquivo temporário" do
    path =
      Path.join(System.tmp_dir!(), "rfb_extractor_test_#{System.unique_integer([:positive])}.csv")

    File.write!(path, "1\n2\n3\n4\n5\n")
    on_exit(fn -> File.rm(path) end)

    assert {3, 0} =
             Extractor.process_csv(
               path,
               fn chunk_path ->
                 assert File.exists?(chunk_path)
                 :ok
               end,
               2,
               2
             )

    assert Path.wildcard(Path.rootname(path) <> "_chunk_*.csv") == []
  end

  test "schema de estabelecimentos acompanha as 30 colunas oficiais" do
    columns = Schema.columns("estabelecimentos")

    assert length(columns) == 30
    assert Enum.at(columns, 25) == "ddd_fax"
    assert Enum.at(columns, 26) == "fax"
    assert Enum.at(columns, 27) == "email"
  end
end
