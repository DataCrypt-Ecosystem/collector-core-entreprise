defmodule DatacryptRfb.CliTest do
  use ExUnit.Case, async: true

  alias DatacryptRfb.Cli

  test "expande um ano para as doze competências" do
    assert Cli.expand_partitions("2026") == [
             "2026-01",
             "2026-02",
             "2026-03",
             "2026-04",
             "2026-05",
             "2026-06",
             "2026-07",
             "2026-08",
             "2026-09",
             "2026-10",
             "2026-11",
             "2026-12"
           ]
  end

  test "mantém uma competência mensal específica" do
    assert Cli.expand_partitions("2026-08") == ["2026-08"]
  end

  test "rejeita partição fora do formato esperado" do
    assert_raise ArgumentError, ~r/use YYYY ou YYYY-MM/, fn ->
      Cli.expand_partitions("2026-13")
    end
  end

  test "usa um diretório temporário no volume do projeto por padrão" do
    refute String.starts_with?(Cli.temporary_root(), System.tmp_dir!())
    assert Path.basename(Cli.temporary_root()) == "rfb"
  end
end
