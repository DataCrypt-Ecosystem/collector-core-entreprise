defmodule DatacryptRfb.Pipeline.Schema do
  @moduledoc """
  Schemas dos arquivos disponibilizados pela Receita Federal.

  Os CSVs da Receita não possuem cabeçalho. As listas abaixo mantêm a
  posição oficial das colunas para que o restante do pipeline possa usar
  nomes estáveis.
  """

  @schemas %{
    "empresas" => ~w(
      cnpj_basico razao_social natureza_juridica qualificacao_responsavel
      capital_social porte_empresa ente_federativo_responsavel
    ),
    "estabelecimentos" => ~w(
      cnpj_basico cnpj_ordem cnpj_dv identificador_matriz_filial nome_fantasia
      situacao_cadastral data_situacao_cadastral motivo_situacao_cadastral
      nome_cidade_exterior pais data_inicio_atividade cnae_fiscal_principal
      cnae_fiscal_secundaria tipo_logradouro logradouro numero complemento bairro
      cep uf municipio ddd1 telefone1 ddd2 telefone2 ddd_fax fax email
      situacao_especial data_situacao_especial
    ),
    "socios" => ~w(
      cnpj_basico identificador_socio nome_socio_razao_social cpf_cnpj_socio
      qualificacao_socio data_entrada_sociedade pais representante_legal
      nome_representante qualificacao_representante_legal faixa_etaria
    ),
    "simples" => ~w(
      cnpj_basico opcao_pelo_simples data_opcao_simples data_exclusao_simples
      opcao_mei data_opcao_mei data_exclusao_mei
    ),
    "cnaes" => ~w(codigo descricao),
    "motivos" => ~w(codigo descricao),
    "municipios" => ~w(codigo descricao),
    "naturezas" => ~w(codigo descricao),
    "paises" => ~w(codigo descricao),
    "qualificacoes" => ~w(codigo descricao)
  }

  @doc "Retorna os nomes das colunas para uma entidade conhecida."
  @spec columns(String.t()) :: [String.t()] | nil
  def columns(entity), do: Map.get(@schemas, normalize(entity))

  defp normalize(entity), do: entity |> String.downcase() |> String.trim()
end
