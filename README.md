# Datacrypt Enterprise - Worker Receita Federal

Este é o módulo de processamento (Worker) da plataforma Datacrypt, focado exclusivamente na extração e organização dos dados públicos da Receita Federal do Brasil (CNPJ, Sócios, Empresas, etc.).

O objetivo principal deste projeto é automatizar o download de bases de dados massivas, realizar o tratamento (limpeza) desses dados e organizá-los de forma estruturada para consultas rápidas.

## 🚀 Como Funciona

O pipeline de dados deste Worker segue 4 passos principais:

1. **Download:** Conecta-se à fonte dos dados e baixa os arquivos compactados (`.zip`) da Receita Federal.
2. **Descompactação:** Extrai e divide os arquivos CSV originais gigantes em pequenos lotes (chunks) para que não sobrecarreguem a memória do servidor.
3. **Tratamento:** Limpa, formata e compara os dados recebidos com os do mês anterior para identificar o que é registro novo ou atualizado.
4. **Armazenamento:** Salva os arquivos finais de forma altamente otimizada e comprimida (formato Apache Parquet) na pasta local `datalake/`.

## ⚙️ Tecnologias

O projeto é focado em alta performance de processamento e concorrência:
* **Elixir:** Linguagem principal, utilizada para orquestrar o processo e paralelizar o download e a leitura dos arquivos de forma confiável.
* **Explorer (Polars):** Motor interno que realiza o cruzamento de milhares de linhas de dados em frações de segundo.

## 🏃 Como Executar

Para iniciar o projeto em seu ambiente local, siga os comandos abaixo:

```bash
# 1. Instale as dependências
mix deps.get

# 2. Opcional: substitua o token do compartilhamento público
export RFB_PUBLIC_SHARE_TOKEN='gn672Ad4CF8N6TK'

# Para um WebDAV privado, use Basic Auth no formato usuario:senha:
# export RFB_WEBDAV_AUTH='usuario:senha'

# 3. Execute a tarefa principal de orquestração e download
mix rfb.baixar
```

Por padrão, os arquivos temporários são gravados em `./tmp/rfb`, no volume
principal do projeto. Para alterar o local, defina `RFB_TMP_DIR`.

Os CSVs são lidos com os schemas oficiais por entidade, têm os espaços
externos removidos e o campo `capital_social` é convertido para número.

Após o processamento concluído com sucesso, todos os arquivos organizados estarão disponíveis dentro da pasta `datalake/` na raiz do seu repositório.
