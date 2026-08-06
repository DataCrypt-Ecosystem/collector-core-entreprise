# 🕵️‍♂️ Contexto do Agente: Datacrypt Enterprise (Worker Receita Federal)

## 📌 Visão Geral do Projeto
O **Datacrypt** é uma plataforma de inteligência investigativa e *Due Diligence* governamental/corporativa. Este repositório/módulo específico refere-se ao **Worker da Receita Federal (Módulo Enterprise)**.
Trata-se de um pipeline de processamento em lote (*batch*) concorrente, desenvolvido em Elixir. É projetado para extrair dezenas de gigabytes de dados públicos de CNPJs e Sócios via WebDAV, processá-los paralelamente e guardá-los no nosso Data Lake local.

## 🛠️ Stack Tecnológica e Infraestrutura
* **Linguagem:** Elixir (Erlang/OTP)
* **Motor de Processamento:** `Explorer` (Bindings Elixir nativos para Polars/Rust) para dados, e `GenServer` para gerir estado, concorrência e I/O de ficheiros.
* **Protocolo de Extração:** WebDAV (via API do Nextcloud) utilizando cliente HTTP moderno (ex: `Req` ou `Finch`).
* **Armazenamento (Data Lake):** Sistema de ficheiros local (diretório `./datalake` montado no ambiente).
* **Formato de Saída:** Apache Parquet (compressão `zstd`, particionado logicamente em pastas).
* **Infraestrutura de Execução:** Contentor Docker efémero (ou binário compilado via escript/Burrito) a correr numa instância Oracle Cloud ARM (Aarch64) com 24GB de RAM.

## 🏗️ Padrões Arquiteturais e Regras de Negócio (Strict Rules)

1.  **Zero Banco de Dados Relacional:**
    * Nenhum dado deste repositório deve ser guardado em PostgreSQL, MySQL, etc. Não utilizaremos o `Ecto` neste projeto. A persistência é **100% baseada em ficheiros Parquet** guardados localmente na pasta `/datalake/`.
2.  **Processamento Concorrente e Gestão de Memória (Prevenção de OOM):**
    * Os ficheiros da Receita Federal são massivos (CSVs de múltiplos Gigabytes dentro de `.zip`).
    * **NUNCA** carregue o dataset inteiro na memória de uma vez.
    * **SEMPRE** utilize fluxos baseados em `GenServer` (Atores) a trabalhar em conjunto com `Streams` para particionar e orquestrar o processamento de forma controlada. Cada ficheiro/etapa pode ter o seu próprio worker a gerir o ciclo de vida.
    * **Paralelismo de Dados (`Task.async_stream`):** Utilize `Task.async_stream` como a ferramenta principal para processar múltiplos ficheiros em simultâneo com concorrência limitada (ex: processar apenas 4 CSVs de cada vez para não esgotar a RAM).
    * **Streaming Nativo HTTP:** O download de ficheiros via cliente HTTP (como o `Req`) deve ser feito obrigatoriamente em modo de streaming contínuo para o disco (ex: usando a opção `into: File.stream!()`), garantindo que os gigabytes do `.zip` não passem pela memória RAM.
    * O cruzamento e formatação final dos DataFrames deve ser feito usando `Explorer.DataFrame.lazy/1` (Lazy Frames) para processamento ultrarrápido otimizado pelo motor em Rust.
3.  **Natureza Efémera (CLI Desacoplado):**
    * Este sistema não é uma aplicação Phoenix/Web. É uma ferramenta de linha de comandos (CLI), orquestrada via *Mix Tasks* (ex: `mix rfb.baixar`) ou executável empacotado.
    * Deve inicializar a Árvore de Supervisão (*Supervision Tree*), processar os ficheiros com resiliência a falhas, persistir no disco local, libertar os recursos e encerrar o sistema operativo com sucesso (Exit Code 0).
4.  **Estratégia de Delta (Upsert Lógico):**
    * Como a Receita atualiza os ficheiros mensalmente, o pipeline deve ler os ficheiros Parquet do mês anterior (usando `Explorer.Polars`), fazer o cruzamento (*Anti-Join/Update*) com os novos dados processados, e gerar a nova partição do mês atual.

## 🗺️ Estrutura de Diretórios Esperada

```text
/datacrypt-worker-rfb
│
├── agents.md                  # Este ficheiro de contexto
├── mix.exs                    # Manifesto de dependências do Elixir (Explorer, Req, etc)
├── README.md                  
├── Dockerfile                 # Configuração para compilação ARM64 / Deploy
│
├── /datalake/                 # 🗂️ Ponto de montagem externo dos Parquets (Ignorado no Git)
│   └── /receita_federal/
│       ├── /empresas/
│       └── /socios/
│
├── /lib/
│   ├── /datacrypt_rfb/
│   │   ├── application.ex     # Entrypoint da Árvore de Supervisão (Supervisor)
│   │   ├── cli.ex             # Ponto de entrada das Mix Tasks e parse de argumentos
│   │   │
│   │   ├── /workers/          # Atores GenServer para processamento paralelo
│   │   │   ├── downloader.ex  # GenServer para gerir estado do download via WebDAV (com Req + File.stream!)
│   │   │   └── extractor.ex   # GenServer para descompactação e conversão em chunks via Task.async_stream
│   │   │
│   │   └── /pipeline/
│   │       ├── processor.ex   # Lógica com Explorer/Polars (Limpeza, Tipagem, Delta)
│   │       └── storage.ex     # Lógica I/O para leitura/escrita Parquet no disco
│   │
│   └── datacrypt_rfb.ex       # Interface pública / API Módulo Base
│
└── /test/                     # Suíte de testes com ExUnit
