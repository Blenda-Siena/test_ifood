# iFood Case Técnico - Engenharia de Dados e Governança

## Visão Executiva

Esta solução implementa um pipeline Lakehouse para dados Yellow Taxi da TLC, com foco em governança e qualidade de dados.

Principais entregas:

- ingestão dos arquivos de janeiro a maio de 2023;
- organização em camadas Landing, Bronze, Silver e Gold;
- validações de qualidade com segregação de registros rejeitados;
- tabelas de auditoria e métricas de qualidade;
- respostas analíticas persistidas em tabelas Gold.

## Objetivo

Este projeto resolve o case técnico de Engenharia de Dados com foco em Governança e Qualidade de Dados, utilizando os dados públicos de corridas de Yellow Taxi da cidade de Nova York, disponibilizados pela TLC.

O objetivo é ingerir os arquivos de janeiro a maio de 2023 em um Data Lake, aplicar validações de qualidade, disponibilizar uma camada confiável para consumo analítico e responder às perguntas de negócio solicitadas.

## Tecnologias Utilizadas

- Databricks Free Edition
- PySpark
- Delta Lake
- SQL
- Arquitetura Lakehouse
- Modelo Medalhão: Bronze, Silver e Gold

## Estrutura do Repositório

```txt
ifood-case/
├─ src/
│  ├─ 00_setup_and_config.py
│  ├─ 01_ingestion_bronze.py
│  └─ 02_quality_and_cleansing_silver.py
├─ analysis/
│  └─ 03_business_gold_analysis.py
├─ docs/
│  ├─ architecture.md
│  ├─ governance_and_quality.md
│  ├─ data_dictionary.md
│  └─ production_evolution.md
├─ assets/
│  └─ architecture_diagram.md
├─ README.md
└─ requirements.txt
```


## Arquitetura da Solução

A solução utiliza a arquitetura Medalhão, separando o pipeline em camadas com responsabilidades claras:

- Landing: armazenamento dos arquivos originais disponibilizados pela TLC.
- Bronze: ingestão estruturada dos dados e inclusão de metadados técnicos.
- Silver: aplicação de contrato de dados, regras de qualidade e separação de registros rejeitados.
- Gold: tabelas agregadas para responder às perguntas de negócio.

## Tabelas Criadas

| Camada | Tabela | Finalidade |
|---|---|---|
| Bronze | `ifood_case.bronze_yellow_taxi` | Dados ingeridos com metadados técnicos |
| Bronze | `ifood_case.bronze_file_inventory` | Inventário dos arquivos processados |
| Silver | `ifood_case.silver_yellow_taxi` | Dados limpos e aprovados |
| Silver | `ifood_case.silver_yellow_taxi_rejected` | Registros rejeitados pelas regras de qualidade |
| Silver | `ifood_case.dq_metrics` | Métricas de qualidade por execução |
| Gold | `ifood_case.gold_avg_total_amount_by_month` | Média de valor total por mês |
| Gold | `ifood_case.gold_avg_passenger_count_by_hour_may` | Média de passageiros por hora em maio |

## Como Executar

1. Importar os arquivos do repositório no Databricks.
2. Executar `src/00_setup_and_config.py`.
3. Executar `src/01_ingestion_bronze.py`.
4. Executar `src/02_quality_and_cleansing_silver.py`.
5. Executar `analysis/03_business_gold_analysis.py`.

Ao final da execução, as tabelas estarão disponíveis para consulta SQL no schema `ifood_case`.