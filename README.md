# iFood Case Tecnico - Engenharia de Dados e Governanca

## Visao Executiva

Esta solucao implementa um pipeline Lakehouse para dados de Yellow Taxi da TLC NYC, com foco em engenharia de dados, governanca, qualidade e disponibilizacao analitica.

O pipeline foi executado no Databricks utilizando PySpark, Delta Lake e Unity Catalog Volume como landing zone. A solucao processa os arquivos de janeiro a maio de 2023, aplica regras de qualidade, separa registros rejeitados e publica tabelas Gold com as respostas do case.

Principais entregas:

- ingestao dos arquivos Yellow Taxi de janeiro a maio de 2023;
- armazenamento dos arquivos originais em Unity Catalog Volume;
- criacao das camadas Bronze, Silver e Gold em Delta Lake;
- padronizacao de schema e tratamento de schema drift;
- validacoes de qualidade com segregacao de registros rejeitados;
- persistencia de metricas de qualidade para auditoria;
- respostas analiticas materializadas em tabelas Gold.

## Objetivo

O objetivo do case e ingerir dados publicos de corridas de Yellow Taxi da cidade de Nova York, disponibilizados pela TLC, em um Data Lake, disponibiliza-los para consumo por SQL/PySpark e responder as perguntas de negocio solicitadas:

1. Qual a media de `total_amount` recebido em um mes considerando todos os Yellow Taxis da frota?
2. Qual a media de `passenger_count` por hora do dia no mes de maio considerando todos os taxis da frota?

## Tecnologias Utilizadas

- Databricks
- PySpark
- Delta Lake
- Unity Catalog Volume
- SQL
- Arquitetura Lakehouse
- Modelo Medalhao: Landing, Bronze, Silver e Gold

## Estrutura do Repositorio

> **Nota:** Os arquivos `.py` em `src/` e `analysis/` sao notebooks Python do Databricks exportados como arquivos-fonte, podendo ser importados novamente como notebooks no Databricks. Eles nao sao scripts Python convencionais.

```txt
ifood-case/
|- src/
|  |- 00_setup_and_config.py
|  |- 01_ingestion_bronze.py
|  `- 02_quality_and_cleansing_silver.py
|- analysis/
|  |- 03_business_gold_analysis.py
|  `- business_answers.md
|- docs/
|  |- architecture.md
|  |- governance_and_quality.md
|  |- data_dictionary.md
|  `- production_evolution.md
|- assets/
|  `- architecture_diagram.md
|- README.md
`- requirements.txt
```

## Arquitetura da Solucao

A solucao utiliza arquitetura Medalhao:

- Landing: arquivos Parquet originais da TLC armazenados em Unity Catalog Volume.
- Bronze: dados estruturados, com colunas obrigatorias e metadados de ingestao.
- Silver: dados aprovados por regras de qualidade, prontos para consumo analitico.
- Rejected: registros rejeitados, com motivo da rejeicao.
- DQ Metrics: metricas persistidas por execucao e por regra de qualidade.
- Gold: tabelas agregadas com as respostas finais do case.

Fluxo:

```txt
TLC Parquet Files
  -> Unity Catalog Volume
  -> Bronze Delta Table
  -> Silver Delta Table + Rejected Table + DQ Metrics
  -> Gold Delta Tables
  -> SQL / BI / Analises
```

## Storage e Catalogo

Landing zone (Unity Catalog Volume path):

Landing zone:

```txt
/Volumes/workspace/ifood_case/landing/yellow_taxi
Para leitura Spark, o caminho utilizado e:

> **Nota:** O prefixo `dbfs:/` refere-se ao Databricks File System (DBFS), um sistema de arquivos virtual utilizado no Databricks para acessar dados armazenados em diferentes camadas de armazenamento. Ao utilizar caminhos iniciados por `dbfs:/`, voce garante compatibilidade com APIs Spark e notebooks Databricks.

Para leitura Spark, o caminho utilizado e:

```txt
dbfs:/Volumes/workspace/ifood_case/landing/yellow_taxi
```

Essa escolha evita dependencia do DBFS publico (`/FileStore`), que pode estar desabilitado em ambientes Databricks mais recentes, e aproxima a solucao de um padrao mais adequado para governanca.

## Tabelas Criadas

| Camada | Tabela | Finalidade |
|---|---|---|
| Bronze | `workspace.ifood_case.bronze_file_inventory` | Inventario dos arquivos processados, com status, tamanho e checksum |
| Bronze | `workspace.ifood_case.bronze_yellow_taxi` | Dados padronizados com metadados tecnicos |
| Silver | `workspace.ifood_case.silver_yellow_taxi` | Dados aprovados pelas regras de qualidade |
| Silver | `workspace.ifood_case.silver_yellow_taxi_rejected` | Registros rejeitados, com motivo da rejeicao |
| Silver | `workspace.ifood_case.dq_metrics` | Metricas de qualidade por execucao e regra |
| Gold | `workspace.ifood_case.gold_avg_total_amount_by_month` | Media mensal de `total_amount` |
| Gold | `workspace.ifood_case.gold_avg_passenger_count_by_hour_may` | Media de passageiros por hora em maio |

## Contrato de Dados da Silver

A camada Silver garante a presenca das colunas obrigatorias do case:

| Coluna | Tipo | Descricao |
|---|---|---|
| `VendorID` | long | Identificador do fornecedor da corrida |
| `tpep_pickup_datetime` | timestamp | Data e hora de inicio da corrida |
| `tpep_dropoff_datetime` | timestamp | Data e hora de fim da corrida |
| `passenger_count` | double | Quantidade de passageiros |
| `total_amount` | double | Valor total cobrado |

Alem delas, a Silver preserva metadados de governanca como `source_file`, `source_url`, `ingestion_run_id`, `ingestion_timestamp`, `quality_run_id` e `quality_timestamp`.

## Regras de Qualidade

| Regra | Dimensao | Acao |
|---|---|---|
| `VendorID` nao nulo | Completude | Rejeita registro |
| `tpep_pickup_datetime` nao nulo | Completude | Rejeita registro |
| `tpep_dropoff_datetime` nao nulo | Completude | Rejeita registro |
| `passenger_count` nao nulo | Completude | Rejeita registro |
| `passenger_count >= 0` | Validade | Rejeita registro |
| `total_amount` nao nulo | Completude | Rejeita registro |
| `total_amount >= 0` | Validade | Rejeita registro |
| `tpep_dropoff_datetime > tpep_pickup_datetime` | Consistencia | Rejeita registro |
| pickup entre `2023-01-01` e `2023-06-01` | Conformidade temporal | Rejeita registro |

## Resultados de Qualidade

| Registros na Bronze | 16,186,386 |
| Registros aprovados na Silver | 15,611,119 |
| Registros rejeitados | 575,267 |
| Taxa aprovada | 96.45% |
| Taxa rejeitada | 3.55% |
| Registros aprovados na Silver | 15.611.119 |
| Registros rejeitados | 575.267 |
| Taxa aprovada | 96,45% |
| Taxa rejeitada | 3,55% |

| `passenger_count_null` | 428,665 | 2.65% |
| `total_amount_negative` | 141,407 | 0.87% |
| `dropoff_before_or_equal_pickup` | 6,181 | 0.038% |
| `pickup_outside_expected_period` | 104 | 0.0006% |
| `passenger_count_null` | 428.665 | 2,65% |
| `total_amount_negative` | 141.407 | 0,87% |
| `dropoff_before_or_equal_pickup` | 6.181 | 0,038% |
| `pickup_outside_expected_period` | 104 | 0,0006% |

Observacao: um mesmo registro pode violar mais de uma regra, por isso a soma das violacoes por regra pode ser maior que o total de registros rejeitados.

## Resultados Analiticos

| Mes | Total de corridas | Media `total_amount` |
|---|---:|---:|
| 2023-01 | 2,968,728 | 27.40 |
| 2023-02 | 2,811,363 | 27.31 |
| 2023-03 | 3,285,263 | 28.23 |
| 2023-04 | 3,166,753 | 28.72 |
| 2023-05 | 3,379,012 | 29.38 |
| 2023-02 | 2.811.363 | 27,31 |
| 2023-03 | 3.285.263 | 28,23 |
| 2023-04 | 3.166.753 | 28,72 |
| 2023-05 | 3.379.012 | 29,38 |

A maior media mensal ocorreu em maio de 2023, com `avg_total_amount` de 29,38.

| Hora | Total de corridas | Media passageiros |
|---:|---:|---:|
| 0 | 89,563 | 1.41 |
| 1 | 58,148 | 1.42 |
| 2 | 37,459 | 1.44 |
| 3 | 24,335 | 1.44 |
| 4 | 15,901 | 1.39 |
| 5 | 18,458 | 1.27 |
| 6 | 46,407 | 1.23 |
| 7 | 93,854 | 1.25 |
| 8 | 128,339 | 1.27 |
| 9 | 143,942 | 1.28 |
| 10 | 156,819 | 1.32 |
| 11 | 170,800 | 1.33 |
| 12 | 184,013 | 1.35 |
| 13 | 188,413 | 1.36 |
| 14 | 204,814 | 1.36 |
| 15 | 209,120 | 1.37 |
| 16 | 208,970 | 1.37 |
| 17 | 228,062 | 1.37 |
| 18 | 242,091 | 1.36 |
| 19 | 217,169 | 1.37 |
| 20 | 192,633 | 1.38 |
| 21 | 196,571 | 1.40 |
| 22 | 181,573 | 1.41 |
| 23 | 141,558 | 1.41 |
| 20 | 192.633 | 1,38 |
| 21 | 196.571 | 1,40 |
| 22 | 181.573 | 1,41 |
| 23 | 141.558 | 1,41 |

A maior media de passageiros por corrida ocorreu entre 2h e 3h, com media de 1,44 passageiros. A menor media ocorreu as 6h, com 1,23 passageiros.

## Como Executar
src/00_setup_and_config (Databricks notebook, e.g., `00_setup_and_config.py` or `.dbc`)
src/01_ingestion_bronze (Databricks notebook, e.g., `01_ingestion_bronze.py` or `.dbc`)
src/02_quality_and_cleansing_silver (Databricks notebook, e.g., `02_quality_and_cleansing_silver.py` or `.dbc`)
analysis/03_business_gold_analysis (Databricks notebook, e.g., `03_business_gold_analysis.py` or `.dbc`)
src/00_setup_and_config
src/01_ingestion_bronze
src/02_quality_and_cleansing_silver
analysis/03_business_gold_analysis
```

Execute na seguinte ordem:

1. `src/00_setup_and_config`
Nos notebooks `01`, `02` e `03`, o comando `%run` (específico do Databricks) deve ficar isolado na primeira célula. **Atenção:** `%run` não é um comando Python padrão e só funciona em notebooks Databricks.
3. `src/02_quality_and_cleansing_silver`
4. `analysis/03_business_gold_analysis`

Nos notebooks `01`, `02` e `03`, o comando `%run` deve ficar isolado na primeira celula:

```python
%run "./00_setup_and_config"
```

No notebook de analise, como ele esta na pasta `analysis`, o caminho e:

```python
%run "../src/00_setup_and_config"
```

## Consultas de Validacao

Listar tabelas criadas:

```sql
SHOW TABLES IN workspace.ifood_case;
```

Consultar metricas de qualidade:

```sql
SELECT *
FROM workspace.ifood_case.dq_metrics
ORDER BY invalid_records DESC;
```

Consultar resultados Gold:

```sql
SELECT *
FROM workspace.ifood_case.gold_avg_total_amount_by_month
ORDER BY reference_month;
```

```sql
SELECT *
FROM workspace.ifood_case.gold_avg_passenger_count_by_hour_may
ORDER BY pickup_hour;
```

## Evolucao Para Producao

Em um ambiente corporativo, a solucao pode evoluir com:
- orquestração via Databricks Workflows ou Airflow;
- CI/CD com GitHub Actions, GitLab CI ou Azure DevOps;
- Unity Catalog com permissões por grupo, lineage e auditoria;
- expectativas de qualidade com Delta Live Tables ou Great Expectations;
- alertas para falha de ingestão, queda de volumetria e alta taxa de rejeição;
- dashboards em Databricks SQL, Power BI, Tableau ou Looker;
- ambientes separados de desenvolvimento, homologação e produção.