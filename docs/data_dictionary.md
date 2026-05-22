# Dicionário de Dados

## silver_yellow_taxi

| Coluna | Tipo | Descrição |
|---|---|---|
| VendorID | long | Identificador do fornecedor da corrida |
| tpep_pickup_datetime | timestamp | Data e hora de início da corrida |
| tpep_dropoff_datetime | timestamp | Data e hora de fim da corrida |
| passenger_count | double | Quantidade de passageiros |
| total_amount | double | Valor total da corrida |
| source_file | string | Arquivo de origem |
| source_url | string | URL pública da fonte |
| ingestion_run_id | string | ID da execução de ingestão |
| ingestion_timestamp | timestamp | Data/hora da ingestão |
| quality_run_id | string | ID da execução de qualidade |
| quality_timestamp | timestamp | Data/hora da validação |
| pickup_year | integer | Ano extraído da data de pickup |
| pickup_month | integer | Mês extraído da data de pickup |