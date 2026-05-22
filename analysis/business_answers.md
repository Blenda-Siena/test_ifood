# Business Answers

Este arquivo documenta as respostas finais do case com base na camada Silver processada e nas tabelas Gold materializadas.

## Pergunta 1

**Qual a media de `total_amount` recebido em um mes considerando todos os Yellow Taxis da frota?**

A resposta foi calculada a partir da tabela `workspace.ifood_case.silver_yellow_taxi`, agrupando por mes de `tpep_pickup_datetime` e calculando a media de `total_amount`.

Tabela Gold:

```txt
workspace.ifood_case.gold_avg_total_amount_by_month
```

SQL equivalente:

```sql
SELECT
  date_format(tpep_pickup_datetime, 'yyyy-MM') AS reference_month,
  COUNT(*) AS total_trips,
  ROUND(AVG(total_amount), 2) AS avg_total_amount
FROM workspace.ifood_case.silver_yellow_taxi
GROUP BY 1
ORDER BY 1;
```

Resultado:

| Mes | Total de corridas | Media `total_amount` |
|---|---:|---:|
| 2023-01 | 2.968.728 | 27,40 |
| 2023-02 | 2.811.363 | 27,31 |
| 2023-03 | 3.285.263 | 28,23 |
| 2023-04 | 3.166.753 | 28,72 |
| 2023-05 | 3.379.012 | 29,38 |

## Pergunta 2

**Qual a media de `passenger_count` por cada hora do dia que pegaram taxi no mes de maio considerando todos os taxis da frota?**

A resposta foi calculada filtrando corridas com pickup entre `2023-05-01` e `2023-06-01`, extraindo a hora de `tpep_pickup_datetime` e calculando a media de `passenger_count`.

Tabela Gold:

```txt
workspace.ifood_case.gold_avg_passenger_count_by_hour_may
```

SQL equivalente:

```sql
SELECT
  hour(tpep_pickup_datetime) AS pickup_hour,
  COUNT(*) AS total_trips,
  ROUND(AVG(passenger_count), 2) AS avg_passenger_count
FROM workspace.ifood_case.silver_yellow_taxi
WHERE tpep_pickup_datetime >= TIMESTAMP '2023-05-01 00:00:00'
  AND tpep_pickup_datetime < TIMESTAMP '2023-06-01 00:00:00'
GROUP BY 1
ORDER BY 1;
```

Resultado:

| Hora | Total de corridas | Media passageiros |
|---:|---:|---:|
| 0 | 89.563 | 1,41 |
| 1 | 58.148 | 1,42 |
| 2 | 37.459 | 1,44 |
| 3 | 24.335 | 1,44 |
| 4 | 15.901 | 1,39 |
| 5 | 18.458 | 1,27 |
| 6 | 46.407 | 1,23 |
| 7 | 93.854 | 1,25 |
| 8 | 128.339 | 1,27 |
| 9 | 143.942 | 1,28 |
| 10 | 156.819 | 1,32 |
| 11 | 170.800 | 1,33 |
| 12 | 184.013 | 1,35 |
| 13 | 188.413 | 1,36 |
| 14 | 204.814 | 1,36 |
| 15 | 209.120 | 1,37 |
| 16 | 208.970 | 1,37 |
| 17 | 228.062 | 1,37 |
| 18 | 242.091 | 1,36 |
| 19 | 217.169 | 1,37 |
| 20 | 192.633 | 1,38 |
| 21 | 196.571 | 1,40 |
| 22 | 181.573 | 1,41 |
| 23 | 141.558 | 1,41 |

## Notas de Implementacao

- A Bronze preserva rastreabilidade por `source_file`, `source_url`, `ingestion_run_id` e `ingestion_timestamp`.
- A Silver contem somente registros aprovados pelas regras de qualidade.
- Registros rejeitados sao preservados em `workspace.ifood_case.silver_yellow_taxi_rejected`, com `dq_reject_reason`.
- As metricas de qualidade sao persistidas em `workspace.ifood_case.dq_metrics`.
