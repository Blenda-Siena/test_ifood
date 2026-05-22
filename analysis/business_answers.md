# Business Answers

Este arquivo descreve as respostas do case técnico com base na camada Silver processada.

## Pergunta 1
**Qual a média de `total_amount` recebido em um mês considerando todos os yellow táxis da frota?**

Resposta: o cálculo é feito a partir da tabela Silver `ifood_case.silver_yellow_taxi`, agrupando pela extração do mês de `tpep_pickup_datetime` e calculando a média de `total_amount`.

SQL de exemplo:

```sql
SELECT
  month(tpep_pickup_datetime) AS pickup_month,
  ROUND(AVG(total_amount), 2) AS avg_total_amount
FROM FROM ifood_case.silver_yellow_taxi
GROUP BY 1
ORDER BY 1;
```

A tabela Gold resultante é `ifood_case.gold_avg_total_amount_by_month`.

## Pergunta 2
**Qual a média de `passenger_count` por cada hora do dia que pegaram táxi no mês de maio considerando todos os táxis da frota?**

Resposta: a lógica filtra somente o mês de maio (`month(tpep_pickup_datetime) = 5`), extrai a hora da viagem e calcula a média do `passenger_count` por hora.

SQL de exemplo:

```sql
SELECT
  hour(tpep_pickup_datetime) AS pickup_hour,
  ROUND(AVG(passenger_count), 2) AS avg_passenger_count
FROM FROM ifood_case.silver_yellow_taxi
WHERE month(tpep_pickup_datetime) = 5
GROUP BY 1
ORDER BY 1;
```

A tabela Gold resultante é `ifood_case.gold_avg_passenger_count_by_hour_may`.

## Notas de implementação

- A camada Bronze mantém os arquivos originais de janeiro a maio de 2023.
- A camada Silver garante que as colunas obrigatórias existam e que registros inválidos sejam removidos antes do consumo.
- A camada Gold expõe resultados prontos para consulta e consumo de usuários finais.
