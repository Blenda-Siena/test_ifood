# Governança e Qualidade de Dados

## Práticas Implementadas

- Inventário de arquivos ingeridos
- Registro de origem dos dados
- Identificador único de execução
- Checksum dos arquivos
- Métricas de qualidade persistidas
- Tabela de registros rejeitados
- Separação entre dados brutos, tratados e agregados
- Particionamento por ano e mês

## Regras de Qualidade

| Regra | Dimensão | Ação |
|---|---|---|
| VendorID não nulo | Completude | Rejeita registro |
| passenger_count não nulo | Completude | Rejeita registro |
| passenger_count >= 0 | Validade | Rejeita registro |
| total_amount não nulo | Completude | Rejeita registro |
| total_amount >= 0 | Validade | Rejeita registro |
| dropoff > pickup | Consistência | Rejeita registro |
| pickup entre Jan-Mai/2023 | Conformidade | Rejeita registro |