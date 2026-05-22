# Arquitetura da Solução

## Fluxo

TLC Parquet Files -> Landing Zone -> Bronze -> Silver -> Gold -> SQL/BI

## Camadas

### Landing
Preserva os arquivos originais.

### Bronze
Cria a primeira representação estruturada e adiciona metadados técnicos.

### Silver
Aplica contrato de dados, regras de qualidade e separa registros inválidos.

### Gold
Disponibiliza agregações prontas para consumo analítico.