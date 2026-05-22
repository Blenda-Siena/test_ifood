# Databricks notebook source

SCHEMA = "ifood_case"
VOLUME = "landing"

CATALOG = spark.catalog.currentCatalog()

TLC_URL_TEMPLATE = (
    "https://d37ci6vzurychx.cloudfront.net/trip-data/"
    "yellow_tripdata_2023-{month:02d}.parquet"
)

MONTHS = [1, 2, 3, 4, 5]

VOLUME_BASE_PATH = f"/Volumes/{CATALOG}/{SCHEMA}/{VOLUME}"
LANDING_LOCAL_PATH = f"{VOLUME_BASE_PATH}/yellow_taxi"
LANDING_SPARK_PATH = f"dbfs:{LANDING_LOCAL_PATH}"

BRONZE_TAXI_TABLE = f"{CATALOG}.{SCHEMA}.bronze_yellow_taxi"
BRONZE_INVENTORY_TABLE = f"{CATALOG}.{SCHEMA}.bronze_file_inventory"

SILVER_TAXI_TABLE = f"{CATALOG}.{SCHEMA}.silver_yellow_taxi"
SILVER_REJECTED_TABLE = f"{CATALOG}.{SCHEMA}.silver_yellow_taxi_rejected"
DQ_METRICS_TABLE = f"{CATALOG}.{SCHEMA}.dq_metrics"

GOLD_AVG_AMOUNT_MONTH_TABLE = f"{CATALOG}.{SCHEMA}.gold_avg_total_amount_by_month"
GOLD_AVG_PASSENGERS_HOUR_MAY_TABLE = f"{CATALOG}.{SCHEMA}.gold_avg_passenger_count_by_hour_may"


def ensure_environment():
    if CATALOG == "hive_metastore":
        raise ValueError(
            "O catalogo atual e hive_metastore. Selecione um catalogo com Unity Catalog, "
            "como workspace ou main, antes de executar o notebook."
        )

    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")
    spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.{SCHEMA}.{VOLUME}")
    dbutils.fs.mkdirs(LANDING_SPARK_PATH)


ensure_environment()

print("Setup concluido com sucesso.")
print(f"Catalog: {CATALOG}")
print(f"Schema: {SCHEMA}")
print(f"Volume: {CATALOG}.{SCHEMA}.{VOLUME}")
print(f"Landing local path: {LANDING_LOCAL_PATH}")
print(f"Landing Spark path: {LANDING_SPARK_PATH}")
print(f"Bronze table: {BRONZE_TAXI_TABLE}")
print(f"Silver table: {SILVER_TAXI_TABLE}")