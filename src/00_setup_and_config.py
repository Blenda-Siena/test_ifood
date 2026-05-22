# Databricks notebook source

SCHEMA = "ifood_case"
BASE_DBFS_PATH = "dbfs:/FileStore/ifood_case_taxi"
LOCAL_BASE_PATH = "/dbfs/FileStore/ifood_case_taxi"

LANDING_PATH = f"{BASE_DBFS_PATH}/landing/yellow_taxi"
LANDING_LOCAL_PATH = f"{LOCAL_BASE_PATH}/landing/yellow_taxi"

TLC_URL_TEMPLATE = (
    "https://d37ci6vzurychx.cloudfront.net/trip-data/"
    "yellow_tripdata_2023-{month:02d}.parquet"
)

MONTHS = [1, 2, 3, 4, 5]

BRONZE_TAXI_TABLE = f"{SCHEMA}.bronze_yellow_taxi"
BRONZE_INVENTORY_TABLE = f"{SCHEMA}.bronze_file_inventory"
SILVER_TAXI_TABLE = f"{SCHEMA}.silver_yellow_taxi"
SILVER_REJECTED_TABLE = f"{SCHEMA}.silver_yellow_taxi_rejected"
DQ_METRICS_TABLE = f"{SCHEMA}.dq_metrics"

GOLD_AVG_AMOUNT_MONTH_TABLE = f"{SCHEMA}.gold_avg_total_amount_by_month"
GOLD_AVG_PASSENGERS_HOUR_MAY_TABLE = f"{SCHEMA}.gold_avg_passenger_count_by_hour_may"


def ensure_environment():
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {SCHEMA}")
    dbutils.fs.mkdirs(LANDING_PATH)

ensure_environment()

print("Setup concluído com sucesso.")
print(f"Schema: {SCHEMA}")
print(f"Landing path: {LANDING_PATH}")