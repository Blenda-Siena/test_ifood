# Databricks notebook source
# MAGIC %run "./00_setup_and_config"

# COMMAND ----------

import os
import hashlib
import urllib.request
from uuid import uuid4
from datetime import datetime
from functools import reduce

from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    LongType,
    TimestampType,
)

ensure_environment()

run_id = str(uuid4())
ingestion_ts = datetime.utcnow()

os.makedirs(LANDING_LOCAL_PATH, exist_ok=True)

print("Iniciando ingestao Bronze")
print(f"Run ID: {run_id}")
print(f"Landing local path: {LANDING_LOCAL_PATH}")
print(f"Landing Spark path: {LANDING_SPARK_PATH}")


def file_sha256(path):
    hash_value = hashlib.sha256()

    with open(path, "rb") as file:
        for chunk in iter(lambda: file.read(1024 * 1024), b""):
            hash_value.update(chunk)

    return hash_value.hexdigest()


inventory_rows = []
bronze_dfs = []

for month in MONTHS:
    file_name = f"yellow_tripdata_2023-{month:02d}.parquet"
    source_url = TLC_URL_TEMPLATE.format(month=month)
    local_path = f"{LANDING_LOCAL_PATH}/{file_name}"
    spark_path = f"{LANDING_SPARK_PATH}/{file_name}"

    status = "SUCCESS"
    error_message = None
    checksum = None
    file_size = None

    print(f"Processando mes {month:02d}: {file_name}")

    try:
        if not os.path.exists(local_path):
            print(f"Baixando arquivo: {source_url}")
            urllib.request.urlretrieve(source_url, local_path)
        else:
            print("Arquivo ja existe no volume. Reutilizando.")

        checksum = file_sha256(local_path)
        file_size = os.path.getsize(local_path)

        print(f"Lendo parquet com Spark: {spark_path}")
        raw_df = spark.read.parquet(spark_path)

        bronze_df = (
            raw_df.select(
                F.col("VendorID").cast("long").alias("VendorID"),
                F.col("tpep_pickup_datetime").cast("timestamp").alias("tpep_pickup_datetime"),
                F.col("tpep_dropoff_datetime").cast("timestamp").alias("tpep_dropoff_datetime"),
                F.col("passenger_count").cast("double").alias("passenger_count"),
                F.col("total_amount").cast("double").alias("total_amount"),
            )
            .withColumn("source_file", F.lit(file_name))
            .withColumn("source_url", F.lit(source_url))
            .withColumn("ingestion_run_id", F.lit(run_id))
            .withColumn("ingestion_timestamp", F.lit(ingestion_ts))
            .withColumn("pickup_year", F.year("tpep_pickup_datetime"))
            .withColumn("pickup_month", F.month("tpep_pickup_datetime"))
        )

        bronze_dfs.append(bronze_df)
        print(f"Mes {month:02d} processado com sucesso.")

    except Exception as exc:
        status = "ERROR"
        error_message = str(exc)
        print(f"Erro no mes {month:02d}: {error_message}")

    inventory_rows.append(
        (
            run_id,
            ingestion_ts,
            month,
            file_name,
            source_url,
            status,
            file_size,
            checksum,
            error_message,
        )
    )


inventory_schema = StructType(
    [
        StructField("ingestion_run_id", StringType(), False),
        StructField("ingestion_timestamp", TimestampType(), False),
        StructField("reference_month", IntegerType(), False),
        StructField("file_name", StringType(), False),
        StructField("source_url", StringType(), False),
        StructField("status", StringType(), False),
        StructField("file_size_bytes", LongType(), True),
        StructField("sha256_checksum", StringType(), True),
        StructField("error_message", StringType(), True),
    ]
)

inventory_df = spark.createDataFrame(inventory_rows, inventory_schema)

inventory_df.write.format("delta").mode("append").saveAsTable(BRONZE_INVENTORY_TABLE)

print(f"Inventario gravado em: {BRONZE_INVENTORY_TABLE}")
display(inventory_df)

if len(bronze_dfs) != len(MONTHS):
    raise Exception(
        f"Ingestao incompleta: {len(bronze_dfs)} de {len(MONTHS)} arquivos foram processados com sucesso."
    )

bronze_final_df = reduce(lambda left, right: left.unionByName(right), bronze_dfs)

total_bronze_records = bronze_final_df.count()
print(f"Total de registros Bronze: {total_bronze_records}")

bronze_final_df.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .partitionBy("pickup_year", "pickup_month") \
    .saveAsTable(BRONZE_TAXI_TABLE)

print(f"Tabela Bronze criada com sucesso: {BRONZE_TAXI_TABLE}")

print("Tabelas disponiveis no schema:")
spark.sql(f"SHOW TABLES IN {CATALOG}.{SCHEMA}").show(truncate=False)

print("Amostra da tabela Bronze:")
display(spark.table(BRONZE_TAXI_TABLE).limit(10))