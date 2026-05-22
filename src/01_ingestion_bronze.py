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
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, LongType, TimestampType

ensure_environment()

run_id = str(uuid4())
ingestion_ts = datetime.utcnow()
os.makedirs(LANDING_LOCAL_PATH, exist_ok=True)


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
    status = "SUCCESS"
    error_message = None

    try:
        if not os.path.exists(local_path):
            urllib.request.urlretrieve(source_url, local_path)

        checksum = file_sha256(local_path)
        file_size = os.path.getsize(local_path)

        raw_df = spark.read.parquet(f"{LANDING_PATH}/{file_name}")

        bronze_df = raw_df.select(
            F.col("VendorID").cast("long").alias("VendorID"),
            F.col("tpep_pickup_datetime").cast("timestamp").alias("tpep_pickup_datetime"),
            F.col("tpep_dropoff_datetime").cast("timestamp").alias("tpep_dropoff_datetime"),
            F.col("passenger_count").cast("double").alias("passenger_count"),
            F.col("total_amount").cast("double").alias("total_amount"),
        ).withColumn("source_file", F.lit(file_name)) \
         .withColumn("source_url", F.lit(source_url)) \
         .withColumn("ingestion_run_id", F.lit(run_id)) \
         .withColumn("ingestion_timestamp", F.lit(ingestion_ts)) \
         .withColumn("pickup_year", F.year("tpep_pickup_datetime")) \
         .withColumn("pickup_month", F.month("tpep_pickup_datetime"))

        bronze_dfs.append(bronze_df)

    except Exception as exc:
        status = "ERROR"
        error_message = str(exc)
        checksum = None
        file_size = None

    inventory_rows.append(
        (run_id, ingestion_ts, month, file_name, source_url, status, file_size, checksum, error_message)
    )

inventory_schema = StructType([
    StructField("ingestion_run_id", StringType(), False),
    StructField("ingestion_timestamp", TimestampType(), False),
    StructField("reference_month", IntegerType(), False),
    StructField("file_name", StringType(), False),
    StructField("source_url", StringType(), False),
    StructField("status", StringType(), False),
    StructField("file_size_bytes", LongType(), True),
    StructField("sha256_checksum", StringType(), True),
    StructField("error_message", StringType(), True),
])

spark.createDataFrame(inventory_rows, inventory_schema) \
    .write.format("delta") \
    .mode("append") \
    .saveAsTable(BRONZE_INVENTORY_TABLE)

if len(bronze_dfs) != len(MONTHS):
    raise Exception("Ingestion failed: not all monthly files were processed successfully.")

bronze_final_df = reduce(lambda left, right: left.unionByName(right), bronze_dfs)

bronze_final_df.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .partitionBy("pickup_year", "pickup_month") \
    .saveAsTable(BRONZE_TAXI_TABLE)

display(spark.table(BRONZE_INVENTORY_TABLE).orderBy(F.desc("ingestion_timestamp")))