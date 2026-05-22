# Databricks notebook source
# MAGIC %run "../src/00_setup_and_config"

# COMMAND ----------

from pyspark.sql import functions as F

ensure_environment()

print("Iniciando criacao da camada Gold")
print(f"Lendo tabela Silver: {SILVER_TAXI_TABLE}")

df_silver = spark.table(SILVER_TAXI_TABLE)

total_silver_records = df_silver.count()
print(f"Total de registros na Silver: {total_silver_records}")

avg_total_amount_by_month = (
    df_silver
    .withColumn("reference_month", F.date_format("tpep_pickup_datetime", "yyyy-MM"))
    .groupBy("reference_month")
    .agg(
        F.count("*").alias("total_trips"),
        F.round(F.avg("total_amount"), 2).alias("avg_total_amount"),
    )
    .orderBy("reference_month")
)

avg_passenger_count_by_hour_may = (
    df_silver
    .filter(
        (F.col("tpep_pickup_datetime") >= F.lit("2023-05-01 00:00:00")) &
        (F.col("tpep_pickup_datetime") < F.lit("2023-06-01 00:00:00"))
    )
    .withColumn("pickup_hour", F.hour("tpep_pickup_datetime"))
    .groupBy("pickup_hour")
    .agg(
        F.count("*").alias("total_trips"),
        F.round(F.avg("passenger_count"), 2).alias("avg_passenger_count"),
    )
    .orderBy("pickup_hour")
)

avg_total_amount_by_month.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(GOLD_AVG_AMOUNT_MONTH_TABLE)

avg_passenger_count_by_hour_may.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(GOLD_AVG_PASSENGERS_HOUR_MAY_TABLE)

print(f"Tabela Gold criada: {GOLD_AVG_AMOUNT_MONTH_TABLE}")
print(f"Tabela Gold criada: {GOLD_AVG_PASSENGERS_HOUR_MAY_TABLE}")

print("Tabelas disponiveis apos Gold:")
spark.sql(f"SHOW TABLES IN {CATALOG}.{SCHEMA}").show(truncate=False)

print("Pergunta 1: media de total_amount por mes")
display(avg_total_amount_by_month)

print("Pergunta 2: media de passenger_count por hora em maio")
display(avg_passenger_count_by_hour_may)