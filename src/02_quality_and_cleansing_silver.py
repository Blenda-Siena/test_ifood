# Databricks notebook source
# MAGIC %run "./00_setup_and_config"

# COMMAND ----------

from uuid import uuid4
from datetime import datetime

from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    LongType,
    DoubleType,
    TimestampType,
)

ensure_environment()

quality_run_id = str(uuid4())
quality_ts = datetime.utcnow()

print("Iniciando validacao de qualidade Silver")
print(f"Quality Run ID: {quality_run_id}")
print(f"Lendo tabela Bronze: {BRONZE_TAXI_TABLE}")

print("Tabelas disponiveis antes da Silver:")
spark.sql(f"SHOW TABLES IN {CATALOG}.{SCHEMA}").show(truncate=False)

df_bronze = spark.table(BRONZE_TAXI_TABLE)

total_bronze_records = df_bronze.count()
print(f"Total de registros na Bronze: {total_bronze_records}")

rule_expressions = {
    "vendor_id_null": F.col("VendorID").isNull(),
    "pickup_datetime_null": F.col("tpep_pickup_datetime").isNull(),
    "dropoff_datetime_null": F.col("tpep_dropoff_datetime").isNull(),
    "passenger_count_null": F.col("passenger_count").isNull(),
    "passenger_count_negative": F.col("passenger_count") < 0,
    "total_amount_null": F.col("total_amount").isNull(),
    "total_amount_negative": F.col("total_amount") < 0,
    "dropoff_before_or_equal_pickup": F.col("tpep_dropoff_datetime") <= F.col("tpep_pickup_datetime"),
    "pickup_outside_expected_period": (
        (F.col("tpep_pickup_datetime") < F.lit("2023-01-01 00:00:00")) |
        (F.col("tpep_pickup_datetime") >= F.lit("2023-06-01 00:00:00"))
    ),
}

reject_reason_expr = F.concat_ws(
    "|",
    *[
        F.when(condition, F.lit(rule_name)).otherwise(F.lit(None))
        for rule_name, condition in rule_expressions.items()
    ]
)

df_validated = (
    df_bronze
    .withColumn("dq_reject_reason", reject_reason_expr)
    .withColumn(
        "dq_status",
        F.when(F.length(F.col("dq_reject_reason")) == 0, F.lit("APPROVED"))
        .otherwise(F.lit("REJECTED"))
    )
    .withColumn("quality_run_id", F.lit(quality_run_id))
    .withColumn("quality_timestamp", F.lit(quality_ts))
)

df_silver = df_validated.filter(F.col("dq_status") == "APPROVED").select(
    "VendorID",
    "tpep_pickup_datetime",
    "tpep_dropoff_datetime",
    "passenger_count",
    "total_amount",
    "source_file",
    "source_url",
    "ingestion_run_id",
    "ingestion_timestamp",
    "quality_run_id",
    "quality_timestamp",
    "pickup_year",
    "pickup_month",
)

df_rejected = df_validated.filter(F.col("dq_status") == "REJECTED")

approved_count = df_silver.count()
rejected_count = df_rejected.count()

print(f"Registros aprovados para Silver: {approved_count}")
print(f"Registros rejeitados: {rejected_count}")

print("Gravando tabela Silver...")
df_silver.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .partitionBy("pickup_year", "pickup_month") \
    .saveAsTable(SILVER_TAXI_TABLE)

print(f"Tabela Silver criada: {SILVER_TAXI_TABLE}")

print("Gravando tabela de rejeitados...")
df_rejected.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(SILVER_REJECTED_TABLE)

print(f"Tabela de rejeitados criada: {SILVER_REJECTED_TABLE}")

metrics_rows = []

for rule_name, condition in rule_expressions.items():
    invalid_records = df_validated.filter(condition).count()
    invalid_percentage = invalid_records / total_bronze_records if total_bronze_records else 0

    metrics_rows.append(
        (
            quality_run_id,
            quality_ts,
            SILVER_TAXI_TABLE,
            rule_name,
            invalid_records,
            total_bronze_records,
            invalid_percentage,
        )
    )

metrics_schema = StructType(
    [
        StructField("quality_run_id", StringType(), False),
        StructField("quality_timestamp", TimestampType(), False),
        StructField("target_table", StringType(), False),
        StructField("quality_rule", StringType(), False),
        StructField("invalid_records", LongType(), False),
        StructField("total_records", LongType(), False),
        StructField("invalid_percentage", DoubleType(), False),
    ]
)

metrics_df = spark.createDataFrame(metrics_rows, metrics_schema)

print("Gravando metricas de qualidade...")
metrics_df.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(DQ_METRICS_TABLE)

print(f"Tabela de metricas criada: {DQ_METRICS_TABLE}")

print("Tabelas disponiveis apos Silver:")
spark.sql(f"SHOW TABLES IN {CATALOG}.{SCHEMA}").show(truncate=False)

print("Metricas de qualidade:")
display(spark.table(DQ_METRICS_TABLE).orderBy(F.desc("invalid_records")))

print("Amostra Silver:")
display(spark.table(SILVER_TAXI_TABLE).limit(10))

print("Amostra rejeitados:")
display(spark.table(SILVER_REJECTED_TABLE).limit(10))