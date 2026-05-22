# Databricks notebook source
# MAGIC %run "./00_setup_and_config"

# COMMAND ----------

from uuid import uuid4
from datetime import datetime
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, LongType, DoubleType, TimestampType

ensure_environment()

quality_run_id = str(uuid4())
quality_ts = datetime.utcnow()

df_bronze = spark.table(BRONZE_TAXI_TABLE)

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

df_validated = df_bronze.withColumn("dq_reject_reason", reject_reason_expr) \
    .withColumn(
        "dq_status",
        F.when(F.length(F.col("dq_reject_reason")) == 0, F.lit("APPROVED")).otherwise(F.lit("REJECTED"))
    ) \
    .withColumn("quality_run_id", F.lit(quality_run_id)) \
    .withColumn("quality_timestamp", F.lit(quality_ts))

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

df_silver.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .partitionBy("pickup_year", "pickup_month") \
    .saveAsTable(SILVER_TAXI_TABLE)

df_rejected.write.format("delta") \
    .mode("append") \
    .option("mergeSchema", "true") \
    .saveAsTable(SILVER_REJECTED_TABLE)

total_records = df_validated.count()

metrics_rows = []
for rule_name, condition in rule_expressions.items():
    invalid_records = df_validated.filter(condition).count()
    invalid_percentage = invalid_records / total_records if total_records else 0

    metrics_rows.append(
        (
            quality_run_id,
            quality_ts,
            SILVER_TAXI_TABLE,
            rule_name,
            invalid_records,
            total_records,
            invalid_percentage,
        )
    )

metrics_schema = StructType([
    StructField("quality_run_id", StringType(), False),
    StructField("quality_timestamp", TimestampType(), False),
    StructField("target_table", StringType(), False),
    StructField("quality_rule", StringType(), False),
    StructField("invalid_records", LongType(), False),
    StructField("total_records", LongType(), False),
    StructField("invalid_percentage", DoubleType(), False),
])

spark.createDataFrame(metrics_rows, metrics_schema) \
    .write.format("delta") \
    .mode("append") \
    .saveAsTable(DQ_METRICS_TABLE)

display(spark.table(DQ_METRICS_TABLE).orderBy(F.desc("quality_timestamp")))