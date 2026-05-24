# Databricks notebook source
# ================================================================
# nb_02_transform
# Project_001: Insurance Claims Risk Scoring
# Purpose: PySpark transformation, risk scoring, Delta lake write
# Author: Purusottam Swain | purusottam.builds@gmail.com
# ================================================================

# COMMAND ----------

# cell-1: imports and storage config

from pyspark.sql.functions import (
    col,
    when,
    lit,
    upper,
    trim,
    current_timestamp,
    round as spark_round,
    count,
    avg,
    max,
    min,
)

from pyspark.sql.types import DoubleType, IntegerType
from delta.tables import DeltaTable
import uuid

storage_account_name = "sainsuranceps01"
storage_account_key = "YOUR_STORAGE_KEY_HERE"

spark.conf.set(
    f"fs.azure.account.key.{storage_account_name}.dfs.core.windows.net",
    storage_account_key,
)

raw_path = (
    f"abfss://raw@{storage_account_name}.dfs.core.windows.net/insurance_claims.csv"
)
delta_path = f"abfss://transformed@{storage_account_name}.dfs.core.windows.net/insurance_claims_delta"
summary_path = (
    f"abfss://curated@{storage_account_name}.dfs.core.windows.net/risk_summary"
)

pipeline_run_id = str(uuid.uuid4())[:8]

print(f"Pipeline run ID: {pipeline_run_id}")

# COMMAND ----------

# cell-2: read raw csv

df_raw = (
    spark.read.format("csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .option("nullValue", "?")
    .load(raw_path)
)

print(f"Records loaded: {df_raw.count()}")
display(df_raw.limit(5))

# COMMAND ----------

# cell-3: clean and standardise

df_clean = (
    df_raw.dropDuplicates(["policy_number"])
    .na.drop(subset=["policy_number", "total_claim_amount"])
    .withColumn("total_claim_amount", col("total_claim_amount").cast(DoubleType()))
    .withColumn("injury_claim", col("injury_claim").cast(DoubleType()))
    .withColumn("property_claim", col("property_claim").cast(DoubleType()))
    .withColumn("vehicle_claim", col("vehicle_claim").cast(DoubleType()))
    .withColumn("fraud_reeported", upper(trim(col("fraud_reported"))))
    .withColumn("incident_severity", upper(trim(col("incident_severity"))))
    .withColumn("incident_type", upper(trim(col("incident_type"))))
)

print(f"Clean records: {df_clean.count()}")

# COMMAND ----------

# cell-4: multi-factor risk scoring

# Risk factor 1: Fraud flag
# Risk factor 2: Total claim amount
# Risk factor 3: Incident severity
# Risk factor 4: Claim composition (injury vs property vs vehicle)

# Scoring logic:
#   HIGH: fraud = Y AND (claim > 5000 OR severity = MAJOR DAMAGE)
#   MEDIUM: fraud = Y OR claim > 3000 OR severity in (MAJOR DAMAGE, TOTAL LOSS)
#   LOW: all others

df_scored = (
    df_clean.withColumn(
        "risk_score",
        when(
            (col("fraud_reported") == "Y")
            & (
                (col("total_claim_amount") > 5000)
                | (col("incident_severity") == "MAJOR DAMAGE")
            ),
            lit("HIGH"),
        )
        .when(
            (col("fraud_reeported") == "Y")
            | (col("total_claim_amount") > 3000)
            | (col("incident_severity").isin("MAJOR DAMAGE", "TOTAL LOSS")),
            lit("MEDIUM"),
        )
        .otherwise(lit("LOW")),
    )
    .withColumn("risk_version", lit("v2.0"))
    .withColumn("pipeline_run_id", lit(pipeline_run_id))
    .withColumn("processed_at", current_timestamp())
)

print("Risk Score Distribution:")
display(df_scored.groupBy("risk_score").count().orderBy("risk_score"))

# COMMAND ----------

# cell-5: write Delta Lake with schema evolution

df_scored.write.format("delta").mode("overwrite").option("mergeSchema", "true").save(
    delta_path
)

print(f"Delta Lake written with schema evolution: {delta_path}")

# show Delta Table history (each run creates a new version)
delta_table = DeltaTable.forPath(spark, delta_path)
display(delta_table.history())

# COMMAND ----------

# cell-6: create aggregated risk summary (BI-ready Gold table)

df_summary = (
    df_scored.groupBy(
        "risk_score", "incident_severity", "incident_type", "policy_state"
    )
    .agg(
        count("policy_number").alias("total_claims"),
        spark_round(avg("total_claim_amount"), 2).alias("avg_claim_amount"),
        spark_round(max("total_claim_amount"), 2).alias("max_claim_amount"),
        spark_round(min("total_claim_amount"), 2).alias("min_claim_amount"),
        count(when(col("fraud_reported") == "Y", 1)).alias("fraud_count"),
    )
    .withColumn("processed_at", current_timestamp())
)

df_summary.write.format("delta").mode("overwrite").option("mergeSchema", "true").save(
    summary_path
)

print(f"Risk summary written: {summary_path}")
display(df_summary.orderBy("risk_score", "incident_severity"))

# COMMAND ----------

#  cell-7: final verification

df_delta_verify = spark.read.format("delta").load(delta_path)
df_summary_verify = spark.read.format("delta").load(summary_path)

print(f"Main table: {df_delta_verify.count()} records")
print(f"Summary table: {df_summary_verify.count()} records")
print(f"Pipeline run: {pipeline_run_id}")

print("Transformation complete - ready for ADF Copy activities")

# COMMAND ----------
