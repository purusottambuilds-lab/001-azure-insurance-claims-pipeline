# Databricks notebook source
# ================================================================
# nb_02_transform
# Project 001   : Insurance Claims Risk Scoring
# Purpose       : Clean, risk-score, write Delta Lake
# Folder        : dir_001_claims_insurance
# Author        : Purusottam Swain | purusottam.builds@gmail.com
# ================================================================

# COMMAND ----------

# CELL 1 - Imports and Config

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
from pyspark.sql.types import DoubleType
from delta.tables import DeltaTable
import uuid

storage_account_name = "sainsuranceps01"
storage_account_key = "YOUR_STORAGE_ACCOUNT_KEY_HERE"

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
run_id = str(uuid.uuid4())[:8]

print(f"Pipeline run ID: {run_id}")


# COMMAND ----------

# CELL 2 - Read Raw CSV

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

# CELL 3 - Clean and Standardize

df_clean = (
    df_raw.dropDuplicates(["policy_number"])
    .na.drop(subset=["policy_number", "total_claim_amount"])
    .withColumn("total_claim_amount", col("total_claim_amount").cast(DoubleType()))
    .withColumn("injury_claim", col("injury_claim").cast(DoubleType()))
    .withColumn("property_claim", col("property_claim").cast(DoubleType()))
    .withColumn("vehicle_claim", col("vehicle_claim").cast(DoubleType()))
    .withColumn("fraud_reported", upper(trim(col("fraud_reported"))))
    .withColumn("incident_severity", upper(trim(col("incident_severity"))))
    .withColumn("incident_type", upper(trim(col("incident_type"))))
)

print(f"Clean records: {df_clean.count()}")


# COMMAND ----------

# CELL 4 - Multi-Factor Risk Scoring

# HIGH   : fraud = Y AND (claim > 50000 OR severity = MAJOR DAMAGE)
# MEDIUM : fraud = Y OR claim > 30000 OR severity in (MAJOR DAMAGE, TOTAL LOSS)
# LOW    : all others

df_scored = (
    df_clean.withColumn(
        "risk_score",
        when(
            (col("fraud_reported") == "Y")
            & (
                (col("total_claim_amount") > 50000)
                | (col("incident_severity") == "MAJOR DAMAGE")
            ),
            lit("HIGH"),
        )
        .when(
            (col("fraud_reported") == "Y")
            | (col("total_claim_amount") > 30000)
            | (col("incident_severity").isin("MAJOR DAMAGE", "TOTAL LOSS")),
            lit("MEDIUM"),
        )
        .otherwise(lit("LOW")),
    )
    .withColumn("risk_version", lit("v2.0"))
    .withColumn("pipeline_run_id", lit(run_id))
    .withColumn("processed_at", current_timestamp())
)

print("Risk Score Distribution:")
display(df_scored.groupBy("risk_score").count().orderBy("risk_score"))


# COMMAND ----------

# CELL 5 - Write Delta Lake with Schema Evolution

df_scored.write.format("delta").mode("overwrite").option("mergeSchema", "true").save(
    delta_path
)

delta_table = DeltaTable.forPath(spark, delta_path)
display(delta_table.history())
print(f"Delta written: {delta_path}")


# COMMAND ----------

# CELL 6 - Write Risk Summary (Aggregated BI Table)

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

print(f"Summary written: {summary_path}")
display(df_summary.orderBy("risk_score"))


# COMMAND ----------

# CELL 7 - Final Verification

df_delta_verify = spark.read.format("delta").load(delta_path)
df_summary_verify = spark.read.format("delta").load(summary_path)

print(f"Main table   : {df_delta_verify.count()} records")
print(f"Summary table: {df_summary_verify.count()} records")
print(f"Pipeline run : {run_id}")
print("Transformation complete")


# COMMAND ----------
