# Databricks notebook source
# ================================================================
# nb_01_dq_check
# Project_001: Insurance Claims Risk Scoring
# Purpose: Data Quality framework - profile, validate, score
# Author: Purusottam Swain | purusottam.builds@gmail.com
# ================================================================

# COMMAND ----------

# cell-1: imports and storage configuration

from pyspark.sql.functions import col, count, when, isnan, lit, current_timestamp
from pyspark.sql import Row
from pyspark.sql.types import StructType
import json
from datetime import datetime

# define storage: standalone, never rely on other notebooks
storage_account_name = "sainsuranceps01"
storage_account_key = "YOUR_STORAGE_KEY_HERE"

spark.conf.set(
    f"fs.azure.account.key.{storage_account_name}.dfs.core.windows.net",
    storage_account_key,
)

raw_path = (
    f"abfss://raw@{storage_account_name}.dfs.core.windows.net/insurance_claims.csv"
)
dq_report_path = (
    f"abfss://curated@{storage_account_name}.dfs.core.windows.net/dq_reports"
)
run_date = datetime.now().strftime("%Y-%m-%d")

print(f"Storage: {storage_account_name}")
print(f"DQ check run date: {run_date}")

# COMMAND ----------

# cell-2: read raw csv

df_raw = (
    spark.read.format("csv")
    .option("header", "true")
    .option("inferschema", "true")
    .option("nullValue", "?")
    .load(raw_path)
)

total_rows = df_raw.count()
total_cols = len(df_raw.columns)

print(f"Raw dataset: {total_rows} rows x {total_cols} columns")

display(df_raw.limit(5))

# COMMAND ----------

# cell-3: null profiling per column

null_counts = df_raw.select(
    [
        count(
            when(
                col(c).isNull()
                | (
                    isnan(col(c))
                    if dict(df_raw.dtypes)[c] in ["double", "float"]
                    else lit(False)
                ),
                c,
            )
        ).alias(c)
        for c in df_raw.columns
    ]
)

null_df = spark.createDataFrame(
    [
        (
            c,
            int(null_counts.collect()[0][c]),
            round(int(null_counts.collect()[0][c]) / total_rows * 100, 2),
        )
        for c in df_raw.columns
    ],
    ["column_name", "null_count", "null_pct"],
).orderBy("null_pct", ascending=False)

print("Null profile per column:")
display(null_df)

# columns with more than 30% nulls
high_null_cols = [
    row["column_name"] for row in null_df.collect() if row["null_pct"] > 30
]
print(f"Columns with > 30% nulls: {high_null_cols}")

# COMMAND ----------

# cell-4: duplicate deletion

total_records = df_raw.count()
distinct_records = df_raw.dropDuplicates().count()
duplicate_count = total_records - distinct_records
duplicate_pct = round((duplicate_count / total_records) * 100, 2)

print(f"Total records: {total_records}")
print(f"Distinct records: {distinct_records}")
print(f"Duplicate records: {duplicate_count}{duplicate_pct}%")

# check duplicates on key columns
key_col = "policy_number"
key_dups = total_records - df_raw.dropDuplicates([key_col]).count()
print(f"Duplicates on {key_col}: {key_dups}")

# COMMAND ----------

# cell-5: schema validation

expected_columns = [
    "policy_number",
    "policy_bind_date",
    "policy_state",
    "insured_zip",
    "insured_education_level",
    "insured_occupation",
    "incident_type",
    "collision_type",
    "incident_severity",
    "authorities_contacted",
    "incident_city",
    "incident_state",
    "total_claim_amount",
    "injury_claim",
    "property_claim",
    "vehicle_claim",
    "auto_make",
    "auto_model",
    "auto_year",
    "fraud_reported",
]

actual_columns = df_raw.columns
missing_columns = [c for c in expected_columns if c not in actual_columns]
extra_columns = [c for c in actual_columns if c not in expected_columns]

print(
    f"Expected columns present: {len(expected_columns) - len(missing_columns)}/{len(expected_columns)}"
)
print(f"Missing columns: {missing_columns}")
print(f"Extra columns: {extra_columns}")

# COMMAND ----------

# cell-6: calculate DQ score and write DQ report

# score components:
#   completeness: % of non-null values across all columns
#   uniqueness: % of distinct records
#   schema: % of expected columns present

completeness_score = round(
    (
        1
        - (
            null_df.agg({"null_count": "sum"}).collect()[0][0]
            / (total_rows * total_cols)
        )
    )
    * 100,
    2,
)

uniqueness_score = round((distinct_records / total_records) * 100, 2)
schema_score = round(
    (len(expected_columns) - len(missing_columns)) / len(expected_columns) * 100, 2
)
dq_score = round((completeness_score + uniqueness_score + schema_score) / 3, 2)

print(f"Completeness score: {completeness_score}%")
print(f"Uniqueness score: {uniqueness_score}%")
print(f"Schema score: {schema_score}%")
print(f"Overall DQ score: {dq_score}%")

# write DQ score as json
dq_report_row = Row(
    run_date=run_date,
    dataset="insurance_claims",
    total_rows=total_rows,
    total_columns=total_cols,
    duplicate_count=duplicate_count,
    missing_columns=str(missing_columns),
    completeness_score=completeness_score,
    uniqueness_score=uniqueness_score,
    schema_score=schema_score,
    overall_dq_score=dq_score,
)

dq_report_df = spark.createDataFrame([dq_report_row])

dq_report_df.coalesce(1).write.format("json").mode("append").save(
    f"{dq_report_path}/{run_date}"
)

print(f"DQ report written to: {dq_report_path}/{run_date}")

# COMMAND ----------

# cell-7: DQ gate: fail pipeline if score below threshold

DQ_THRESHOLD = 70  # minimum acceptable DQ score

if dq_score < DQ_THRESHOLD:
    raise Exception(
        f"DATA QUALITY GATE FAILED: DQ Score {dq_score}% is below threshold {DQ_THRESHOLD}%."
        f" Pipeline will not proceed. Fix data quality issues first."
    )
else:
    print(
        f"DATA QUALITY GATE PASSED: DQ Score {dq_score}% >= threshold {DQ_THRESHOLD}%"
    )
    print(f"Proceeding to transformation notebook...")

# COMMAND ----------
