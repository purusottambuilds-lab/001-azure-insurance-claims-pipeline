# Databricks notebook source
# importing all libraries needed

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, lit, upper, trim, round, current_timestamp
from pyspark.sql.types import DoubleType, IntegerType, StringType
from delta.tables import DeltaTable
from pyspark.sql.functions import count, isnan, when, col

print(f"All libraries imported successfully")
print(f"Spark version: {spark.version}")

# COMMAND ----------

# Define storage config + Read CSV from ADLS
storage_account_name = "sainsuranceps01"
storage_account_key = "YOUR_STORAGE_KEY_HERE"

# Set Spark config for ADLS access
spark.conf.set(
    f"fs.azure.account.key.{storage_account_name}.dfs.core.windows.net",
    storage_account_key,
)


# COMMAND ----------

# read raw csv from ADLS

df_raw = (
    spark.read.format("csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .option("nullValue", "?")
    .load(
        f"abfss://raw@{storage_account_name}.dfs.core.windows.net/insurance_claims.csv"
    )
)

print(f"Total reccords loaded: {df_raw.count()}")
print(f"Total columns: {len(df_raw.columns)}")
print(f"Columns: {df_raw.columns}")

display(df_raw.limit(5))

# COMMAND ----------

# count nulls per column

null_counts = df_raw.select(
    [count(when(col(c).isNull(), c)).alias(c) for c in df_raw.columns]
)

print("Null counts per column:")
display(null_counts)

print(f"Duplicate rows: {df_raw.count() - df_raw.dropDuplicates().count()}")

# COMMAND ----------

# clean the data

df_cleaned = (
    df_raw.dropDuplicates()
    .na.drop(subset=["policy_number", "total_claim_amount"])
    .withColumn("total_claim_amount", col("total_claim_amount").cast(DoubleType()))
    .withColumn("fraud_reported", upper(trim(col("fraud_reported"))))
    .withColumn("incident_severity", upper(trim(col("incident_severity"))))
)

print(f"Records after cleaning: {df_cleaned.count()}")
print(f"Records removed: {df_raw.count() - df_cleaned.count()}")

# COMMAND ----------

# add risk score column (custom business logic)

# Risk logic:
#   High = fraud reported (Y) AND claim amount > 50,000
#   Medium = fraud reported (Y) OR claim_amount > 30,000
#   Low = everything else


df_scored = df_cleaned.withColumn(
    "risk_score",
    when(
        ((col("fraud_reported") == "Y") & (col("total_claim_amount") > 50000)),
        lit("High"),
    )
    .when(
        ((col("fraud_reported") == "Y") | (col("total_claim_amount") > 30000)),
        lit("Medium"),
    )
    .otherwise(lit("Low")),
).withColumn("processed_timestamp", current_timestamp())

# show risk distribution
print("Risk Score Distribution:")
display(df_scored.groupBy("risk_score").count().orderBy("risk_score"))

# COMMAND ----------

# save as Delta Lake format (partition by risk_score)

output_path = f"abfss://transformed@{storage_account_name}.dfs.core.windows.net/insurance_claims_delta"

dbutils.fs.rm(output_path, recurse=True)

df_scored.write.format("delta").mode("overwrite").save(output_path)

print(f"Data saved as Delta Lake at: {output_path}")
print(f"Total records: {df_scored.count()}")

# COMMAND ----------

# verify Delta output

verify_path = f"abfss://transformed@{storage_account_name}.dfs.core.windows.net/insurance_claims_delta"

df_delta = spark.read.format("delta").load(verify_path)
print(f"Total records in Delta: {df_delta.count()}")

display(
    df_delta.select(
        "policy_number",
        "total_claim_amount",
        "fraud_reported",
        "risk_score",
        "processed_timestamp",
    ).limit(10)
)

# COMMAND ----------
