# Databricks notebook source
# ================================================================
# nb_03_load_to_sql
# Project 001   : Insurance Claims Risk Scoring
# Purpose       : Load Delta Lake output to Azure SQL Database via JDBC
# Folder        : dir_001_claims_insurance
# Author        : Purusottam Swain | purusottam.builds@gmail.com
# ================================================================

# COMMAND ----------

# CELL 1 - Storage and SQL Configuration

storage_account_name = "sainsuranceps01"
storage_account_key = "YOUR_STORAGE_ACCOUNT_KEY_HERE"

spark.conf.set(
    f"fs.azure.account.key.{storage_account_name}.dfs.core.windows.net",
    storage_account_key,
)

sql_server = "sql-buildlab-de-ps01.database.windows.net"
sql_database = "db-insurance-claims"
sql_user = "sqladmin-buildlab-de-ps01"
sql_password = "YOUR_SQL_PASSWORD_HERE"

sql_url = (
    f"jdbc:sqlserver://{sql_server}:1433;"
    f"database={sql_database};"
    f"user={sql_user};"
    f"password={sql_password};"
    f"encrypt=true;"
    f"trustServerCertificate=false;"
    f"hostNameInCertificate=*.database.windows.net;"
    f"loginTimeout=30"
)

delta_path = f"abfss://transformed@{storage_account_name}.dfs.core.windows.net/insurance_claims_delta"
summary_path = (
    f"abfss://curated@{storage_account_name}.dfs.core.windows.net/risk_summary"
)

print("Config set")


# COMMAND ----------

# CELL 2 - Write insurance_claims_scored to SQL

df_scored = spark.read.format("delta").load(delta_path)
print(f"Delta records to load: {df_scored.count()}")

df_scored.write.format("jdbc").option("url", sql_url).option(
    "dbtable", "dbo.insurance_claims_scored"
).option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver").mode(
    "overwrite"
).save()

print("dbo.insurance_claims_scored: written successfully")


# COMMAND ----------

# CELL 3 - Write risk_summary_by_severity to SQL

df_summary = spark.read.format("delta").load(summary_path)
print(f"Summary records to load: {df_summary.count()}")

df_summary.write.format("jdbc").option("url", sql_url).option(
    "dbtable", "dbo.risk_summary_by_severity"
).option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver").mode(
    "overwrite"
).save()

print("dbo.risk_summary_by_severity: written successfully")


# COMMAND ----------

# CELL 4 - Verify SQL Tables

df_verify = (
    spark.read.format("jdbc")
    .option("url", sql_url)
    .option("dbtable", "dbo.insurance_claims_scored")
    .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver")
    .load()
)

print(f"SQL rows verified: {df_verify.count()}")
display(
    df_verify.select(
        "policy_number", "total_claim_amount", "risk_score", "processed_at"
    ).limit(10)
)


# COMMAND ----------
