# 001 - Azure Insurance Claims Analytics Pipeline

## Overview
End-to-end data pipeline that ingests raw insurance claims data,
applies PySpark-based risk scoring logic, stores output as Delta Lake,
and loads curated data into Azure SQL Database — scheduled weekly via ADF.


## Architecture
```
Raw CSV (Kaggle)
    |
    v
ADLS Gen2 [raw container]
    |
    v
Azure Data Factory Pipeline  <-- Weekly Trigger (Monday 8AM)
    |
    v
Azure Databricks + PySpark
  - Clean nulls, remove duplicates
  - Cast data types
  - Risk Score: HIGH / MEDIUM / LOW
    |
    v
Delta Lake [transformed container]  <-- Partitioned by risk_score
    |
    v
Azure SQL Database [dbo.insurance_claims_scored]
```

## Tech Stack
| Tool | Purpose |
|------|---------|
| Azure Data Factory V2 | Orchestration & weekly scheduling |
| Azure Data Lake Storage Gen2 | Raw & transformed storage |
| Azure Databricks (PySpark) | Transformation & risk scoring |
| Delta Lake | Partitioned versioned storage format |
| Azure SQL Database | Analytics-ready curated output |
| Python / SQL | Scripting & querying |

## Key Features
- Automated ETL with ADF Schedule Trigger (weekly, Monday 8AM IST)
- PySpark Risk Scoring: HIGH / MEDIUM / LOW classification logic
- Delta Lake partitioned by risk_score for optimised querying
- Data quality: null handling, deduplication, type casting
- Error handling and monitoring via ADF Monitor

## Dataset
Public auto insurance claims dataset from Kaggle
(kaggle.com/datasets/buntyshah/insurance-claims-data)

## Repository Structure
```
notebooks/   -- PySpark transformation scripts (.py)
adf-pipelines/ -- ADF pipeline JSON definitions
data/         -- Sample data (20 rows for reference)
docs/         -- Architecture screenshots
README.md     -- This file
```

