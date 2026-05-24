# 001 - Azure Insurance Claims Risk Scoring Pipeline

## Overview
An enterprise-grade, end-to-end Azure Data Engineering pipeline that ingests
raw insurance claims data, runs a multi-stage data quality framework with a
scoring gate, applies PySpark multi-factor risk scoring, stores output as
Delta Lake with schema evolution, loads to Azure SQL Database via JDBC,
and sends email alerts - automated weekly via ADF.

---

## Architecture
```
Kaggle Dataset: insurance_claims.csv
           |
           v
ADLS Gen2: sainsuranceps01 / raw /
           |
           v
ADF: pl_001_insurance_claims  [Weekly -- every Monday 8AM IST]
  Notebook: nb_01_dq_check
    - Null profiling, duplicate detection, schema validation
    - DQ Score calculation -- FAILS pipeline if score < 70%
    - DQ report written to curated/dq_reports/
  Notebook: nb_02_transform  [on DQ pass]
    - Multi-factor risk scoring: HIGH / MEDIUM / LOW
    - Delta Lake write with mergeSchema (schema evolution)
    - Aggregated risk summary for BI consumption
  Notebook: nb_03_load_to_sql  [on transform success]
    - JDBC connection test before write
    - Write dbo.insurance_claims_scored
    - Write dbo.risk_summary_by_severity
  Web Activity: Email via Logic App on success
  Failure path: Email alert on any activity failure
           |
           v
Azure SQL: db-insurance-claims
  dbo.insurance_claims_scored     -- full scored records
  dbo.risk_summary_by_severity    -- aggregated BI summary
```

---

## Tech Stack
| Tool | Purpose |
|------|---------|
| Azure Data Factory V2 | Orchestration and weekly scheduling |
| Azure Data Lake Storage Gen2 | Raw, transformed, curated storage |
| Azure Databricks + PySpark | DQ framework, risk scoring, SQL load |
| Delta Lake | Versioned storage with schema evolution |
| Azure SQL Database | Analytics-ready curated output |
| Azure Logic Apps | Email alerting on success and failure |

---

## Advanced Concepts
| Concept | Implementation |
|---------|---------------|
| Data Quality Framework | Null profiling, duplicate detection, schema validation, DQ score gate |
| Delta Schema Evolution | mergeSchema=True -- new columns handled gracefully |
| Delta Versioning | Each run = new Delta version -- full audit trail |
| Multi-Factor Risk Scoring | fraud flag + claim amount + incident severity combined |
| Dual SQL Output | Full scored table + aggregated BI summary table |
| JDBC from Databricks | Direct Spark JDBC -- reliable, avoids ADF Copy Delta issues |
| dbutils.notebook.exit() | Notebooks return status + row counts back to ADF |
| ADF Email Alerting | Logic App integration -- success and failure notifications |

---

## Repository Structure
```
001-azure-insurance-claims-pipeline/
├── components/
|    ├── notebooks/
|    |    ├── nb_01_dq_check.py
|    |    ├── nb_02_transform.py
|    |    └── nb_03_load_to_sql.py
|    └── adf-pipelines/
|         └── pl_001_insurance_claims.json
├── data/
|    └── sample_data.csv
├── docs/
|    ├── adf_pipeline_overview.png
|    ├── adf_pipeline_success.png
|    ├── dq_score_output.png
|    ├── risk_score_distribution.png
|    ├── delta_lake_storage.png
|    ├── sql_output_verified.png
|    └── email_alert_received.png
└── README.md
```

---
