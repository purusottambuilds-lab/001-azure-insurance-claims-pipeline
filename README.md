# 001 - Azure Insurance Claims Risk Scoring Pipeline

## Overview
An enterprise-grade, end-to-end Azure Data Engineering pipeline that ingests raw
insurance claims data, runs a multi-stage data quality framework with a scoring gate,
applies PySpark multi-factor risk scoring, stores output as Delta Lake with schema
evolution, loads to Azure SQL Database via Spark JDBC, and sends Gmail alerts
on both success and failure - automated weekly via ADF.

---

## Architecture
```
Kaggle Dataset: insurance_claims.csv
         |
         v

ADLS Gen2: sainsuranceps01 / raw /
         |
         v

ADF: pl_001_insurance_claims  [trigger: every Monday 8AM IST]
  
  Notebook: nb_01_dq_check
    - Null profiling per column
    - Duplicate detection on policy_number
    - Schema validation against expected columns
    - DQ Score = (Completeness + Uniqueness + Schema) / 3
    - DQ report JSON written to curated/dq_reports/
    - FAILS pipeline if DQ Score < 70%
  
  Notebook: nb_02_transform  [on DQ pass]
    - Multi-factor risk scoring: HIGH / MEDIUM / LOW
    - Delta Lake write with mergeSchema (schema evolution + versioning)
    - Aggregated risk summary for BI consumption
  
  Notebook: nb_03_load_to_sql  [on transform success]
    - Spark JDBC write: dbo.insurance_claims_scored
    - Spark JDBC write: dbo.risk_summary_by_severity
    - SQL verification
  
  Web Activity: Gmail alert via Logic App (success + failure)
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
| Azure Data Factory V2 | Orchestration, weekly scheduling, alerting |
| Azure Data Lake Storage Gen2 | Raw, transformed, curated storage layers |
| Azure Databricks + PySpark | DQ framework, risk scoring, SQL load |
| Delta Lake | Versioned storage with schema evolution and audit trail |
| Azure SQL Database | Analytics-ready curated output for BI |
| Azure Logic Apps | Gmail alerting on success and failure |

---

## Advanced Concepts Demonstrated
| Concept | Implementation |
|---------|---------------|
| Data Quality Framework | Null profiling, duplicate detection, schema validation, DQ score gate |
| Delta Schema Evolution | mergeSchema=True - new columns handled without pipeline failure |
| Delta Versioning | Each run creates a new Delta version - full audit trail |
| Multi-Factor Risk Scoring | fraud_reported + total_claim_amount + incident_severity combined |
| Dual SQL Output | Full scored table + aggregated BI summary table |
| JDBC from Databricks | Spark JDBC - more reliable than ADF Copy for Delta sources |
| ADF Email Alerting | Logic App integration - Gmail notifications on success and failure |

---

## Repository Structure
```
001-azure-insurance-claims-pipeline/
├── notebooks/
|   ├── nb_01_dq_check.py
|   ├── nb_02_transform.py
|   └── nb_03_load_to_sql.py
├── adf-pipelines/
|   └── pl_001_insurance_claims.json
├── data/
|   └── sample_data.csv  (20 rows - no PII)
├── docs/
|   ├── adf_pipeline_overview.png
|   ├── adf_pipeline_success.png
|   ├── dq_score_output.png
|   ├── risk_score_distribution.png
|   ├── delta_lake_storage.png
|   ├── sql_output_verified.png
|   └── email_alert_received.png
└── README.md
```


---
