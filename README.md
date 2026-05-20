# 001 - Azure Insurance Claims Risk Scoring Pipeline

## Overview
An enterprise-grade, end-to-end Azure Data Engineering pipeline that ingests
raw insurance claims data, runs a multi-stage data quality framework,
applies PySpark-based multi-factor risk scoring, stores output as Delta Lake
with schema evolution, loads curated data into Azure SQL Database, and sends
email alerts on pipeline success or failure - fully automated weekly via ADF.

---

## Architecture
```
Kaggle Dataset (insurance_claims.csv)
           |
           v
ADLS Gen2 sainsuranceps01 [raw container]
           |
           v
Azure Data Factory: pl_001_insurance_claims
  Activity 1: nb_001_dq_check (Databricks)
    - Null profiling per column
    - Duplicate detection on policy_number
    - Schema validation against expected columns
    - DQ Score calculation (Completeness + Uniqueness + Schema)
    - DQ report written to curated/dq_reports/
    - Pipeline fails if DQ Score < 70%
  Activity 2: nb_001_transform (Databricks) [on DQ pass]
    - Multi-factor risk scoring: HIGH / MEDIUM / LOW
    - Delta Lake write with mergeSchema (schema evolution)
    - Delta versioning for full audit trail
    - Aggregated risk summary table for BI consumption
  Activity 3: Copy to Azure SQL — dbo.insurance_claims_scored
  Activity 4: Copy to Azure SQL — dbo.risk_summary_by_severity
  Activity 5: Email notification via Logic App
  Failure path: Email alert on any activity failure
           |
           v
Azure SQL: db-insurance-claims
  dbo.insurance_claims_scored  (full scored records)
  dbo.risk_summary_by_severity (aggregated BI summary)
```

---

## Tech Stack
| Tool | Purpose |
|------|---------|
| Azure Data Factory V2 | Orchestration, scheduling, alerting |
| Azure Data Lake Storage Gen2 | Raw, transformed, curated storage |
| Azure Databricks + PySpark | DQ framework, transformation, risk scoring |
| Delta Lake | Versioned storage with schema evolution and audit trail |
| Azure SQL Database | Analytics-ready curated output |
| Azure Logic Apps | Email alerting on pipeline success/failure |
| Python | Scripting, UUID generation, DQ logic |

---

## Advanced Concepts Demonstrated
| Concept | Implementation |
|---------|---------------|
| Data Quality Framework | Null profiling, duplicate detection, schema validation, DQ score gate |
| Delta Lake Schema Evolution | mergeSchema option — handles new columns without pipeline failure |
| Delta Versioning | Each pipeline run creates a new Delta version — full audit trail |
| Multi-Factor Risk Scoring | Weighted analysis across fraud flag, claim amount, incident severity |
| Dual SQL Output | Full scored table + aggregated BI summary table |
| ADF Email Alerting | Logic App integration — success and failure notifications |
| DQ Gate | Pipeline hard-stops if DQ score below 70% threshold |

---

## Data Source
Public auto insurance claims dataset from Kaggle
URL: kaggle.com/datasets/buntyshah/auto-insurance-claims-data

1000 rows, 40 columns 

---

## ADF Pipeline
Pipeline: pl_001_insurance_claims
Trigger: Weekly - every Monday 11:30 PM IST (GMT +5:30)

| Activity | Type | Purpose |
|----------|------|---------|
| RunDQCheck | Databricks Notebook | Data quality profiling and gate |
| RunTransform | Databricks Notebook | Risk scoring and Delta write |
| CopyToInsuranceSQL | Copy Data | Delta to Azure SQL main table |
| CopySummaryToSQL | Copy Data | Delta summary to Azure SQL BI table |
| SendSuccessNotification | Web Activity | Email via Logic App |
| SendFailureAlert | Web Activity | Failure email on any activity error |

---

## Infrastructure
| Resource | Name |
|----------|------|
| Resource Group | rg-buildlab-de-ps01 |
| Data Factory | adf-buildlab-de-ps01 |
| Databricks | dbw-buildlab-de-ps01 |
| Storage | sainsuranceps01 |
| SQL Server | sql-buildlab-de-ps01 |
| SQL Database | db-insurance-claims |
| Logic App | la-adf-alerts-buidlab-de-ps01 |

---

## Repository Structure
```
001-azure-insurance-claims-pipeline/
├── notebooks/
|   ├── nb_01_dq_check.py         (DQ framework notebook)
|   └── nb_02_transform.py        (Risk scoring notebook)
├── adf-pipelines/
|   └── pl_001_insurance_claims.json
├── data/
|   └── sample_data.csv            (20 rows — no PII)
├── docs/
|   ├── adf_pipeline_overview.png
|   ├── adf_pipeline_success.png
|   ├── risk_score_distribution.png
|   ├── risk_summary_output.png
|   ├── delta_lake_storage.png
|   └── sql_tables_output.png
└── README.md
```

---
