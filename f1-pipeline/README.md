# 🏎️ F1 Data Engineering Pipeline

An end-to-end data engineering portfolio project that ingests Formula 1 data, transforms it, and serves it through interactive dashboards.

---

## Architecture Overview

```
Kaggle (historical) ──┐                                          ┌──► Grafana
                      ├──► S3 (raw) ──► AWS Glue ──► S3 (processed) ──► Snowflake ──┤
OpenF1 + Jolpica ─────┘         (PySpark)                             (dbt)         └──► Ad-hoc SQL
        APIs
```

## Components

| Directory | Description |
|---|---|
| **`terraform/`** |  Infrastructure-as-Code (AWS + Snowflake). Supports pause/resume to control costs. |
| **`ingestion/kaggle/`** | One-time script to download historical F1 data from Kaggle into S3. |
| **`ingestion/api/`** | Incremental ingestion scripts for OpenF1 and Jolpica REST APIs (runs after each race). |
| **`processing/glue_jobs/`** | AWS Glue PySpark job to clean, deduplicate, and convert raw data to partitioned Parquet. |
| **`warehouse/dbt/`** | dbt project modelling F1 data in Snowflake (staging → intermediate → marts). |
| **`orchestration/dags/`** | Apache Airflow DAG orchestrating the full pipeline: ingest → process → load → model. |

## Infrastructure

> **All cloud resources are managed with Terraform.** Run `terraform apply` from the `terraform/` directory to provision or update infrastructure.
>
> To **pause** resources and save costs, set `enable_glue_jobs = false` in `terraform.tfvars` and re-apply.

## Getting Started

1. **Clone the repo**
   ```bash
   git clone <repo-url> && cd f1-pipeline
   ```

2. **Configure secrets**
   ```bash
   cp .env.example .env                       # Python scripts
   cp terraform/terraform.tfvars.example terraform/terraform.tfvars  # Terraform
   ```
   Fill in real values in both files.

3. **Provision infrastructure**
   ```bash
   cd terraform && terraform init && terraform apply
   ```

4. **Run historical load** (one-time)
   ```bash
   python ingestion/kaggle/historical_load.py
   ```

5. **Trigger the pipeline** (after each race)
   - Manually trigger the `f1_pipeline` DAG in Airflow, or
   - Let the scheduled cron run automatically.

## Tech Stack

- **Ingestion**: Python, Kaggle API, requests
- **Storage**: AWS S3
- **Processing**: AWS Glue + PySpark
- **Warehouse**: Snowflake
- **Modelling**: dbt
- **Orchestration**: Apache Airflow
- **Visualisation**: Grafana
- **Infrastructure**: Terraform
