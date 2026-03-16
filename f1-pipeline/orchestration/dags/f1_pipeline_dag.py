"""
f1_pipeline_dag.py — Apache Airflow DAG for the F1 data pipeline

Purpose:
    Orchestrate the end-to-end pipeline that runs after each F1 race:
        1. Ingest  → Pull new data from OpenF1 and Jolpica APIs
        2. Process → Trigger the AWS Glue raw-to-processed job
        3. Load    → Copy processed Parquet into Snowflake stage
        4. Model   → Run dbt to refresh staging → intermediate → marts

Schedule:
    Triggered manually or on a cron (e.g. every Monday after a race
    weekend) — adjust schedule_interval to suit the race calendar.

Diagram:
    ingest_openf1  ──┐
                     ├──► glue_raw_to_processed ──► snowflake_load ──► dbt_run
    ingest_jolpica ──┘
"""

# from datetime import datetime, timedelta
# from airflow import DAG
# from airflow.operators.python import PythonOperator
# from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
# from airflow.operators.bash import BashOperator

# default_args = {
#     "owner": "f1-pipeline",
#     "depends_on_past": False,
#     "email_on_failure": False,
#     "retries": 1,
#     "retry_delay": timedelta(minutes=5),
# }

# with DAG(
#     dag_id="f1_pipeline",
#     default_args=default_args,
#     description="End-to-end F1 data pipeline: ingest → process → load → model",
#     schedule_interval=None,  # manual trigger; change to cron as needed
#     start_date=datetime(2025, 1, 1),
#     catchup=False,
#     tags=["f1", "data-engineering"],
# ) as dag:

    # ── Ingest ──────────────────────────────────────────────────
    # ingest_openf1 = PythonOperator(
    #     task_id="ingest_openf1",
    #     python_callable=...,  # import from ingestion.api.openf1
    # )

    # ingest_jolpica = PythonOperator(
    #     task_id="ingest_jolpica",
    #     python_callable=...,  # import from ingestion.api.jolpica
    # )

    # ── Process ─────────────────────────────────────────────────
    # glue_raw_to_processed = GlueJobOperator(
    #     task_id="glue_raw_to_processed",
    #     job_name="f1-raw-to-processed",
    #     script_location="s3://<SCRIPTS_BUCKET>/raw_to_processed.py",
    #     aws_conn_id="aws_default",
    # )

    # ── Load ────────────────────────────────────────────────────
    # snowflake_load = PythonOperator(
    #     task_id="snowflake_copy_into",
    #     python_callable=...,  # COPY INTO from S3 stage
    # )

    # ── Model ───────────────────────────────────────────────────
    # dbt_run = BashOperator(
    #     task_id="dbt_run",
    #     bash_command="cd /opt/airflow/dbt && dbt run --profiles-dir .",
    # )

    # ── Dependencies ────────────────────────────────────────────
    # [ingest_openf1, ingest_jolpica] >> glue_raw_to_processed >> snowflake_load >> dbt_run
