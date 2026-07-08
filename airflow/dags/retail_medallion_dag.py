"""Retail medallion pipeline - final project DAG.

data/raw/{unique_id}/{table}/*.csv
    -> bronze.{table}   (raw, append-only, incremental via bronze.load_log)
    -> silver.{table}   (cleaned, deduped, upserted via silver.load_log watermark)
    -> gold.dim_*/fact_sales   (star-schema datamart, core tables only)
    -> gold.vw_*        (analysis views)

--unique-id is read from the Airflow Variable `retail_unique_id`
(Admin -> Variables in the UI), defaulting to "zidan" if unset.

Every task is idempotent, so re-running (or Airflow retrying a failed task)
is always safe: bronze skips already-logged files, silver only reprocesses
rows past its watermark, gold upserts with a "newer wins" guard.
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

default_args = {
    "owner": "zidan",
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
}


def _init_schemas():
    from retail_pipeline.ddl import run_all_ddl

    run_all_ddl()


def _bronze_ingest():
    from retail_pipeline.bronze import ingest_all

    print(ingest_all())


def _silver_transform():
    from retail_pipeline.silver import transform_all

    print(transform_all())


def _gold_build_dims():
    from retail_pipeline.gold import build_dims

    build_dims()


def _gold_build_fact():
    from retail_pipeline.gold import build_fact

    build_fact()


def _refresh_analysis_views():
    from retail_pipeline.analysis import create_views

    create_views()


with DAG(
    dag_id="retail_medallion_pipeline",
    description="Incremental medallion ETL: raw CSV -> bronze -> silver -> gold star schema",
    default_args=default_args,
    start_date=datetime(2026, 1, 1),
    schedule=timedelta(minutes=15),
    catchup=False,
    max_active_runs=1,
    tags=["final-project", "medallion", "retail"],
) as dag:
    init_schemas = PythonOperator(task_id="init_schemas", python_callable=_init_schemas)
    bronze_ingest = PythonOperator(task_id="bronze_ingest", python_callable=_bronze_ingest)
    silver_transform = PythonOperator(task_id="silver_transform", python_callable=_silver_transform)
    gold_build_dims = PythonOperator(task_id="gold_build_dims", python_callable=_gold_build_dims)
    gold_build_fact = PythonOperator(task_id="gold_build_fact", python_callable=_gold_build_fact)
    refresh_analysis_views = PythonOperator(
        task_id="refresh_analysis_views", python_callable=_refresh_analysis_views
    )

    init_schemas >> bronze_ingest >> silver_transform >> gold_build_dims >> gold_build_fact >> refresh_analysis_views
