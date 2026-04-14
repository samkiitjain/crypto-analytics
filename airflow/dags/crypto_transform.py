"""
crypto_transform_dag.py
───────────────────────
dbt transformation layer — runs after both prices and news assets are updated.

Schedule:  Asset-based — triggers automatically when BOTH prices_bq_asset
           AND news_bq_asset are marked as updated in the same day.
           No time schedule needed — data readiness drives execution.

This DAG demonstrates event-driven orchestration:
  crypto_prices_daily ─→ [prices_bq_asset] ─┐
                                              ├─→ crypto_transform (this DAG)
  crypto_news_daily   ─→ [news_bq_asset]   ──┘
"""

import os
from datetime import datetime, timezone, timedelta
from pathlib import Path

from airflow.decorators import dag, task

from assets import prices_bq_asset, news_bq_asset

DBT_PROJECT_DIR = os.environ.get("DBT_PROJECT_DIR", "/opt/airflow/dbt")


@dag(
    dag_id="crypto_transform",
    description="Run dbt models after prices and news assets are updated",
    schedule=[prices_bq_asset, news_bq_asset],  # event-driven — no cron needed
    start_date=datetime(2025, 1, 1, tzinfo=timezone.utc),
    catchup=False,
    max_active_runs=1,
    default_args={
        "owner": "crypto_pipeline",
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
    },
    tags=["crypto", "transform", "dbt"],
)
def crypto_transform_dag():

    @task(inlets=[prices_bq_asset, news_bq_asset])
    def dbt_run() -> None:
        """
        Execute dbt models.
        Uses subprocess so dbt runs in its own process with its own venv,
        exactly as it would from the command line.
 
        inlets declares which assets this task consumes — renders the full
        lineage graph in the Airflow UI:
          [prices_bq_asset] ─┐
                              ├─→ dbt_run → dbt_test
          [news_bq_asset]  ──┘
        """
        import subprocess

        result = subprocess.run(
            ["dbt", "run", "--project-dir", DBT_PROJECT_DIR],
            capture_output=True,
            text=True,
        )

        print(result.stdout)

        if result.returncode != 0:
            print(result.stderr)
            raise RuntimeError(f"dbt run failed:\n{result.stderr}")

    @task
    def dbt_test() -> None:
        """
        Run dbt tests after models complete.
        Failures here raise an exception — Airflow marks the task red
        and retries according to default_args.
        """
        import subprocess

        result = subprocess.run(
            ["dbt", "test", "--project-dir", DBT_PROJECT_DIR],
            capture_output=True,
            text=True,
        )

        print(result.stdout)

        if result.returncode != 0:
            print(result.stderr)
            raise RuntimeError(f"dbt test failed:\n{result.stderr}")

    # Sequential — tests only run if models succeed
    dbt_run() >> dbt_test()


crypto_transform_dag()