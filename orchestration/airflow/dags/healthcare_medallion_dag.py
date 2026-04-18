from __future__ import annotations

import sys
from datetime import datetime
from pathlib import Path

from airflow import DAG
from airflow.operators.python import PythonOperator

PROJECT_ROOT = Path(__file__).resolve().parents[3]
SRC_DIR = PROJECT_ROOT / "src"
CONFIG_PATH = PROJECT_ROOT / "conf" / "pipeline.yml"

if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from healthcare_medallion.pipeline import run_pipeline


def run_layer(layer: str) -> None:
    run_pipeline(layer=layer, config_path=CONFIG_PATH)


with DAG(
    dag_id="healthcare_medallion_pipeline",
    description="Healthcare medallion batch pipeline using Spark, Delta Lake, and Airflow.",
    schedule="0 6 * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["healthcare", "medallion", "delta"],
) as dag:
    bronze_ingestion = PythonOperator(
        task_id="bronze_ingestion",
        python_callable=run_layer,
        op_kwargs={"layer": "bronze"},
    )

    silver_transformations = PythonOperator(
        task_id="silver_transformations",
        python_callable=run_layer,
        op_kwargs={"layer": "silver"},
    )

    gold_serving = PythonOperator(
        task_id="gold_serving",
        python_callable=run_layer,
        op_kwargs={"layer": "gold"},
    )

    bronze_ingestion >> silver_transformations >> gold_serving
