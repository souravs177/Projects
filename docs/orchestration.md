# Orchestration Notes

## Airflow DAG

The Airflow DAG is stored at `orchestration/airflow/dags/healthcare_medallion_dag.py`.

It runs three sequential tasks:

1. `bronze_ingestion`
2. `silver_transformations`
3. `gold_serving`

Each task calls the same Python pipeline entry point used for local execution, so your orchestration path stays aligned with your developer workflow.

The silver task also writes a small impacted-month manifest so the gold task can refresh only the months touched by late-arriving claims.

## Running Airflow

Airflow is usually easiest on Linux, WSL, Docker, or a managed service. On Windows, treat the DAG as a repo artifact and run Airflow through WSL or containers.

For quick local validation on a Windows machine, use the repo's `compose.yml` first and keep Airflow itself in WSL, Docker, or a managed scheduler.

Typical install on Linux or WSL:

```bash
pip install -e .[dev,airflow]
```

Then point Airflow at the DAG folder:

```bash
export AIRFLOW__CORE__DAGS_FOLDER=$(pwd)/orchestration/airflow/dags
```

The DAG will call `conf/pipeline.yml` from the repo root.

## Operational follow-ups

- add SLA alerts for delayed gold refreshes
- push DAG task logs to cloud object storage
- add data quality gates between silver and gold
