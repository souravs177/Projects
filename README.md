# Healthcare Medallion Data Engineering Project

Starter repository for a healthcare analytics pipeline built with Python, Spark, SQL, Delta Lake, Airflow, and a medallion-style data lake layout.

## What this repo gives you

- `bronze` ingestion for raw healthcare source files
- `silver` cleansing and dimensional enrichment for claims analytics
- `gold` business-ready aggregates for cost and provider reporting
- Delta Lake storage for ACID tables and schema-aware batch writes
- incremental Delta merge handling for late-arriving healthcare claims
- Airflow DAG for scheduled bronze, silver, and gold orchestration
- synthetic sample datasets so you can run the pipeline locally
- SQL equivalents for common transformations
- a small test suite and GitHub Actions workflow

## Project layout

```text
.
|-- conf/
|   `-- pipeline.yml
|-- data/
|   |-- raw/
|   |-- bronze/
|   |-- silver/
|   `-- gold/
|-- docs/
|   |-- architecture.md
|   `-- orchestration.md
|-- orchestration/
|   `-- airflow/
|       `-- dags/
|-- sql/
|   |-- bronze/
|   |-- silver/
|   `-- gold/
|-- src/healthcare_medallion/
|   |-- jobs/
|   |-- config.py
|   |-- io.py
|   |-- pipeline.py
|   |-- schemas.py
|   `-- spark.py
`-- tests/
```

## Medallion flow

1. `bronze`: land raw claims, members, and providers files into Delta tables with ingestion metadata.
2. `silver`: standardize types, validate records, and join claims with member/provider dimensions.
3. `gold`: publish curated Delta tables for monthly plan cost and provider specialty performance.

## Quick start

Recommended runtime: Python 3.11. Spark and Delta Lake are much happier there than on Python 3.14.

```bash
py -3.11 -m venv .venv
.venv\Scripts\Activate.ps1
python -m pip install -e .[dev]
python -m healthcare_medallion.pipeline --layer all
python -m pytest
```

On native Windows, Spark file writes can still fail with Hadoop `NativeIO` errors even after the Python version is fixed. When that happens, use WSL, Linux, or the Docker runner below. The pipeline code is ready; the blocker is the Windows Hadoop layer.

You can also run a single layer:

```bash
python -m healthcare_medallion.pipeline --layer bronze
python -m healthcare_medallion.pipeline --layer silver
python -m healthcare_medallion.pipeline --layer gold
```

## Storage format

The repo writes `bronze`, `silver`, and `gold` as Delta tables by default. That gives you:

- ACID transactions for batch updates
- time-travel-ready table storage once you move to a managed lakehouse runtime
- cleaner schema evolution than plain parquet folders

If you want a simpler local fallback later, you can switch the layer formats in [conf/pipeline.yml](conf/pipeline.yml).

## Docker run

This repo includes a Linux container path for reliable local execution on Windows hosts.

```bash
docker compose build
docker compose run --rm healthcare-pipeline
```

The container uses the same project files from the repo root, so outputs still land in `data/`.

## Incremental processing

Late-arriving claims are handled without forcing a full rebuild:

- `bronze` merges claims, members, and providers on business keys
- unchanged source rows keep their prior ingestion timestamp
- `silver` merges refreshed claim rows on `claim_id`
- `silver` writes the affected claim months to a small manifest under `data/system`
- `gold` refreshes only the months touched by the latest silver run

This keeps the project closer to how a real healthcare batch pipeline behaves once corrections and delayed claims start showing up.

## Sample business outputs

- `data/gold/monthly_cost_by_plan_state`
- `data/gold/provider_specialty_summary`

These are useful starting points for Power BI, Tableau, or downstream SQL reporting.

## Healthcare use case covered here

The synthetic dataset models:

- member enrollment data
- provider reference data
- medical claims with diagnosis, procedure, billed, and paid amounts

That gives you a realistic base for common data engineering interview projects, portfolio repos, or internal accelerators.

## Airflow orchestration

The DAG lives at [orchestration/airflow/dags/healthcare_medallion_dag.py](orchestration/airflow/dags/healthcare_medallion_dag.py). It runs:

1. `bronze_ingestion`
2. `silver_transformations`
3. `gold_serving`

For setup notes, see [docs/orchestration.md](docs/orchestration.md).

## Next upgrades

- publish Delta tables to Unity Catalog, Hive Metastore, or a warehouse
- add data quality checks with Great Expectations or Deequ
- add delete handling for source tombstones and member/provider deactivation
