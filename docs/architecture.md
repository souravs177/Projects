# Architecture Notes

## Why medallion architecture for healthcare data

Healthcare pipelines usually need traceability, validation, and business-friendly outputs. A medallion layout helps separate those concerns:

- `bronze` keeps raw source fidelity and ingestion metadata in Delta tables
- `silver` applies standardization and joins for analytics readiness
- `gold` exposes curated KPI tables for reporting and machine learning features
- Great Expectations adds quality gates and validation result artifacts across the medallion flow

## Domain model in this starter

- `claims`: transactional healthcare utilization
- `members`: patient or member dimension
- `providers`: physician or facility reference data

## Processing pattern

1. Ingest CSV source files from `data/raw`
2. Write typed Delta datasets to `data/bronze`
3. Read bronze data, clean and enrich in Spark
4. Merge changed claim rows into silver tables
5. Refresh only the affected gold claim months
6. Validate each layer with Great Expectations quality checks
7. Schedule the layer execution in Airflow
8. Reuse the matching SQL scripts in `sql/` for warehouse implementations

## Suggested extensions

- Add PHI masking for member-level fields
- Partition large claims tables by service month
- Add CDC handling for incremental member/provider changes
- Attach orchestration and data quality frameworks
