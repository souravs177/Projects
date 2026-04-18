from __future__ import annotations

from pathlib import Path

from pyspark.sql import DataFrame, DataFrameReader, SparkSession


def build_reader(
    spark: SparkSession,
    format_name: str,
    options: dict[str, str] | None = None,
    schema=None,
) -> DataFrameReader:
    reader = spark.read.format(format_name)
    if schema is not None:
        reader = reader.schema(schema)
    for key, value in (options or {}).items():
        reader = reader.option(key, value)
    return reader


def read_dataset(
    spark: SparkSession,
    path: str | Path,
    format_name: str,
    options: dict[str, str] | None = None,
    schema=None,
) -> DataFrame:
    return build_reader(spark, format_name, options=options, schema=schema).load(str(Path(path)))


def dataset_exists(spark: SparkSession, path: str | Path, format_name: str) -> bool:
    resolved_path = Path(path)
    if format_name.lower() == "delta":
        from delta.tables import DeltaTable

        return DeltaTable.isDeltaTable(spark, str(resolved_path))
    return resolved_path.exists()


def write_dataset(
    frame: DataFrame,
    path: str | Path,
    format_name: str,
    mode: str = "overwrite",
    partition_by: list[str] | None = None,
    options: dict[str, str] | None = None,
) -> None:
    writer = frame.write.mode(mode).format(format_name)
    for key, value in (options or {}).items():
        writer = writer.option(key, value)
    if format_name.lower() == "delta" and mode == "overwrite":
        writer = writer.option("overwriteSchema", "true")
    if partition_by:
        writer = writer.partitionBy(*partition_by)
    writer.save(str(Path(path)))


def merge_delta_dataset(
    spark: SparkSession,
    frame: DataFrame,
    path: str | Path,
    merge_keys: list[str],
    matched_update_condition: str | None = None,
    partition_by: list[str] | None = None,
) -> None:
    from delta.tables import DeltaTable

    resolved_path = Path(path)
    if not merge_keys:
        raise ValueError("Delta merge requires at least one merge key.")

    if not DeltaTable.isDeltaTable(spark, str(resolved_path)):
        write_dataset(
            frame=frame,
            path=resolved_path,
            format_name="delta",
            mode="overwrite",
            partition_by=partition_by,
        )
        return

    merge_condition = " AND ".join(f"target.{key} = source.{key}" for key in merge_keys)
    merge_builder = DeltaTable.forPath(spark, str(resolved_path)).alias("target").merge(
        frame.alias("source"),
        merge_condition,
    )

    if matched_update_condition:
        merge_builder = merge_builder.whenMatchedUpdateAll(condition=matched_update_condition)
    else:
        merge_builder = merge_builder.whenMatchedUpdateAll()

    merge_builder.whenNotMatchedInsertAll().execute()
