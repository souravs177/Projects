from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession, functions as F

from healthcare_medallion.config import PipelineConfig
from healthcare_medallion.io import merge_delta_dataset, read_dataset, write_dataset
from healthcare_medallion.schemas import get_schema


def with_ingestion_metadata(frame: DataFrame, dataset_name: str) -> DataFrame:
    record_hash = F.sha2(
        F.concat_ws(
            "||",
            *[F.coalesce(F.col(column_name).cast("string"), F.lit("<null>")) for column_name in frame.columns],
        ),
        256,
    )
    return (
        frame.withColumn("source_file", F.input_file_name())
        .withColumn("record_hash", record_hash)
        .withColumn("ingested_at", F.current_timestamp())
        .withColumn("record_source", F.lit(dataset_name))
    )


def ingest_dataset(spark: SparkSession, config: PipelineConfig, dataset_name: str) -> DataFrame:
    dataset = config.datasets[dataset_name]
    raw_path = config.raw_dataset_path(dataset_name)

    frame = read_dataset(
        spark=spark,
        path=raw_path,
        format_name=dataset.format,
        options=dataset.options,
        schema=get_schema(dataset.schema),
    )

    bronze_frame = with_ingestion_metadata(frame, dataset_name)
    bronze_path = config.output_path("bronze", dataset_name)
    bronze_format = config.output_format("bronze")
    merge_keys = config.merge_keys("bronze", dataset_name)

    if config.incremental.enabled and bronze_format == "delta" and merge_keys:
        merge_delta_dataset(
            spark=spark,
            frame=bronze_frame,
            path=bronze_path,
            merge_keys=merge_keys,
            matched_update_condition="target.record_hash <> source.record_hash",
        )
    else:
        write_dataset(
            frame=bronze_frame,
            path=bronze_path,
            format_name=bronze_format,
            mode=config.write_mode,
        )
    return bronze_frame


def run(spark: SparkSession, config: PipelineConfig) -> dict[str, DataFrame]:
    return {dataset_name: ingest_dataset(spark, config, dataset_name) for dataset_name in config.datasets}
