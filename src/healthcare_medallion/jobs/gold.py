from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession, functions as F

from healthcare_medallion.config import PipelineConfig
from healthcare_medallion.incremental import (
    build_replace_where,
    clear_impacted_months_manifest,
    read_impacted_months_manifest,
)
from healthcare_medallion.io import dataset_exists, read_dataset, write_dataset


def build_monthly_cost_by_plan_state(member_claims: DataFrame) -> DataFrame:
    aggregated = (
        member_claims.groupBy("claim_month", "plan_id", "member_state")
        .agg(
            F.count("*").alias("total_claims"),
            F.countDistinct("member_id").alias("distinct_members"),
            F.countDistinct("provider_id").alias("distinct_providers"),
            F.round(F.sum("billed_amount"), 2).alias("total_billed_amount"),
            F.round(F.sum("paid_amount"), 2).alias("total_paid_amount"),
            F.round(F.avg("paid_amount"), 2).alias("average_paid_amount"),
            F.max("last_touched_at").alias("source_max_last_touched_at"),
        )
        .withColumn(
            "paid_to_billed_ratio",
            F.when(F.col("total_billed_amount") == 0, F.lit(0.0)).otherwise(
                F.round(F.col("total_paid_amount") / F.col("total_billed_amount"), 4)
            ),
        )
    )
    return aggregated.orderBy("claim_month", "plan_id", "member_state")


def build_provider_specialty_summary(member_claims: DataFrame) -> DataFrame:
    return (
        member_claims.groupBy("claim_month", "provider_specialty", "provider_state")
        .agg(
            F.count("*").alias("total_claims"),
            F.countDistinct("provider_id").alias("distinct_providers"),
            F.countDistinct("member_id").alias("distinct_members"),
            F.round(F.sum("paid_amount"), 2).alias("total_paid_amount"),
            F.round(F.avg("patient_responsibility"), 2).alias("average_patient_responsibility"),
            F.sum(F.when(F.col("is_high_cost_claim"), 1).otherwise(0)).alias("high_cost_claim_count"),
            F.max("last_touched_at").alias("source_max_last_touched_at"),
        )
        .orderBy("claim_month", "provider_specialty", "provider_state")
    )


def collect_claim_months(frame: DataFrame) -> list[object]:
    return [
        row["claim_month"]
        for row in frame.select("claim_month").where(F.col("claim_month").isNotNull()).distinct().collect()
    ]


def latest_watermark(frame: DataFrame, column_name: str) -> object:
    return frame.agg(F.max(column_name).alias("watermark")).first()["watermark"]


def run(spark: SparkSession, config: PipelineConfig) -> dict[str, DataFrame]:
    silver_format = config.output_format("silver")
    gold_format = config.output_format("gold")

    member_claims = read_dataset(spark, config.output_path("silver", "member_claims"), silver_format)
    monthly_cost_by_plan_state_path = config.output_path("gold", "monthly_cost_by_plan_state")
    provider_specialty_summary_path = config.output_path("gold", "provider_specialty_summary")
    manifest_path = config.metadata_path("silver_impacted_claim_months.json")

    monthly_exists = dataset_exists(spark, monthly_cost_by_plan_state_path, gold_format)
    provider_exists = dataset_exists(spark, provider_specialty_summary_path, gold_format)
    incremental_enabled = config.incremental.enabled and silver_format == "delta" and gold_format == "delta"

    if incremental_enabled and monthly_exists and provider_exists:
        impacted_months = read_impacted_months_manifest(manifest_path)
        if not impacted_months:
            existing_monthly_summary = read_dataset(spark, monthly_cost_by_plan_state_path, gold_format)
            watermark = latest_watermark(existing_monthly_summary, "source_max_last_touched_at")
            if watermark is not None:
                impacted_months = collect_claim_months(
                    member_claims.filter(F.col("last_touched_at") > F.lit(watermark))
                )

        if not impacted_months:
            return {
                "monthly_cost_by_plan_state": read_dataset(spark, monthly_cost_by_plan_state_path, gold_format),
                "provider_specialty_summary": read_dataset(spark, provider_specialty_summary_path, gold_format),
            }

        affected_member_claims = member_claims.filter(F.col("claim_month").isin(impacted_months))
        monthly_cost_by_plan_state = build_monthly_cost_by_plan_state(affected_member_claims)
        provider_specialty_summary = build_provider_specialty_summary(affected_member_claims)
        overwrite_options = {"replaceWhere": build_replace_where("claim_month", impacted_months)}
    else:
        monthly_cost_by_plan_state = build_monthly_cost_by_plan_state(member_claims)
        provider_specialty_summary = build_provider_specialty_summary(member_claims)
        overwrite_options = None

    write_dataset(
        frame=monthly_cost_by_plan_state,
        path=monthly_cost_by_plan_state_path,
        format_name=gold_format,
        mode="overwrite",
        partition_by=["claim_month"],
        options=overwrite_options,
    )
    write_dataset(
        frame=provider_specialty_summary,
        path=provider_specialty_summary_path,
        format_name=gold_format,
        mode="overwrite",
        partition_by=["claim_month"],
        options=overwrite_options,
    )
    clear_impacted_months_manifest(manifest_path)

    return {
        "monthly_cost_by_plan_state": monthly_cost_by_plan_state,
        "provider_specialty_summary": provider_specialty_summary,
    }
