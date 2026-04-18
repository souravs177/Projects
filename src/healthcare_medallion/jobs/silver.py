from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession, functions as F

from healthcare_medallion.config import PipelineConfig
from healthcare_medallion.incremental import build_replace_where, write_impacted_months_manifest
from healthcare_medallion.io import dataset_exists, merge_delta_dataset, read_dataset, write_dataset

DEFAULT_TIMESTAMP = "1900-01-01 00:00:00"


def clean_claims(claims: DataFrame) -> DataFrame:
    return (
        claims.withColumn("service_date", F.to_date("service_date"))
        .withColumn("claim_status", F.upper(F.trim("claim_status")))
        .filter(F.col("claim_id").isNotNull())
        .filter(F.col("member_id").isNotNull())
        .filter(F.col("provider_id").isNotNull())
        .filter(F.col("paid_amount").isNotNull())
        .filter(F.col("paid_amount") >= 0)
    )


def clean_members(members: DataFrame) -> DataFrame:
    return (
        members.withColumn("date_of_birth", F.to_date("date_of_birth"))
        .withColumn("state", F.upper(F.trim("state")))
        .withColumn("city", F.initcap(F.trim("city")))
    )


def clean_providers(providers: DataFrame) -> DataFrame:
    return (
        providers.withColumn("state", F.upper(F.trim("state")))
        .withColumn("city", F.initcap(F.trim("city")))
        .withColumn("specialty", F.initcap(F.trim("specialty")))
    )


def build_member_claims(claims: DataFrame, members: DataFrame, providers: DataFrame) -> DataFrame:
    cleaned_claims = clean_claims(claims).alias("c")
    cleaned_members = clean_members(members).alias("m")
    cleaned_providers = clean_providers(providers).alias("p")

    return (
        cleaned_claims.join(cleaned_members, on="member_id", how="left")
        .join(cleaned_providers, on="provider_id", how="left")
        .withColumn("claim_month", F.date_trunc("month", F.col("service_date")).cast("date"))
        .withColumn(
            "patient_responsibility",
            F.round(F.col("billed_amount") - F.col("paid_amount"), 2),
        )
        .withColumn("is_high_cost_claim", F.col("paid_amount") >= F.lit(200.00))
        .withColumn(
            "last_touched_at",
            F.greatest(
                F.coalesce(F.col("c.ingested_at"), F.to_timestamp(F.lit(DEFAULT_TIMESTAMP))),
                F.coalesce(F.col("m.ingested_at"), F.to_timestamp(F.lit(DEFAULT_TIMESTAMP))),
                F.coalesce(F.col("p.ingested_at"), F.to_timestamp(F.lit(DEFAULT_TIMESTAMP))),
            ),
        )
        .select(
            F.col("c.claim_id").alias("claim_id"),
            F.col("member_id"),
            F.col("m.full_name").alias("member_name"),
            F.col("m.date_of_birth").alias("date_of_birth"),
            F.col("m.gender").alias("gender"),
            F.col("m.city").alias("member_city"),
            F.col("m.state").alias("member_state"),
            F.col("m.plan_id").alias("plan_id"),
            F.col("provider_id"),
            F.col("p.provider_name").alias("provider_name"),
            F.col("p.specialty").alias("provider_specialty"),
            F.col("p.npi").alias("npi"),
            F.col("p.city").alias("provider_city"),
            F.col("p.state").alias("provider_state"),
            F.col("c.service_date").alias("service_date"),
            F.col("claim_month"),
            F.col("c.diagnosis_code").alias("diagnosis_code"),
            F.col("c.procedure_code").alias("procedure_code"),
            F.col("c.claim_status").alias("claim_status"),
            F.col("c.billed_amount").alias("billed_amount"),
            F.col("c.paid_amount").alias("paid_amount"),
            F.col("patient_responsibility"),
            F.col("is_high_cost_claim"),
            F.col("c.source_file").alias("source_file"),
            F.col("c.ingested_at").alias("ingested_at"),
            F.col("last_touched_at"),
            F.col("c.record_source").alias("record_source"),
        )
    )


def build_claim_quality_metrics(member_claims: DataFrame) -> DataFrame:
    return (
        member_claims.groupBy("claim_month", "plan_id", "claim_status")
        .agg(
            F.count("*").alias("claim_count"),
            F.round(F.sum("billed_amount"), 2).alias("total_billed_amount"),
            F.round(F.sum("paid_amount"), 2).alias("total_paid_amount"),
            F.round(F.avg("patient_responsibility"), 2).alias("average_patient_responsibility"),
            F.max("last_touched_at").alias("source_max_last_touched_at"),
        )
        .orderBy("claim_month", "plan_id", "claim_status")
    )


def has_rows(frame: DataFrame) -> bool:
    return frame.limit(1).count() > 0


def collect_claim_months(frame: DataFrame) -> list[object]:
    return [
        row["claim_month"]
        for row in frame.select("claim_month").where(F.col("claim_month").isNotNull()).distinct().collect()
    ]


def latest_watermark(frame: DataFrame, column_name: str) -> object:
    return frame.agg(F.max(column_name).alias("watermark")).first()["watermark"]


def impacted_claim_ids(
    claims: DataFrame,
    members: DataFrame,
    providers: DataFrame,
    watermark: object | None,
) -> DataFrame:
    if watermark is None:
        return claims.select("claim_id").distinct()

    changed_claims = claims.filter(F.col("ingested_at") > F.lit(watermark)).select("claim_id")
    changed_members = members.filter(F.col("ingested_at") > F.lit(watermark)).select("member_id").distinct()
    changed_providers = providers.filter(F.col("ingested_at") > F.lit(watermark)).select("provider_id").distinct()

    claims_from_members = claims.join(changed_members, on="member_id", how="inner").select("claim_id")
    claims_from_providers = claims.join(changed_providers, on="provider_id", how="inner").select("claim_id")

    return changed_claims.unionByName(claims_from_members).unionByName(claims_from_providers).distinct()


def collect_impacted_months(
    existing_member_claims: DataFrame,
    changed_claim_ids: DataFrame,
    refreshed_member_claims: DataFrame,
) -> list[object]:
    existing_months = existing_member_claims.join(changed_claim_ids, on="claim_id", how="inner").select("claim_month")
    new_months = refreshed_member_claims.select("claim_month")
    combined_months = existing_months.unionByName(new_months).where(F.col("claim_month").isNotNull()).distinct()
    return [row["claim_month"] for row in combined_months.collect()]


def run(spark: SparkSession, config: PipelineConfig) -> dict[str, DataFrame]:
    bronze_format = config.output_format("bronze")
    silver_format = config.output_format("silver")

    claims = read_dataset(spark, config.output_path("bronze", "claims"), bronze_format)
    members = read_dataset(spark, config.output_path("bronze", "members"), bronze_format)
    providers = read_dataset(spark, config.output_path("bronze", "providers"), bronze_format)
    member_claims_path = config.output_path("silver", "member_claims")
    claim_quality_metrics_path = config.output_path("silver", "claim_quality_metrics")
    member_claims_merge_keys = config.merge_keys("silver", "member_claims")
    manifest_path = config.metadata_path("silver_impacted_claim_months.json")

    incremental_enabled = (
        config.incremental.enabled
        and bronze_format == "delta"
        and silver_format == "delta"
        and bool(member_claims_merge_keys)
        and dataset_exists(spark, member_claims_path, silver_format)
    )

    if not incremental_enabled:
        member_claims = build_member_claims(claims, members, providers)
        claim_quality_metrics = build_claim_quality_metrics(member_claims)

        write_dataset(
            frame=member_claims,
            path=member_claims_path,
            format_name=silver_format,
            mode=config.write_mode,
            partition_by=["claim_month"],
        )
        write_dataset(
            frame=claim_quality_metrics,
            path=claim_quality_metrics_path,
            format_name=silver_format,
            mode=config.write_mode,
            partition_by=["claim_month"],
        )
        write_impacted_months_manifest(manifest_path, collect_claim_months(member_claims))
        return {
            "member_claims": member_claims,
            "claim_quality_metrics": claim_quality_metrics,
        }

    existing_member_claims = read_dataset(spark, member_claims_path, silver_format)
    watermark = latest_watermark(existing_member_claims, "last_touched_at")
    changed_claims = impacted_claim_ids(claims, members, providers, watermark)

    if not has_rows(changed_claims):
        claim_quality_metrics = (
            read_dataset(spark, claim_quality_metrics_path, silver_format)
            if dataset_exists(spark, claim_quality_metrics_path, silver_format)
            else build_claim_quality_metrics(existing_member_claims)
        )
        return {
            "member_claims": existing_member_claims,
            "claim_quality_metrics": claim_quality_metrics,
        }

    incremental_claims = claims.join(changed_claims, on="claim_id", how="inner")
    incremental_member_claims = build_member_claims(incremental_claims, members, providers)
    impacted_months = collect_impacted_months(existing_member_claims, changed_claims, incremental_member_claims)

    merge_delta_dataset(
        spark=spark,
        frame=incremental_member_claims,
        path=member_claims_path,
        merge_keys=member_claims_merge_keys,
        partition_by=["claim_month"],
    )

    refreshed_member_claims = read_dataset(spark, member_claims_path, silver_format)
    if not dataset_exists(spark, claim_quality_metrics_path, silver_format):
        impacted_months = collect_claim_months(refreshed_member_claims)

    affected_member_claims = refreshed_member_claims.filter(F.col("claim_month").isin(impacted_months))
    claim_quality_metrics = build_claim_quality_metrics(affected_member_claims)
    claim_quality_metrics_options = (
        {"replaceWhere": build_replace_where("claim_month", impacted_months)}
        if silver_format == "delta"
        else None
    )

    write_dataset(
        frame=claim_quality_metrics,
        path=claim_quality_metrics_path,
        format_name=silver_format,
        mode="overwrite",
        partition_by=["claim_month"],
        options=claim_quality_metrics_options,
    )
    write_impacted_months_manifest(manifest_path, impacted_months)

    return {
        "member_claims": refreshed_member_claims,
        "claim_quality_metrics": claim_quality_metrics,
    }
