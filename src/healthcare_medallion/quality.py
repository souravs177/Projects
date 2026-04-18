from __future__ import annotations

import json
import logging

import great_expectations as gx
from great_expectations import expectations as gxe
from pyspark.sql import DataFrame, SparkSession

from healthcare_medallion.config import PipelineConfig

LOGGER = logging.getLogger(__name__)

STATE_CODE_REGEX = r"^[A-Z]{2}$"
NPI_REGEX = r"^\d{10}$"

BRONZE_COLUMNS = {
    "claims": [
        "claim_id",
        "member_id",
        "provider_id",
        "service_date",
        "diagnosis_code",
        "procedure_code",
        "billed_amount",
        "paid_amount",
        "claim_status",
        "source_file",
        "record_hash",
        "ingested_at",
        "record_source",
    ],
    "members": [
        "member_id",
        "full_name",
        "date_of_birth",
        "gender",
        "city",
        "state",
        "plan_id",
        "source_file",
        "record_hash",
        "ingested_at",
        "record_source",
    ],
    "providers": [
        "provider_id",
        "provider_name",
        "specialty",
        "npi",
        "city",
        "state",
        "source_file",
        "record_hash",
        "ingested_at",
        "record_source",
    ],
}

SILVER_COLUMNS = {
    "member_claims": [
        "claim_id",
        "member_id",
        "member_name",
        "date_of_birth",
        "gender",
        "member_city",
        "member_state",
        "plan_id",
        "provider_id",
        "provider_name",
        "provider_specialty",
        "npi",
        "provider_city",
        "provider_state",
        "service_date",
        "claim_month",
        "diagnosis_code",
        "procedure_code",
        "claim_status",
        "billed_amount",
        "paid_amount",
        "patient_responsibility",
        "is_high_cost_claim",
        "source_file",
        "ingested_at",
        "last_touched_at",
        "record_source",
    ],
    "claim_quality_metrics": [
        "claim_month",
        "plan_id",
        "claim_status",
        "claim_count",
        "total_billed_amount",
        "total_paid_amount",
        "average_patient_responsibility",
        "source_max_last_touched_at",
    ],
}

GOLD_COLUMNS = {
    "monthly_cost_by_plan_state": [
        "claim_month",
        "plan_id",
        "member_state",
        "total_claims",
        "distinct_members",
        "distinct_providers",
        "total_billed_amount",
        "total_paid_amount",
        "average_paid_amount",
        "source_max_last_touched_at",
        "paid_to_billed_ratio",
    ],
    "provider_specialty_summary": [
        "claim_month",
        "provider_specialty",
        "provider_state",
        "total_claims",
        "distinct_providers",
        "distinct_members",
        "total_paid_amount",
        "average_patient_responsibility",
        "high_cost_claim_count",
        "source_max_last_touched_at",
    ],
}


class DataQualityValidationError(RuntimeError):
    """Raised when a GX validation fails and the pipeline is configured to stop."""


def build_expectation_suite(layer: str, dataset_name: str) -> gx.ExpectationSuite:
    expectations: list[object]

    if layer == "bronze" and dataset_name == "claims":
        expectations = [
            gxe.ExpectTableColumnsToMatchOrderedList(column_list=BRONZE_COLUMNS["claims"]),
            gxe.ExpectColumnValuesToNotBeNull(column="claim_id"),
            gxe.ExpectColumnValuesToNotBeNull(column="member_id"),
            gxe.ExpectColumnValuesToNotBeNull(column="provider_id"),
            gxe.ExpectColumnValuesToNotBeNull(column="service_date"),
            gxe.ExpectColumnValuesToNotBeNull(column="paid_amount"),
            gxe.ExpectColumnValuesToNotBeNull(column="record_hash"),
            gxe.ExpectColumnValuesToNotBeNull(column="ingested_at"),
            gxe.ExpectColumnValuesToBeInSet(column="claim_status", value_set=["paid", "denied", "pending", "PAID", "DENIED", "PENDING"]),
            gxe.ExpectColumnValuesToBeBetween(column="billed_amount", min_value=0.0),
            gxe.ExpectColumnValuesToBeBetween(column="paid_amount", min_value=0.0),
            gxe.ExpectColumnDistinctValuesToContainSet(column="record_source", value_set=["claims"]),
        ]
    elif layer == "bronze" and dataset_name == "members":
        expectations = [
            gxe.ExpectTableColumnsToMatchOrderedList(column_list=BRONZE_COLUMNS["members"]),
            gxe.ExpectColumnValuesToNotBeNull(column="member_id"),
            gxe.ExpectColumnValuesToNotBeNull(column="full_name"),
            gxe.ExpectColumnValuesToNotBeNull(column="plan_id"),
            gxe.ExpectColumnValuesToNotBeNull(column="record_hash"),
            gxe.ExpectColumnValuesToBeInSet(column="gender", value_set=["F", "M", "U", "O"]),
            gxe.ExpectColumnValuesToMatchRegex(column="state", regex=STATE_CODE_REGEX),
            gxe.ExpectColumnDistinctValuesToContainSet(column="record_source", value_set=["members"]),
        ]
    elif layer == "bronze" and dataset_name == "providers":
        expectations = [
            gxe.ExpectTableColumnsToMatchOrderedList(column_list=BRONZE_COLUMNS["providers"]),
            gxe.ExpectColumnValuesToNotBeNull(column="provider_id"),
            gxe.ExpectColumnValuesToNotBeNull(column="provider_name"),
            gxe.ExpectColumnValuesToNotBeNull(column="specialty"),
            gxe.ExpectColumnValuesToNotBeNull(column="npi"),
            gxe.ExpectColumnValuesToNotBeNull(column="record_hash"),
            gxe.ExpectColumnValuesToMatchRegex(column="npi", regex=NPI_REGEX),
            gxe.ExpectColumnValuesToMatchRegex(column="state", regex=STATE_CODE_REGEX),
            gxe.ExpectColumnDistinctValuesToContainSet(column="record_source", value_set=["providers"]),
        ]
    elif layer == "silver" and dataset_name == "member_claims":
        expectations = [
            gxe.ExpectTableColumnsToMatchOrderedList(column_list=SILVER_COLUMNS["member_claims"]),
            gxe.ExpectColumnValuesToNotBeNull(column="claim_id"),
            gxe.ExpectColumnValuesToNotBeNull(column="member_id"),
            gxe.ExpectColumnValuesToNotBeNull(column="provider_id"),
            gxe.ExpectColumnValuesToNotBeNull(column="member_name"),
            gxe.ExpectColumnValuesToNotBeNull(column="provider_name"),
            gxe.ExpectColumnValuesToNotBeNull(column="plan_id"),
            gxe.ExpectColumnValuesToNotBeNull(column="claim_month"),
            gxe.ExpectColumnValuesToBeInSet(column="claim_status", value_set=["PAID", "DENIED", "PENDING"]),
            gxe.ExpectColumnValuesToMatchRegex(column="member_state", regex=STATE_CODE_REGEX),
            gxe.ExpectColumnValuesToMatchRegex(column="provider_state", regex=STATE_CODE_REGEX),
            gxe.ExpectColumnValuesToMatchRegex(column="npi", regex=NPI_REGEX),
            gxe.ExpectColumnValuesToBeBetween(column="paid_amount", min_value=0.0),
            gxe.ExpectColumnValuesToBeBetween(column="patient_responsibility", min_value=0.0),
            gxe.ExpectColumnValuesToBeInSet(column="is_high_cost_claim", value_set=[True, False]),
            gxe.ExpectColumnDistinctValuesToContainSet(column="record_source", value_set=["claims"]),
        ]
    elif layer == "silver" and dataset_name == "claim_quality_metrics":
        expectations = [
            gxe.ExpectTableColumnsToMatchOrderedList(column_list=SILVER_COLUMNS["claim_quality_metrics"]),
            gxe.ExpectColumnValuesToNotBeNull(column="claim_month"),
            gxe.ExpectColumnValuesToNotBeNull(column="plan_id"),
            gxe.ExpectColumnValuesToNotBeNull(column="claim_status"),
            gxe.ExpectColumnValuesToBeInSet(column="claim_status", value_set=["PAID", "DENIED", "PENDING"]),
            gxe.ExpectColumnValuesToBeBetween(column="claim_count", min_value=1),
            gxe.ExpectColumnValuesToBeBetween(column="total_billed_amount", min_value=0.0),
            gxe.ExpectColumnValuesToBeBetween(column="total_paid_amount", min_value=0.0),
            gxe.ExpectColumnValuesToBeBetween(column="average_patient_responsibility", min_value=0.0),
        ]
    elif layer == "gold" and dataset_name == "monthly_cost_by_plan_state":
        expectations = [
            gxe.ExpectTableColumnsToMatchOrderedList(column_list=GOLD_COLUMNS["monthly_cost_by_plan_state"]),
            gxe.ExpectColumnValuesToNotBeNull(column="claim_month"),
            gxe.ExpectColumnValuesToNotBeNull(column="plan_id"),
            gxe.ExpectColumnValuesToNotBeNull(column="member_state"),
            gxe.ExpectColumnValuesToMatchRegex(column="member_state", regex=STATE_CODE_REGEX),
            gxe.ExpectColumnValuesToBeBetween(column="total_claims", min_value=1),
            gxe.ExpectColumnValuesToBeBetween(column="distinct_members", min_value=1),
            gxe.ExpectColumnValuesToBeBetween(column="distinct_providers", min_value=1),
            gxe.ExpectColumnValuesToBeBetween(column="total_billed_amount", min_value=0.0),
            gxe.ExpectColumnValuesToBeBetween(column="total_paid_amount", min_value=0.0),
            gxe.ExpectColumnValuesToBeBetween(column="average_paid_amount", min_value=0.0),
            gxe.ExpectColumnValuesToBeBetween(column="paid_to_billed_ratio", min_value=0.0, max_value=1.0),
        ]
    elif layer == "gold" and dataset_name == "provider_specialty_summary":
        expectations = [
            gxe.ExpectTableColumnsToMatchOrderedList(column_list=GOLD_COLUMNS["provider_specialty_summary"]),
            gxe.ExpectColumnValuesToNotBeNull(column="claim_month"),
            gxe.ExpectColumnValuesToNotBeNull(column="provider_specialty"),
            gxe.ExpectColumnValuesToNotBeNull(column="provider_state"),
            gxe.ExpectColumnValuesToMatchRegex(column="provider_state", regex=STATE_CODE_REGEX),
            gxe.ExpectColumnValuesToBeBetween(column="total_claims", min_value=1),
            gxe.ExpectColumnValuesToBeBetween(column="distinct_providers", min_value=1),
            gxe.ExpectColumnValuesToBeBetween(column="distinct_members", min_value=1),
            gxe.ExpectColumnValuesToBeBetween(column="total_paid_amount", min_value=0.0),
            gxe.ExpectColumnValuesToBeBetween(column="average_patient_responsibility", min_value=0.0),
            gxe.ExpectColumnValuesToBeBetween(column="high_cost_claim_count", min_value=0),
        ]
    else:
        raise ValueError(f"No Great Expectations suite is defined for '{layer}.{dataset_name}'.")

    return gx.ExpectationSuite(
        name=f"{layer}.{dataset_name}.quality",
        expectations=expectations,
    )


def failed_expectations(validation_result: dict[str, object]) -> list[str]:
    failures: list[str] = []
    for result in validation_result.get("results", []):
        if result.get("success"):
            continue
        expectation_config = result.get("expectation_config", {})
        expectation_type = expectation_config.get("type", "unknown_expectation")
        failures.append(str(expectation_type))
    return failures


def write_validation_result(config: PipelineConfig, layer: str, dataset_name: str, payload: dict[str, object]) -> None:
    result_path = config.data_quality_path(layer, dataset_name)
    result_path.parent.mkdir(parents=True, exist_ok=True)
    result_path.write_text(json.dumps(payload, indent=2, default=str), encoding="utf-8")


def validate_dataset(
    datasource,
    config: PipelineConfig,
    layer: str,
    dataset_name: str,
    frame: DataFrame,
) -> dict[str, object]:
    asset = datasource.add_dataframe_asset(name=f"{layer}_{dataset_name}")
    batch_definition = asset.add_batch_definition_whole_dataframe("full_dataframe")
    batch = batch_definition.get_batch(batch_parameters={"dataframe": frame})
    suite = build_expectation_suite(layer, dataset_name)
    validation_result = batch.validate(suite).to_json_dict()
    validation_result["layer"] = layer
    validation_result["dataset_name"] = dataset_name
    write_validation_result(config, layer, dataset_name, validation_result)
    return validation_result


def run_quality_checks(
    spark: SparkSession,
    config: PipelineConfig,
    layer: str,
    datasets: dict[str, DataFrame],
) -> dict[str, dict[str, object]]:
    if not config.data_quality.enabled:
        return {}

    context = gx.get_context(mode="ephemeral")
    datasource = context.data_sources.add_spark(
        name=f"{config.project_name}_{layer}_quality",
        persist=False,
    )
    validation_results: dict[str, dict[str, object]] = {}
    failed_datasets: list[str] = []

    for dataset_name, frame in datasets.items():
        validation_result = validate_dataset(datasource, config, layer, dataset_name, frame)
        validation_results[dataset_name] = validation_result
        if validation_result["success"]:
            LOGGER.info("Great Expectations validation passed for %s.%s.", layer, dataset_name)
            continue

        failure_summary = ", ".join(failed_expectations(validation_result))
        failed_datasets.append(f"{layer}.{dataset_name} [{failure_summary}]")
        LOGGER.error("Great Expectations validation failed for %s.%s: %s", layer, dataset_name, failure_summary)

    if failed_datasets and config.data_quality.fail_on_error:
        failures = "; ".join(failed_datasets)
        raise DataQualityValidationError(f"Data quality checks failed: {failures}")

    return validation_results
