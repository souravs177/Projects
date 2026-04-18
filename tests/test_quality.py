from __future__ import annotations

import json
import sys
from pathlib import Path
from uuid import uuid4

import pytest

pytest.importorskip("great_expectations")
pyspark = pytest.importorskip("pyspark")
from pyspark.sql import SparkSession

from healthcare_medallion.config import (
    DataQualityConfig,
    IncrementalConfig,
    PipelineConfig,
    SparkConfig,
    StorageConfig,
)
from healthcare_medallion.quality import (
    DataQualityValidationError,
    build_expectation_suite,
    run_quality_checks,
)
from healthcare_medallion.spark import ensure_valid_spark_home

pytestmark = pytest.mark.skipif(
    sys.version_info >= (3, 13),
    reason="PySpark 3.5.x is not supported for local execution on Python 3.13+.",
)


@pytest.fixture(scope="session")
def spark():
    ensure_valid_spark_home()
    session = SparkSession.builder.master("local[2]").appName("healthcare-medallion-quality-tests").getOrCreate()
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()


def make_config() -> PipelineConfig:
    root_dir = Path(__file__).resolve().parents[1]
    results_path = f"data/system/quality-tests/{uuid4().hex}"
    return PipelineConfig(
        project_name="healthcare_medallion",
        root_dir=root_dir,
        storage=StorageConfig(
            raw_path="data/raw",
            bronze_path="data/bronze",
            silver_path="data/silver",
            gold_path="data/gold",
            metadata_path="data/system",
        ),
        spark=SparkConfig(app_name="quality-tests"),
        incremental=IncrementalConfig(enabled=True),
        data_quality=DataQualityConfig(enabled=True, fail_on_error=True, results_path=results_path),
        datasets={},
    )


def test_build_expectation_suite_includes_claim_quality_rules() -> None:
    suite = build_expectation_suite("silver", "member_claims")

    expectation_names = [type(expectation).__name__ for expectation in suite.expectations]

    assert suite.name == "silver.member_claims.quality"
    assert "ExpectTableColumnsToMatchOrderedList" in expectation_names
    assert "ExpectColumnValuesToBeInSet" in expectation_names
    assert "ExpectColumnValuesToMatchRegex" in expectation_names


def test_run_quality_checks_writes_success_report(spark: SparkSession) -> None:
    config = make_config()
    claims = spark.createDataFrame(
        [
            (
                "C1001",
                "M100",
                "P200",
                "2026-01-05",
                "E11.9",
                "83036",
                280.0,
                225.0,
                "paid",
                "claims_sample.csv",
                "abc123",
                "2026-01-06 00:00:00",
                "claims",
            )
        ],
        [
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
    )

    results = run_quality_checks(spark, config, "bronze", {"claims": claims})

    report_path = config.data_quality_path("bronze", "claims")
    report_payload = json.loads(report_path.read_text(encoding="utf-8"))

    assert results["claims"]["success"] is True
    assert report_payload["success"] is True
    assert report_payload["layer"] == "bronze"
    assert report_payload["dataset_name"] == "claims"


def test_run_quality_checks_raises_for_failed_expectations(spark: SparkSession) -> None:
    config = make_config()
    providers = spark.createDataFrame(
        [
            (
                "P200",
                "Northside Family Clinic",
                "Primary Care",
                "BAD_NPI",
                "Dallas",
                "TX",
                "providers_sample.csv",
                "providerhash",
                "2026-01-06 00:00:00",
                "providers",
            )
        ],
        [
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
    )

    with pytest.raises(DataQualityValidationError, match="bronze.providers"):
        run_quality_checks(spark, config, "bronze", {"providers": providers})

    report_payload = json.loads(config.data_quality_path("bronze", "providers").read_text(encoding="utf-8"))
    assert report_payload["success"] is False
