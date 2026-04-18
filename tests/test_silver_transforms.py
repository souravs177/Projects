from datetime import datetime
import sys

import pytest

pyspark = pytest.importorskip("pyspark")
from pyspark.sql import SparkSession

from healthcare_medallion.jobs.silver import build_member_claims, clean_claims
from healthcare_medallion.spark import ensure_valid_spark_home

pytestmark = pytest.mark.skipif(
    sys.version_info >= (3, 13),
    reason="PySpark 3.5.x is not supported for local execution on Python 3.13+.",
)


@pytest.fixture(scope="session")
def spark():
    ensure_valid_spark_home()
    session = SparkSession.builder.master("local[2]").appName("healthcare-medallion-tests").getOrCreate()
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()


def test_clean_claims_filters_invalid_rows(spark: SparkSession) -> None:
    claims = spark.createDataFrame(
        [
            ("C1", "M1", "P1", "2026-01-01", "E11.9", "83036", 100.0, 90.0, "paid"),
            ("C2", None, "P1", "2026-01-02", "I10", "93000", 120.0, 95.0, "paid"),
            ("C3", "M2", "P2", "2026-01-03", "R73.03", "82947", 80.0, -5.0, "denied"),
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
        ],
    )

    cleaned = clean_claims(claims)

    assert cleaned.count() == 1
    assert cleaned.first()["claim_status"] == "PAID"


def test_build_member_claims_derives_business_columns(spark: SparkSession) -> None:
    claims = spark.createDataFrame(
        [
            (
                "C1",
                "M1",
                "P1",
                "2026-01-01",
                "E11.9",
                "83036",
                300.0,
                240.0,
                "paid",
                "sample.csv",
                datetime(2026, 1, 2, 8, 30, 0),
                "claims",
            ),
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
            "ingested_at",
            "record_source",
        ],
    )
    members = spark.createDataFrame(
        [("M1", "Avery Carter", "1985-06-14", "F", "Dallas", "TX", "PLAN_GOLD", datetime(2026, 1, 2, 7, 0, 0))],
        ["member_id", "full_name", "date_of_birth", "gender", "city", "state", "plan_id", "ingested_at"],
    )
    providers = spark.createDataFrame(
        [
            (
                "P1",
                "Northside Family Clinic",
                "Primary Care",
                "1457392841",
                "Dallas",
                "TX",
                datetime(2026, 1, 2, 6, 0, 0),
            )
        ],
        ["provider_id", "provider_name", "specialty", "npi", "city", "state", "ingested_at"],
    )

    enriched = build_member_claims(claims, members, providers)
    row = enriched.first()

    assert row["member_name"] == "Avery Carter"
    assert row["provider_specialty"] == "Primary Care"
    assert row["patient_responsibility"] == 60.0
    assert row["is_high_cost_claim"] is True
    assert row["last_touched_at"] is not None
