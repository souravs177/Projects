from datetime import date
from pathlib import Path
from uuid import uuid4

from healthcare_medallion.incremental import (
    build_replace_where,
    clear_impacted_months_manifest,
    read_impacted_months_manifest,
    write_impacted_months_manifest,
)


def test_build_replace_where_supports_date_partitions() -> None:
    clause = build_replace_where("claim_month", [date(2026, 1, 1), date(2026, 2, 1)])

    assert clause == "claim_month IN (DATE '2026-01-01', DATE '2026-02-01')"


def test_impacted_month_manifest_appends_and_clears() -> None:
    scratch_dir = Path(__file__).resolve().parent / ".tmp" / str(uuid4())
    scratch_dir.mkdir(parents=True, exist_ok=True)
    manifest_path = scratch_dir / "silver_impacted_claim_months.json"

    try:
        write_impacted_months_manifest(manifest_path, ["2026-01-01"])
        write_impacted_months_manifest(manifest_path, ["2026-02-01", "2026-01-01"])

        assert read_impacted_months_manifest(manifest_path) == ["2026-01-01", "2026-02-01"]

        clear_impacted_months_manifest(manifest_path)

        assert read_impacted_months_manifest(manifest_path) == []
    finally:
        if manifest_path.exists():
            manifest_path.unlink()
        if scratch_dir.exists():
            scratch_dir.rmdir()
