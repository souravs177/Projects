from pathlib import Path

from healthcare_medallion.config import load_config


def test_load_config_reads_expected_datasets() -> None:
    config_path = Path(__file__).resolve().parents[1] / "conf" / "pipeline.yml"
    config = load_config(config_path)

    assert config.project_name == "healthcare_medallion"
    assert config.spark.app_name == "healthcare-medallion-pipeline"
    assert set(config.datasets) == {"claims", "members", "providers"}
    assert config.raw_dataset_path("claims").name == "claims_sample.csv"
    assert config.output_format("bronze") == "delta"
    assert config.output_format("silver") == "delta"
    assert config.output_format("gold") == "delta"
    assert config.incremental.enabled is True
    assert config.merge_keys("bronze", "claims") == ["claim_id"]
    assert config.merge_keys("silver", "member_claims") == ["claim_id"]
