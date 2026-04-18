from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

from healthcare_medallion.config import load_config
from healthcare_medallion.jobs import bronze, gold, silver
from healthcare_medallion.quality import run_quality_checks
from healthcare_medallion.spark import build_spark_session

LOGGER = logging.getLogger(__name__)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run the healthcare medallion Spark pipeline.")
    parser.add_argument(
        "--config",
        default="conf/pipeline.yml",
        help="Path to the pipeline configuration file.",
    )
    parser.add_argument(
        "--layer",
        choices=["bronze", "silver", "gold", "all"],
        default="all",
        help="Pipeline layer to execute.",
    )
    return parser.parse_args()


def run_pipeline(layer: str, config_path: str | Path) -> None:
    if sys.version_info >= (3, 13):
        raise RuntimeError(
            "This project targets Python 3.10-3.12. "
            "Use Python 3.11 for local Spark execution because PySpark 3.5.x is not stable on Python 3.13+."
        )

    config = load_config(config_path)
    spark = build_spark_session(config.spark, config)

    try:
        if layer in {"bronze", "all"}:
            LOGGER.info("Running bronze layer.")
            bronze_outputs = bronze.run(spark, config)
            run_quality_checks(spark, config, "bronze", bronze_outputs)

        if layer in {"silver", "all"}:
            LOGGER.info("Running silver layer.")
            silver_outputs = silver.run(spark, config)
            run_quality_checks(spark, config, "silver", silver_outputs)

        if layer in {"gold", "all"}:
            LOGGER.info("Running gold layer.")
            gold_outputs = gold.run(spark, config)
            run_quality_checks(spark, config, "gold", gold_outputs)
    finally:
        spark.stop()


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s - %(message)s")
    args = parse_args()
    run_pipeline(layer=args.layer, config_path=args.config)


if __name__ == "__main__":
    main()
