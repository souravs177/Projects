import sys
from pathlib import Path

import pytest

from healthcare_medallion.pipeline import run_pipeline


@pytest.mark.skipif(sys.version_info < (3, 13), reason="The runtime guard only applies on unsupported Python versions.")
def test_run_pipeline_blocks_unsupported_python() -> None:
    config_path = Path(__file__).resolve().parents[1] / "conf" / "pipeline.yml"

    with pytest.raises(RuntimeError, match="Python 3.10-3.12"):
        run_pipeline(layer="all", config_path=config_path)
