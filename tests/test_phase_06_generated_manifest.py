from __future__ import annotations

from pathlib import Path

import pytest

from market_risk_analysis.ingestion.manifest import (
    build_manifest,
    calculate_sha256,
)
from market_risk_analysis.ingestion.phase_06_market_inputs import (
    materialize_market_inputs,
)

PROJECT_ROOT = Path(__file__).resolve().parents[1]
SCENARIO_OBJECT_PATH = "data/fixtures/phase_06_analytics_scenario.yml"
SCENARIO_PATH = PROJECT_ROOT / SCENARIO_OBJECT_PATH
LANDING_ROOT = "/Volumes/workspace/devin_market_risk_dev/bronze_landing"
GENERATOR_MODULE = (
    "market_risk_analysis.ingestion.phase_06_market_inputs"
)
GENERATOR_CODE_VERSION = "f7f6f3d20df8170d4b492e17e26236f251dc47e4"


def test_generated_market_input_manifest_preserves_full_lineage(
    tmp_path: Path,
) -> None:
    results = materialize_market_inputs(
        project_root=PROJECT_ROOT,
        output_root=tmp_path / "generated",
    )
    price_result = results["DAILY_PRICES"]

    manifest = build_manifest(
        source_path=price_result["path"],
        source_object_path=(
            "data/raw/phase_06_analytics_foundation/daily_prices.csv"
        ),
        landing_volume_root=LANDING_ROOT,
        dataset_name="DAILY_PRICES",
        source_id="PROJECT_GIT_FIXTURE",
        source_contract_version="1.1.0",
        generation_source_path=SCENARIO_PATH,
        generation_source_object_path=SCENARIO_OBJECT_PATH,
        generator_module=GENERATOR_MODULE,
        generator_code_version=GENERATOR_CODE_VERSION,
    )

    assert manifest["source_record_count"] == 60
    assert manifest["source_sha256"] == price_result["source_sha256"]
    assert manifest["generation"] == {
        "source_object_path": SCENARIO_OBJECT_PATH,
        "source_sha256": calculate_sha256(SCENARIO_PATH),
        "generator_module": GENERATOR_MODULE,
        "generator_code_version": GENERATOR_CODE_VERSION,
    }


def test_manifest_rejects_partial_generation_lineage(tmp_path: Path) -> None:
    results = materialize_market_inputs(
        project_root=PROJECT_ROOT,
        output_root=tmp_path / "generated",
    )

    with pytest.raises(ValueError, match="Generation lineage requires"):
        build_manifest(
            source_path=results["CORPORATE_ACTIONS"]["path"],
            source_object_path=(
                "data/raw/phase_06_analytics_foundation/corporate_actions.csv"
            ),
            landing_volume_root=LANDING_ROOT,
            dataset_name="CORPORATE_ACTIONS",
            source_id="PROJECT_GIT_FIXTURE",
            source_contract_version="1.1.0",
            generation_source_path=SCENARIO_PATH,
        )


@pytest.mark.parametrize(
    "generator_code_version",
    ["f7f6f3d", "F7F6F3D20DF8170D4B492E17E26236F251DC47E4", "not-a-sha"],
)
def test_manifest_requires_full_lowercase_generator_sha(
    tmp_path: Path,
    generator_code_version: str,
) -> None:
    results = materialize_market_inputs(
        project_root=PROJECT_ROOT,
        output_root=tmp_path / "generated",
    )

    with pytest.raises(ValueError, match="full lowercase Git SHA"):
        build_manifest(
            source_path=results["DAILY_PRICES"]["path"],
            source_object_path=(
                "data/raw/phase_06_analytics_foundation/daily_prices.csv"
            ),
            landing_volume_root=LANDING_ROOT,
            dataset_name="DAILY_PRICES",
            source_id="PROJECT_GIT_FIXTURE",
            source_contract_version="1.1.0",
            generation_source_path=SCENARIO_PATH,
            generation_source_object_path=SCENARIO_OBJECT_PATH,
            generator_module=GENERATOR_MODULE,
            generator_code_version=generator_code_version,
        )
