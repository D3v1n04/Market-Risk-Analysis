from __future__ import annotations

import json
from pathlib import Path

import pytest

from market_risk_analysis.ingestion.manifest import (
    build_manifest,
    calculate_sha256,
    inspect_csv,
    write_manifest,
)

PROJECT_ROOT = Path(__file__).resolve().parents[1]
PORTFOLIO_FIXTURE = PROJECT_ROOT / "data" / "fixtures" / "portfolios.csv"

EXPECTED_SOURCE_SHA256 = (
    "21a3f3aa997b0038b0518cafd85e1bda"
    "1612c42857da5f776ddcd8ef8ff6bd78"
)
LANDING_VOLUME_ROOT = (
    "/Volumes/workspace/devin_market_risk_dev/bronze_landing"
)


def test_calculate_sha256_matches_known_portfolio_fixture() -> None:
    assert calculate_sha256(PORTFOLIO_FIXTURE) == EXPECTED_SOURCE_SHA256


def test_build_manifest_records_portfolio_source_evidence() -> None:
    manifest = build_manifest(
        source_path=PORTFOLIO_FIXTURE,
        source_object_path="data/fixtures/portfolios.csv",
        landing_volume_root=LANDING_VOLUME_ROOT,
        dataset_name="PORTFOLIOS",
        source_id="PROJECT_GIT_FIXTURE",
        source_contract_version="1.0.0",
    )

    assert manifest["dataset_name"] == "PORTFOLIOS"
    assert manifest["source_id"] == "PROJECT_GIT_FIXTURE"
    assert manifest["source_sha256"] == EXPECTED_SOURCE_SHA256
    assert manifest["source_size_bytes"] == 768
    assert manifest["source_record_count"] == 2
    assert manifest["has_header"] is True
    assert manifest["request_window"] is None
    assert manifest["source_columns"][0] == "portfolio_id"
    assert manifest["source_columns"][-1] == "record_hash"
    assert manifest["landed_object_path"] == (
        f"{LANDING_VOLUME_ROOT}/portfolios/"
        f"{EXPECTED_SOURCE_SHA256}/portfolios.csv"
    )


def test_write_manifest_is_deterministic(tmp_path: Path) -> None:
    manifest = {
        "dataset_name": "PORTFOLIOS",
        "source_record_count": 2,
        "source_sha256": EXPECTED_SOURCE_SHA256,
    }
    output_path = tmp_path / "manifest.json"

    first_hash = write_manifest(manifest, output_path)
    first_payload = output_path.read_bytes()

    second_hash = write_manifest(manifest, output_path)
    second_payload = output_path.read_bytes()

    assert first_hash == second_hash
    assert first_payload == second_payload
    assert output_path.read_text(encoding="utf-8").endswith("\n")
    assert json.loads(second_payload) == manifest


def test_inspect_csv_rejects_incorrect_record_width(tmp_path: Path) -> None:
    malformed_csv = tmp_path / "malformed.csv"
    malformed_csv.write_text(
        "portfolio_id,portfolio_name\n"
        "PORTFOLIO_001\n",
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match="physical line 2"):
        inspect_csv(malformed_csv)


@pytest.mark.parametrize(
    (
        "filename",
        "dataset_name",
        "expected_sha256",
        "expected_record_count",
        "expected_column_count",
    ),
    [
        (
            "instruments.csv",
            "INSTRUMENTS",
            (
                "7eb9b32d1f4ef5689404b7f88c5f685a"
                "c55fa2c27ea930e8d5de35b638200b7a"
            ),
            15,
            18,
        ),
        (
            "target_allocations.csv",
            "TARGET_ALLOCATIONS",
            (
                "6182be5e69859a138aa2b94c642943460"
                "aa1914439d340f68d63e4dffccb1e66"
            ),
            30,
            7,
        ),
    ],
)
def test_phase_06_reference_fixture_manifests_are_exact(
    filename: str,
    dataset_name: str,
    expected_sha256: str,
    expected_record_count: int,
    expected_column_count: int,
) -> None:
    source_object_path = f"data/fixtures/{filename}"
    source_path = PROJECT_ROOT / source_object_path

    manifest = build_manifest(
        source_path=source_path,
        source_object_path=source_object_path,
        landing_volume_root=LANDING_VOLUME_ROOT,
        dataset_name=dataset_name,
        source_id="PROJECT_GIT_FIXTURE",
        source_contract_version="1.0.0",
    )

    assert calculate_sha256(source_path) == expected_sha256
    assert manifest["source_sha256"] == expected_sha256
    assert manifest["source_record_count"] == expected_record_count
    assert len(manifest["source_columns"]) == expected_column_count
    assert manifest["source_id"] == "PROJECT_GIT_FIXTURE"
    assert manifest["source_contract_version"] == "1.0.0"
    assert manifest["landed_object_path"] == (
        f"{LANDING_VOLUME_ROOT}/"
        f"{dataset_name.lower()}/"
        f"{expected_sha256}/"
        f"{filename}"
    )
