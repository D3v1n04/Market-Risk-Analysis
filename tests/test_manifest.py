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
