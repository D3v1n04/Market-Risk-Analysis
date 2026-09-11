from __future__ import annotations

import hashlib
import json
from pathlib import Path

import pytest

from market_risk_analysis.ingestion.phase_12_prepare_bronze_landing import (
    prepare_bronze_landing,
)


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _write_complete_attempt(raw_root: Path, *, status: str = "SUCCEEDED") -> Path:
    prices = raw_root / "daily_prices" / "prices-sha" / "daily_prices.csv"
    actions = (
        raw_root
        / "corporate_actions"
        / "actions-sha"
        / "corporate_actions.csv"
    )
    prices.parent.mkdir(parents=True)
    actions.parent.mkdir(parents=True)
    prices.write_text("instrument_id,price_date\nNVDA_US,2020-01-02\n")
    actions.write_text("instrument_id,effective_date\nNVDA_US,2020-01-03\n")
    manifest = {
        "source_id": "YAHOO_FINANCE",
        "status": status,
        "failures": [] if status == "SUCCEEDED" else ["NVDA_US: EMPTY_RESPONSE"],
        "attempt_id": "attempt-1",
        "snapshot_sha256": "snapshot-sha",
        "daily_prices": {
            "path": prices.relative_to(raw_root).as_posix(),
            "sha256": _sha256(prices),
            "record_count": 1,
        },
        "corporate_actions": {
            "path": actions.relative_to(raw_root).as_posix(),
            "sha256": _sha256(actions),
            "record_count": 1,
        },
    }
    attempt_path = raw_root / "attempts" / "attempt-1" / "manifest.json"
    attempt_path.parent.mkdir(parents=True)
    attempt_path.write_text(json.dumps(manifest))
    return attempt_path


def test_complete_attempt_creates_verified_publishable_landing(
    tmp_path: Path,
) -> None:
    raw_root = tmp_path / "raw"
    landing_root = tmp_path / "landing"
    attempt_path = _write_complete_attempt(raw_root)

    prepared = prepare_bronze_landing(
        raw_root=raw_root,
        attempt_manifest_path=attempt_path,
        landing_root=landing_root,
    )

    assert set(prepared) == {"DAILY_PRICES", "CORPORATE_ACTIONS"}
    assert prepared["DAILY_PRICES"]["publication_status"] == "APPROVED"
    manifest = json.loads(
        Path(prepared["DAILY_PRICES"]["manifest_path"]).read_text()
    )
    assert manifest["record_count"] == 1
    assert manifest["attempt_id"] == "attempt-1"


def test_partial_attempt_cannot_be_prepared_for_publication(tmp_path: Path) -> None:
    raw_root = tmp_path / "raw"
    attempt_path = _write_complete_attempt(raw_root, status="PARTIAL")

    with pytest.raises(ValueError, match="Only a SUCCEEDED extraction"):
        prepare_bronze_landing(
            raw_root=raw_root,
            attempt_manifest_path=attempt_path,
            landing_root=tmp_path / "landing",
        )
