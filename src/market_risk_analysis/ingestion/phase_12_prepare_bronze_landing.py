"""Prepare a complete Yahoo Finance snapshot for governed Bronze landing."""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import shutil
from pathlib import Path
from typing import Any

SOURCE_ID = "YAHOO_FINANCE"
DATASETS = {
    "DAILY_PRICES": ("daily_prices", "daily_prices.csv"),
    "CORPORATE_ACTIONS": ("corporate_actions", "corporate_actions.csv"),
}


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def load_complete_attempt(attempt_manifest_path: Path) -> dict[str, Any]:
    """Load an attempt manifest and reject anything ineligible for publication."""
    manifest = json.loads(attempt_manifest_path.read_text(encoding="utf-8"))
    if manifest.get("source_id") != SOURCE_ID:
        raise ValueError("Attempt manifest must identify YAHOO_FINANCE")
    if manifest.get("status") != "SUCCEEDED":
        raise ValueError(
            "Only a SUCCEEDED extraction may be prepared for Bronze publication"
        )
    if manifest.get("failures"):
        raise ValueError("A publishable extraction cannot contain failures")
    return manifest


def prepare_bronze_landing(
    *,
    raw_root: Path,
    attempt_manifest_path: Path,
    landing_root: Path,
) -> dict[str, dict[str, object]]:
    """Copy verified immutable snapshot objects into a publishable landing package."""
    attempt = load_complete_attempt(attempt_manifest_path)
    prepared: dict[str, dict[str, object]] = {}

    for dataset_name, (manifest_key, filename) in DATASETS.items():
        source_metadata = attempt[manifest_key]
        source_path = raw_root / source_metadata["path"]
        expected_sha256 = source_metadata["sha256"]
        if _sha256(source_path) != expected_sha256:
            raise ValueError(f"{dataset_name} source SHA-256 does not match attempt")
        with source_path.open(newline="", encoding="utf-8") as source_file:
            record_count = sum(1 for _ in csv.DictReader(source_file))
        if record_count != source_metadata["record_count"]:
            raise ValueError(
                f"{dataset_name} source record count does not match attempt"
            )

        destination_dir = landing_root / dataset_name.lower() / expected_sha256
        destination_path = destination_dir / filename
        destination_dir.mkdir(parents=True, exist_ok=True)
        if not destination_path.exists():
            shutil.copyfile(source_path, destination_path)
        if _sha256(destination_path) != expected_sha256:
            raise ValueError(f"{dataset_name} landing copy SHA-256 does not match")

        manifest = {
            "dataset_name": dataset_name,
            "source_id": SOURCE_ID,
            "attempt_id": attempt["attempt_id"],
            "snapshot_sha256": attempt["snapshot_sha256"],
            "source_object_path": source_metadata["path"],
            "source_sha256": expected_sha256,
            "record_count": record_count,
            "publication_status": "APPROVED",
        }
        landing_manifest_path = destination_dir / "manifest.json"
        landing_manifest_path.write_text(
            json.dumps(manifest, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )
        prepared[dataset_name] = {
            "data_path": destination_path,
            "manifest_path": landing_manifest_path,
            **manifest,
        }
    return prepared


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Prepare one complete Yahoo snapshot for Bronze landing."
    )
    parser.add_argument("--project-root", type=Path, default=Path.cwd())
    parser.add_argument(
        "--attempt-manifest",
        type=Path,
        required=True,
        help="Path relative to the Yahoo raw root for one extraction manifest.",
    )
    parser.add_argument(
        "--landing-root",
        type=Path,
        default=Path("data/bronze_landing/yahoo_finance"),
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    args = _build_parser().parse_args(argv)
    project_root = args.project_root.resolve()
    raw_root = project_root / "data/raw/yahoo_finance"
    attempt_manifest_path = raw_root / args.attempt_manifest
    prepared = prepare_bronze_landing(
        raw_root=raw_root,
        attempt_manifest_path=attempt_manifest_path,
        landing_root=(project_root / args.landing_root).resolve(),
    )
    print(
        json.dumps(
            {
                name: {
                    **values,
                    "data_path": str(values["data_path"]),
                    "manifest_path": str(values["manifest_path"]),
                }
                for name, values in prepared.items()
            },
            default=str,
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
