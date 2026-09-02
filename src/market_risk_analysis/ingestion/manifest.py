from __future__ import annotations

import argparse
import csv
import hashlib
import json
import sys
from collections.abc import Sequence
from pathlib import Path
from typing import Any

from market_risk_analysis.config import Settings

MANIFEST_VERSION = "1.0.0"
HASH_CHUNK_SIZE = 1024 * 1024


def calculate_sha256(path: Path) -> str:
    """Return the lowercase SHA-256 digest of a file's exact bytes."""
    if not path.is_file():
        raise FileNotFoundError(f"Source file does not exist: {path}")

    digest = hashlib.sha256()
    with path.open("rb") as source_file:
        while chunk := source_file.read(HASH_CHUNK_SIZE):
            digest.update(chunk)

    return digest.hexdigest()


def inspect_csv(path: Path) -> tuple[list[str], int]:
    """Return the source header and number of data records."""
    with path.open(newline="", encoding="utf-8") as source_file:
        reader = csv.reader(source_file)

        try:
            header = next(reader)
        except StopIteration as exc:
            raise ValueError("CSV source is empty") from exc

        if not header or any(not column.strip() for column in header):
            raise ValueError("CSV header contains an empty column name")

        if len(header) != len(set(header)):
            raise ValueError("CSV header contains duplicate column names")

        record_count = 0
        for line_number, row in enumerate(reader, start=2):
            if len(row) != len(header):
                raise ValueError(
                    "CSV record width does not match the header at "
                    f"physical line {line_number}"
                )
            record_count += 1

    return header, record_count


def build_manifest(
    *,
    source_path: Path,
    source_object_path: str,
    landing_volume_root: str,
    dataset_name: str,
    source_id: str,
    source_contract_version: str,
) -> dict[str, Any]:
    """Build source metadata without writing to the filesystem."""
    if not landing_volume_root.startswith("/Volumes/"):
        raise ValueError("Landing root must begin with /Volumes/")

    source_columns, source_record_count = inspect_csv(source_path)
    source_sha256 = calculate_sha256(source_path)
    dataset_slug = dataset_name.lower()
    source_filename = Path(source_object_path).name

    landed_object_path = (
        f"{landing_volume_root.rstrip('/')}/"
        f"{dataset_slug}/{source_sha256}/{source_filename}"
    )

    return {
        "manifest_version": MANIFEST_VERSION,
        "dataset_name": dataset_name,
        "source_id": source_id,
        "source_object_path": source_object_path,
        "landed_object_path": landed_object_path,
        "source_format": "CSV",
        "source_sha256": source_sha256,
        "source_size_bytes": source_path.stat().st_size,
        "source_record_count": source_record_count,
        "source_columns": source_columns,
        "has_header": True,
        "request_window": None,
        "source_contract_version": source_contract_version,
    }


def serialize_manifest(manifest: dict[str, Any]) -> str:
    """Return stable JSON with a final newline."""
    return json.dumps(
        manifest,
        ensure_ascii=False,
        indent=2,
        sort_keys=True,
    ) + "\n"


def write_manifest(manifest: dict[str, Any], output_path: Path) -> str:
    """Write a deterministic manifest and return its SHA-256 digest."""
    payload = serialize_manifest(manifest)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(payload, encoding="utf-8")
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Build a deterministic source landing manifest."
    )
    parser.add_argument(
        "--project-root",
        type=Path,
        default=Path.cwd(),
        help="Project root used to resolve source and generated-data paths.",
    )
    parser.add_argument("--source-object-path", required=True)
    parser.add_argument("--landing-volume-root", required=True)
    parser.add_argument("--dataset-name", required=True)
    parser.add_argument("--source-id", required=True)
    parser.add_argument("--source-contract-version", required=True)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    """Generate one manifest and print its audit identifiers."""
    args = _build_parser().parse_args(argv)
    project_root = args.project_root.resolve()
    source_relative_path = Path(args.source_object_path)

    if source_relative_path.is_absolute():
        print(
            "source-object-path must be repository-relative",
            file=sys.stderr,
        )
        return 1

    settings = Settings.from_environment(project_root)
    source_path = project_root / source_relative_path

    try:
        manifest = build_manifest(
            source_path=source_path,
            source_object_path=source_relative_path.as_posix(),
            landing_volume_root=args.landing_volume_root,
            dataset_name=args.dataset_name,
            source_id=args.source_id,
            source_contract_version=args.source_contract_version,
        )
    except (FileNotFoundError, ValueError) as exc:
        print(f"manifest generation failed: {exc}", file=sys.stderr)
        return 1

    dataset_slug = args.dataset_name.lower()
    source_sha256 = manifest["source_sha256"]
    output_path = (
        settings.data_dir
        / "raw"
        / dataset_slug
        / source_sha256
        / "manifest.json"
    )
    manifest_sha256 = write_manifest(manifest, output_path)

    print(f"manifest_path={output_path}")
    print(f"source_sha256={source_sha256}")
    print(f"manifest_sha256={manifest_sha256}")
    print(f"source_record_count={manifest['source_record_count']}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
