from __future__ import annotations

import re
from pathlib import Path
from typing import Any

import yaml

PROJECT_ROOT = Path(__file__).resolve().parents[1]
DDL_PATH = PROJECT_ROOT / "sql" / "bronze" / "phase_04_create_bronze_tables.sql"
CONTRACT_DIR = PROJECT_ROOT / "contracts"

PORTFOLIO_TABLE = "workspace.devin_market_risk_dev.bronze_portfolios"
BATCH_TABLE = "workspace.devin_market_risk_dev.bronze_ingestion_batches"


def _load_contract(dataset: str) -> dict[str, Any]:
    path = CONTRACT_DIR / f"{dataset}.yml"
    loaded = yaml.safe_load(path.read_text(encoding="utf-8"))
    assert isinstance(loaded, dict)
    return loaded


def _extract_column_types(ddl: str, table_name: str) -> dict[str, str]:
    pattern = (
        rf"CREATE TABLE IF NOT EXISTS\s+{re.escape(table_name)}\s*"
        rf"\((.*?)\)\s*USING DELTA"
    )
    match = re.search(pattern, ddl, flags=re.DOTALL)
    assert match is not None, f"Missing Delta table definition: {table_name}"

    return dict(
        re.findall(
            r"^\s+([a-z][a-z0-9_]*)\s+([A-Z]+(?:\(\d+,\d+\))?)",
            match.group(1),
            flags=re.MULTILINE,
        )
    )


def test_bronze_ddl_creates_two_unpartitioned_delta_tables() -> None:
    ddl = DDL_PATH.read_text(encoding="utf-8")

    assert ddl.count("CREATE TABLE IF NOT EXISTS") == 2
    assert ddl.count("USING DELTA") == 2
    assert "PARTITIONED BY" not in ddl.upper()


def test_bronze_portfolio_columns_match_source_and_lineage_design() -> None:
    ddl = DDL_PATH.read_text(encoding="utf-8")
    columns = _extract_column_types(ddl, PORTFOLIO_TABLE)
    contract = _load_contract("portfolios")

    expected_source_columns = {
        field["name"]: "STRING" for field in contract["fields"]
    }
    expected_lineage_columns = {
        "batch_id": "STRING",
        "source_id": "STRING",
        "source_object_path": "STRING",
        "source_sha256": "STRING",
        "source_row_number": "BIGINT",
        "source_record_sha256": "STRING",
        "raw_record": "STRING",
        "ingested_at_utc": "TIMESTAMP",
        "contract_version": "STRING",
    }

    assert columns == expected_source_columns | expected_lineage_columns


def test_bronze_batch_columns_match_ingestion_contract() -> None:
    ddl = DDL_PATH.read_text(encoding="utf-8")
    columns = _extract_column_types(ddl, BATCH_TABLE)
    contract = _load_contract("ingestion_batches")

    expected_columns = {
        field["name"]: field["type"] for field in contract["fields"]
    }

    assert columns == expected_columns
