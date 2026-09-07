from __future__ import annotations

import re
from pathlib import Path
from typing import Any

import yaml

PROJECT_ROOT = Path(__file__).resolve().parents[1]
DDL_PATH = PROJECT_ROOT / "sql" / "gold" / "phase_06_create_gold_tables.sql"
CONTRACT_DIR = PROJECT_ROOT / "contracts"

TABLE_CONTRACTS = {
    "workspace.devin_market_risk_dev.gold_analytics_runs": "analytics_runs",
    "workspace.devin_market_risk_dev.gold_position_market_values": (
        "position_market_values"
    ),
}


def _load_contract(dataset: str) -> dict[str, Any]:
    loaded = yaml.safe_load(
        (CONTRACT_DIR / f"{dataset}.yml").read_text(encoding="utf-8")
    )
    assert isinstance(loaded, dict)
    return loaded


def _table_body(ddl: str, table_name: str) -> str:
    match = re.search(
        rf"CREATE TABLE IF NOT EXISTS\s+{re.escape(table_name)}\s*"
        rf"\((.*?)\)\s*USING DELTA",
        ddl,
        flags=re.DOTALL,
    )
    assert match is not None, f"Missing Delta table: {table_name}"
    return match.group(1)


def _column_types(ddl: str, table_name: str) -> dict[str, str]:
    return dict(
        re.findall(
            r"^\s+([a-z][a-z0-9_]*)\s+"
            r"([A-Z]+(?:\(\d+,\d+\)|<[A-Z]+>)?)",
            _table_body(ddl, table_name),
            flags=re.MULTILINE,
        )
    )


def _required_columns(ddl: str, table_name: str) -> set[str]:
    return set(
        re.findall(
            r"^\s+([a-z][a-z0-9_]*)\s+"
            r"[A-Z]+(?:\(\d+,\d+\)|<[A-Z]+>)?\s+NOT NULL\b",
            _table_body(ddl, table_name),
            flags=re.MULTILINE,
        )
    )


def test_gold_ddl_matches_contract_columns_and_nullability() -> None:
    ddl = DDL_PATH.read_text(encoding="utf-8")

    for table_name, contract_name in TABLE_CONTRACTS.items():
        contract = _load_contract(contract_name)
        expected_columns = {
            field["name"]: field["type"] for field in contract["fields"]
        }
        expected_required = {
            field["name"]
            for field in contract["fields"]
            if field["nullable"] is False
        }
        assert _column_types(ddl, table_name) == expected_columns
        assert _required_columns(ddl, table_name) == expected_required


def test_gold_ddl_is_safe_and_unpartitioned() -> None:
    ddl = DDL_PATH.read_text(encoding="utf-8")
    normalized = " ".join(ddl.upper().split())

    assert normalized.count("CREATE TABLE IF NOT EXISTS") == 2
    assert normalized.count("USING DELTA") == 2
    assert "PARTITIONED BY" not in normalized

    for statement in [
        "CREATE OR REPLACE TABLE",
        "DROP TABLE",
        "TRUNCATE TABLE",
        "ALTER TABLE",
        "INSERT INTO",
        "UPDATE ",
        "DELETE FROM",
        "MERGE INTO",
    ]:
        assert statement not in normalized


def test_gold_market_value_table_preserves_metric_lineage() -> None:
    ddl = DDL_PATH.read_text(encoding="utf-8")
    columns = _column_types(
        ddl,
        "workspace.devin_market_risk_dev.gold_position_market_values",
    )

    assert {
        "input_position_record_sha256",
        "input_price_record_sha256",
        "analytics_run_id",
        "calculation_version",
        "calculated_at_utc",
        "contract_version",
        "record_hash",
    } <= columns.keys()
    assert "derivation_run_id" not in columns
    assert "processing_run_id" not in columns
