from __future__ import annotations

import re
from pathlib import Path
from typing import Any

import yaml

PROJECT_ROOT = Path(__file__).resolve().parents[1]
DDL_PATH = PROJECT_ROOT / "sql" / "gold" / "phase_07_create_risk_tables.sql"
CONTRACT_DIR = PROJECT_ROOT / "contracts"

TABLE_CONTRACTS = {
    "workspace.devin_market_risk_dev.gold_risk_runs": "risk_runs",
    "workspace.devin_market_risk_dev.gold_historical_pnl_scenarios": (
        "historical_pnl_scenarios"
    ),
    "workspace.devin_market_risk_dev.gold_var_measures": "var_measures",
    "workspace.devin_market_risk_dev.gold_stress_results": "stress_results",
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
            r"([A-Z]+(?:\([^)]*\)|<[^>]*>)?)",
            _table_body(ddl, table_name),
            flags=re.MULTILINE,
        )
    )


def _required_columns(ddl: str, table_name: str) -> set[str]:
    return set(
        re.findall(
            r"^\s+([a-z][a-z0-9_]*)\s+"
            r"[A-Z]+(?:\([^)]*\)|<[^>]*>)?\s+NOT NULL\b",
            _table_body(ddl, table_name),
            flags=re.MULTILINE,
        )
    )


def test_risk_ddl_matches_contract_columns_and_nullability() -> None:
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


def test_risk_ddl_is_safe_and_unpartitioned() -> None:
    ddl = DDL_PATH.read_text(encoding="utf-8")
    normalized = " ".join(ddl.upper().split())

    assert normalized.count("CREATE TABLE IF NOT EXISTS") == 4
    assert normalized.count("USING DELTA") == 4
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


def test_risk_ddl_preserves_required_audit_and_lineage_columns() -> None:
    ddl = DDL_PATH.read_text(encoding="utf-8")

    risk_run_columns = _column_types(
        ddl,
        "workspace.devin_market_risk_dev.gold_risk_runs",
    )
    assert {
        "risk_run_id",
        "input_manifest_sha256",
        "input_static_exposure_sha256",
        "input_price_history_sha256",
        "input_stress_scenario_set_sha256",
        "input_stress_shock_set_sha256",
        "calculation_version",
        "code_version",
        "contract_version",
    } <= risk_run_columns.keys()

    for table_name in [
        "workspace.devin_market_risk_dev.gold_historical_pnl_scenarios",
        "workspace.devin_market_risk_dev.gold_var_measures",
        "workspace.devin_market_risk_dev.gold_stress_results",
    ]:
        columns = _column_types(ddl, table_name)
        assert {"risk_run_id", "calculation_version", "record_hash"} <= (
            columns.keys()
        )
        assert "analytics_run_id" not in columns
        assert "derivation_run_id" not in columns
        assert "processing_run_id" not in columns
