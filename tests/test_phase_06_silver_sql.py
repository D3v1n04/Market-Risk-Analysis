from __future__ import annotations

import re
from pathlib import Path
from typing import Any

import yaml

PROJECT_ROOT = Path(__file__).resolve().parents[1]
DDL_PATH = (
    PROJECT_ROOT
    / "sql"
    / "silver"
    / "phase_06_create_derived_silver_tables.sql"
)
CONTRACT_DIR = PROJECT_ROOT / "contracts"

DERIVATION_RUN_TABLE = (
    "workspace.devin_market_risk_dev.silver_derivation_runs"
)
POSITION_TABLE = (
    "workspace.devin_market_risk_dev.silver_positions"
)
CASH_BALANCE_TABLE = (
    "workspace.devin_market_risk_dev.silver_cash_balances"
)

TABLE_CONTRACTS = {
    DERIVATION_RUN_TABLE: "derivation_runs",
    POSITION_TABLE: "positions",
    CASH_BALANCE_TABLE: "cash_balances",
}


def _load_contract(dataset: str) -> dict[str, Any]:
    path = CONTRACT_DIR / f"{dataset}.yml"
    loaded = yaml.safe_load(path.read_text(encoding="utf-8"))
    assert isinstance(loaded, dict)
    return loaded


def _extract_column_types(
    ddl: str,
    table_name: str,
) -> dict[str, str]:
    pattern = (
        rf"CREATE TABLE IF NOT EXISTS\s+{re.escape(table_name)}\s*"
        rf"\((.*?)\)\s*USING DELTA"
    )
    match = re.search(pattern, ddl, flags=re.DOTALL)
    assert match is not None, f"Missing Delta table: {table_name}"

    return dict(
        re.findall(
            (
                r"^\s+([a-z][a-z0-9_]*)\s+"
                r"([A-Z]+(?:\(\d+,\d+\)|<[A-Z]+>)?)"
            ),
            match.group(1),
            flags=re.MULTILINE,
        )
    )


def _extract_not_null_columns(
    ddl: str,
    table_name: str,
) -> set[str]:
    pattern = (
        rf"CREATE TABLE IF NOT EXISTS\s+{re.escape(table_name)}\s*"
        rf"\((.*?)\)\s*USING DELTA"
    )
    match = re.search(pattern, ddl, flags=re.DOTALL)
    assert match is not None, f"Missing Delta table: {table_name}"

    return set(
        re.findall(
            (
                r"^\s+([a-z][a-z0-9_]*)\s+"
                r"[A-Z]+(?:\(\d+,\d+\)|<[A-Z]+>)?"
                r"\s+NOT NULL\b"
            ),
            match.group(1),
            flags=re.MULTILINE,
        )
    )


def test_phase_06_silver_columns_match_contracts() -> None:
    ddl = DDL_PATH.read_text(encoding="utf-8")

    for table_name, contract_name in TABLE_CONTRACTS.items():
        actual_columns = _extract_column_types(ddl, table_name)
        contract = _load_contract(contract_name)
        expected_columns = {
            field["name"]: field["type"]
            for field in contract["fields"]
        }

        assert actual_columns == expected_columns, (
            f"{table_name} columns do not match {contract_name}"
        )


def test_phase_06_silver_nullability_matches_contracts() -> None:
    ddl = DDL_PATH.read_text(encoding="utf-8")

    for table_name, contract_name in TABLE_CONTRACTS.items():
        actual_required = _extract_not_null_columns(
            ddl,
            table_name,
        )
        contract = _load_contract(contract_name)
        expected_required = {
            field["name"]
            for field in contract["fields"]
            if field["nullable"] is False
        }

        assert actual_required == expected_required, (
            f"{table_name} nullability does not match "
            f"{contract_name}"
        )


def test_phase_06_silver_ddl_is_safe_and_unpartitioned() -> None:
    ddl = DDL_PATH.read_text(encoding="utf-8")
    normalized_ddl = " ".join(ddl.upper().split())

    assert normalized_ddl.count(
        "CREATE TABLE IF NOT EXISTS"
    ) == 3
    assert normalized_ddl.count("USING DELTA") == 3
    assert "PARTITIONED BY" not in normalized_ddl

    prohibited_statements = [
        "CREATE OR REPLACE TABLE",
        "DROP TABLE",
        "TRUNCATE TABLE",
        "ALTER TABLE",
        "INSERT INTO",
        "UPDATE ",
        "DELETE FROM",
        "MERGE INTO",
    ]
    for statement in prohibited_statements:
        assert statement not in normalized_ddl

    for table_name in TABLE_CONTRACTS:
        expected = f"CREATE TABLE IF NOT EXISTS {table_name}"
        assert expected.upper() in normalized_ddl


def test_phase_06_derived_tables_use_derivation_lineage() -> None:
    ddl = DDL_PATH.read_text(encoding="utf-8")

    for table_name in [
        POSITION_TABLE,
        CASH_BALANCE_TABLE,
    ]:
        columns = _extract_column_types(ddl, table_name)

        assert "derivation_run_id" in columns
        assert "processing_run_id" not in columns
        assert "batch_id" not in columns
        assert "source_record_id" not in columns

    derivation_columns = _extract_column_types(
        ddl,
        DERIVATION_RUN_TABLE,
    )
    assert {
        "output_dataset_name",
        "portfolio_id",
        "business_date",
        "attempt_number",
    } <= derivation_columns.keys()
