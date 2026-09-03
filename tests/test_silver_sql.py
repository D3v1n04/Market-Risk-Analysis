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
    / "phase_05_create_silver_tables.sql"
)

CONTRACT_DIR = PROJECT_ROOT / "contracts"

PORTFOLIO_TABLE = (
    "workspace.devin_market_risk_dev.silver_portfolios"
)

PROCESSING_RUN_TABLE = (
    "workspace.devin_market_risk_dev.silver_processing_runs"
)
OUTCOME_TABLE = (
    "workspace.devin_market_risk_dev."
    "silver_portfolio_record_outcomes"
)
VIOLATION_TABLE = (
    "workspace.devin_market_risk_dev."
    "silver_data_quality_violations"
)

AUDIT_TABLE_CONTRACTS = {
    PROCESSING_RUN_TABLE: "processing_runs",
    OUTCOME_TABLE: "portfolio_record_outcomes",
    VIOLATION_TABLE: "data_quality_violations",
}

SILVER_TABLES = {
    "workspace.devin_market_risk_dev.silver_portfolios",
    "workspace.devin_market_risk_dev.silver_processing_runs",
    "workspace.devin_market_risk_dev.silver_portfolio_record_outcomes",
    "workspace.devin_market_risk_dev.silver_data_quality_violations",
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


def test_silver_ddl_nullability_matches_contracts() -> None:
    ddl = DDL_PATH.read_text(encoding="utf-8")

    portfolio_contract = _load_contract("portfolios")
    expected_portfolio_required = {
        field["name"]
        for field in portfolio_contract["fields"]
        if field["nullable"] is False
    }
    expected_portfolio_required |= {
        "processing_run_id",
        "source_batch_id",
        "source_record_id",
        "source_row_number",
        "source_record_sha256",
        "canonicalized_at_utc",
        "dataset_contract_version",
    }

    assert (
        _extract_not_null_columns(ddl, PORTFOLIO_TABLE)
        == expected_portfolio_required
    )

    for table_name, contract_name in AUDIT_TABLE_CONTRACTS.items():
        contract = _load_contract(contract_name)
        expected_required = {
            field["name"]
            for field in contract["fields"]
            if field["nullable"] is False
        }

        assert (
            _extract_not_null_columns(ddl, table_name)
            == expected_required
        ), f"{table_name} nullability does not match {contract_name}"


def test_silver_ddl_contains_no_data_mutation_or_destructive_sql() -> None:
    ddl = DDL_PATH.read_text(encoding="utf-8")
    normalized_ddl = " ".join(ddl.upper().split())

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


def test_silver_ddl_creates_four_unpartitioned_delta_tables() -> None:
    ddl = DDL_PATH.read_text(encoding="utf-8")
    normalized_ddl = " ".join(ddl.upper().split())

    assert normalized_ddl.count("CREATE TABLE IF NOT EXISTS") == 4
    assert normalized_ddl.count("USING DELTA") == 4
    assert "PARTITIONED BY" not in normalized_ddl

    for table_name in SILVER_TABLES:
        expected = f"CREATE TABLE IF NOT EXISTS {table_name}"
        assert expected.upper() in normalized_ddl


def test_silver_portfolio_columns_match_contract_and_lineage() -> None:
    ddl = DDL_PATH.read_text(encoding="utf-8")
    columns = _extract_column_types(ddl, PORTFOLIO_TABLE)
    contract = _load_contract("portfolios")

    expected_business_columns = {
        field["name"]: field["type"]
        for field in contract["fields"]
    }
    expected_lineage_columns = {
        "processing_run_id": "STRING",
        "source_batch_id": "STRING",
        "source_record_id": "STRING",
        "source_row_number": "BIGINT",
        "source_record_sha256": "STRING",
        "canonicalized_at_utc": "TIMESTAMP",
        "dataset_contract_version": "STRING",
    }

    assert columns == (
        expected_business_columns | expected_lineage_columns
    )


def test_silver_audit_table_columns_match_contracts() -> None:
    ddl = DDL_PATH.read_text(encoding="utf-8")

    for table_name, contract_name in AUDIT_TABLE_CONTRACTS.items():
        columns = _extract_column_types(ddl, table_name)
        contract = _load_contract(contract_name)
        expected_columns = {
            field["name"]: field["type"]
            for field in contract["fields"]
        }

        assert columns == expected_columns, (
            f"{table_name} does not match {contract_name}"
        )
