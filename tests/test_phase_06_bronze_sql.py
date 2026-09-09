from __future__ import annotations

import re
from pathlib import Path
from typing import Any

import yaml

PROJECT_ROOT = Path(__file__).resolve().parents[1]
CONTRACT_DIR = PROJECT_ROOT / "contracts"
DDL_PATH = (
    PROJECT_ROOT
    / "sql"
    / "bronze"
    / "phase_06_create_analytics_input_tables.sql"
)

TABLES = {
    "instruments": (
        "workspace.devin_market_risk_dev.bronze_instruments"
    ),
    "target_allocations": (
        "workspace.devin_market_risk_dev.bronze_target_allocations"
    ),
    "stress_scenarios": (
        "workspace.devin_market_risk_dev.bronze_stress_scenarios"
    ),
    "stress_scenario_shocks": (
        "workspace.devin_market_risk_dev."
        "bronze_stress_scenario_shocks"
    ),
    "daily_prices": (
        "workspace.devin_market_risk_dev.bronze_daily_prices"
    ),
    "corporate_actions": (
        "workspace.devin_market_risk_dev.bronze_corporate_actions"
    ),
}

LINEAGE_TYPES = {
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


def _load_contract(dataset: str) -> dict[str, Any]:
    path = CONTRACT_DIR / f"{dataset}.yml"
    loaded = yaml.safe_load(path.read_text(encoding="utf-8"))
    assert isinstance(loaded, dict)
    return loaded


def _extract_table_body(ddl: str, table_name: str) -> str:
    pattern = (
        rf"CREATE TABLE IF NOT EXISTS\s+{re.escape(table_name)}\s*"
        rf"\((.*?)\)\s*USING DELTA"
    )
    match = re.search(pattern, ddl, flags=re.DOTALL)
    assert match is not None, f"Missing Delta table: {table_name}"
    return match.group(1)


def _extract_column_types(ddl: str, table_name: str) -> dict[str, str]:
    body = _extract_table_body(ddl, table_name)
    return dict(
        re.findall(
            r"^\s+([a-z][a-z0-9_]*)\s+"
            r"([A-Z]+(?:\(\d+,\d+\))?)",
            body,
            flags=re.MULTILINE,
        )
    )


def _extract_required_columns(ddl: str, table_name: str) -> set[str]:
    body = _extract_table_body(ddl, table_name)
    return set(
        re.findall(
            r"^\s+([a-z][a-z0-9_]*)\s+"
            r"[A-Z]+(?:\(\d+,\d+\))?\s+NOT NULL",
            body,
            flags=re.MULTILINE,
        )
    )


def test_phase_06_bronze_ddl_creates_six_safe_delta_tables() -> None:
    ddl = DDL_PATH.read_text(encoding="utf-8")
    normalized = " ".join(ddl.upper().split())

    assert ddl.count("CREATE TABLE IF NOT EXISTS") == 6
    assert ddl.count("USING DELTA") == 6
    assert "PARTITIONED BY" not in normalized

    for table_name in TABLES.values():
        assert table_name.upper() in normalized

    prohibited_statements = {
        "CREATE OR REPLACE",
        "INSERT INTO",
        "MERGE INTO",
        "UPDATE ",
        "DELETE FROM",
        "DROP TABLE",
        "TRUNCATE TABLE",
        "ALTER TABLE",
    }
    assert not {
        statement
        for statement in prohibited_statements
        if statement in normalized
    }


def test_phase_06_bronze_columns_match_contracts_and_lineage() -> None:
    ddl = DDL_PATH.read_text(encoding="utf-8")

    for dataset, table_name in TABLES.items():
        contract = _load_contract(dataset)
        columns = _extract_column_types(ddl, table_name)

        source_columns = {
            field["name"]: "STRING"
            for field in contract["fields"]
        }
        expected_columns = source_columns | LINEAGE_TYPES

        assert columns == expected_columns


def test_phase_06_bronze_requires_only_technical_lineage() -> None:
    ddl = DDL_PATH.read_text(encoding="utf-8")

    for table_name in TABLES.values():
        required_columns = _extract_required_columns(
            ddl,
            table_name,
        )
        assert required_columns == set(LINEAGE_TYPES)


def test_phase_06_bronze_defers_business_type_conversion() -> None:
    ddl = DDL_PATH.read_text(encoding="utf-8")

    expected_raw_strings = {
        TABLES["instruments"]: {
            "classification_as_of_date",
            "active_from",
            "is_active",
        },
        TABLES["target_allocations"]: {
            "effective_from",
            "target_weight",
        },
        TABLES["stress_scenarios"]: {
            "is_active",
            "effective_from",
        },
        TABLES["stress_scenario_shocks"]: {
            "shock_ratio",
        },
        TABLES["daily_prices"]: {
            "price_date",
            "open_price",
            "close_price",
            "volume",
            "source_updated_at_utc",
        },
        TABLES["corporate_actions"]: {
            "effective_date",
            "dividend_amount_per_share",
            "split_ratio",
            "source_updated_at_utc",
        },
    }

    for table_name, column_names in expected_raw_strings.items():
        columns = _extract_column_types(ddl, table_name)
        assert all(
            columns[column_name] == "STRING"
            for column_name in column_names
        )
