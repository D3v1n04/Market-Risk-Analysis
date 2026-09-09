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
    / "silver"
    / "phase_06_create_analytics_input_tables.sql"
)

TABLES = {
    "instruments": (
        "workspace.devin_market_risk_dev.silver_instruments"
    ),
    "target_allocations": (
        "workspace.devin_market_risk_dev.silver_target_allocations"
    ),
    "stress_scenarios": (
        "workspace.devin_market_risk_dev.silver_stress_scenarios"
    ),
    "stress_scenario_shocks": (
        "workspace.devin_market_risk_dev."
        "silver_stress_scenario_shocks"
    ),
    "daily_prices": (
        "workspace.devin_market_risk_dev.silver_daily_prices"
    ),
    "corporate_actions": (
        "workspace.devin_market_risk_dev.silver_corporate_actions"
    ),
    "trading_calendar": (
        "workspace.devin_market_risk_dev.silver_trading_calendar"
    ),
    "source_record_outcomes": (
        "workspace.devin_market_risk_dev."
        "silver_source_record_outcomes"
    ),
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


def _extract_columns(
    ddl: str,
    table_name: str,
) -> dict[str, tuple[str, bool]]:
    body = _extract_table_body(ddl, table_name)
    matches = re.findall(
        r"^\s+([a-z][a-z0-9_]*)\s+"
        r"([A-Z]+(?:\(\d+,\d+\))?(?:<[^>]+>)?)"
        r"(\s+NOT NULL)?",
        body,
        flags=re.MULTILINE,
    )

    return {
        name: (data_type, not bool(required))
        for name, data_type, required in matches
    }


def test_phase_06_input_silver_columns_match_contracts() -> None:
    ddl = DDL_PATH.read_text(encoding="utf-8")

    expected_counts = {
        "instruments": 18,
        "target_allocations": 7,
        "stress_scenarios": 9,
        "stress_scenario_shocks": 6,
        "daily_prices": 17,
        "corporate_actions": 15,
        "trading_calendar": 12,
        "source_record_outcomes": 16,
    }

    for dataset, table_name in TABLES.items():
        contract = _load_contract(dataset)
        columns = _extract_columns(ddl, table_name)

        expected_types = {
            field["name"]: field["type"]
            for field in contract["fields"]
        }
        actual_types = {
            name: definition[0]
            for name, definition in columns.items()
        }

        assert len(columns) == expected_counts[dataset]
        assert actual_types == expected_types

    assert sum(expected_counts.values()) == 100


def test_phase_06_input_silver_nullability_matches_contracts() -> None:
    ddl = DDL_PATH.read_text(encoding="utf-8")

    expected_required_counts = {
        "instruments": 17,
        "target_allocations": 6,
        "stress_scenarios": 8,
        "stress_scenario_shocks": 6,
        "daily_prices": 15,
        "corporate_actions": 10,
        "trading_calendar": 9,
        "source_record_outcomes": 13,
    }

    for dataset, table_name in TABLES.items():
        contract = _load_contract(dataset)
        columns = _extract_columns(ddl, table_name)

        expected_nullable = {
            field["name"]: field["nullable"] is not False
            for field in contract["fields"]
        }
        actual_nullable = {
            name: definition[1]
            for name, definition in columns.items()
        }

        assert actual_nullable == expected_nullable
        assert sum(
            not nullable
            for nullable in actual_nullable.values()
        ) == expected_required_counts[dataset]


def test_phase_06_input_silver_ddl_is_safe_and_unpartitioned() -> None:
    ddl = DDL_PATH.read_text(encoding="utf-8")
    normalized = " ".join(ddl.upper().split())

    assert ddl.count("CREATE TABLE IF NOT EXISTS") == 8
    assert ddl.count("USING DELTA") == 8
    assert "PARTITIONED BY" not in normalized

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

    for table_name in TABLES.values():
        assert table_name.upper() in normalized


def test_phase_06_input_silver_keeps_audit_data_separate() -> None:
    ddl = DDL_PATH.read_text(encoding="utf-8")

    outcome_columns = _extract_columns(
        ddl,
        TABLES["source_record_outcomes"],
    )
    assert {
        "processing_run_id",
        "batch_id",
        "dataset_name",
        "source_record_id",
        "source_record_sha256",
        "outcome",
    } <= outcome_columns.keys()

    for dataset in [
        "instruments",
        "target_allocations",
        "stress_scenarios",
        "stress_scenario_shocks",
        "daily_prices",
        "corporate_actions",
        "trading_calendar",
    ]:
        business_columns = _extract_columns(
            ddl,
            TABLES[dataset],
        )
        assert "outcome" not in business_columns
        assert "violation_count" not in business_columns
