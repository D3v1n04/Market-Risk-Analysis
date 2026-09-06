from __future__ import annotations

import ast
from decimal import Decimal
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
NOTEBOOK_PATH = (
    PROJECT_ROOT
    / "notebooks"
    / "silver"
    / "phase_06_derive_positions.py"
)


def _read_notebook() -> str:
    return NOTEBOOK_PATH.read_text(encoding="utf-8")


def test_position_derivation_is_valid_databricks_source() -> None:
    source = _read_notebook()

    assert source.startswith("# Databricks notebook source\n")
    assert source.count("# COMMAND ----------") == 3
    ast.parse(source)


def test_position_derivation_has_partition_parameters() -> None:
    source = _read_notebook()

    for parameter in [
        "portfolio_id",
        "position_date",
        "code_version",
        "trigger_type",
    ]:
        assert f'"{parameter}"' in source

    assert source.count("dbutils.widgets.text(") == 4
    assert "PORTFOLIO_ID_PATTERN" in source
    assert "CODE_VERSION_PATTERN" in source
    assert "date.fromisoformat" in source


def test_position_derivation_reads_only_trusted_silver_inputs() -> None:
    source = _read_notebook()

    required_tables = {
        "silver_portfolios",
        "silver_instruments",
        "silver_target_allocations",
        "silver_daily_prices",
        "silver_corporate_actions",
        "silver_trading_calendar",
        "silver_positions",
        "silver_derivation_runs",
    }
    for table_name in required_tables:
        assert table_name in source

    assert "bronze_" not in source.lower()
    assert "gold_" not in source.lower()


def test_position_derivation_implements_approved_formulas() -> None:
    source = _read_notebook()

    assert '"INITIAL_ALLOCATION"' in source
    assert '"BUY_AND_HOLD_CARRY_FORWARD"' in source
    assert '"SPLIT_ADJUSTED_CARRY_FORWARD"' in source
    assert 'F.col("target_weight").cast("decimal(18,10)")' in source
    assert 'F.lit(portfolio["initial_nav"]).cast("decimal(18,2)")' in source
    assert 'F.col("close_price").cast("decimal(20,8)")' in source
    assert '"prior_signed_quantity"' in source
    assert '"split_multiplier"' in source
    assert '"STOCK_SPLIT"' in source


def test_position_derivation_preserves_contract_lineage() -> None:
    source = _read_notebook()

    for field in [
        "input_allocation_record_sha256",
        "input_price_record_sha256",
        "input_prior_position_record_sha256",
        "input_corporate_action_set_sha256",
        "derivation_run_id",
        "calculation_version",
        "derived_at_utc",
        "record_hash",
    ]:
        assert field in source

    assert "POSITION_HASH_FIELDS" in source
    assert '"derivation_run_id",' not in source.split(
        "POSITION_HASH_FIELDS = [", maxsplit=1
    )[1].split("]", maxsplit=1)[0]


def test_position_derivation_builds_complete_input_manifest() -> None:
    source = _read_notebook()

    for dataset_name in [
        "PORTFOLIOS",
        "INSTRUMENTS",
        "TARGET_ALLOCATIONS",
        "DAILY_PRICES",
        "CORPORATE_ACTIONS",
        "TRADING_CALENDAR",
        "POSITIONS",
    ]:
        assert f'"{dataset_name}"' in source

    assert "input_manifest_sha256" in source
    assert "input_record_count" in source
    assert "input_dataset_names" in source
    assert "position_input_manifest=PASS" in source
    assert "EMPTY_SET_SHA256" in source


def test_position_derivation_validates_before_publication() -> None:
    source = _read_notebook()

    validation_position = source.index("failed_rule_ids = sorted")
    publication_position = source.index("if published:")

    assert validation_position < publication_position
    for rule_id in [
        "POSITION_BUSINESS_KEY_UNIQUE",
        "POSITION_REFERENCES_VALID",
        "POSITION_NONZERO_QUANTITY",
        "POSITION_STRATEGY_SIGN",
        "POSITION_VALID_SESSION",
        "POSITION_DAILY_COVERAGE",
        "POSITION_INPUT_COVERAGE",
        "POSITION_DATE_SEQUENCE",
        "POSITION_INCEPTION_RECONCILIATION",
        "POSITION_BUY_AND_HOLD_CONTINUITY",
        "POSITION_SPLIT_RECONCILIATION",
        "POSITION_DIVIDEND_QUANTITY_UNCHANGED",
        "POSITION_RECORD_HASH_VALID",
    ]:
        assert rule_id in source


def test_position_derivation_is_atomic_audited_and_idempotent() -> None:
    source = _read_notebook()

    assert "merge_position_partition" in source
    assert ".whenMatchedUpdateAll()" in source
    assert ".whenNotMatchedInsertAll()" in source
    assert ".whenNotMatchedBySourceDelete(" in source
    assert "insert_derivation_audit" in source
    assert '"output_dataset_name": "POSITIONS"' in source
    assert "attempt_number = previous_runs[0]" in source
    assert "reprocess_of_derivation_run_id" in source
    assert "candidate_sha256 != canonical_before_sha256" in source
    assert "if published:" in source
    assert "persisted derivation audit count" in source


def test_position_derivation_is_serverless_safe() -> None:
    source = _read_notebook()
    normalized = " ".join(source.upper().split())

    for prohibited in [
        ".CACHE(",
        ".PERSIST(",
        "CACHE TABLE",
        "PERSIST TABLE",
        "UNCACHE TABLE",
    ]:
        assert prohibited not in normalized

    assert "freeze_small_dataframe" in source


def test_known_position_math_reconciles_independently() -> None:
    nav = Decimal("1000000.00")
    price = Decimal("100.00000000")

    long_quantity = Decimal("0.0950000000") * nav / price
    short_quantity = Decimal("-0.1000000000") * nav / price

    assert long_quantity == Decimal("950.00000000")
    assert short_quantity == Decimal("-1000.00000000")
    assert long_quantity * Decimal("2.0000000000") == Decimal(
        "1900.000000000000000000"
    )
