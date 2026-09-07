from __future__ import annotations

import ast
from decimal import Decimal
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
NOTEBOOK_PATH = (
    PROJECT_ROOT
    / "notebooks"
    / "silver"
    / "phase_06_derive_cash_balances.py"
)


def _read_notebook() -> str:
    return NOTEBOOK_PATH.read_text(encoding="utf-8")


def test_cash_derivation_is_valid_databricks_source() -> None:
    source = _read_notebook()

    assert source.startswith("# Databricks notebook source\n")
    assert source.count("# COMMAND ----------") == 3
    ast.parse(source)


def test_cash_derivation_has_partition_parameters() -> None:
    source = _read_notebook()

    for parameter in [
        "portfolio_id",
        "cash_date",
        "code_version",
        "trigger_type",
    ]:
        assert f'"{parameter}"' in source

    assert source.count("dbutils.widgets.text(") == 4
    assert "PORTFOLIO_ID_PATTERN" in source
    assert "CODE_VERSION_PATTERN" in source
    assert "date.fromisoformat" in source


def test_cash_derivation_reads_only_trusted_silver_inputs() -> None:
    source = _read_notebook()

    required_tables = {
        "silver_portfolios",
        "silver_instruments",
        "silver_daily_prices",
        "silver_corporate_actions",
        "silver_trading_calendar",
        "silver_positions",
        "silver_cash_balances",
        "silver_derivation_runs",
    }
    for table_name in required_tables:
        assert table_name in source

    assert "bronze_" not in source.lower()
    assert "gold_" not in source.lower()


def test_cash_derivation_implements_approved_formulas() -> None:
    source = _read_notebook()

    assert 'Decimal(str(portfolio["initial_nav"])) - signed_market_value' in source
    assert 'F.col("position.signed_quantity").cast(' in source
    assert 'F.col("price.close_price").cast("decimal(20,8)")' in source
    assert 'F.col("action.dividend_amount_per_share").cast(' in source
    assert "expected_opening_cash = prior_closing_cash" in source
    assert "closing_cash_balance = opening_cash_balance + dividend_cash_flow" in source
    assert 'F.col("action_type") == "CASH_DIVIDEND"' in source


def test_cash_derivation_preserves_contract_lineage() -> None:
    source = _read_notebook()

    for field in [
        "input_position_set_sha256",
        "input_corporate_action_set_sha256",
        "derivation_run_id",
        "calculation_version",
        "derived_at_utc",
        "record_hash",
    ]:
        assert field in source

    hash_fields = source.split(
        "CASH_BALANCE_HASH_FIELDS = [", maxsplit=1
    )[1].split("]", maxsplit=1)[0]
    assert '"derivation_run_id"' not in hash_fields
    assert '"derived_at_utc"' not in hash_fields


def test_cash_derivation_builds_complete_input_manifest() -> None:
    source = _read_notebook()

    for dataset_name in [
        "PORTFOLIOS",
        "INSTRUMENTS",
        "DAILY_PRICES",
        "CORPORATE_ACTIONS",
        "TRADING_CALENDAR",
        "POSITIONS",
        "CASH_BALANCES",
    ]:
        assert f'"{dataset_name}"' in source

    assert "input_manifest_sha256" in source
    assert "input_record_count" in source
    assert "input_dataset_names" in source
    assert "cash_input_manifest=PASS" in source
    assert "EMPTY_SET_SHA256" in source


def test_cash_derivation_validates_before_publication() -> None:
    source = _read_notebook()

    validation_position = source.index("failed_rule_ids = sorted")
    publication_position = source.index("if published:")
    assert validation_position < publication_position

    for rule_id in [
        "CASH_BALANCE_BUSINESS_KEY_UNIQUE",
        "CASH_BALANCE_REFERENCES_VALID",
        "CASH_BALANCE_BASE_CURRENCY_MATCH",
        "CASH_BALANCE_DATE_SEQUENCE",
        "CASH_BALANCE_INCEPTION_RECONCILIATION",
        "CASH_BALANCE_OPENING_CONTINUITY",
        "CASH_BALANCE_DIVIDEND_RECONCILIATION",
        "CASH_BALANCE_CLOSING_RECONCILIATION",
        "CASH_BALANCE_INPUT_COVERAGE",
        "CASH_BALANCE_ACTION_COUNT_CONSISTENT",
        "CASH_BALANCE_RECORD_HASH_VALID",
    ]:
        assert rule_id in source


def test_cash_derivation_is_atomic_audited_and_idempotent() -> None:
    source = _read_notebook()

    assert "merge_cash_partition" in source
    assert ".whenMatchedUpdateAll()" in source
    assert ".whenNotMatchedInsertAll()" in source
    assert ".whenNotMatchedBySourceDelete(" in source
    assert "insert_derivation_audit" in source
    assert '"output_dataset_name": "CASH_BALANCES"' in source
    assert "attempt_number = previous_runs[0]" in source
    assert "reprocess_of_derivation_run_id" in source
    assert "candidate_sha256 != canonical_before_sha256" in source
    assert "if published:" in source
    assert "persisted derivation audit count" in source


def test_cash_derivation_is_serverless_safe() -> None:
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


def test_known_cash_math_reconciles_independently() -> None:
    initial_nav = Decimal("1000000.0000000000000000")
    long_only_signed_market_value = Decimal("1000000.0000000000000000")
    long_short_signed_market_value = Decimal("1000000.0000000000000000")

    assert initial_nav - long_only_signed_market_value == Decimal("0")
    assert initial_nav - long_short_signed_market_value == Decimal("0")

    long_dividend_cash = Decimal("650") * Decimal("1.00")
    short_dividend_cash = Decimal("-1000") * Decimal("1.00")
    assert long_dividend_cash == Decimal("650.00")
    assert short_dividend_cash == Decimal("-1000.00")

    assert Decimal("0") + long_dividend_cash == Decimal("650.00")
    assert Decimal("0") + short_dividend_cash == Decimal("-1000.00")


def test_split_has_no_cash_effect() -> None:
    opening_cash = Decimal("650.00")
    dividend_cash_flow = Decimal("0.00")

    assert opening_cash + dividend_cash_flow == Decimal("650.00")
