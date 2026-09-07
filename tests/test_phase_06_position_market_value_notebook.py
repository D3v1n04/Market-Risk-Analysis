from __future__ import annotations

import ast
import csv
from decimal import Decimal
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
NOTEBOOK_PATH = (
    PROJECT_ROOT
    / "notebooks"
    / "gold"
    / "phase_06_calculate_position_market_values.py"
)
FIXTURE_PATH = PROJECT_ROOT / "data" / "fixtures" / "target_allocations.csv"


def _read_notebook() -> str:
    return NOTEBOOK_PATH.read_text(encoding="utf-8")


def test_market_value_notebook_is_valid_databricks_source() -> None:
    source = _read_notebook()

    assert source.startswith("# Databricks notebook source\n")
    assert source.count("# COMMAND ----------") == 3
    ast.parse(source)


def test_market_value_notebook_has_partition_parameters() -> None:
    source = _read_notebook()

    for parameter in [
        "portfolio_id",
        "valuation_date",
        "code_version",
        "trigger_type",
    ]:
        assert f'"{parameter}"' in source

    assert source.count("dbutils.widgets.text(") == 4
    assert "PORTFOLIO_ID_PATTERN" in source
    assert "CODE_VERSION_PATTERN" in source
    assert "date.fromisoformat" in source


def test_market_value_notebook_reads_only_trusted_silver_inputs() -> None:
    source = _read_notebook()

    for table_name in [
        "silver_portfolios",
        "silver_instruments",
        "silver_daily_prices",
        "silver_trading_calendar",
        "silver_positions",
    ]:
        assert table_name in source

    assert "bronze_" not in source.lower()
    assert "silver_cash_balances" not in source


def test_market_value_notebook_implements_approved_formula() -> None:
    source = _read_notebook()

    assert 'F.col("position.signed_quantity").cast("decimal(38,16)")' in source
    assert 'F.col("price.close_price").cast("decimal(20,8)")' in source
    assert 'F.abs(signed_market_value_expression)' in source
    assert 'F.lit("LONG")' in source
    assert 'F.lit("SHORT")' in source
    assert "adjusted_close_price" not in source


def test_market_value_notebook_preserves_gold_lineage() -> None:
    source = _read_notebook()

    for field in [
        "input_position_record_sha256",
        "input_price_record_sha256",
        "analytics_run_id",
        "input_manifest_sha256",
        "calculation_version",
        "calculated_at_utc",
        "record_hash",
    ]:
        assert field in source

    hash_fields = source.split(
        "MARKET_VALUE_HASH_FIELDS = [", maxsplit=1
    )[1].split("]", maxsplit=1)[0]
    assert '"analytics_run_id"' not in hash_fields
    assert '"calculated_at_utc"' not in hash_fields


def test_market_value_notebook_builds_complete_input_manifest() -> None:
    source = _read_notebook()

    for dataset_name in [
        "PORTFOLIOS",
        "INSTRUMENTS",
        "DAILY_PRICES",
        "TRADING_CALENDAR",
        "POSITIONS",
    ]:
        assert f'"{dataset_name}"' in source

    assert "market_value_input_manifest=PASS" in source
    assert "input_record_count" in source
    assert "input_dataset_names" in source


def test_market_value_notebook_validates_before_publication() -> None:
    source = _read_notebook()

    assert source.index("failed_rule_ids = sorted") < source.index(
        "if published:"
    )
    for rule_id in [
        "POSITION_MARKET_VALUE_KEY_UNIQUE",
        "POSITION_MARKET_VALUE_INPUT_COVERAGE",
        "POSITION_MARKET_VALUE_DATE_ALIGNMENT",
        "POSITION_MARKET_VALUE_CURRENCY_MATCH",
        "POSITION_MARKET_VALUE_FORMULA_RECONCILES",
        "POSITION_MARKET_VALUE_ABSOLUTE_RECONCILES",
        "POSITION_MARKET_VALUE_SIGN_RECONCILES",
        "POSITION_MARKET_VALUE_RECORD_HASH_VALID",
    ]:
        assert rule_id in source


def test_market_value_notebook_is_atomic_audited_and_idempotent() -> None:
    source = _read_notebook()

    assert "merge_market_value_partition" in source
    assert ".whenMatchedUpdateAll()" in source
    assert ".whenNotMatchedInsertAll()" in source
    assert ".whenNotMatchedBySourceDelete(" in source
    assert "insert_analytics_audit" in source
    assert '"output_dataset_name": "POSITION_MARKET_VALUES"' in source
    assert "attempt_number = previous_runs[0]" in source
    assert "reprocess_of_analytics_run_id" in source
    assert "candidate_sha256 != canonical_before_sha256" in source
    assert "persisted analytics audit count" in source


def test_market_value_notebook_is_serverless_safe() -> None:
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


def test_inception_market_values_reconcile_independently() -> None:
    with FIXTURE_PATH.open(newline="", encoding="utf-8") as file:
        rows = list(csv.DictReader(file))

    initial_nav = Decimal("1000000.00")
    expected = {
        "CORE_15_LONG": {
            "long": Decimal("1000000.00"),
            "short": Decimal("0.00"),
            "gross": Decimal("1000000.00"),
            "net": Decimal("1000000.00"),
        },
        "LONG_SHORT_130_30": {
            "long": Decimal("1300000.00"),
            "short": Decimal("300000.00"),
            "gross": Decimal("1600000.00"),
            "net": Decimal("1000000.00"),
        },
    }

    for portfolio_id, portfolio_expected in expected.items():
        values = [
            Decimal(row["target_weight"]) * initial_nav
            for row in rows
            if row["portfolio_id"] == portfolio_id
        ]
        long_value = sum((value for value in values if value > 0), Decimal())
        short_value = sum((-value for value in values if value < 0), Decimal())
        gross_value = sum((abs(value) for value in values), Decimal())
        net_value = sum(values, Decimal())

        assert long_value == portfolio_expected["long"]
        assert short_value == portfolio_expected["short"]
        assert gross_value == portfolio_expected["gross"]
        assert net_value == portfolio_expected["net"]


def test_market_value_sign_and_split_math_are_exact() -> None:
    long_quantity = Decimal("650.0000000000000000")
    short_quantity = Decimal("-1000.0000000000000000")
    price = Decimal("98.00000000")

    assert long_quantity * price == Decimal("63700.000000000000000000000000")
    assert short_quantity * price == Decimal("-98000.000000000000000000000000")
    assert abs(short_quantity * price) == Decimal(
        "98000.000000000000000000000000"
    )

    prior_quantity = Decimal("950.0000000000000000")
    prior_price = Decimal("110.00000000")
    split_quantity = Decimal("1900.0000000000000000")
    split_price = Decimal("55.00000000")
    assert prior_quantity * prior_price == split_quantity * split_price
