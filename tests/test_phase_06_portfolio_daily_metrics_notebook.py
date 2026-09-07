from __future__ import annotations

import ast
from decimal import Decimal
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
NOTEBOOK_PATH = (
    PROJECT_ROOT
    / "notebooks"
    / "gold"
    / "phase_06_calculate_portfolio_daily_metrics.py"
)

EXPECTED_COMPONENTS = [
    (
        "CORE_15_LONG",
        "2016-01-04",
        Decimal("1000000"),
        Decimal("0"),
        Decimal("0"),
        Decimal("1000000"),
    ),
    (
        "CORE_15_LONG",
        "2016-01-05",
        Decimal("1007499.999997"),
        Decimal("0"),
        Decimal("0"),
        Decimal("1000000"),
    ),
    (
        "CORE_15_LONG",
        "2016-01-06",
        Decimal("1007499.999997"),
        Decimal("0"),
        Decimal("1000"),
        Decimal("1007499.999997"),
    ),
    (
        "CORE_15_LONG",
        "2016-01-07",
        Decimal("1007499.999997"),
        Decimal("0"),
        Decimal("1000"),
        Decimal("1008499.999997"),
    ),
    (
        "LONG_SHORT_130_30",
        "2016-01-04",
        Decimal("1300000"),
        Decimal("300000"),
        Decimal("0"),
        Decimal("1000000"),
    ),
    (
        "LONG_SHORT_130_30",
        "2016-01-05",
        Decimal("1324440"),
        Decimal("284000"),
        Decimal("0"),
        Decimal("1000000"),
    ),
    (
        "LONG_SHORT_130_30",
        "2016-01-06",
        Decimal("1324440"),
        Decimal("284000"),
        Decimal("-1000"),
        Decimal("1040440"),
    ),
    (
        "LONG_SHORT_130_30",
        "2016-01-07",
        Decimal("1324440"),
        Decimal("284000"),
        Decimal("-1000"),
        Decimal("1039440"),
    ),
]


def _read_notebook() -> str:
    return NOTEBOOK_PATH.read_text(encoding="utf-8")


def test_portfolio_daily_notebook_is_valid_databricks_source() -> None:
    source = _read_notebook()

    assert source.startswith("# Databricks notebook source\n")
    assert source.count("# COMMAND ----------") == 3
    ast.parse(source)


def test_portfolio_daily_notebook_has_partition_parameters() -> None:
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


def test_portfolio_daily_notebook_reads_governed_inputs() -> None:
    source = _read_notebook()

    for table_name in [
        "silver_portfolios",
        "silver_cash_balances",
        "gold_position_market_values",
        "gold_portfolio_daily_metrics",
        "gold_analytics_runs",
    ]:
        assert table_name in source

    assert "bronze_" not in source.lower()


def test_portfolio_daily_notebook_implements_approved_metrics() -> None:
    source = _read_notebook()

    for field_name in [
        "long_market_value",
        "short_market_value",
        "gross_market_value",
        "net_security_market_value",
        "closing_cash_balance",
        "closing_nav",
        "baseline_nav",
        "daily_pnl",
        "daily_return",
        "long_exposure_ratio",
        "short_exposure_ratio",
        "gross_exposure_ratio",
        "net_exposure_ratio",
    ]:
        assert field_name in source

    assert "INITIAL_NAV" in source
    assert "PRIOR_PUBLISHED_NAV" in source
    assert "intermediate rounding" in source.lower()


def test_portfolio_daily_notebook_is_audited_and_atomic() -> None:
    source = _read_notebook()

    for required_text in [
        "input_market_value_partition_sha256",
        "input_cash_balance_record_sha256",
        "input_portfolio_record_sha256",
        "input_prior_metric_record_sha256",
        "input_manifest_sha256",
        "merge_portfolio_daily_partition",
        "insert_analytics_audit",
        '"output_dataset_name": "PORTFOLIO_DAILY_METRICS"',
        "candidate_sha256 != canonical_before_sha256",
        "reprocess_of_analytics_run_id",
        ".whenMatchedUpdateAll()",
        ".whenNotMatchedInsertAll()",
    ]:
        assert required_text in source


def test_eight_expected_portfolio_dates_reconcile_independently() -> None:
    assert len(EXPECTED_COMPONENTS) == 8

    results: dict[tuple[str, str], dict[str, Decimal]] = {}

    for portfolio_id, valuation_date, long_value, short_value, cash, baseline in (
        EXPECTED_COMPONENTS
    ):
        gross = long_value + short_value
        net = long_value - short_value
        nav = net + cash
        pnl = nav - baseline

        results[(portfolio_id, valuation_date)] = {
            "gross": gross,
            "net": net,
            "nav": nav,
            "pnl": pnl,
            "return": pnl / baseline,
            "long_ratio": long_value / nav,
            "short_ratio": short_value / nav,
            "gross_ratio": gross / nav,
            "net_ratio": net / nav,
        }

    inception = results[("LONG_SHORT_130_30", "2016-01-04")]
    assert inception["long_ratio"] == Decimal("1.30")
    assert inception["short_ratio"] == Decimal("0.30")
    assert inception["gross_ratio"] == Decimal("1.60")
    assert inception["net_ratio"] == Decimal("1.00")

    core_dividend = results[("CORE_15_LONG", "2016-01-06")]
    assert core_dividend["pnl"] == Decimal("1000")

    short_dividend = results[("LONG_SHORT_130_30", "2016-01-06")]
    assert short_dividend["pnl"] == Decimal("-1000")

    for portfolio_id in ["CORE_15_LONG", "LONG_SHORT_130_30"]:
        split_day = results[(portfolio_id, "2016-01-07")]
        assert split_day["pnl"] == Decimal("0")


def test_portfolio_daily_notebook_builds_complete_input_manifest() -> None:
    source = _read_notebook()

    for dataset_name in [
        "PORTFOLIOS",
        "CASH_BALANCES",
        "POSITION_MARKET_VALUES",
        "PORTFOLIO_DAILY_METRICS",
    ]:
        assert f'dataset_name="{dataset_name}"' in source

    assert "portfolio_daily_input_manifest=PASS" in source
    assert "input_record_count" in source
    assert "input_dataset_names" in source
    assert "prior_cash_date" in source
    assert source.count('snapshot_reference="SILVER_CURRENT"') == 2
    assert source.count('snapshot_reference="PUBLISHED_GOLD"') == 2


def test_portfolio_daily_notebook_validates_before_publication() -> None:
    source = _read_notebook()

    assert source.index("failed_rule_ids = sorted") < source.index(
        "if published:"
    )

    for rule_id in [
        "PORTFOLIO_DAILY_KEY_UNIQUE",
        "PORTFOLIO_DAILY_INPUT_COVERAGE",
        "PORTFOLIO_DAILY_DATE_ALIGNMENT",
        "PORTFOLIO_DAILY_POSITION_COUNTS_RECONCILE",
        "PORTFOLIO_DAILY_MARKET_VALUES_RECONCILE",
        "PORTFOLIO_DAILY_NAV_RECONCILES",
        "PORTFOLIO_DAILY_NAV_POSITIVE",
        "PORTFOLIO_DAILY_BASELINE_RECONCILES",
        "PORTFOLIO_DAILY_PNL_RECONCILES",
        "PORTFOLIO_DAILY_RETURN_RECONCILES",
        "PORTFOLIO_DAILY_EXPOSURE_RATIOS_RECONCILE",
        "PORTFOLIO_DAILY_CURRENCY_MATCH",
        "PORTFOLIO_DAILY_RECORD_HASH_VALID",
    ]:
        assert rule_id in source

    hash_fields = source.split(
        "PORTFOLIO_DAILY_HASH_FIELDS = [",
        maxsplit=1,
    )[1].split("]", maxsplit=1)[0]

    assert '"analytics_run_id"' not in hash_fields
    assert '"calculated_at_utc"' not in hash_fields
