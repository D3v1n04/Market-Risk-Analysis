from __future__ import annotations

from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
DAILY_SERVING_SQL_PATH = (
    PROJECT_ROOT / "sql" / "serving" / "phase_08_create_serving_views.sql"
)
RISK_SERVING_SQL_PATH = (
    PROJECT_ROOT
    / "sql"
    / "serving"
    / "phase_08_create_risk_serving_views.sql"
)

DAILY_VIEW = "workspace.devin_market_risk_dev.vw_portfolio_daily_analytics"
POSITION_VIEW = "workspace.devin_market_risk_dev.vw_position_exposure_detail"
RISK_RUN_VIEW = "workspace.devin_market_risk_dev.vw_latest_published_risk_runs"
VAR_VIEW = "workspace.devin_market_risk_dev.vw_latest_published_var"
STRESS_VIEW = (
    "workspace.devin_market_risk_dev.vw_latest_published_stress_results"
)

QA_SQL_PATH = PROJECT_ROOT / "sql" / "qa" / "phase_08_serving_qa.sql"


def _executable_sql(sql: str) -> str:
    return "\n".join(
        line for line in sql.splitlines() if not line.lstrip().startswith("--")
    )


def _assert_no_governed_data_mutation(sql: str) -> None:
    normalized = " ".join(_executable_sql(sql).upper().split())

    assert "SELECT *" not in normalized

    for prohibited_statement in [
        "CREATE OR REPLACE TABLE",
        "DROP TABLE",
        "TRUNCATE TABLE",
        "ALTER TABLE",
        "INSERT INTO",
        "UPDATE ",
        "DELETE FROM",
        "MERGE INTO",
    ]:
        assert prohibited_statement not in normalized


def test_daily_analytics_view_uses_the_canonical_gold_source() -> None:
    sql = DAILY_SERVING_SQL_PATH.read_text(encoding="utf-8")

    assert f"CREATE OR REPLACE VIEW {DAILY_VIEW}" in sql
    assert (
        "FROM workspace.devin_market_risk_dev."
        "gold_portfolio_daily_metrics AS metrics"
    ) in sql
    assert " JOIN " not in _executable_sql(sql).upper()


def test_daily_analytics_view_preserves_consumer_context() -> None:
    sql = DAILY_SERVING_SQL_PATH.read_text(encoding="utf-8")

    expected_select_expressions = {
        "metrics.portfolio_id",
        "metrics.valuation_date",
        "metrics.base_currency",
        "metrics.long_market_value AS long_exposure_usd",
        "metrics.short_market_value AS short_exposure_magnitude_usd",
        "metrics.closing_nav AS closing_nav_usd",
        "metrics.daily_pnl AS daily_pnl_usd",
        "metrics.daily_return AS daily_return_ratio",
        "metrics.analytics_run_id",
        "metrics.calculation_version",
        "metrics.calculated_at_utc",
    }

    for expression in expected_select_expressions:
        assert expression in sql

    for hidden_audit_field in [
        "input_market_value_partition_sha256",
        "input_cash_balance_record_sha256",
        "input_portfolio_record_sha256",
        "input_prior_metric_record_sha256",
        "record_hash",
    ]:
        assert hidden_audit_field not in sql


def test_position_exposure_view_preserves_signed_and_absolute_values() -> None:
    sql = DAILY_SERVING_SQL_PATH.read_text(encoding="utf-8")

    assert f"CREATE OR REPLACE VIEW {POSITION_VIEW}" in sql
    assert (
        "FROM workspace.devin_market_risk_dev."
        "gold_position_market_values AS positions"
    ) in sql
    assert "positions.signed_market_value AS signed_market_value_usd" in sql
    assert "positions.absolute_market_value AS absolute_market_value_usd" in sql
    assert "positions.close_price AS close_price_usd_per_share" in sql


def test_risk_run_selection_is_complete_and_deterministic() -> None:
    sql = RISK_SERVING_SQL_PATH.read_text(encoding="utf-8")
    normalized = " ".join(_executable_sql(sql).upper().split())

    assert f"CREATE OR REPLACE VIEW {RISK_RUN_VIEW}" in sql
    assert (
        "FROM workspace.devin_market_risk_dev.gold_risk_runs AS runs"
    ) in sql
    assert "RUNS.STATUS = 'SUCCEEDED'" in normalized
    assert "RUNS.PUBLISHED = TRUE" in normalized
    assert "RUNS.CALCULATED_HISTORICAL_PNL_SCENARIO_COUNT = 251" in normalized
    assert "RUNS.CALCULATED_VAR_MEASURE_COUNT = 2" in normalized
    assert "RUNS.CALCULATED_STRESS_RESULT_COUNT = 3" in normalized
    assert "ROW_NUMBER() OVER (" in normalized
    assert "PARTITION BY ELIGIBLE.PORTFOLIO_ID, ELIGIBLE.AS_OF_DATE" in normalized
    assert "ELIGIBLE.PUBLISHED_AT_UTC DESC" in normalized
    assert "ELIGIBLE.COMPLETED_AT_UTC DESC" in normalized
    assert "ELIGIBLE.ATTEMPT_NUMBER DESC" in normalized
    assert "ELIGIBLE.RISK_RUN_ID DESC" in normalized
    assert "WHERE RANKED.PUBLISHED_RUN_RANK = 1" in normalized
    assert "MAX(RISK_RUN_ID)" not in normalized


def test_risk_consumer_views_share_the_controlled_run_selection() -> None:
    sql = RISK_SERVING_SQL_PATH.read_text(encoding="utf-8")

    assert f"CREATE OR REPLACE VIEW {VAR_VIEW}" in sql
    assert f"CREATE OR REPLACE VIEW {STRESS_VIEW}" in sql
    assert sql.count(
        "FROM workspace.devin_market_risk_dev."
        "vw_latest_published_risk_runs AS selected"
    ) == 2
    assert (
        "INNER JOIN workspace.devin_market_risk_dev."
        "gold_var_measures AS measures"
    ) in sql
    assert (
        "INNER JOIN workspace.devin_market_risk_dev."
        "gold_stress_results AS stress"
    ) in sql
    assert "measures.var_amount AS var_amount_usd" in sql
    assert "stress.stress_pnl AS stress_pnl_usd" in sql
    assert "stress.stressed_nav AS stressed_nav_usd" in sql


def test_serving_sql_has_no_governed_data_mutation() -> None:
    for sql_path in [DAILY_SERVING_SQL_PATH, RISK_SERVING_SQL_PATH]:
        _assert_no_governed_data_mutation(
            sql_path.read_text(encoding="utf-8")
        )


def test_risk_serving_sql_uses_aliases_in_their_correct_scope() -> None:
    sql = RISK_SERVING_SQL_PATH.read_text(encoding="utf-8")

    risk_run_sql, remaining_sql = sql.split(
        f"CREATE OR REPLACE VIEW {VAR_VIEW}",
        maxsplit=1,
    )
    var_sql, _ = remaining_sql.split(
        f"CREATE OR REPLACE VIEW {STRESS_VIEW}",
        maxsplit=1,
    )

    eligible_cte_sql, _ = risk_run_sql.split(
        "ranked_eligible_runs AS (",
        maxsplit=1,
    )

    assert "FROM workspace.devin_market_risk_dev.gold_risk_runs AS runs" in (
        eligible_cte_sql
    )
    assert "runs.risk_run_id" in eligible_cte_sql
    assert "ranked." not in eligible_cte_sql

    assert "FROM ranked_eligible_runs AS ranked" in risk_run_sql
    assert "ranked.risk_run_id" in risk_run_sql
    assert "WHERE ranked.published_run_rank = 1" in risk_run_sql
    assert "selected." not in risk_run_sql

    assert (
        "INNER JOIN workspace.devin_market_risk_dev.gold_var_measures "
        "AS measures"
    ) in var_sql
    assert "measures.risk_run_id" in var_sql
    assert "measures.var_amount AS var_amount_usd" in var_sql
    assert "var." not in var_sql


def test_serving_qa_pack_is_read_only_and_covers_all_serving_views() -> None:
    sql = QA_SQL_PATH.read_text(encoding="utf-8")

    _assert_no_governed_data_mutation(sql)

    for view_name in [
        "vw_portfolio_daily_analytics",
        "vw_position_exposure_detail",
        "vw_latest_published_risk_runs",
        "vw_latest_published_var",
        "vw_latest_published_stress_results",
    ]:
        assert view_name in sql

    assert "COUNT(DISTINCT portfolio_id, valuation_date)" in sql
    assert "COUNT(DISTINCT portfolio_id, instrument_id, valuation_date)" in sql
    assert "COUNT(DISTINCT portfolio_id, as_of_date)" in sql
