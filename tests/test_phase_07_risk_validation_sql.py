from __future__ import annotations

import re
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
SQL_PATH = PROJECT_ROOT / "sql" / "gold" / "phase_07_validate_risk_results.sql"


def _read_sql() -> str:
    return SQL_PATH.read_text(encoding="utf-8")


def _executable_statements(sql: str) -> list[str]:
    without_comments = re.sub(r"--[^\n]*", "", sql)
    without_strings = re.sub(r"'(?:''|[^'])*'", "''", without_comments)
    return [
        statement.strip()
        for statement in without_strings.split(";")
        if statement.strip()
    ]


def _normalized_sql(sql: str) -> str:
    return " ".join(sql.split())


def test_risk_validation_sql_is_select_or_cte_only() -> None:
    executable_statements = _executable_statements(_read_sql())

    assert len(executable_statements) == 3
    assert all(
        re.match(r"^(WITH|SELECT)\b", statement)
        for statement in executable_statements
    )

    normalized = " ".join(executable_statements).upper()
    for forbidden_keyword in [
        "CREATE",
        "INSERT",
        "UPDATE",
        "DELETE",
        "MERGE",
        "ALTER",
        "DROP",
        "TRUNCATE",
        "OPTIMIZE",
        "VACUUM",
        "CALL",
        "SET",
        "BEGIN",
        "COMMIT",
        "ROLLBACK",
    ]:
        assert re.search(rf"\b{forbidden_keyword}\b", normalized) is None


def test_risk_validation_sql_reads_the_governed_gold_and_silver_sources() -> None:
    sql = _read_sql()

    expected_sources = {
        "workspace.devin_market_risk_dev.gold_risk_runs",
        "workspace.devin_market_risk_dev.gold_historical_pnl_scenarios",
        "workspace.devin_market_risk_dev.gold_var_measures",
        "workspace.devin_market_risk_dev.gold_stress_results",
        "workspace.devin_market_risk_dev.silver_positions",
        "workspace.devin_market_risk_dev.silver_cash_balances",
        "workspace.devin_market_risk_dev.silver_daily_prices",
        "workspace.devin_market_risk_dev.silver_stress_scenarios",
        "workspace.devin_market_risk_dev.silver_stress_scenario_shocks",
    }
    assert expected_sources <= set(
        re.findall(r"workspace\.devin_market_risk_dev\.[a-z_]+", sql)
    )
    assert "SELECT DISTINCT" not in _normalized_sql(sql).upper()


def test_every_result_set_scopes_to_exact_succeeded_and_published_runs() -> None:
    normalized = _normalized_sql(_read_sql())
    exact_predicate = "WHERE status = 'SUCCEEDED' AND published = true"

    assert normalized.count(exact_predicate) == 3
    assert "SUCCEEDED_WITH_WARNINGS" not in normalized


def test_var_contributions_enforce_approved_confidence_rank_mapping() -> None:
    normalized = _normalized_sql(_read_sql())

    assert (
        "SELECT CAST(0.95 AS DECIMAL(3,2)) AS confidence_level, "
        "239 AS quantile_rank" in normalized
    )
    assert (
        "SELECT CAST(0.99 AS DECIMAL(3,2)) AS confidence_level, "
        "249 AS quantile_rank" in normalized
    )
    assert "measures.quantile_rank = expected.quantile_rank" in normalized
    assert "losses.deterministic_loss_rank = expected.quantile_rank" in normalized
    assert "AS var_measure_mapping_valid" in normalized
    assert "AS approved_confidence_rank_mapping" in normalized
    assert re.search(
        r"selected\.var_measure_mapping_valid AND "
        r"selected\.approved_confidence_rank_mapping.*?AS reconciles",
        normalized,
    )


def test_var_contributions_use_signed_formulas_and_deterministic_ranking() -> None:
    normalized = _normalized_sql(_read_sql())

    assert re.search(
        r"CAST\( position\.signed_quantity \* price\.close_price AS "
        r"DECIMAL\(38,16\) \) AS signed_market_value",
        normalized,
    )
    assert re.search(
        r"CAST\( CAST\(scenario_price\.adjusted_close_price AS "
        r"DECIMAL\(38,16\)\) / CAST\(prior_price\.adjusted_close_price AS "
        r"DECIMAL\(38,16\)\) - CAST\(1 AS DECIMAL\(38,16\)\) AS "
        r"DECIMAL\(38,16\) \) AS historical_return",
        normalized,
    )
    assert re.search(
        r"CAST\( exposure\.signed_market_value \* returns\.historical_return "
        r"AS DECIMAL\(38,16\) \) AS component_pnl",
        normalized,
    )
    assert re.search(
        r"CAST\( -\(exposure\.signed_market_value \* "
        r"returns\.historical_return\) AS DECIMAL\(38,16\) \) "
        r"AS component_loss",
        normalized,
    )
    assert re.search(
        r"ROW_NUMBER\(\) OVER \( PARTITION BY historical\.risk_run_id "
        r"ORDER BY historical\.loss_amount ASC, historical\.scenario_date ASC "
        r"\) AS deterministic_loss_rank",
        normalized,
    )


def test_reconciliations_require_component_and_shock_cardinality() -> None:
    normalized = _normalized_sql(_read_sql())

    assert (
        "COUNT(*) = 15 AND COUNT(DISTINCT instrument_id) = 15 "
        "AS component_cardinality_valid" in normalized
    )
    assert (
        "COUNT(*) = 15 AND COUNT(DISTINCT component.instrument_id) = 15 "
        "AS stress_cardinality_valid" in normalized
    )
    assert re.search(
        r"COALESCE\(reconciliation\.component_cardinality_valid, false\).*?"
        r"component_loss_total = selected\.var_amount.*?AS reconciles",
        normalized,
    )
    assert re.search(
        r"COALESCE\(reconciliation\.stress_cardinality_valid, false\).*?"
        r"result\.stress_pnl = reconciliation\.recalculated_stress_pnl.*?"
        r"AS reconciles",
        normalized,
    )


def test_risk_validation_sql_has_no_misleading_contribution_language() -> None:
    normalized = _read_sql().lower()

    assert "var percentage" not in normalized
    assert "average attribution" not in normalized
    assert "average contribution" not in normalized
