from __future__ import annotations

import ast
from datetime import date, timedelta
from decimal import Decimal
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
NOTEBOOK_PATH = (
    PROJECT_ROOT / "notebooks" / "gold" / "phase_07_calculate_risk_measures.py"
)


def _read_notebook() -> str:
    return NOTEBOOK_PATH.read_text(encoding="utf-8")


def _load_pure_calendar_helpers() -> dict[str, object]:
    """Execute only the notebook's date constants and pure calendar helpers."""
    module = ast.parse(_read_notebook())
    required_assignments = {
        "EXPECTED_HISTORY_START_DATE",
        "EXPECTED_AS_OF_DATE",
        "EXPECTED_SESSION_COUNT",
        "US_EQUITIES_2016_HOLIDAYS",
    }
    required_functions = {
        "build_expected_price_dates",
        "build_expected_price_date_pairs",
    }
    selected_nodes = []
    for node in module.body:
        if (
            isinstance(node, ast.Assign)
            and any(
                isinstance(target, ast.Name) and target.id in required_assignments
                for target in node.targets
            )
        ) or (isinstance(node, ast.FunctionDef) and node.name in required_functions):
            selected_nodes.append(node)
    namespace: dict[str, object] = {"date": date, "timedelta": timedelta}
    exec(
        compile(
            ast.Module(body=selected_nodes, type_ignores=[]),
            str(NOTEBOOK_PATH),
            "exec",
        ),
        namespace,
        namespace,
    )
    return namespace


def _load_failure_wrapper() -> object:
    """Execute the notebook's pure failure wrapper without PySpark."""
    module = ast.parse(_read_notebook())
    wrapper = next(
        node
        for node in module.body
        if isinstance(node, ast.FunctionDef)
        and node.name == "execute_with_failed_audit"
    )
    namespace: dict[str, object] = {}
    exec(
        compile(
            ast.Module(body=[wrapper], type_ignores=[]),
            str(NOTEBOOK_PATH),
            "exec",
        ),
        namespace,
        namespace,
    )
    return namespace["execute_with_failed_audit"]


def _discrete_var(
    losses: list[tuple[Decimal, str]], confidence: Decimal
) -> tuple[int, Decimal]:
    """Use ascending loss/date order and a one-based discrete rank."""
    ordered = sorted(losses, key=lambda value: (value[0], value[1]))
    rank = int((confidence * len(ordered)).to_integral_value(rounding="ROUND_CEILING"))
    return rank, ordered[rank - 1][0]


def test_risk_notebook_is_valid_databricks_source() -> None:
    source = _read_notebook()

    assert source.startswith("# Databricks notebook source\n")
    assert source.count("# COMMAND ----------") == 3
    ast.parse(source)


def test_risk_notebook_has_required_risk_parameters() -> None:
    source = _read_notebook()

    for parameter in ["portfolio_id", "as_of_date", "code_version", "trigger_type"]:
        assert f'"{parameter}"' in source

    assert source.count("dbutils.widgets.text(") == 4
    assert "EXPECTED_AS_OF_DATE = date(2016, 12, 30)" in source
    assert (
        'require_equal("approved risk as_of_date", as_of_date, '
        "EXPECTED_AS_OF_DATE)" in source
    )
    assert "PORTFOLIO_ID_PATTERN" in source
    assert "CODE_VERSION_PATTERN" in source


def test_risk_notebook_reads_only_the_approved_trusted_inputs() -> None:
    source = _read_notebook()

    for table_name in [
        "silver_portfolios",
        "silver_instruments",
        "silver_daily_prices",
        "silver_positions",
        "silver_cash_balances",
        "silver_stress_scenarios",
        "silver_stress_scenario_shocks",
    ]:
        assert table_name in source

    assert "bronze_" not in source.lower()
    assert "gold_position_market_values" not in source
    assert "gold_portfolio_daily_metrics" not in source


def test_risk_notebook_implements_approved_static_exposure_and_history_policy() -> None:
    source = _read_notebook()

    for required_text in [
        "STATIC_EXPOSURE_SOURCE_DATE = date(2016, 1, 7)",
        "BUY_AND_HOLD_NO_REBALANCE_NO_LATER_CORPORATE_ACTIONS",
        "EXPECTED_HISTORY_START_DATE = date(2016, 1, 4)",
        "EXPECTED_SESSION_COUNT = 252",
        "EXPECTED_RETURN_COUNT = 251",
        "MINIMUM_RETURN_COUNT = 250",
        "US_EQUITIES_2016_HOLIDAYS",
        "def build_expected_price_dates()",
        "EXPECTED_PRICE_DATES = build_expected_price_dates()",
        "price_dates != list(EXPECTED_PRICE_DATES)",
        "EXPECTED_PRICE_DATE_PAIRS",
        "adjusted_close_price",
        "prior_adjusted_close_price",
        "signed_market_value",
        "static_nav = static_security_value + static_cash_balance",
    ]:
        assert required_text in source

    assert "No later rebalances or corporate actions are consulted" in source


def test_notebook_calendar_helpers_build_the_exact_approved_spine_and_pairs() -> None:
    helpers = _load_pure_calendar_helpers()
    dates = helpers["build_expected_price_dates"]()
    pairs = helpers["build_expected_price_date_pairs"](dates)
    holidays = helpers["US_EQUITIES_2016_HOLIDAYS"]

    assert len(dates) == 252
    assert len(pairs) == 251
    assert pairs[0] == (date(2016, 1, 4), date(2016, 1, 5))
    assert pairs[-1] == (date(2016, 12, 29), date(2016, 12, 30))
    assert all(trading_date.weekday() < 5 for trading_date in dates)
    assert all(trading_date not in holidays for trading_date in dates)


def test_risk_notebook_preserves_signed_loss_and_discrete_var_policy() -> None:
    source = _read_notebook()

    assert (
        '"loss_amount", (-F.col("simulated_portfolio_pnl")).cast('
        '"decimal(38,16)")' in source
    )
    assert 'F.col("loss_amount").asc(), F.col("scenario_date").asc()' in source
    assert 'Decimal("0.95"): 239' in source
    assert 'Decimal("0.99"): 249' in source
    assert 'F.col("loss_amount") != -F.col("simulated_portfolio_pnl")' in source
    assert "max(" not in source.lower()


def test_loss_sign_and_discrete_ranks_are_independently_exact() -> None:
    simulated_gain = Decimal("12.5000000000000000")
    simulated_loss = Decimal("-12.5000000000000000")

    assert -simulated_gain == Decimal("-12.5000000000000000")
    assert -simulated_loss == Decimal("12.5000000000000000")

    losses = [(Decimal(index), f"2016-01-{index:03d}") for index in range(1, 252)]
    losses[100] = (Decimal("100"), "2016-01-001")
    losses[101] = (Decimal("100"), "2016-01-000")
    assert _discrete_var(losses, Decimal("0.95"))[0] == 239
    assert _discrete_var(losses, Decimal("0.99"))[0] == 249
    assert _discrete_var(losses, Decimal("0.95"))[1] == Decimal("239")


def test_failure_wrapper_audits_once_before_propagating_the_original_error() -> None:
    wrapper = _load_failure_wrapper()
    audit_errors: list[Exception] = []

    def failing_callback() -> None:
        raise RuntimeError("deliberate failure")

    def failed_audit_callback(error: Exception) -> None:
        audit_errors.append(error)

    try:
        wrapper(failing_callback, failed_audit_callback)
    except RuntimeError as error:
        assert str(error) == "deliberate failure"
    else:
        raise AssertionError("failure wrapper did not propagate the error")

    assert len(audit_errors) == 1
    assert str(audit_errors[0]) == "deliberate failure"


def test_risk_notebook_validates_complete_bundle_before_any_write() -> None:
    source = _read_notebook()

    assert source.index("failed_rule_ids = sorted") < source.index(
        "if publish_allowed:"
    )
    assert source.index("if publish_allowed:") < source.index(
        "append_immutable(historical_candidate"
    )
    for rule_id in [
        "RISK_RUN_PRICE_HISTORY_COVERAGE_COMPLETE",
        "HISTORICAL_PNL_SCENARIO_COUNT_COMPLETE",
        "HISTORICAL_PNL_SCENARIO_FORMULA_RECONCILES",
        "VAR_MEASURE_DISCRETE_QUANTILE_RECONCILES",
        "STRESS_RESULT_SHOCK_COVERAGE_VALID",
        "STRESS_RESULT_FORMULA_RECONCILES",
        "VAR_MEASURE_AMOUNT_RECONCILES",
        "expected_record_hash",
        "stress_reconciliation_failures",
    ]:
        assert rule_id in source


def test_risk_notebook_is_immutable_audited_and_serverless_safe() -> None:
    source = _read_notebook()
    normalized = " ".join(source.upper().split())

    for output_name in [
        "gold_risk_runs",
        "gold_historical_pnl_scenarios",
        "gold_var_measures",
        "gold_stress_results",
    ]:
        assert output_name in source

    assert "risk_run_id = str(uuid4())" in source
    assert "attempt_number" in source
    assert "reprocess_of_risk_run_id" in source
    assert 'mode("append")' in source
    assert "persisted risk audit count" in source
    assert "input_manifest_sha256" in source
    assert "input_static_exposure_sha256" in source
    assert "input_price_history_sha256" in source
    assert "input_stress_scenario_set_sha256" in source
    assert "input_stress_shock_set_sha256" in source
    assert "hypothetical assumptions, never\nforecasts" in source
    assert "LOGICAL_SUCCESSFUL_RISK_RUN" in source or ("logical publication" in source)
    assert "RISK_RUN_UNHANDLED_FAILURE" in source
    assert '"published": False' in source
    assert "sys.excepthook" not in source
    assert source.index("def execute_risk_run()") < source.index("previous_runs =")
    assert source.index("previous_runs =") < source.index("portfolio_inputs =")
    assert source.rindex("execute_with_failed_audit(") > source.index(
        "def execute_risk_run()"
    )
    assert source.index("append_immutable(stress_candidate") < source.index(
        "append_immutable(audit_candidate, RISK_RUN_TABLE)"
    )

    for prohibited in [
        ".CACHE(",
        ".PERSIST(",
        "CACHE TABLE",
        "PERSIST TABLE",
        "UNCACHE TABLE",
        ".MERGE(",
        ".DELETE(",
        ".UPDATE(",
        ".OVERWRITE",
    ]:
        assert prohibited not in normalized
