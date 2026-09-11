from __future__ import annotations

from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
NOTEBOOK_PATH = (
    PROJECT_ROOT / "notebooks" / "qa"
    / "phase_11_validate_serving_outputs.py"
)


def test_serving_validation_notebook_is_read_only_and_parameterized() -> None:
    source = NOTEBOOK_PATH.read_text(encoding="utf-8")
    normalized = source.upper()

    assert "from pyspark.dbutils import DBUtils" in source
    assert 'dbutils.widgets.text("portfolio_id"' in source
    assert 'dbutils.widgets.text("valuation_date"' in source
    assert 'dbutils.widgets.text("risk_as_of_date"' in source

    assert "vw_portfolio_daily_analytics" in source
    assert "vw_position_exposure_detail" in source
    assert "vw_latest_published_risk_runs" in source
    assert "vw_latest_published_var" in source
    assert "vw_latest_published_stress_results" in source

    assert "gold_portfolio_daily_metrics" in source
    assert "gold_position_market_values" in source

    assert "EXPECTED_INSTRUMENT_COUNT = 15" in source
    assert "EXPECTED_VAR_MEASURE_COUNT = 2" in source
    assert "EXPECTED_STRESS_RESULT_COUNT = 3" in source

    assert "served daily analytics row count" in source
    assert "served distinct position instrument count" in source
    assert "selected published risk run count" in source
    assert "served distinct VaR confidence count" in source
    assert "served distinct stress scenario count" in source
    assert 'print("serving_validation=PASS")' in source

    for prohibited in [
        ".WRITE",
        ".SAVEASTABLE",
        ".INSERTINTO",
        ".MERGE(",
        ".DELETE(",
        ".UPDATE(",
        ".OVERWRITE",
        "CREATE TABLE",
        "DROP TABLE",
    ]:
        assert prohibited not in normalized
