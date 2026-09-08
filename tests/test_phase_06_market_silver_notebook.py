import ast
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
NOTEBOOK_PATH = (
    PROJECT_ROOT / "notebooks" / "silver" / "phase_06_process_market_data.py"
)


def _source() -> str:
    return NOTEBOOK_PATH.read_text(encoding="utf-8")


def _tree() -> ast.Module:
    return ast.parse(_source())


def _literal_assignment(name: str):
    for node in _tree().body:
        if isinstance(node, ast.Assign):
            for target in node.targets:
                if isinstance(target, ast.Name) and target.id == name:
                    return ast.literal_eval(node.value)
    raise AssertionError(f"Missing literal assignment: {name}")


def test_market_silver_processor_is_valid_notebook_source() -> None:
    source = _source()
    assert source.startswith("# Databricks notebook source")
    assert source.count("# COMMAND ----------") >= 5
    _tree()


def test_market_silver_processor_supports_exact_datasets() -> None:
    specs = _literal_assignment("DATASET_SPECS")
    assert set(specs) == {"DAILY_PRICES", "CORPORATE_ACTIONS"}
    assert specs["DAILY_PRICES"]["expected_source_count"] == 3780
    assert "EXPECTED_PRICE_DATE_COUNT = 252" in _source()
    assert "def build_expected_price_dates()" in _source()
    assert specs["CORPORATE_ACTIONS"]["expected_source_count"] == 2
    assert specs["DAILY_PRICES"]["key_columns"] == [
        "instrument_id",
        "price_date",
        "source_id",
    ]
    assert specs["CORPORATE_ACTIONS"]["key_columns"] == [
        "instrument_id",
        "effective_date",
        "action_type",
        "source_id",
    ]


def test_market_silver_processor_preserves_price_semantics() -> None:
    source = _source()
    price_spec = _literal_assignment("DATASET_SPECS")["DAILY_PRICES"]
    assert "close_price" in price_spec["output_columns"]
    assert "adjusted_close_price" in price_spec["output_columns"]
    assert "close_price" in price_spec["hash_fields"]
    assert "adjusted_close_price" in price_spec["hash_fields"]
    assert '.cast("decimal(20,8)").alias("close_price")' in source
    assert '.alias("adjusted_close_price")' in source
    assert "prohibit_silent_substitution" not in source


def test_market_silver_processor_enforces_contract_rules() -> None:
    source = _source()
    required_rules = {
        "PRICE_REQUIRED_FIELDS",
        "PRICE_TYPES_CASTABLE",
        "PRICE_SOURCE_PROVENANCE_VALID",
        "PRICE_INSTRUMENT_MAPPING",
        "PRICE_POSITIVE_VALUES",
        "PRICE_OHLC_CONSISTENCY",
        "PRICE_VOLUME_MISSING_OR_ZERO",
        "PRICE_VOLUME_NEGATIVE",
        "PRICE_VALID_SESSION",
        "PRICE_SUPPORTED_CURRENCY",
        "PRICE_RECORD_HASH_VALID",
        "ACTION_REQUIRED_FIELDS",
        "ACTION_TYPES_CASTABLE",
        "ACTION_SOURCE_PROVENANCE_VALID",
        "ACTION_INSTRUMENT_MAPPING",
        "ACTION_TYPE_ALLOWED",
        "ACTION_DIVIDEND_FIELDS",
        "ACTION_SPLIT_FIELDS",
        "ACTION_EFFECTIVE_SESSION",
        "ACTION_RECORD_HASH_VALID",
    }
    assert all(rule in source for rule in required_rules)
    assert "SILVER_INSTRUMENT_TABLE" in source
    assert "SILVER_CALENDAR_TABLE" in source
    assert "reference_is_trading_day" in source
    assert "PROJECT_GIT_FIXTURE" in source
    assert "PRICE_EXPECTED_DATE_GRID" in source


def test_market_silver_processor_assigns_one_final_outcome() -> None:
    source = _source()
    assert 'F.lit("REJECTED")' in source
    assert 'F.lit("QUARANTINED")' in source
    assert 'F.lit("DEDUPLICATED")' in source
    assert 'F.lit("UNCHANGED")' in source
    assert 'F.lit("ACCEPTED_CORRECTION")' in source
    assert 'F.lit("ACCEPTED_NEW")' in source
    assert "lowest_source_row_number" not in source
    assert 'F.col("source_row_number").asc()' in source
    assert "processing outcome reconciliation" in source


def test_market_silver_processor_gates_publication() -> None:
    source = _source()
    publish_gate = source.index("publish_allowed = (")
    publication = source.index("if published:")
    assert publish_gate < publication
    assert "rejected_count == 0" in source
    assert "quarantined_count == 0" in source
    assert "and not failed_rule_ids" in source
    assert 'run_status = "FAILED"' in source
    assert ".whenMatchedUpdateAll()" in source
    assert ".whenNotMatchedInsertAll()" in source


def test_market_silver_processor_freezes_audit_evidence() -> None:
    source = _source()
    violation_freeze = source.index(
        "complete_rule_violations = freeze_small_dataframe("
    )
    outcome_freeze = source.index("outcomes = freeze_small_dataframe(")
    publication = source.index("if published:")
    assert violation_freeze < publication
    assert outcome_freeze < publication
    assert "frame.limit(max_rows + 1).collect()" in source
    assert "spark.createDataFrame(rows, schema=frame.schema)" in source
    assert 'label="market record violations"' in source
    assert 'label="market final outcomes"' in source


def test_market_silver_processor_preserves_audit_evidence() -> None:
    source = _source()
    required_tables = {
        "silver_processing_runs",
        "silver_source_record_outcomes",
        "silver_data_quality_violations",
    }
    assert all(table in source for table in required_tables)
    assert source.count("insert_only_audit_records(") >= 4
    assert "persisted processing-run audit count" in source
    assert "persisted source-outcome count" in source
    assert "persisted violation count" in source


def test_market_silver_processor_is_serverless_safe() -> None:
    source = _source().lower()
    forbidden = (
        ".cache()",
        ".persist(",
        ".unpersist(",
        "cache table",
        "persist table",
        'mode("overwrite")',
        "delete from",
        "drop table",
        "truncate table",
    )
    for token in forbidden:
        assert token not in source
