from __future__ import annotations

import ast
from pathlib import Path
from typing import Any

PROJECT_ROOT = Path(__file__).resolve().parents[1]
NOTEBOOK_PATH = (
    PROJECT_ROOT
    / "notebooks"
    / "silver"
    / "phase_06_process_reference_data.py"
)


def _read_notebook() -> str:
    return NOTEBOOK_PATH.read_text(encoding="utf-8")


def _literal_assignment(name: str) -> Any:
    tree = ast.parse(_read_notebook())

    for node in tree.body:
        if not isinstance(node, ast.Assign):
            continue
        if any(
            isinstance(target, ast.Name)
            and target.id == name
            for target in node.targets
        ):
            return ast.literal_eval(node.value)

    raise AssertionError(f"Missing assignment: {name}")


def test_reference_silver_processor_is_valid_notebook_source() -> None:
    source = _read_notebook()

    assert source.startswith("# Databricks notebook source\n")
    assert source.count("# COMMAND ----------") == 5
    ast.parse(source)


def test_reference_silver_processor_supports_exact_datasets() -> None:
    specifications = _literal_assignment("DATASET_SPECS")

    assert set(specifications) == {
        "INSTRUMENTS",
        "TARGET_ALLOCATIONS",
        "STRESS_SCENARIOS",
        "STRESS_SCENARIO_SHOCKS",
    }
    assert specifications["INSTRUMENTS"]["contract_version"] == "1.1.0"
    assert specifications["TARGET_ALLOCATIONS"]["contract_version"] == "1.1.0"
    assert specifications["STRESS_SCENARIOS"]["contract_version"] == "1.0.0"
    assert (
        specifications["STRESS_SCENARIO_SHOCKS"]["contract_version"]
        == "1.0.0"
    )
    assert specifications["INSTRUMENTS"]["expected_source_count"] == 15
    assert (
        specifications["TARGET_ALLOCATIONS"]["expected_source_count"]
        == 30
    )
    assert specifications["INSTRUMENTS"]["key_columns"] == [
        "instrument_id"
    ]
    assert specifications["TARGET_ALLOCATIONS"]["key_columns"] == [
        "portfolio_id",
        "instrument_id",
        "effective_from",
    ]
    assert specifications["STRESS_SCENARIOS"]["expected_source_count"] == 3
    assert specifications["STRESS_SCENARIOS"]["key_columns"] == [
        "scenario_id"
    ]
    assert (
        specifications["STRESS_SCENARIO_SHOCKS"]["expected_source_count"]
        == 45
    )
    assert specifications["STRESS_SCENARIO_SHOCKS"]["key_columns"] == [
        "scenario_id",
        "instrument_id",
    ]


def test_reference_silver_processor_pins_audit_versions() -> None:
    source = _read_notebook()

    assert 'PROCESSING_RUN_CONTRACT_VERSION = "2.0.0"' in source
    assert 'OUTCOME_CONTRACT_VERSION = "1.0.0"' in source
    assert 'VIOLATION_CONTRACT_VERSION = "2.0.0"' in source
    assert 'dbutils.widgets.text("source_batch_id"' in source
    assert 'dbutils.widgets.text("code_version"' in source
    assert '"SUCCEEDED", "SUCCEEDED_WITH_WARNINGS"' in source
    assert "require_code_version(" in source
    assert "reprocess_of_processing_run_id" in source
    assert "attempt_number" in source


def test_reference_silver_processor_enforces_contract_rules() -> None:
    source = _read_notebook()
    required_rules = {
        "INSTRUMENT_REQUIRED_FIELDS",
        "INSTRUMENT_TYPES_CASTABLE",
        "INSTRUMENT_ALLOWED_VALUES",
        "INSTRUMENT_ID_FORMAT",
        "INSTRUMENT_CONFIG_VERSION_VALID",
        "INSTRUMENT_CLASSIFICATION_REQUIRED",
        "INSTRUMENT_ACTIVE_DATE_RANGE",
        "INSTRUMENT_RECORD_HASH_VALID",
        "INSTRUMENT_ACTIVE_COUNT",
        "INSTRUMENT_ID_UNIQUE",
        "INSTRUMENT_PROVIDER_SYMBOL_UNIQUE",
        "ALLOCATION_REQUIRED_FIELDS",
        "ALLOCATION_TYPES_CASTABLE",
        "ALLOCATION_VERSION_VALID",
        "ALLOCATION_FOREIGN_KEYS_VALID",
        "ALLOCATION_WEIGHT_NONZERO",
        "ALLOCATION_EFFECTIVE_DATE_RANGE",
        "ALLOCATION_RECORD_HASH_VALID",
        "ALLOCATION_ACTIVE_COVERAGE",
        "ALLOCATION_BUSINESS_KEY_UNIQUE",
        "ALLOCATION_EFFECTIVE_RANGES_NONOVERLAPPING",
        "ALLOCATION_LONG_ONLY_TOTALS",
        "ALLOCATION_LONG_SHORT_TOTALS",
        "STRESS_SCENARIO_REQUIRED_FIELDS",
        "STRESS_SCENARIO_TYPES_CASTABLE",
        "STRESS_SCENARIO_TYPE_VALID",
        "STRESS_SCENARIO_LANGUAGE_HYPOTHETICAL",
        "STRESS_SCENARIO_VERSION_VALID",
        "STRESS_SCENARIO_EFFECTIVE_RANGE",
        "STRESS_SCENARIO_RECORD_HASH_VALID",
        "STRESS_SCENARIO_ACTIVE_SET",
        "STRESS_SCENARIO_ID_UNIQUE",
        "STRESS_SHOCK_REQUIRED_FIELDS",
        "STRESS_SHOCK_TYPES_CASTABLE",
        "STRESS_SHOCK_FOREIGN_KEYS_VALID",
        "STRESS_SHOCK_VERSION_MATCH",
        "STRESS_SHOCK_MINIMUM_VALID",
        "STRESS_SHOCK_RATIONALE_REQUIRED",
        "STRESS_BROAD_MARKET_BASELINE",
        "STRESS_SHOCK_RECORD_HASH_VALID",
        "STRESS_SHOCK_BUSINESS_KEY_UNIQUE",
        "STRESS_SHOCK_ACTIVE_COVERAGE",
        "STRESS_SHOCK_CANONICAL_SCENARIOS_AVAILABLE",
    }

    assert all(rule_id in source for rule_id in required_rules)
    assert "RULE_PREFIXES" in source
    for generated_rule_suffix in {
        "_SAME_BATCH_IDENTICAL_DUPLICATE",
        "_SAME_BATCH_CONFLICT",
        "_INVALID_CORRECTION",
    }:
        assert f'f"{{prefix}}{generated_rule_suffix}"' in source
    assert source.count("TRY_CAST(") >= 11
    assert "canonical_hash_expression(" in source
    assert "calculate_ordered_set_sha256(" in source
    assert "def typed_stress_scenario_candidates(" in source
    assert "def typed_stress_shock_candidates(" in source
    assert "def stress_scenario_dataset_failures(" in source
    assert "def stress_shock_dataset_failures(" in source
    assert (
        source.index("def typed_stress_scenario_candidates(")
        < source.index("def typed_stress_shock_candidates(")
    )
    assert 'DATASET_SPECS["STRESS_SCENARIOS"]["silver_table"]' in source
    assert 'F.col("is_active") == F.lit(True)' in source


def test_reference_silver_processor_assigns_one_final_outcome() -> None:
    source = _read_notebook()

    for outcome in {
        "ACCEPTED_NEW",
        "ACCEPTED_CORRECTION",
        "UNCHANGED",
        "DEDUPLICATED",
        "QUARANTINED",
        "REJECTED",
    }:
        assert f'"{outcome}"' in source

    assert '"final outcome count"' in source
    assert '"distinct final source count"' in source
    assert '"null final outcome count"' in source
    assert '"processing outcome reconciliation"' in source
    assert "deduplicated_to_source_record_id" in source


def test_reference_silver_processor_gates_publication() -> None:
    source = _read_notebook()
    publish_gate = source.index("publish_allowed = (")
    publication = source.index("if published:")

    assert publish_gate < publication
    assert "rejected_count == 0" in source
    assert "quarantined_count == 0" in source
    assert "and not failed_rule_ids" in source
    assert 'run_status = "FAILED"' in source
    assert "canonical_after_count = canonical_before_count" in source
    assert "canonical_after_sha256 = canonical_before_sha256" in source
    assert ".whenMatchedUpdateAll()" in source
    assert ".whenNotMatchedInsertAll()" in source


def test_reference_silver_processor_freezes_prepublication_evidence() -> None:
    source = _read_notebook()
    violation_assignment = source.index(
        "complete_rule_violations = freeze_small_dataframe("
    )
    violation_materialization = source.index(
        "frozen_violation_count = complete_rule_violations.count()"
    )
    outcome_assignment = source.index(
        "outcomes = freeze_small_dataframe("
    )
    outcome_materialization = source.index(
        "frozen_outcome_count = outcomes.count()"
    )
    publication = source.index("if published:")

    assert "def freeze_small_dataframe(" in source
    assert "frame.limit(max_rows + 1).collect()" in source
    assert "spark.createDataFrame(rows, schema=frame.schema)" in source
    assert 'label="record violations"' in source[
        violation_assignment:violation_materialization
    ]
    assert 'label="final outcomes"' in source[
        outcome_assignment:outcome_materialization
    ]
    assert violation_materialization < publication
    assert outcome_materialization < publication
    assert '"frozen final outcome count"' in source
    assert ".cache()" not in source
    assert ".persist(" not in source


def test_reference_silver_processor_preserves_audit_evidence() -> None:
    source = _read_notebook()

    required_tables = {
        "silver_processing_runs",
        "silver_source_record_outcomes",
        "silver_data_quality_violations",
    }
    assert all(table in source for table in required_tables)
    assert source.count("insert_only_audit_records(") >= 4
    assert '"persisted processing-run audit count"' in source
    assert '"persisted source-outcome count"' in source
    assert '"persisted violation count"' in source
    assert ".whenNotMatchedInsertAll()" in source

    normalized = " ".join(source.upper().split())
    prohibited_sql = {
        "DELETE FROM",
        "DROP TABLE",
        "TRUNCATE TABLE",
    }
    assert not {
        statement
        for statement in prohibited_sql
        if statement in normalized
    }
