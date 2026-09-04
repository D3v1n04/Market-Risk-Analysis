from __future__ import annotations

import ast
import re
from pathlib import Path

PORTFOLIO_CONTRACT_VERSION = "1.1.0"
VIOLATION_CONTRACT_VERSION = "1.2.0"
OUTCOME_CONTRACT_VERSION = "1.0.0"
PROCESSING_RUN_CONTRACT_VERSION = "1.0.0"

PROJECT_ROOT = Path(__file__).resolve().parents[1]
NOTEBOOK_PATH = (
    PROJECT_ROOT
    / "notebooks"
    / "silver"
    / "phase_05_process_portfolios.py"
)

BRONZE_PORTFOLIO_TABLE = (
    "workspace.devin_market_risk_dev.bronze_portfolios"
)

SILVER_TABLES = {
    "workspace.devin_market_risk_dev.silver_portfolios",
    "workspace.devin_market_risk_dev.silver_processing_runs",
    (
        "workspace.devin_market_risk_dev."
        "silver_portfolio_record_outcomes"
    ),
    (
        "workspace.devin_market_risk_dev."
        "silver_data_quality_violations"
    ),
}


def _read_notebook() -> str:
    return NOTEBOOK_PATH.read_text(encoding="utf-8")


def _save_as_table_targets(source: str) -> list[str]:
    tree = ast.parse(source)
    targets: list[str] = []

    for node in ast.walk(tree):
        if not isinstance(node, ast.Call):
            continue
        if not isinstance(node.func, ast.Attribute):
            continue
        if node.func.attr != "saveAsTable" or not node.args:
            continue

        target = node.args[0]
        if isinstance(target, ast.Name):
            targets.append(target.id)

    return targets


def test_silver_notebook_reads_bronze_without_writing_it() -> None:
    source = _read_notebook()
    normalized_source = " ".join(source.upper().split())

    declared_table_names = set(_string_constants(source).values())

    assert BRONZE_PORTFOLIO_TABLE in declared_table_names

    for table_name in SILVER_TABLES:
        assert table_name in declared_table_names

    assert re.search(
        r'dbutils\.widgets\.text\(\s*"source_batch_id"',
        source,
    )
    assert re.search(
        r'dbutils\.widgets\.text\(\s*"code_version"',
        source,
    )
    assert "TRY_CAST(" in source.upper()
    assert 'spark.conf.set("spark.sql.session.timeZone", "UTC")' in source

    save_targets = _save_as_table_targets(source)
    assert "BRONZE_PORTFOLIO_TABLE" not in save_targets

    assert (
        f"INSERT INTO {BRONZE_PORTFOLIO_TABLE}".upper()
        not in normalized_source
    )
    assert (
        f"MERGE INTO {BRONZE_PORTFOLIO_TABLE}".upper()
        not in normalized_source
    )
    assert (
        f"UPDATE {BRONZE_PORTFOLIO_TABLE}".upper()
        not in normalized_source
    )
    assert (
        f"DELETE FROM {BRONZE_PORTFOLIO_TABLE}".upper()
        not in normalized_source
    )


def _string_constants(source: str) -> dict[str, str]:
    tree = ast.parse(source)
    constants: dict[str, str] = {}

    for node in tree.body:
        if not isinstance(node, ast.Assign):
            continue
        if len(node.targets) != 1:
            continue

        target = node.targets[0]
        value = node.value

        if not isinstance(target, ast.Name):
            continue
        if not isinstance(value, ast.Constant):
            continue
        if not isinstance(value.value, str):
            continue

        constants[target.id] = value.value

    return constants


def test_silver_notebook_try_casts_all_typed_fields() -> None:
    source = _read_notebook()
    compact_source = re.sub(r"\s+", "", source.upper())

    required_casts = [
        (
            "TRY_CAST(TARGET_INCEPTION_DATEASDATE)"
            "ASTYPED_TARGET_INCEPTION_DATE"
        ),
        (
            "TRY_CAST(NULLIF(TRIM(ACTUAL_INCEPTION_DATE),'')ASDATE)"
            "ASTYPED_ACTUAL_INCEPTION_DATE"
        ),
        (
            "TRY_CAST(INITIAL_NAVASDECIMAL(18,2))"
            "ASTYPED_INITIAL_NAV"
        ),
        (
            "TRY_CAST(TARGET_LONG_RATIOASDECIMAL(12,10))"
            "ASTYPED_TARGET_LONG_RATIO"
        ),
        (
            "TRY_CAST(TARGET_SHORT_RATIOASDECIMAL(12,10))"
            "ASTYPED_TARGET_SHORT_RATIO"
        ),
        (
            "TRY_CAST(TARGET_GROSS_RATIOASDECIMAL(12,10))"
            "ASTYPED_TARGET_GROSS_RATIO"
        ),
        (
            "TRY_CAST(TARGET_NET_RATIOASDECIMAL(12,10))"
            "ASTYPED_TARGET_NET_RATIO"
        ),
        (
            "TRY_CAST(IS_ACTIVEASBOOLEAN)"
            "ASTYPED_IS_ACTIVE"
        ),
    ]

    for required_cast in required_casts:
        assert required_cast in compact_source


def test_silver_notebook_evaluates_structural_rules() -> None:
    source = _read_notebook()
    normalized_source = " ".join(source.upper().split())

    required_rule_ids = {
        "PORTFOLIO_REQUIRED_FIELDS",
        "PORTFOLIO_TYPES_CASTABLE",
        "PORTFOLIO_ACTUAL_INCEPTION_DATE_MISSING",
        "PORTFOLIO_ALLOWED_VALUES",
        "PORTFOLIO_ID_FORMAT",
        "PORTFOLIO_CONFIG_VERSION_VALID",
    }

    for rule_id in required_rule_ids:
        assert rule_id in source

    assert "UNION ALL" in normalized_source
    assert "WARN_AND_ACCEPT" in source
    assert "REJECT" in source
    assert "phase_05_portfolio_rule_violations" in source


def test_silver_notebook_evaluates_business_rules() -> None:
    source = _read_notebook()
    normalized_source = " ".join(source.upper().split())

    required_rule_ids = {
        "PORTFOLIO_INITIAL_NAV_POSITIVE",
        "PORTFOLIO_RATIO_RECONCILIATION",
        "PORTFOLIO_STRATEGY_TARGETS",
        "PORTFOLIO_INCEPTION_ORDER",
        "PORTFOLIO_RECORD_HASH_VALID",
    }

    for rule_id in required_rule_ids:
        assert rule_id in source

    assert "SHA2(" in normalized_source
    assert "computed_record_hash" in source
    assert "0.000001" in source
    assert "phase_05_business_portfolio_candidates" in source
    assert "phase_05_business_rule_violations" in source


def test_silver_notebook_resolves_same_batch_keys_deterministically() -> None:
    source = _read_notebook()
    normalized_source = " ".join(source.upper().split())

    required_rule_ids = {
        "PORTFOLIO_SAME_BATCH_IDENTICAL_DUPLICATE",
        "PORTFOLIO_SAME_BATCH_CONFLICT",
    }

    for rule_id in required_rule_ids:
        assert rule_id in source

    assert "COUNT(DISTINCT RECORD_HASH)" in normalized_source
    assert "GROUP BY PORTFOLIO_ID" in normalized_source

    assert "ROW_NUMBER() OVER (" in normalized_source
    assert (
        "PARTITION BY PORTFOLIO_ID, RECORD_HASH"
        in normalized_source
    )
    assert "ORDER BY SOURCE_ROW_NUMBER" in normalized_source

    assert '"DEDUPLICATED"' in source
    assert '"REJECTED"' in source


def test_silver_notebook_compares_canonical_versions_semantically() -> None:
    source = _read_notebook()
    normalized_source = " ".join(source.upper().split())
    compact_source = re.sub(r"\s+", "", source)

    assert "spark.table(SILVER_PORTFOLIO_TABLE)" in compact_source

    expected_outcomes = {
        "ACCEPTED_NEW",
        "ACCEPTED_CORRECTION",
        "UNCHANGED",
        "REJECTED",
    }

    for outcome in expected_outcomes:
        assert f'"{outcome}"' in source

    expected_version_fields = {
        "candidate_version_major",
        "candidate_version_minor",
        "candidate_version_patch",
        "current_version_major",
        "current_version_minor",
        "current_version_patch",
    }

    for field_name in expected_version_fields:
        assert field_name in source

    assert "SAME_BATCH_OUTCOME IS NULL" in normalized_source
    assert (
        "CANDIDATE_VERSION_MAJOR > CURRENT_VERSION_MAJOR"
        in normalized_source
    )
    assert (
        "CANDIDATE_VERSION_MINOR > CURRENT_VERSION_MINOR"
        in normalized_source
    )
    assert (
        "CANDIDATE_VERSION_PATCH > CURRENT_VERSION_PATCH"
        in normalized_source
    )

    assert "PORTFOLIO_LATER_BATCH_UNCHANGED" in source
    assert "PORTFOLIO_LATER_BATCH_CORRECTION" in source
    assert "PORTFOLIO_INVALID_CORRECTION" in source


def test_silver_notebook_assigns_one_final_outcome_per_record() -> None:
    source = _read_notebook()
    normalized_source = " ".join(source.upper().split())
    compact_source = re.sub(r"\s+", "", source)
    compact_upper_source = re.sub(r"\s+", "", source.upper())

    expected_objects = {
        "complete_rule_violations",
        "violation_summary",
        "final_record_outcomes",
        "final_outcome_count",
        "final_distinct_source_count",
    }

    for object_name in expected_objects:
        assert object_name in source

    assert "GROUP BY SOURCE_RECORD_ID" in normalized_source
    assert "COUNT(*) AS VIOLATION_COUNT" in normalized_source
    assert (
        "SUM(CASEWHENSEVERITY='WARNING'THEN1ELSE0END)"
        "ASWARNING_COUNT"
        in compact_upper_source
    )
    assert (
        "SUM(CASEWHENSEVERITYIN('ERROR','CRITICAL')"
        "THEN1ELSE0END)ASVIOLATION_ERROR_COUNT"
        in compact_upper_source
    )

    rejected_position = normalized_source.index(
        "WHEN COALESCE(VIOLATION_ERROR_COUNT, 0) > 0"
    )
    conflict_position = normalized_source.index(
        "WHEN SAME_BATCH_OUTCOME = 'REJECTED'"
    )
    duplicate_position = normalized_source.index(
        "WHEN SAME_BATCH_OUTCOME = 'DEDUPLICATED'"
    )
    comparison_position = normalized_source.index(
        "ELSE CANONICAL_COMPARISON_OUTCOME"
    )

    assert rejected_position < conflict_position
    assert conflict_position < duplicate_position
    assert duplicate_position < comparison_position

    assert "final_outcome_count!=evaluated_count" in compact_source
    assert (
        "final_distinct_source_count!=evaluated_count"
        in compact_source
    )


def test_silver_notebook_reconciles_processing_run_counts() -> None:
    source = _read_notebook()
    compact_source = re.sub(r"\s+", "", source)

    expected_counts = {
        "accepted_count",
        "quarantined_count",
        "rejected_count",
        "unchanged_count",
        "deduplicated_count",
        "warning_count",
        "record_outcome_total",
    }

    for count_name in expected_counts:
        assert count_name in source

    expected_outcomes = {
        "ACCEPTED_NEW",
        "ACCEPTED_CORRECTION",
        "QUARANTINED",
        "REJECTED",
        "UNCHANGED",
        "DEDUPLICATED",
    }

    for outcome in expected_outcomes:
        assert f'"{outcome}"' in source

    assert (
        'complete_rule_violations.where('
        'F.col("severity")=="WARNING").count()'
        in compact_source
    )

    assert (
        "record_outcome_total=("
        "accepted_count"
        "+quarantined_count"
        "+rejected_count"
        "+unchanged_count"
        "+deduplicated_count"
        ")"
        in compact_source
    )

    assert "ifrecord_outcome_total!=evaluated_count:" in compact_source


def test_silver_notebook_blocks_unsafe_canonical_publication() -> None:
    source = _read_notebook()
    compact_source = re.sub(r"\s+", "", source)

    expected_objects = {
        "prospective_canonical_snapshot",
        "prospective_canonical_count",
        "prospective_active_count",
        "prospective_distinct_portfolio_count",
        "failed_rule_ids",
        "publish_allowed",
        "run_status",
        "published",
        "canonical_before_count",
        "canonical_after_count",
    }

    for object_name in expected_objects:
        assert object_name in source

    assert "PORTFOLIO_ACTIVE_COUNT" in source
    assert "PORTFOLIO_ID_UNIQUE" in source

    assert "ifprospective_active_count!=2:" in compact_source
    assert re.search(
        (
            r"if\(?prospective_distinct_portfolio_count"
            r"!=prospective_canonical_count\)?:"
        ),
        compact_source,
    )

    assert "rejected_count==0" in compact_source
    assert "quarantined_count==0" in compact_source
    assert "notfailed_rule_ids" in compact_source

    assert (
        "published=publish_allowedandaccepted_count>0"
        in compact_source
    )

    assert 'run_status="FAILED"' in compact_source
    assert 'run_status="SUCCEEDED"' in compact_source
    assert (
        'run_status="SUCCEEDED_WITH_WARNINGS"'
        in compact_source
    )

    assert (
        "ifnotpublish_allowed:"
        "canonical_after_count=canonical_before_count"
        in compact_source
    )

def test_silver_notebook_creates_auditable_processing_run_identity() -> None:
    source = _read_notebook()
    compact_source = re.sub(r"\s+", "", source)

    expected_objects = {
        "processing_run_id",
        "processing_run_started_at_utc",
        "trigger_type",
        "previous_processing_run_rows",
        "attempt_number",
        "reprocess_of_processing_run_id",
    }

    for object_name in expected_objects:
        assert object_name in source

    assert "fromdatetimeimportUTC,datetime" in compact_source
    assert "fromuuidimportUUID,uuid4" in compact_source
    assert "processing_run_id=str(uuid4())" in compact_source

    assert re.search(
        (
            r'dbutils\.widgets\.text\('
            r'"trigger_type","MANUAL"(?:,|\))'
        ),
        compact_source,
    )

    assert "spark.table(SILVER_PROCESSING_RUN_TABLE)" in compact_source
    assert '.where(F.col("source_batch_id")==source_batch_id)' in (
        compact_source
    )
    assert '.orderBy(F.col("attempt_number").desc())' in (
        compact_source
    )

    assert "attempt_number=1" in compact_source
    assert "reprocess_of_processing_run_id=None" in compact_source
    assert (
        'previous_processing_run["attempt_number"]+1'
        in compact_source
    )
    assert (
        'previous_processing_run["processing_run_id"]'
        in compact_source
    )

    assert (
        'trigger_typenotin{"MANUAL","SCHEDULED","RECOVERY"}'
        in compact_source
    )


def test_silver_notebook_calculates_deterministic_set_hashes() -> None:
    source = _read_notebook()
    compact_source = re.sub(r"\s+", "", source)

    expected_objects = {
        "calculate_ordered_set_sha256",
        "input_record_set_sha256",
        "canonical_before_sha256",
        "prospective_canonical_sha256",
        "canonical_after_sha256",
    }

    for object_name in expected_objects:
        assert object_name in source

    assert "F.array_sort(" in source
    assert "F.collect_list(" in source
    assert "F.sha2(" in source
    assert ",256" in compact_source

    assert (
        'order_columns=["source_row_number"]'
        in compact_source
    )
    assert re.search(
        (
            r'value_columns=\['
            r'"source_row_number",'
            r'"source_record_sha256",?'
            r"\]"
        ),
        compact_source,
    )

    assert 'order_columns=["portfolio_id"]' in compact_source
    assert (
        'value_columns=["portfolio_id","record_hash"]'
        in compact_source
    )

    assert (
        "ifnotpublish_allowed:"
        "canonical_after_count=canonical_before_count"
        "canonical_after_sha256=canonical_before_sha256"
        in compact_source
    )


def test_silver_notebook_builds_violation_audit_records() -> None:
    source = _read_notebook()
    normalized_source = " ".join(source.upper().split())
    compact_source = re.sub(r"\s+", "", source)

    assert 'VIOLATION_CONTRACT_VERSION = "1.2.0"' in source
    assert (
        "VIOLATION_AUDIT_RECORDS = "
        "COMPLETE_RULE_VIOLATIONS.SELECT("
        in normalized_source
    )

    expected_fields = {
        "violation_id",
        "processing_run_id",
        "batch_id",
        "dataset_name",
        "source_record_id",
        "source_row_number",
        "source_record_sha256",
        "rule_id",
        "rule_version",
        "severity",
        "disposition",
        "affected_field",
        "observed_value",
        "expected_condition",
        "message",
        "detected_at_utc",
        "resolution_status",
        "resolution_action",
        "resolution_batch_id",
        "resolved_at_utc",
        "resolution_note",
        "contract_version",
    }

    for field_name in expected_fields:
        assert f'.alias("{field_name}")' in compact_source

    assert re.search(
        (
            r'F\.concat_ws\('
            r'"\|",'
            r"F\.lit\(processing_run_id\),"
            r'F\.col\("source_record_id"\),'
            r'F\.col\("rule_id"\),?'
            r"\)"
        ),
        compact_source,
    )
    assert 'F.sha2(' in source
    assert 'F.lit("PORTFOLIOS").alias("dataset_name")' in (
        compact_source
    )
    assert 'F.lit("OPEN").alias("resolution_status")' in (
        compact_source
    )

    assert "violation_audit_count" in source
    assert "distinct_violation_id_count" in source
    assert (
        "ifviolation_audit_count!=complete_violation_count:"
        in compact_source
    )
    assert (
        "ifdistinct_violation_id_count!=violation_audit_count:"
        in compact_source
    )


def test_silver_notebook_builds_record_outcome_audit_records() -> None:
    source = _read_notebook()
    normalized_source = " ".join(source.upper().split())
    compact_source = re.sub(r"\s+", "", source)

    assert 'OUTCOME_CONTRACT_VERSION = "1.0.0"' in source
    assert "FIRST_VALUE(SOURCE_RECORD_ID) OVER (" in normalized_source
    assert (
        "AS DUPLICATE_WINNER_SOURCE_RECORD_ID"
        in normalized_source
    )

    assert "record_outcome_audit_records" in source
    assert (
        'final_record_outcomes.alias("outcome")'
        in compact_source
    )

    expected_fields = {
        "outcome_id",
        "processing_run_id",
        "batch_id",
        "dataset_name",
        "source_record_id",
        "source_row_number",
        "source_record_sha256",
        "portfolio_id",
        "outcome",
        "warning_count",
        "violation_count",
        "canonical_record_hash",
        "deduplicated_to_source_record_id",
        "evaluated_at_utc",
        "dataset_contract_version",
        "contract_version",
    }

    for field_name in expected_fields:
        assert f'.alias("{field_name}")' in compact_source

    assert re.search(
        (
            r'F\.concat_ws\('
            r'"\|",'
            r"F\.lit\(processing_run_id\),"
            r'F\.col\("outcome\.source_record_id"\),?'
            r"\)"
        ),
        compact_source,
    )

    assert (
        'F.col("outcome.final_outcome")'
        '.alias("outcome")'
        in compact_source
    )
    assert '"DEDUPLICATED"' in source
    assert "duplicate_winner_source_record_id" in source

    assert "record_outcome_audit_count" in source
    assert "distinct_outcome_id_count" in source
    assert (
        "ifrecord_outcome_audit_count!=evaluated_count:"
        in compact_source
    )
    assert (
        "ifdistinct_outcome_id_count"
        "!=record_outcome_audit_count:"
        in compact_source
    )


def test_silver_notebook_defines_processing_run_audit_builder() -> None:
    source = _read_notebook()
    compact_source = re.sub(r"\s+", "", source)

    assert 'PROCESSING_RUN_CONTRACT_VERSION = "1.0.0"' in source
    assert "def build_processing_run_audit_records(" in source
    assert "processing_run_schema" in source
    assert "processing_run_record" in source
    assert "spark.createDataFrame(" in source

    expected_fields = {
        "processing_run_id",
        "source_batch_id",
        "dataset_name",
        "attempt_number",
        "reprocess_of_processing_run_id",
        "trigger_type",
        "started_at_utc",
        "completed_at_utc",
        "status",
        "evaluated_count",
        "accepted_count",
        "quarantined_count",
        "rejected_count",
        "unchanged_count",
        "deduplicated_count",
        "warning_count",
        "canonical_before_count",
        "canonical_after_count",
        "input_record_set_sha256",
        "canonical_before_sha256",
        "canonical_after_sha256",
        "published",
        "published_at_utc",
        "failed_rule_ids",
        "error_code",
        "error_message",
        "dataset_contract_version",
        "code_version",
        "contract_version",
    }

    for field_name in expected_fields:
        assert f'"{field_name}":' in compact_source

    assert (
        "ifpublished_value!=(published_at_utcisnotNone):"
        in compact_source
    )
    assert (
        'ifstatus=="FAILED"'
        "and(error_codeisNoneorerror_messageisNone):"
        in compact_source
    )

def test_silver_notebook_builds_canonical_publication_records() -> None:
    source = _read_notebook()
    compact_source = re.sub(r"\s+", "", source)

    assert (
        source.count(
            'PORTFOLIO_CONTRACT_VERSION = "1.1.0"'
        )
        == 1
    )

    assert "canonical_publication_records" in source
    assert "canonical_publication_count" in source

    expected_fields = {
        "portfolio_id",
        "portfolio_name",
        "strategy_code",
        "base_currency",
        "target_inception_date",
        "actual_inception_date",
        "initial_nav",
        "target_long_ratio",
        "target_short_ratio",
        "target_gross_ratio",
        "target_net_ratio",
        "rebalance_policy",
        "cash_policy",
        "is_active",
        "config_version",
        "record_hash",
        "processing_run_id",
        "source_batch_id",
        "source_record_id",
        "source_row_number",
        "source_record_sha256",
        "canonicalized_at_utc",
        "dataset_contract_version",
    }

    for field_name in expected_fields:
        assert f'.alias("{field_name}")' in compact_source

    assert (
        'F.col("outcome.final_outcome").isin('
        in compact_source
    )
    assert "ACCEPTED_NEW_OUTCOME" in source
    assert "ACCEPTED_CORRECTION_OUTCOME" in source

    assert (
        "ifcanonical_publication_count!=accepted_count:"
        in compact_source
    )

    assert (
        'F.lit(processing_run_id)'
        '.alias("processing_run_id")'
        in compact_source
    )
    assert (
        'F.col("candidate.batch_id")'
        '.alias("source_batch_id")'
        in compact_source
    )


def test_silver_notebook_uses_controlled_delta_writes() -> None:
    source = _read_notebook()
    compact_source = re.sub(r"\s+", "", source)
    normalized_source = " ".join(source.upper().split())

    assert "from delta.tables import DeltaTable" in source
    assert "def insert_only_audit_records(" in source

    assert _save_as_table_targets(source) == []
    assert '.mode("overwrite")' not in compact_source
    assert "INSERT OVERWRITE" not in normalized_source
    assert ".whenMatchedDelete(" not in source

    assert re.search(
        r"DeltaTable\.forName\(spark,target_table,?\)",
        compact_source,
    )
    assert ".whenNotMatchedInsertAll()" in source

    expected_audit_writes = {
        (
            "violation_audit_records",
            "SILVER_VIOLATION_TABLE",
            "violation_id",
        ),
        (
            "record_outcome_audit_records",
            "SILVER_OUTCOME_TABLE",
            "outcome_id",
        ),
        (
            "processing_run_audit_records",
            "SILVER_PROCESSING_RUN_TABLE",
            "processing_run_id",
        ),
    }

    for frame_name, table_name, key_name in expected_audit_writes:
        expected_call = (
            f"insert_only_audit_records({frame_name},"
            f'{table_name},"{key_name}"'
        )
        assert expected_call in compact_source

    assert "ifpublish_allowedandaccepted_count>0:" in (
        compact_source
    )
    assert re.search(
        (
            r"DeltaTable\.forName\("
            r"spark,SILVER_PORTFOLIO_TABLE,?\)"
        ),
        compact_source,
    )
    assert (
        '"target.portfolio_id = source.portfolio_id"'
        in source
    )
    assert ".whenMatchedUpdateAll()" in source
    assert ".whenNotMatchedInsertAll()" in source

    assert "canonical_after_verification_count" in source
    assert "canonical_after_verification_sha256" in source
    assert (
        "processing_run_audit_records="
        "build_processing_run_audit_records("
        in compact_source
    )

    canonical_merge_position = source.index(
        "canonical_delta_table.merge("
    )
    processing_run_position = source.index(
        "processing_run_audit_records ="
    )
    assert canonical_merge_position < processing_run_position
