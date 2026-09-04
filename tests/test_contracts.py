from __future__ import annotations

import csv
import hashlib
import re
from collections import Counter
from decimal import Decimal
from pathlib import Path
from typing import Any

import yaml

PROJECT_ROOT = Path(__file__).resolve().parents[1]
CONTRACT_DIR = PROJECT_ROOT / "contracts"
FIXTURE_DIR = PROJECT_ROOT / "data" / "fixtures"

EXPECTED_CONTRACT_VERSIONS = {
    "cash_balances": "1.0.0",
    "corporate_actions": "1.0.0",
    "daily_prices": "1.0.0",
    "data_quality_violations": "1.2.0",
    "ingestion_batches": "1.2.0",
    "instruments": "1.0.0",
    "portfolios": "1.1.0",
    "portfolio_record_outcomes": "1.0.0",
    "positions": "1.0.0",
    "processing_runs": "1.0.0",
    "stress_scenario_shocks": "1.0.0",
    "stress_scenarios": "1.0.0",
    "target_allocations": "1.0.0",
    "trading_calendar": "1.0.0",
}

EXPECTED_CONTRACTS = set(EXPECTED_CONTRACT_VERSIONS)


def _load_contract(dataset: str) -> dict[str, Any]:
    path = CONTRACT_DIR / f"{dataset}.yml"
    loaded = yaml.safe_load(path.read_text(encoding="utf-8"))
    assert isinstance(loaded, dict), f"{path} must contain a YAML mapping"
    return loaded


def _load_fixture(filename: str) -> list[dict[str, str]]:
    with (FIXTURE_DIR / filename).open(newline="", encoding="utf-8") as file:
        return list(csv.DictReader(file))


def _assert_record_hashes(
    rows: list[dict[str, str]], contract: dict[str, Any]
) -> None:
    hash_fields = contract["record_hash"]["fields"]
    null_token = contract["record_hash"].get("null_token", "<NULL>")

    for row in rows:
        canonical = "|".join(row[field] or null_token for field in hash_fields)
        expected = hashlib.sha256(canonical.encode("utf-8")).hexdigest()
        assert row["record_hash"] == expected, (
            f"Invalid record_hash for {contract['dataset']}: {canonical}"
        )


def _allocation_metrics(
    rows: list[dict[str, str]], portfolio_id: str
) -> tuple[Decimal, Decimal, Decimal, Decimal]:
    weights = [
        Decimal(row["target_weight"])
        for row in rows
        if row["portfolio_id"] == portfolio_id
    ]
    long_total = sum((weight for weight in weights if weight > 0), Decimal())
    short_total = sum((weight for weight in weights if weight < 0), Decimal())
    gross_total = sum((abs(weight) for weight in weights), Decimal())
    net_total = sum(weights, Decimal())
    return long_total, short_total, gross_total, net_total


def test_expected_contracts_are_parseable_and_self_describing() -> None:
    paths = sorted(CONTRACT_DIR.glob("*.yml"))

    assert {path.stem for path in paths} == EXPECTED_CONTRACTS

    for path in paths:
        contract = _load_contract(path.stem)
        assert contract["contract_version"] == EXPECTED_CONTRACT_VERSIONS[path.stem]
        assert contract["dataset"] == path.stem
        assert contract["grain"].strip()

        fields = contract["fields"]
        field_names = [field["name"] for field in fields]
        assert len(field_names) == len(set(field_names))
        assert all(field["type"] for field in fields)
        assert all(
            field["nullable"] in {True, False, "conditional"} for field in fields
        )

        rule_ids = [rule["rule_id"] for rule in contract["quality_rules"]]
        assert rule_ids
        assert len(rule_ids) == len(set(rule_ids))


def test_ingestion_batch_contract_supports_git_fixture_reruns() -> None:
    contract = _load_contract("ingestion_batches")
    fields = {field["name"]: field for field in contract["fields"]}
    rule_ids = {rule["rule_id"] for rule in contract["quality_rules"]}

    assert "PORTFOLIOS" in fields["dataset_name"]["allowed_values"]
    assert "PROJECT_GIT_FIXTURE" in fields["source_id"]["allowed_values"]
    assert "SKIPPED_DUPLICATE" in fields["status"]["allowed_values"]

    assert fields["requested_start_date"]["nullable"] is True
    assert fields["requested_end_date"]["nullable"] is True

    assert {
        "source_object_path",
        "source_sha256",
        "duplicate_of_batch_id",
    } <= fields.keys()

    assert {
        "BATCH_REQUEST_WINDOW_PAIR_VALID",
        "BATCH_REQUEST_WINDOW_SOURCE_CONSISTENT",
        "BATCH_GIT_FIXTURE_LINEAGE_REQUIRED",
        "BATCH_DUPLICATE_LINK_VALID",
        "BATCH_DUPLICATE_STATUS_CONSISTENT",
        "BATCH_ATTEMPT_LINKS_EXCLUSIVE",
    } <= rule_ids

    duplicate_semantics = contract["status_semantics"]["SKIPPED_DUPLICATE"]
    assert duplicate_semantics["terminal"] is True


def test_ingestion_batch_contract_is_bronze_only() -> None:
    contract = _load_contract("ingestion_batches")
    scope = contract["operation_scope"]

    assert scope["input_layer"] == "SOURCE"
    assert scope["output_layer"] == "BRONZE"
    assert scope["includes_silver_processing"] is False
    assert contract["contract_version"] == "1.2.0"


def test_declared_fixture_paths_exist() -> None:
    for dataset in EXPECTED_CONTRACTS:
        contract = _load_contract(dataset)
        fixture = contract.get("fixture")
        if fixture is not None:
            assert (PROJECT_ROOT / fixture["path"]).is_file()


def test_instrument_fixture_has_exact_active_universe_and_valid_hashes() -> None:
    contract = _load_contract("instruments")
    rows = _load_fixture("instruments.csv")

    assert len(rows) == 15
    assert len({row["instrument_id"] for row in rows}) == 15
    assert len({row["yfinance_symbol"] for row in rows}) == 15
    assert {row["quote_currency"] for row in rows} == {"USD"}
    assert all(row["is_active"] == "true" for row in rows)
    berkshire = next(row for row in rows if row["instrument_id"] == "BRK_B_US")
    assert berkshire["display_symbol"] == "BRK.B"
    assert berkshire["yfinance_symbol"] == "BRK-B"
    _assert_record_hashes(rows, contract)


def test_portfolio_fixture_reconciles_strategy_targets_and_hashes() -> None:
    contract = _load_contract("portfolios")
    rows = _load_fixture("portfolios.csv")

    assert {row["portfolio_id"] for row in rows} == {
        "CORE_15_LONG",
        "LONG_SHORT_130_30",
    }
    assert all(Decimal(row["initial_nav"]) == Decimal("1000000.00") for row in rows)
    assert all(row["actual_inception_date"] == "" for row in rows)

    expected = {
        "CORE_15_LONG": ("1.0", "0.0", "1.0", "1.0"),
        "LONG_SHORT_130_30": ("1.3", "0.3", "1.6", "1.0"),
    }
    for row in rows:
        ratios = (
            Decimal(row["target_long_ratio"]),
            Decimal(row["target_short_ratio"]),
            Decimal(row["target_gross_ratio"]),
            Decimal(row["target_net_ratio"]),
        )
        expected_ratios = tuple(
            Decimal(value) for value in expected[row["portfolio_id"]]
        )
        assert ratios == expected_ratios
        assert ratios[0] + ratios[1] == ratios[2]
        assert ratios[0] - ratios[1] == ratios[3]

    _assert_record_hashes(rows, contract)


def test_portfolio_contract_declares_deterministic_silver_policy() -> None:
    contract = _load_contract("portfolios")
    policy = contract["canonicalization_policy"]

    assert policy["business_key"] == ["portfolio_id"]
    assert policy["version_field"] == "config_version"
    assert policy["version_order"] == "semantic_version"

    assert (
        policy["exact_duplicate"]["winner"]
        == "lowest_source_row_number"
    )
    assert (
        policy["same_batch_conflict"]["arrival_order_winner_allowed"]
        is False
    )
    assert (
        policy["later_batch_correction"]["requires_higher_version"]
        is True
    )

    rules = {
        rule["rule_id"]: rule
        for rule in contract["quality_rules"]
    }
    expected_rules = {
        "PORTFOLIO_SAME_BATCH_IDENTICAL_DUPLICATE": (
            "WARNING",
            "WARN_AND_DEDUPLICATE",
        ),
        "PORTFOLIO_SAME_BATCH_CONFLICT": ("ERROR", "REJECT"),
        "PORTFOLIO_LATER_BATCH_UNCHANGED": ("INFO", "UNCHANGED"),
        "PORTFOLIO_LATER_BATCH_CORRECTION": (
            "INFO",
            "ACCEPT_CORRECTION",
        ),
        "PORTFOLIO_INVALID_CORRECTION": ("ERROR", "REJECT"),
    }

    for rule_id, expected in expected_rules.items():
        rule = rules[rule_id]
        assert (rule["severity"], rule["disposition"]) == expected


def test_allocations_cover_both_portfolios_and_reconcile_exposure() -> None:
    contract = _load_contract("target_allocations")
    rows = _load_fixture("target_allocations.csv")
    instruments = {row["instrument_id"] for row in _load_fixture("instruments.csv")}
    portfolios = {row["portfolio_id"] for row in _load_fixture("portfolios.csv")}

    assert len(rows) == 30
    assert Counter(row["portfolio_id"] for row in rows) == {
        "CORE_15_LONG": 15,
        "LONG_SHORT_130_30": 15,
    }
    assert {row["instrument_id"] for row in rows} == instruments
    assert {row["portfolio_id"] for row in rows} == portfolios
    assert len(
        {
            (row["portfolio_id"], row["instrument_id"], row["effective_from"])
            for row in rows
        }
    ) == 30

    assert _allocation_metrics(rows, "CORE_15_LONG") == (
        Decimal("1.0"),
        Decimal("0.0"),
        Decimal("1.0"),
        Decimal("1.0"),
    )
    assert _allocation_metrics(rows, "LONG_SHORT_130_30") == (
        Decimal("1.3"),
        Decimal("-0.3"),
        Decimal("1.6"),
        Decimal("1.0"),
    )
    assert {
        row["instrument_id"]
        for row in rows
        if row["portfolio_id"] == "LONG_SHORT_130_30"
        and Decimal(row["target_weight"]) < 0
    } == {"WMT_US", "UNH_US", "TSLA_US"}

    _assert_record_hashes(rows, contract)


def test_portfolio_contract_warns_on_missing_actual_inception_date() -> None:
    contract = _load_contract("portfolios")
    rules = {
        rule["rule_id"]: rule
        for rule in contract["quality_rules"]
    }

    rule = rules["PORTFOLIO_ACTUAL_INCEPTION_DATE_MISSING"]

    assert rule["scope"] == "RECORD"
    assert rule["severity"] == "WARNING"
    assert rule["disposition"] == "WARN_AND_ACCEPT"
    assert rule["affected_field"] == "actual_inception_date"
    assert rule["canonical_value"] is None


def test_portfolio_record_outcome_contract_assigns_one_final_state() -> None:
    contract = _load_contract("portfolio_record_outcomes")
    fields = {
        field["name"]: field
        for field in contract["fields"]
    }

    assert contract["contract_version"] == "1.0.0"
    assert contract["dataset_class"] == "operational_audit"
    assert contract["primary_key"] == ["outcome_id"]
    assert contract["natural_key"] == [
        "processing_run_id",
        "source_record_id",
    ]

    expected_fields = {
        "outcome_id": ("STRING", False),
        "processing_run_id": ("STRING", False),
        "batch_id": ("STRING", False),
        "source_record_id": ("STRING", False),
        "source_row_number": ("BIGINT", False),
        "source_record_sha256": ("STRING", False),
        "portfolio_id": ("STRING", True),
        "outcome": ("STRING", False),
        "warning_count": ("BIGINT", False),
        "violation_count": ("BIGINT", False),
        "canonical_record_hash": ("STRING", True),
        "deduplicated_to_source_record_id": ("STRING", True),
        "evaluated_at_utc": ("TIMESTAMP", False),
        "dataset_contract_version": ("STRING", False),
        "contract_version": ("STRING", False),
    }

    for field_name, expected in expected_fields.items():
        field = fields[field_name]
        assert (field["type"], field["nullable"]) == expected

    assert fields["outcome"]["allowed_values"] == [
        "ACCEPTED_NEW",
        "ACCEPTED_CORRECTION",
        "UNCHANGED",
        "DEDUPLICATED",
        "QUARANTINED",
        "REJECTED",
    ]
    assert fields["source_row_number"]["minimum_inclusive"] == 1
    assert fields["warning_count"]["minimum_inclusive"] == 0
    assert fields["violation_count"]["minimum_inclusive"] == 0

    foreign_keys = {
        tuple(foreign_key["fields"]): foreign_key["references"]
        for foreign_key in contract["foreign_keys"]
    }
    assert (
        foreign_keys[("processing_run_id",)]
        == "processing_runs.processing_run_id"
    )
    assert (
        foreign_keys[("batch_id",)]
        == "ingestion_batches.batch_id"
    )

    assert contract["outcome_id_derivation"]["fields"] == [
        "processing_run_id",
        "source_record_id",
    ]


def test_portfolio_record_outcome_keeps_warnings_orthogonal() -> None:
    contract = _load_contract("portfolio_record_outcomes")
    fields = {
        field["name"]: field
        for field in contract["fields"]
    }

    outcomes = fields["outcome"]["allowed_values"]
    warning_policy = contract["warning_policy"]

    assert warning_policy["orthogonal_to_outcome"] is True
    assert (
        warning_policy[
            "warning_count_must_not_create_an_additional_outcome"
        ]
        is True
    )
    assert (
        warning_policy["warning_count_must_not_exceed_violation_count"]
        is True
    )
    assert warning_policy["warning_count_may_be_positive_for"] == outcomes

    assert contract["outcome_precedence"] == [
        "REJECTED",
        "QUARANTINED",
        "DEDUPLICATED",
        "UNCHANGED",
        "ACCEPTED_CORRECTION",
        "ACCEPTED_NEW",
    ]

    expected_run_counts = {
        "ACCEPTED_NEW": "accepted_count",
        "ACCEPTED_CORRECTION": "accepted_count",
        "UNCHANGED": "unchanged_count",
        "DEDUPLICATED": "deduplicated_count",
        "QUARANTINED": "quarantined_count",
        "REJECTED": "rejected_count",
    }

    for outcome, run_count in expected_run_counts.items():
        assert (
            contract["outcome_semantics"][outcome][
                "processing_run_count"
            ]
            == run_count
        )


def test_contract_foreign_keys_are_unique() -> None:
    for contract_name in EXPECTED_CONTRACT_VERSIONS:
        contract = _load_contract(contract_name)
        foreign_keys = contract.get("foreign_keys", [])

        signatures = [
            (
                tuple(foreign_key["fields"]),
                foreign_key["references"],
            )
            for foreign_key in foreign_keys
        ]

        assert len(signatures) == len(set(signatures)), (
            f"{contract_name} contains duplicate foreign keys"
        )


def test_stress_fixtures_have_complete_coverage_and_valid_hashes() -> None:
    scenario_contract = _load_contract("stress_scenarios")
    shock_contract = _load_contract("stress_scenario_shocks")
    scenarios = _load_fixture("stress_scenarios.csv")
    shocks = _load_fixture("stress_scenario_shocks.csv")
    instruments = {row["instrument_id"] for row in _load_fixture("instruments.csv")}
    scenario_ids = {row["scenario_id"] for row in scenarios}

    assert len(scenarios) == 3
    assert len(shocks) == 45
    assert Counter(row["scenario_id"] for row in shocks) == {
        scenario_id: 15 for scenario_id in scenario_ids
    }
    assert len({(row["scenario_id"], row["instrument_id"]) for row in shocks}) == 45
    assert {row["instrument_id"] for row in shocks} == instruments
    assert all(Decimal(row["shock_ratio"]) >= Decimal("-1") for row in shocks)
    assert all(row["shock_rationale"].strip() for row in shocks)

    broad_market = [
        row for row in shocks if row["scenario_id"] == "BROAD_MARKET_DOWN_10"
    ]
    assert all(Decimal(row["shock_ratio"]) == Decimal("-0.10") for row in broad_market)

    _assert_record_hashes(scenarios, scenario_contract)
    _assert_record_hashes(shocks, shock_contract)


def test_contract_examples_reference_known_rules_and_expected_outcomes() -> None:
    path = FIXTURE_DIR / "contract_examples.yml"
    examples = yaml.safe_load(path.read_text(encoding="utf-8"))
    cases = examples["cases"]
    allowed_dispositions = set(examples["allowed_expected_dispositions"])

    assert examples["fixture_version"] == "1.0.0"
    assert len(cases) == 28
    assert len({case["case_id"] for case in cases}) == 28

    required_cases = {
        "PRICE_MISSING_VOLUME",
        "PRICE_NEGATIVE_CLOSE",
        "PRICE_IDENTICAL_DUPLICATE",
        "ACTION_VALID_SPLIT",
        "ACTION_MIXED_FIELDS",
        "CALENDAR_VALID_HOLIDAY",
        "POSITION_LONG_ONLY_NEGATIVE",
        "POSITION_VALID_SPLIT_CARRY",
        "BATCH_VALID_WARNING_SUCCESS",
        "BATCH_COUNT_MISMATCH",
        "VIOLATION_VALID_RESOLVED",
        "VIOLATION_MULTIPLE_RULES_ONE_RECORD",
    }
    assert required_cases <= {case["case_id"] for case in cases}

    rule_ids_by_dataset = {
        dataset: {
            rule["rule_id"] for rule in _load_contract(dataset)["quality_rules"]
        }
        for dataset in {case["dataset"] for case in cases}
    }
    for case in cases:
        assert case["rule_id"] in rule_ids_by_dataset[case["dataset"]]
        assert case["expected_disposition"] in allowed_dispositions
        assert isinstance(case["input"], dict) and case["input"]
        assert case["reason"].strip()


def test_contract_artifacts_contain_no_placeholders() -> None:
    placeholder_pattern = re.compile(r"\b(?:HASH_(?:S)?\d+|TODO|TBD)\b")
    artifact_paths = [
        *CONTRACT_DIR.glob("*.yml"),
        *FIXTURE_DIR.glob("*.csv"),
        *FIXTURE_DIR.glob("*.yml"),
    ]

    for path in artifact_paths:
        content = path.read_text(encoding="utf-8")
        assert placeholder_pattern.search(content) is None, path


def test_portfolio_rules_declare_scope_and_disposition() -> None:
    contract = _load_contract("portfolios")
    rules = {
        rule["rule_id"]: rule
        for rule in contract["quality_rules"]
    }

    expected_controls = {
        "PORTFOLIO_ACTIVE_COUNT": (
            "DATASET",
            "ERROR",
            "FAIL_PROCESSING_RUN",
        ),
        "PORTFOLIO_ID_UNIQUE": (
            "DATASET",
            "ERROR",
            "FAIL_PROCESSING_RUN",
        ),
        "PORTFOLIO_INITIAL_NAV_POSITIVE": (
            "RECORD",
            "ERROR",
            "REJECT",
        ),
        "PORTFOLIO_RATIO_RECONCILIATION": (
            "RECORD",
            "ERROR",
            "REJECT",
        ),
        "PORTFOLIO_STRATEGY_TARGETS": (
            "RECORD",
            "ERROR",
            "REJECT",
        ),
        "PORTFOLIO_INCEPTION_ORDER": (
            "RECORD",
            "ERROR",
            "REJECT",
        ),
        "PORTFOLIO_RECORD_HASH_VALID": (
            "RECORD",
            "ERROR",
            "REJECT",
        ),
        "PORTFOLIO_SAME_BATCH_IDENTICAL_DUPLICATE": (
            "BUSINESS_KEY",
            "WARNING",
            "WARN_AND_DEDUPLICATE",
        ),
        "PORTFOLIO_SAME_BATCH_CONFLICT": (
            "BUSINESS_KEY",
            "ERROR",
            "REJECT",
        ),
        "PORTFOLIO_LATER_BATCH_UNCHANGED": (
            "BUSINESS_KEY",
            "INFO",
            "UNCHANGED",
        ),
        "PORTFOLIO_LATER_BATCH_CORRECTION": (
            "BUSINESS_KEY",
            "INFO",
            "ACCEPT_CORRECTION",
        ),
        "PORTFOLIO_INVALID_CORRECTION": (
            "BUSINESS_KEY",
            "ERROR",
            "REJECT",
        ),
    }

    for rule_id, expected in expected_controls.items():
        rule = rules[rule_id]
        actual = (
            rule["scope"],
            rule["severity"],
            rule["disposition"],
        )
        assert actual == expected

    assert all(
        "Contract version 1.0.0" not in rule["assertion"]
        for rule in rules.values()
    )


def test_portfolio_contract_covers_structural_silver_validation() -> None:
    contract = _load_contract("portfolios")
    fields = {
        field["name"]: field
        for field in contract["fields"]
    }
    rules = {
        rule["rule_id"]: rule
        for rule in contract["quality_rules"]
    }

    portfolio_id_pattern = fields["portfolio_id"]["format"]
    config_version_pattern = fields["config_version"]["format"]

    assert re.fullmatch(portfolio_id_pattern, "CORE_15_LONG")
    assert not re.fullmatch(portfolio_id_pattern, "core 15 long")
    assert re.fullmatch(config_version_pattern, "1.1.0")
    assert not re.fullmatch(config_version_pattern, "latest")

    expected_controls = {
        "PORTFOLIO_REQUIRED_FIELDS": ("RECORD", "ERROR", "REJECT"),
        "PORTFOLIO_TYPES_CASTABLE": ("RECORD", "ERROR", "REJECT"),
        "PORTFOLIO_ALLOWED_VALUES": ("RECORD", "ERROR", "REJECT"),
        "PORTFOLIO_ID_FORMAT": ("RECORD", "ERROR", "REJECT"),
        "PORTFOLIO_CONFIG_VERSION_VALID": (
            "RECORD",
            "ERROR",
            "REJECT",
        ),
    }

    for rule_id, expected in expected_controls.items():
        rule = rules[rule_id]
        actual = (
            rule["scope"],
            rule["severity"],
            rule["disposition"],
        )
        assert actual == expected


def test_violation_contract_supports_portfolio_silver_vocabulary() -> None:
    contract = _load_contract("data_quality_violations")
    fields = {
        field["name"]: field
        for field in contract["fields"]
    }

    assert contract["contract_version"] == "1.2.0"

    assert (
        "PORTFOLIOS"
        in fields["dataset_name"]["allowed_values"]
    )
    assert fields["severity"]["allowed_values"] == [
        "WARNING",
        "ERROR",
        "CRITICAL",
    ]

    required_dispositions = {
        "WARN_AND_ACCEPT",
        "WARN_AND_DEDUPLICATE",
        "QUARANTINE",
        "REJECT",
    }
    assert required_dispositions <= set(
        fields["disposition"]["allowed_values"]
    )


def test_violation_contract_preserves_silver_record_traceability() -> None:
    contract = _load_contract("data_quality_violations")
    fields = {
        field["name"]: field
        for field in contract["fields"]
    }

    expected_fields = {
        "processing_run_id": ("STRING", False),
        "batch_id": ("STRING", False),
        "source_record_id": ("STRING", False),
        "source_row_number": ("BIGINT", False),
        "source_record_sha256": ("STRING", False),
        "rule_version": ("STRING", False),
    }

    for field_name, expected in expected_fields.items():
        field = fields[field_name]
        assert (field["type"], field["nullable"]) == expected

    assert fields["source_row_number"]["minimum_inclusive"] == 1
    assert (
        fields["source_record_sha256"]["format"]
        == "^[0-9a-f]{64}$"
    )
    assert (
        fields["rule_version"]["format"]
        == r"^[0-9]+\.[0-9]+\.[0-9]+$"
    )

    expected_identity = [
        "processing_run_id",
        "source_record_id",
        "rule_id",
    ]
    assert contract["natural_key"] == expected_identity
    assert (
        contract["violation_id_derivation"]["fields"]
        == expected_identity
    )

    source_identity = contract["source_record_id_derivation"]
    assert source_identity["fields"] == [
        "batch_id",
        "source_row_number",
    ]
    assert source_identity["delimiter"] == ":"
    assert (
        source_identity["format"]
        == "{batch_id}:{source_row_number}"
    )

    foreign_keys = {
        tuple(foreign_key["fields"]): foreign_key["references"]
        for foreign_key in contract["foreign_keys"]
    }
    assert (
        foreign_keys[("processing_run_id",)]
        == "processing_runs.processing_run_id"
    )

    assert contract["contract_version"] == "1.2.0"


def test_processing_run_contract_declares_silver_run_identity() -> None:
    contract = _load_contract("processing_runs")
    fields = {
        field["name"]: field
        for field in contract["fields"]
    }

    assert contract["contract_version"] == "1.0.0"
    assert contract["dataset_class"] == "operational_audit"
    assert contract["primary_key"] == ["processing_run_id"]
    assert contract["natural_key"] == [
        "source_batch_id",
        "attempt_number",
    ]

    scope = contract["operation_scope"]
    assert scope["input_layer"] == "BRONZE"
    assert scope["output_layer"] == "SILVER"
    assert scope["source_batches_per_run"] == 1

    expected_fields = {
        "processing_run_id": ("STRING", False),
        "source_batch_id": ("STRING", False),
        "dataset_name": ("STRING", False),
        "attempt_number": ("BIGINT", False),
        "reprocess_of_processing_run_id": ("STRING", True),
        "started_at_utc": ("TIMESTAMP", False),
        "completed_at_utc": ("TIMESTAMP", True),
        "status": ("STRING", False),
        "dataset_contract_version": ("STRING", False),
        "code_version": ("STRING", False),
        "contract_version": ("STRING", False),
    }

    for field_name, expected in expected_fields.items():
        field = fields[field_name]
        assert (field["type"], field["nullable"]) == expected

    assert fields["processing_run_id"]["format"] == "UUID"
    assert fields["source_batch_id"]["format"] == "UUID"
    assert fields["attempt_number"]["minimum_inclusive"] == 1
    assert (
        fields["dataset_contract_version"]["format"]
        == r"^[0-9]+\.[0-9]+\.[0-9]+$"
    )
    assert fields["code_version"]["format"] == "^[0-9a-f]{7,40}$"

    foreign_keys = {
        tuple(foreign_key["fields"]): foreign_key["references"]
        for foreign_key in contract["foreign_keys"]
    }
    assert (
        foreign_keys[("source_batch_id",)]
        == "ingestion_batches.batch_id"
    )
    assert (
        foreign_keys[("reprocess_of_processing_run_id",)]
        == "processing_runs.processing_run_id"
    )


def test_processing_run_contract_reconciles_record_outcomes() -> None:
    contract = _load_contract("processing_runs")
    fields = {
        field["name"]: field
        for field in contract["fields"]
    }

    count_fields = [
        "evaluated_count",
        "accepted_count",
        "quarantined_count",
        "rejected_count",
        "unchanged_count",
        "deduplicated_count",
        "warning_count",
    ]

    for field_name in count_fields:
        field = fields[field_name]
        assert field["type"] == "BIGINT"
        assert field["nullable"] is False
        assert field["minimum_inclusive"] == 0

    reconciliation = contract["count_reconciliation"]
    assert reconciliation["mutually_exclusive_outcomes"] == [
        "accepted_count",
        "quarantined_count",
        "rejected_count",
        "unchanged_count",
        "deduplicated_count",
    ]
    assert reconciliation["equation"] == (
        "evaluated_count = accepted_count + quarantined_count "
        "+ rejected_count + unchanged_count + deduplicated_count"
    )
    assert reconciliation["warning_count_is_orthogonal"] is True


def test_processing_run_contract_requires_atomic_publication() -> None:
    contract = _load_contract("processing_runs")
    fields = {
        field["name"]: field
        for field in contract["fields"]
    }

    expected_fields = {
        "canonical_before_count": ("BIGINT", False),
        "canonical_after_count": ("BIGINT", False),
        "input_record_set_sha256": ("STRING", False),
        "canonical_before_sha256": ("STRING", True),
        "canonical_after_sha256": ("STRING", True),
        "published": ("BOOLEAN", False),
        "published_at_utc": ("TIMESTAMP", True),
        "failed_rule_ids": ("ARRAY<STRING>", False),
        "error_code": ("STRING", True),
        "error_message": ("STRING", True),
    }

    for field_name, expected in expected_fields.items():
        field = fields[field_name]
        assert (field["type"], field["nullable"]) == expected

    assert fields["canonical_before_count"]["minimum_inclusive"] == 0
    assert fields["canonical_after_count"]["minimum_inclusive"] == 0

    for field_name in [
        "input_record_set_sha256",
        "canonical_before_sha256",
        "canonical_after_sha256",
    ]:
        assert fields[field_name]["format"] == "^[0-9a-f]{64}$"

    policy = contract["publication_policy"]
    assert policy["mode"] == "ATOMIC"
    assert policy["publish_allowed_statuses"] == [
        "SUCCEEDED",
        "SUCCEEDED_WITH_WARNINGS",
    ]
    assert policy["failed_run_publishes"] is False
    assert policy["preserve_last_good_snapshot_on_failure"] is True
    assert policy["no_change_run_may_skip_publication"] is True

def test_processing_run_contract_defines_complete_status_lifecycle() -> None:
    contract = _load_contract("processing_runs")
    fields = {
        field["name"]: field
        for field in contract["fields"]
    }

    allowed_statuses = fields["status"]["allowed_values"]
    semantics = contract["status_semantics"]

    assert set(semantics) == set(allowed_statuses)

    for status in ["PENDING", "RUNNING"]:
        assert semantics[status]["terminal"] is False
        assert (
            semantics[status]["completed_at_utc"]
            == "must_be_null"
        )
        assert semantics[status]["published"] is False

    for status in [
        "SUCCEEDED",
        "SUCCEEDED_WITH_WARNINGS",
        "FAILED",
    ]:
        assert semantics[status]["terminal"] is True
        assert semantics[status]["completed_at_utc"] == "required"

    assert semantics["FAILED"]["published"] is False


def test_cash_balance_contract_declares_reconciled_derivation() -> None:
    contract = _load_contract("cash_balances")
    fields = {
        field["name"]: field
        for field in contract["fields"]
    }
    rules = {
        rule["rule_id"]: rule
        for rule in contract["quality_rules"]
    }

    assert contract["contract_version"] == "1.0.0"
    assert contract["dataset_class"] == "derived_daily_observation"
    assert contract["primary_key"] == ["portfolio_id", "cash_date"]

    expected_fields = {
        "portfolio_id",
        "cash_date",
        "prior_cash_date",
        "position_input_date",
        "base_currency",
        "opening_cash_balance",
        "dividend_cash_flow",
        "closing_cash_balance",
        "corporate_action_count",
        "input_position_set_sha256",
        "input_corporate_action_set_sha256",
        "processing_run_id",
        "calculation_version",
        "derived_at_utc",
        "contract_version",
        "record_hash",
    }
    assert set(fields) == expected_fields

    for field_name in [
        "opening_cash_balance",
        "dividend_cash_flow",
        "closing_cash_balance",
    ]:
        assert fields[field_name]["type"] == "DECIMAL(38,16)"
        assert fields[field_name]["nullable"] is False
        assert fields[field_name]["unit"] == "base_currency"

    assert fields["base_currency"]["allowed_values"] == ["USD"]
    assert fields["corporate_action_count"]["minimum_inclusive"] == 0

    for field_name in [
        "input_position_set_sha256",
        "input_corporate_action_set_sha256",
        "record_hash",
    ]:
        assert fields[field_name]["format"] == "^[0-9a-f]{64}$"

    derivation = contract["derivation"]

    assert derivation["amount_type"] == "DECIMAL(38,16)"
    assert derivation["intermediate_rounding_allowed"] is False
    assert derivation["presentation_rounding_scale"] == 2
    assert derivation["inception_opening_cash_formula"] == (
        "initial_nav - sum(signed_quantity * inception_close_price)"
    )
    assert derivation["later_opening_cash_formula"] == (
        "prior_complete_closing_cash_balance"
    )
    assert derivation["dividend_cash_flow_formula"] == (
        "sum(prior_signed_quantity * dividend_amount_per_share)"
    )
    assert derivation["closing_cash_formula"] == (
        "opening_cash_balance + dividend_cash_flow"
    )

    expected_rule_ids = {
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
    }
    assert set(rules) == expected_rule_ids
    assert all(
        rule["severity"] == "ERROR"
        and rule["disposition"] == "FAIL_PROCESSING_RUN"
        for rule in rules.values()
    )
