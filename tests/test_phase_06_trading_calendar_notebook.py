import ast
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
NOTEBOOK_PATH = (
    PROJECT_ROOT / "notebooks" / "silver" / "phase_06_generate_trading_calendar.py"
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


def test_trading_calendar_is_valid_databricks_source() -> None:
    source = _source()
    assert source.startswith("# Databricks notebook source")
    assert source.count("# COMMAND ----------") >= 7
    _tree()


def test_trading_calendar_pins_full_2016_exchange_coverage() -> None:
    source = _source()
    assert _literal_assignment("CONTRACT_VERSION") == "1.0.0"
    assert _literal_assignment("CALENDAR_SOURCE") == "PROJECT_GIT_RULESET"
    assert _literal_assignment("CALENDAR_VERSION") == "US_EQUITIES_2016_V1"
    assert _literal_assignment("EXCHANGE_TIMEZONE") == "America/New_York"
    assert _literal_assignment("EXCHANGES") == ("XNAS", "XNYS")
    assert _literal_assignment("EXPECTED_DATE_COUNT") == 366
    assert _literal_assignment("EXPECTED_ROW_COUNT") == 732
    assert _literal_assignment("EXPECTED_TRADING_DAYS_PER_EXCHANGE") == 252
    assert _literal_assignment("EXPECTED_NONTRADING_DAYS_PER_EXCHANGE") == 114
    assert "date(2016, 1, 1)" in source
    assert "date(2016, 12, 31)" in source


def test_trading_calendar_pins_holidays_and_early_close() -> None:
    source = _source()
    expected_holidays = {
        "New Year's Day",
        "Martin Luther King Jr. Day",
        "Washington's Birthday",
        "Good Friday",
        "Memorial Day",
        "Independence Day",
        "Labor Day",
        "Thanksgiving Day",
        "Christmas Day (Observed)",
    }
    for holiday_name in expected_holidays:
        assert holiday_name in source
    assert "date(2016, 11, 25): \"Day After Thanksgiving\"" in source
    assert _literal_assignment("EXPECTED_EARLY_CLOSES_PER_EXCHANGE") == 1
    assert "time(13, 0) if is_early_close else time(16, 0)" in source


def test_trading_calendar_handles_timezone_and_dst() -> None:
    source = _source()
    assert "ZoneInfo(EXCHANGE_TIMEZONE)" in source
    assert ".astimezone(UTC).replace(tzinfo=None)" in source
    assert "datetime(2016, 1, 4, 14, 30)" in source
    assert "datetime(2016, 7, 5, 13, 30)" in source
    assert "datetime(2016, 11, 25, 18, 0)" in source
    assert 'spark.conf.set("spark.sql.session.timeZone", "UTC")' in source


def test_trading_calendar_hash_matches_contract() -> None:
    source = _source()
    normalized = " ".join(source.split())
    assert _literal_assignment("RECORD_HASH_FIELDS") == (
        "exchange_mic",
        "calendar_date",
        "is_trading_day",
        "market_open_utc",
        "market_close_utc",
        "is_early_close",
        "holiday_name",
        "exchange_timezone",
        "calendar_source",
        "calendar_version",
    )
    assert 'return "<NULL>"' in source
    assert "str(value).lower()" in source
    assert 'replace( "+00:00", "Z" )' in normalized
    assert 'hashlib.sha256(canonical_record.encode("utf-8")).hexdigest()' in source
    assert "generated_at_utc" not in _literal_assignment("RECORD_HASH_FIELDS")


def test_trading_calendar_validates_before_publication() -> None:
    source = _source()
    validation_position = source.index("validate_calendar_rows(calendar_rows)")
    merge_position = source.index("spark.sql(merge_sql)")
    assert validation_position < merge_position
    assert "unique exchange-date keys" in source
    assert "contiguous dates" in source
    assert "weekends are nontrading" in source
    assert "weekday closure has holiday name" in source
    assert "session close follows open" in source
    assert "active instrument exchanges are covered" in source
    assert "record hash" in source
    assert "publication counts reconcile" in source


def test_trading_calendar_merge_is_idempotent_and_reconciled() -> None:
    source = _source()
    normalized = " ".join(source.split())
    assert (
        "MERGE INTO {TRADING_CALENDAR_TABLE} AS target "
        "USING phase_06_trading_calendar_candidates AS source"
    ) in normalized
    assert (
        "WHEN MATCHED AND target.record_hash <> source.record_hash "
        "THEN UPDATE SET *"
    ) in normalized
    assert "WHEN NOT MATCHED THEN INSERT *" in normalized
    assert "if published: spark.sql(merge_sql)" in normalized
    assert "published = inserted_count + updated_count > 0" in source
    assert "candidate rows missing from persisted target" in source
    assert "unexpected persisted rows" in source
    assert "persisted calendar lineage" in source
    assert ".exceptAll(" in source

    forbidden = (
        'mode("overwrite")',
        "mode('overwrite')",
        "DELETE FROM",
        "DROP TABLE",
        "TRUNCATE TABLE",
    )
    for token in forbidden:
        assert token not in source


def test_trading_calendar_avoids_serverless_persistence_commands() -> None:
    source = _source().lower()
    forbidden = (
        ".cache()",
        ".persist(",
        ".unpersist(",
        "cache table",
        "persist table",
    )
    for token in forbidden:
        assert token not in source
