from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
RESOURCE_PATH = (
    PROJECT_ROOT
    / "resources"
    / "phase_12_yahoo_silver_publication_job.yml"
)


def _source() -> str:
    return RESOURCE_PATH.read_text(encoding="utf-8")


def test_phase_12_yahoo_silver_job_is_manual_and_pinned() -> None:
    source = _source()

    assert "yahoo_finance_silver_publication:" in source
    assert "max_concurrent_runs: 1" in source
    assert 'default: "REQUIRED_AT_MANUAL_RUN"' in source
    assert 'default: "MANUAL"' in source
    assert 'default: "3e317573-8c81-46a1-bf39-80feadf94fe1"' in source
    assert 'default: "779f9856-9270-467a-b394-dcf01c2acac0"' in source


def test_phase_12_yahoo_silver_job_requires_calendar_first() -> None:
    source = _source()

    assert "publish_exchange_calendar" in source
    assert source.count("depends_on:") == 2
    assert source.count("- task_key: publish_exchange_calendar") == 3
    assert source.count(
        "notebooks/silver/phase_12_process_yahoo_market_data.py"
    ) == 2


def test_phase_12_yahoo_silver_job_passes_exact_batch_lineage() -> None:
    source = _source()

    assert (
        'source_batch_id: "{{job.parameters.daily_prices_batch_id}}"'
        in source
    )
    assert (
        'source_batch_id: "{{job.parameters.corporate_actions_batch_id}}"'
        in source
    )
    assert 'code_version: "{{job.parameters.code_version}}"' in source
    assert 'trigger_type: "{{job.parameters.trigger_type}}"' in source
