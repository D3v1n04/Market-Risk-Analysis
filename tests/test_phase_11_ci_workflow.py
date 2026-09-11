from __future__ import annotations

from pathlib import Path

import yaml

PROJECT_ROOT = Path(__file__).resolve().parents[1]
WORKFLOW_PATH = PROJECT_ROOT / ".github" / "workflows" / "ci.yml"


def test_baseline_ci_is_credential_free_and_runs_local_quality_gate() -> None:
    source = WORKFLOW_PATH.read_text(encoding="utf-8")
    workflow = yaml.load(source, Loader=yaml.BaseLoader)

    assert workflow["name"] == "CI"
    assert workflow["permissions"] == {"contents": "read"}
    assert set(workflow["on"]) == {"push", "pull_request"}

    job = workflow["jobs"]["local-quality"]
    assert job["runs-on"] == "ubuntu-24.04"

    assert "actions/checkout@v7" in source
    assert "persist-credentials: false" in source
    assert "actions/setup-python@v7" in source
    assert "python-version: \"3.12\"" in source
    assert "astral-sh/setup-uv@" in source

    assert "uv sync --all-groups --locked" in source
    assert "uv run ruff check ." in source
    assert "uv run pytest" in source
    assert "uv run market-risk-check" in source

    lower_source = source.lower()

    assert "databricks auth" not in lower_source
    assert "databricks bundle deploy" not in lower_source
    assert "databricks bundle run" not in lower_source
    assert "databricks jobs" not in lower_source
    assert "databricks_token" not in lower_source
    assert "databricks_client_id" not in lower_source
    assert "databricks_client_secret" not in lower_source
    assert "secrets." not in source
