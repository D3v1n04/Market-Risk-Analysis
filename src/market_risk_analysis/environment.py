"""Verify that the local development environment is ready."""

from __future__ import annotations

import shutil
import sys
from dataclasses import dataclass
from pathlib import Path

from market_risk_analysis.config import Settings

MINIMUM_PYTHON = (3, 12)
REQUIRED_PROJECT_PATHS = (
    Path("pyproject.toml"),
    Path("databricks.yml"),
    Path("src/market_risk_analysis"),
    Path("tests"),
    Path("data/README.md"),
    Path("sql/connectivity/phase_02_connectivity_check.sql"),
    Path("sql/setup/phase_02_create_dev_schema.sql"),
)


@dataclass(frozen=True)
class Check:
    """The result of one readiness check."""

    name: str
    passed: bool
    detail: str


def inspect_environment(project_root: Path) -> list[Check]:
    """Return deterministic checks without changing the machine."""
    settings = Settings.from_environment(project_root)
    python_version = sys.version_info[:3]
    checks = [
        Check(
            name="Python version",
            passed=python_version >= MINIMUM_PYTHON,
            detail=(
                f"found {'.'.join(map(str, python_version))}; "
                f"need {'.'.join(map(str, MINIMUM_PYTHON))}+"
            ),
        ),
        Check("Git executable", shutil.which("git") is not None, "git is on PATH"),
        Check("uv executable", shutil.which("uv") is not None, "uv is on PATH"),
    ]
    checks.extend(
        Check(
            name=f"Project path: {relative_path}",
            passed=(project_root / relative_path).exists(),
            detail=str(project_root / relative_path),
        )
        for relative_path in REQUIRED_PROJECT_PATHS
    )
    checks.append(
        Check(
            name="Configuration",
            passed=bool(settings.environment) and settings.data_dir.is_absolute(),
            detail=f"environment={settings.environment}, data_dir={settings.data_dir}",
        )
    )
    return checks


def main() -> int:
    """Print the readiness report and return a shell-friendly exit code."""
    project_root = Path(__file__).resolve().parents[2]
    checks = inspect_environment(project_root)
    for check in checks:
        symbol = "PASS" if check.passed else "FAIL"
        print(f"[{symbol}] {check.name}: {check.detail}")

    failures = sum(not check.passed for check in checks)
    print(f"\n{len(checks) - failures}/{len(checks)} checks passed")
    return 1 if failures else 0
