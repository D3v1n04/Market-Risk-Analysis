from pathlib import Path

from market_risk_analysis.environment import (
    REQUIRED_PROJECT_PATHS,
    inspect_environment,
)


def test_environment_fails_when_required_paths_are_missing(tmp_path: Path) -> None:
    checks = inspect_environment(tmp_path)
    path_checks = [check for check in checks if check.name.startswith("Project path:")]

    assert path_checks
    assert all(not check.passed for check in path_checks)


def test_environment_accepts_required_project_paths(tmp_path: Path) -> None:
    for relative_path in REQUIRED_PROJECT_PATHS:
        if relative_path.suffix:
            (tmp_path / relative_path).parent.mkdir(parents=True, exist_ok=True)
            (tmp_path / relative_path).touch()
        else:
            (tmp_path / relative_path).mkdir(parents=True, exist_ok=True)

    checks = inspect_environment(tmp_path)
    path_checks = [check for check in checks if check.name.startswith("Project path:")]

    assert path_checks
    assert all(check.passed for check in path_checks)
