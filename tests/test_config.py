from pathlib import Path

from market_risk_analysis.config import Settings


def test_settings_use_safe_local_defaults(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.delenv("MARKET_RISK_ENV", raising=False)
    monkeypatch.delenv("MARKET_RISK_DATA_DIR", raising=False)

    settings = Settings.from_environment(tmp_path)

    assert settings.environment == "local"
    assert settings.data_dir == (tmp_path / "data").resolve()


def test_settings_accept_environment_overrides(monkeypatch, tmp_path: Path) -> None:
    custom_data_dir = tmp_path / "sandbox-data"
    monkeypatch.setenv("MARKET_RISK_ENV", "test")
    monkeypatch.setenv("MARKET_RISK_DATA_DIR", str(custom_data_dir))

    settings = Settings.from_environment(tmp_path)

    assert settings.environment == "test"
    assert settings.data_dir == custom_data_dir.resolve()
