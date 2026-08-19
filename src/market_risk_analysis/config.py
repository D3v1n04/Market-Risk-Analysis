"""Small, dependency-free application configuration."""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class Settings:
    """Values that may differ between a laptop, CI, and production."""

    environment: str
    data_dir: Path

    @classmethod
    def from_environment(cls, project_root: Path) -> Settings:
        """Build settings from environment variables with safe local defaults."""
        environment = os.getenv("MARKET_RISK_ENV", "local")
        configured_data_dir = Path(os.getenv("MARKET_RISK_DATA_DIR", "data"))
        data_dir = (
            configured_data_dir
            if configured_data_dir.is_absolute()
            else project_root / configured_data_dir
        )
        return cls(environment=environment, data_dir=data_dir.resolve())
