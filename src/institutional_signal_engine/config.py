"""Validated non-secret configuration snapshots."""

import os
from decimal import Decimal
from pathlib import Path
from urllib.parse import quote

from pydantic import BaseModel, ConfigDict, Field, SecretStr, field_validator


class Thresholds(BaseModel):
    model_config = ConfigDict(frozen=True)
    minimum_volume: int = Field(default=100_000, ge=0)
    maximum_spread: Decimal = Field(default=Decimal("0.01"), ge=0)
    minimum_call_premium: Decimal = Field(default=Decimal(100000), ge=0)
    minimum_option_volume_oi_ratio: Decimal = Field(default=Decimal(1), ge=0)
    take_profit: Decimal = Field(default=Decimal("0.0025"), ge=0)
    minimum_room: Decimal = Field(default=Decimal("0.0025"), ge=0)
    minimum_decay: Decimal = Field(default=Decimal("0.5"), ge=0, le=1)
    freshness_seconds: int = Field(default=300, gt=0)


def read_env_file(path: str | Path) -> dict[str, str]:
    """Read ``NAME=value`` records as data, never as executable syntax."""
    values: dict[str, str] = {}
    for raw_line in Path(path).read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        name, value = line.split("=", 1)
        name = name.strip()
        value = value.strip()
        if len(value) >= 2 and value[0] == value[-1] and value[0] in {"'", '"'}:
            value = value[1:-1]
        values[name] = value
    return values


class Settings(BaseModel):
    """Settings loaded from environment; secrets never enter snapshots/logs."""

    model_config = ConfigDict(frozen=True)
    trading_enabled: bool = False
    alpaca_key_id: SecretStr | None = None
    alpaca_secret_key: SecretStr | None = None
    alpaca_data_feed: str = "iex"
    alpaca_data_url: str = "wss://stream.data.alpaca.markets/v2/iex"
    theta_api_key: SecretStr | None = None
    theta_events_url: str = "ws://127.0.0.1:25520/v1/events"
    database_url: str | None = None
    thresholds: Thresholds = Thresholds()
    capacity: int = Field(default=1, ge=0)
    config_version: str = "phase3-v1"
    engine_version: str = "0.1.0"

    @classmethod
    def from_env(cls, environ: dict[str, str] | None = None) -> "Settings":
        values = os.environ if environ is None else environ
        feed = values.get("ALPACA_DATA_FEED", "iex").strip().lower()
        if "://" in feed:
            raise ValueError("ALPACA_DATA_FEED must be a feed name, not a URL")
        data_url = values.get("ALPACA_DATA_URL") or (
            f"wss://stream.data.alpaca.markets/v2/{quote(feed, safe='')}"
        )
        return cls(
            trading_enabled=values.get("TRADING_ENABLED", "false").strip().lower()
            in {"1", "true", "yes", "on"},
            alpaca_key_id=SecretStr(values["ALPACA_API_KEY_ID"])
            if values.get("ALPACA_API_KEY_ID")
            else None,
            alpaca_secret_key=SecretStr(values["ALPACA_API_SECRET_KEY"])
            if values.get("ALPACA_API_SECRET_KEY")
            else None,
            alpaca_data_feed=feed,
            alpaca_data_url=data_url,
            theta_api_key=SecretStr(values["THETADATA_API_KEY"])
            if values.get("THETADATA_API_KEY")
            else None,
            theta_events_url=values.get(
                "OPTIONS_API_BASE_URL", cls.model_fields["theta_events_url"].default
            ),
            database_url=values.get("DATABASE_URL"),
            capacity=int(values.get("CAPACITY", "1")),
        )

    @classmethod
    def from_env_file(cls, path: str | Path) -> "Settings":
        """Load ``NAME=value`` lines as data; never parse them as shell syntax."""
        return cls.from_env(read_env_file(path))

    @field_validator("trading_enabled", mode="before")
    @classmethod
    def parse_trading_enabled(cls, value: object) -> bool:
        if isinstance(value, bool):
            return value
        return str(value).strip().lower() in {"1", "true", "yes", "on"}

    def public_snapshot(self) -> dict[str, object]:
        return self.model_dump(
            exclude={"alpaca_key_id", "alpaca_secret_key", "theta_api_key", "database_url"}
        )
