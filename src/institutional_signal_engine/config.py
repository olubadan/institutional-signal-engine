"""Validated non-secret configuration snapshots."""

import os
from decimal import Decimal

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


class Settings(BaseModel):
    """Settings loaded from environment; secrets never enter snapshots/logs."""

    model_config = ConfigDict(frozen=True)
    trading_enabled: bool = False
    alpaca_key_id: SecretStr | None = None
    alpaca_secret_key: SecretStr | None = None
    alpaca_data_url: str = "wss://stream.data.alpaca.markets/v2/iex"
    theta_api_key: SecretStr | None = None
    theta_events_url: str = "ws://127.0.0.1:25520/v1/events"
    thresholds: Thresholds = Thresholds()
    capacity: int = Field(default=1, ge=0)
    config_version: str = "phase3-v1"
    engine_version: str = "0.1.0"

    @classmethod
    def from_env(cls, environ: dict[str, str] | None = None) -> "Settings":
        values = os.environ if environ is None else environ
        return cls(
            trading_enabled=values.get("TRADING_ENABLED", "false").strip().lower()
            in {"1", "true", "yes", "on"},
            alpaca_key_id=SecretStr(values["ALPACA_API_KEY_ID"])
            if values.get("ALPACA_API_KEY_ID")
            else None,
            alpaca_secret_key=SecretStr(values["ALPACA_API_SECRET_KEY"])
            if values.get("ALPACA_API_SECRET_KEY")
            else None,
            alpaca_data_url=values.get(
                "ALPACA_DATA_URL",
                values.get("ALPACA_DATA_FEED", cls.model_fields["alpaca_data_url"].default),
            ),
            theta_api_key=SecretStr(values["THETADATA_API_KEY"])
            if values.get("THETADATA_API_KEY")
            else None,
            theta_events_url=values.get(
                "OPTIONS_API_BASE_URL", cls.model_fields["theta_events_url"].default
            ),
            capacity=int(values.get("CAPACITY", "1")),
        )

    @field_validator("trading_enabled", mode="before")
    @classmethod
    def parse_trading_enabled(cls, value: object) -> bool:
        if isinstance(value, bool):
            return value
        return str(value).strip().lower() in {"1", "true", "yes", "on"}

    def public_snapshot(self) -> dict[str, object]:
        return self.model_dump(exclude={"alpaca_key_id", "alpaca_secret_key", "theta_api_key"})
