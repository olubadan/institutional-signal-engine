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
    allowed_lateness_seconds: int = Field(default=5, ge=0)
    persistence_soft_limit: int = Field(default=10_000, gt=0)
    persistence_hard_limit: int = Field(default=20_000, gt=0)
    persistence_batch_size: int = Field(default=100, gt=0)
    persistence_flush_interval: Decimal = Field(default=Decimal("0.05"), gt=0)
    phase4_max_contracts_per_symbol: int = Field(default=1000, gt=0)
    phase4_trade_subscription_limit: int = Field(default=15000, gt=0)
    phase4_quote_subscription_limit: int = Field(default=10000, gt=0)
    phase4_pre_enrichment_max_per_symbol: int = Field(default=100, gt=0)
    phase4_max_enrichment_candidates: int = Field(default=2000, gt=0)
    phase4_startup_timeout_seconds: int = Field(default=120, gt=0)
    phase4_quote_freshness_seconds: int = Field(default=60, gt=0)
    phase4_min_quote_size: int = Field(default=1, gt=0)
    phase4_oi_request_interval_seconds: Decimal = Field(default=Decimal("0.05"), ge=0)
    theta_terminal_http_url: str = "http://127.0.0.1:25503/v3"
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
            allowed_lateness_seconds=int(values.get("ALLOWED_LATENESS_SECONDS", "5")),
            persistence_soft_limit=int(values.get("PERSISTENCE_SOFT_LIMIT", "10000")),
            persistence_hard_limit=int(values.get("PERSISTENCE_HARD_LIMIT", "20000")),
            persistence_batch_size=int(values.get("PERSISTENCE_BATCH_SIZE", "100")),
            persistence_flush_interval=Decimal(values.get("PERSISTENCE_FLUSH_INTERVAL", "0.05")),
            phase4_max_contracts_per_symbol=int(
                values.get("PHASE4_MAX_CONTRACTS_PER_SYMBOL", "1000")
            ),
            phase4_trade_subscription_limit=int(
                values.get("PHASE4_TRADE_SUBSCRIPTION_LIMIT", "15000")
            ),
            phase4_quote_subscription_limit=int(
                values.get("PHASE4_QUOTE_SUBSCRIPTION_LIMIT", "10000")
            ),
            phase4_pre_enrichment_max_per_symbol=int(
                values.get("PHASE4_PRE_ENRICHMENT_MAX_PER_SYMBOL", "100")
            ),
            phase4_max_enrichment_candidates=int(
                values.get("PHASE4_MAX_ENRICHMENT_CANDIDATES", "2000")
            ),
            phase4_startup_timeout_seconds=int(values.get("PHASE4_STARTUP_TIMEOUT_SECONDS", "120")),
            phase4_quote_freshness_seconds=int(values.get("PHASE4_QUOTE_FRESHNESS_SECONDS", "60")),
            phase4_min_quote_size=int(values.get("PHASE4_MIN_QUOTE_SIZE", "1")),
            phase4_oi_request_interval_seconds=Decimal(
                values.get("PHASE4_OI_REQUEST_INTERVAL_SECONDS", "0.05")
            ),
            theta_terminal_http_url=values.get(
                "THETA_TERMINAL_HTTP_URL", "http://127.0.0.1:25503/v3"
            ),
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
