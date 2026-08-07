"""Typed Alpaca option-snapshot quote evidence adapter."""

from collections.abc import Iterable
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal

import httpx

from ..contract_mapping import CanonicalOptionIdentity
from .common import ProviderError


@dataclass(frozen=True)
class OptionQuoteEvidence:
    identity: CanonicalOptionIdentity
    provider_symbol: str
    feed: str
    snapshot_timestamp: datetime
    bid_price: Decimal | None
    bid_size: int | None
    ask_price: Decimal | None
    ask_size: int | None
    latest_trade_price: Decimal | None
    latest_trade_timestamp: datetime | None
    source_provenance: str

    @property
    def spread(self) -> Decimal | None:
        if self.bid_price is None or self.ask_price is None:
            return None
        return self.ask_price - self.bid_price

    @property
    def spread_percentage(self) -> Decimal | None:
        if self.spread is None or self.bid_price is None or self.bid_price <= 0:
            return None
        return self.spread / self.bid_price

    def valid(self, now: datetime, max_age_seconds: int, min_size: int = 1) -> bool:
        now = now.astimezone(UTC)
        age = (now - self.snapshot_timestamp.astimezone(UTC)).total_seconds()
        return (
            self.bid_price is not None
            and self.ask_price is not None
            and self.bid_price > 0
            and self.ask_price > 0
            and self.ask_price >= self.bid_price
            and self.bid_size is not None
            and self.bid_size >= min_size
            and self.ask_size is not None
            and self.ask_size >= min_size
            and 0 <= age <= max_age_seconds
        )


def _timestamp(value: object) -> datetime | None:
    if value is None:
        return None
    return datetime.fromisoformat(str(value)).astimezone(UTC)


class AlpacaOptionSnapshotProvider:
    def __init__(
        self,
        api_base_url: str,
        key_id: str,
        secret_key: str,
        timeout: float = 10.0,
    ) -> None:
        self.api_base_url = api_base_url.rstrip("/")
        self.key_id = key_id
        self.secret_key = secret_key
        self.timeout = timeout

    async def snapshots(
        self,
        records: Iterable[tuple[str, CanonicalOptionIdentity]],
        feed: str = "opra",
        batch_size: int = 100,
    ) -> tuple[OptionQuoteEvidence, ...]:
        if feed not in {"opra", "indicative"}:
            raise ValueError("unsupported Alpaca options feed")
        ordered = tuple(records)
        headers = {
            "APCA-API-KEY-ID": self.key_id,
            "APCA-API-SECRET-KEY": self.secret_key,
        }
        output: list[OptionQuoteEvidence] = []
        async with httpx.AsyncClient(base_url=self.api_base_url, timeout=self.timeout) as client:
            for start in range(0, len(ordered), batch_size):
                batch = ordered[start : start + batch_size]
                params: dict[str, str | int] = {
                    "symbols": ",".join(symbol for symbol, _ in batch),
                    "feed": feed,
                    "limit": min(batch_size, 1000),
                }
                try:
                    response = await client.get(
                        "/v1beta1/options/snapshots", headers=headers, params=params
                    )
                except httpx.HTTPError as exc:
                    raise ProviderError("alpaca", "option_snapshot_transport", True) from exc
                if response.status_code != 200:
                    raise ProviderError(
                        "alpaca", f"option_snapshot_http_{response.status_code}", False
                    )
                body: object = response.json()
                if not isinstance(body, dict):
                    raise ProviderError("alpaca", "option_snapshot_malformed", False)
                snapshots = body.get("snapshots", body)
                if not isinstance(snapshots, dict):
                    raise ProviderError("alpaca", "option_snapshot_malformed", False)
                by_symbol = {symbol: identity for symbol, identity in batch}
                for provider_symbol, raw in snapshots.items():
                    identity = by_symbol.get(provider_symbol)
                    if identity is None or not isinstance(raw, dict):
                        continue
                    quote = raw.get("latestQuote") or raw.get("latest_quote")
                    trade = raw.get("latestTrade") or raw.get("latest_trade")
                    if not isinstance(quote, dict):
                        quote = {}
                    if not isinstance(trade, dict):
                        trade = {}
                    timestamp = _timestamp(
                        quote.get("timestamp", quote.get("t"))
                        or trade.get("timestamp", trade.get("t"))
                    )
                    if timestamp is None:
                        continue
                    output.append(
                        OptionQuoteEvidence(
                            identity,
                            provider_symbol,
                            feed.upper(),
                            timestamp,
                            Decimal(str(quote.get("bid_price", quote.get("bp"))))
                            if quote.get("bid_price", quote.get("bp")) is not None
                            else None,
                            int(str(quote.get("bid_size", quote.get("bs"))))
                            if quote.get("bid_size", quote.get("bs")) is not None
                            else None,
                            Decimal(str(quote.get("ask_price", quote.get("ap"))))
                            if quote.get("ask_price", quote.get("ap")) is not None
                            else None,
                            int(str(quote.get("ask_size", quote.get("as"))))
                            if quote.get("ask_size", quote.get("as")) is not None
                            else None,
                            Decimal(str(trade.get("price", trade.get("p"))))
                            if trade.get("price", trade.get("p")) is not None
                            else None,
                            _timestamp(trade.get("timestamp", trade.get("t"))),
                            "alpaca:v1beta1/options/snapshots",
                        )
                    )
        return tuple(output)
