"""Alpaca live equities market-data WebSocket adapter.

Authentication follows Alpaca's official Trading/Broker WebSocket contract:
the key pair is sent in an auth message within ten seconds of connection.
"""

import asyncio
import json
from collections.abc import AsyncIterator, Iterable
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from typing import Any
from uuid import NAMESPACE_URL, uuid5

import httpx
import websockets

from ..historical import HistoricalBootstrap
from ..indicators import ET, PreviousClose
from ..schemas import CanonicalEvent, EventKind
from .common import ProviderError, reconnecting_stream


class AlpacaEquitiesProvider:
    def __init__(
        self,
        url: str,
        key_id: str,
        secret_key: str,
        timeout: float = 10.0,
        historical_url: str = "https://data.alpaca.markets",
    ) -> None:
        self.url, self.key_id, self.secret_key, self.timeout = url, key_id, secret_key, timeout
        self.historical_url = historical_url.rstrip("/")
        self.authenticated = False
        self.connection_generation = 0

    @staticmethod
    def _authentication_result(messages: list[dict[str, Any]]) -> bool:
        if any(message.get("T") == "error" for message in messages):
            raise ProviderError("alpaca", "authentication_failed", False)
        return any(
            message.get("T") == "success" and message.get("msg") == "authenticated"
            for message in messages
        )

    async def _connection(self, symbols: Iterable[str]) -> AsyncIterator[CanonicalEvent]:
        try:
            async with websockets.connect(
                self.url, open_timeout=self.timeout, ping_interval=20, ping_timeout=10
            ) as ws:
                self.connection_generation += 1
                await ws.send(
                    json.dumps({"action": "auth", "key": self.key_id, "secret": self.secret_key})
                )
                async with asyncio.timeout(self.timeout):
                    while not self.authenticated:
                        auth = json.loads(await ws.recv())
                        self.authenticated = self._authentication_result(auth)
                self.authenticated = True
                await ws.send(
                    json.dumps(
                        {"action": "subscribe", "trades": list(symbols), "quotes": list(symbols)}
                    )
                )
                async for raw in ws:
                    for message in json.loads(raw):
                        if message.get("T") == "error":
                            raise ProviderError("alpaca", "stream_rejected", False)
                        event = self._normalize(message)
                        if event is not None:
                            yield event
        except ProviderError:
            raise
        except websockets.ConnectionClosed as exc:
            raise ProviderError("alpaca", "connection_closed", True) from exc
        except (json.JSONDecodeError, KeyError, TypeError, ValueError) as exc:
            raise ProviderError("alpaca", "malformed_message", True) from exc

    def _normalize(self, message: dict[str, Any]) -> CanonicalEvent | None:
        kind = message.get("T")
        if kind not in {"t", "q"}:
            return None
        timestamp = datetime.fromisoformat(message["t"]).astimezone(UTC)
        symbol = str(message["S"]).upper()
        sequence = int(message.get("i", timestamp.timestamp() * 1_000_000_000))
        price = message.get("p", message.get("ap"))
        if price is None:
            return None
        ask = message.get("ap")
        bid = message.get("bp")
        quote_validity = (
            "VALID"
            if kind == "q"
            and bid is not None
            and ask is not None
            and float(bid) > 0
            and float(ask) >= float(bid)
            else "INVALID"
            if kind == "q"
            else None
        )
        payload = {
            "provider_event_kind": "trade" if kind == "t" else "quote",
            "timestamp_conversion": {
                "precision_converted": False,
                "timezone_converted": not str(message["t"]).endswith("Z"),
            },
            "price": price,
            "volume": int(message.get("s", 0)) if kind == "t" else 0,
            "spread": max(0, float(ask or price) - float(bid or price)),
            "conditions": tuple(message.get("c", [])),
            "exchange": message.get("x"),
            "quote_context": {"bid": bid, "ask": ask},
            "quote_validity": quote_validity,
            "feature_reasons": ("requires_historical_baseline", "requires_stateful_calculation"),
        }
        return CanonicalEvent(
            event_id=uuid5(NAMESPACE_URL, f"alpaca:{symbol}:{timestamp.isoformat()}:{sequence}"),
            kind=EventKind.EQUITY,
            symbol=symbol,
            source="alpaca",
            source_timestamp=timestamp,
            received_timestamp=datetime.now(UTC),
            normalized_timestamp=timestamp,
            sequence=sequence,
            payload=payload,
        )

    async def events(self, symbols: Iterable[str]) -> AsyncIterator[CanonicalEvent]:
        async for event in reconnecting_stream("alpaca", lambda: self._connection(symbols)):
            yield event

    async def historical_bootstrap(
        self, symbols: Iterable[str], session: date
    ) -> HistoricalBootstrap:
        """Load split-adjusted completed-session baselines from Alpaca bars."""
        requested = tuple(sorted({symbol.upper() for symbol in symbols}))
        start = session - timedelta(days=400)
        end = session - timedelta(days=1)
        headers = {
            "APCA-API-KEY-ID": self.key_id,
            "APCA-API-SECRET-KEY": self.secret_key,
        }

        async def fetch(timeframe: str) -> dict[str, list[dict[str, Any]]]:
            async with httpx.AsyncClient(
                base_url=self.historical_url, timeout=self.timeout
            ) as client:
                semaphore = asyncio.Semaphore(5)

                async def fetch_symbol(symbol: str) -> tuple[str, list[dict[str, Any]]]:
                    async with semaphore:
                        params: dict[str, str | int] = {
                            "symbols": symbol,
                            "timeframe": timeframe,
                            "start": f"{start.isoformat()}T00:00:00Z",
                            "end": f"{end.isoformat()}T23:59:59Z",
                            "adjustment": "split",
                            "feed": self.url.rsplit("/", 1)[-1],
                            "limit": 10000,
                        }
                        rows: list[dict[str, Any]] = []
                        token: str | None = None
                        while True:
                            if token is not None:
                                params["page_token"] = token
                            try:
                                response = await client.get(
                                    "/v2/stocks/bars", headers=headers, params=params
                                )
                            except httpx.TimeoutException as exc:
                                raise ProviderError("alpaca", "historical_timeout", True) from exc
                            if response.status_code != 200:
                                raise ProviderError(
                                    "alpaca", f"historical_http_{response.status_code}", False
                                )
                            body = response.json()
                            if not isinstance(body, dict):
                                raise ProviderError("alpaca", "historical_malformed", False)
                            bars = body.get("bars", {})
                            if isinstance(bars, dict):
                                symbol_rows = bars.get(symbol, [])
                                if isinstance(symbol_rows, list):
                                    rows.extend(row for row in symbol_rows if isinstance(row, dict))
                            token_value = body.get("next_page_token")
                            token = str(token_value) if token_value else None
                            if token is None:
                                return symbol, rows

                fetched = await asyncio.gather(*(fetch_symbol(symbol) for symbol in requested))
                return dict(fetched)

        daily = await fetch("1Day")
        minute = await fetch("1Min")
        previous: dict[str, PreviousClose] = {}
        highs: dict[str, dict[str, Decimal]] = {}
        profiles: dict[str, dict[int, tuple[Decimal, ...]]] = {}
        for symbol in requested:
            daily_rows = sorted(daily[symbol], key=lambda row: str(row.get("t", "")))
            completed: list[tuple[str, Decimal, Decimal]] = []
            for row in daily_rows:
                timestamp = datetime.fromisoformat(str(row["t"]))
                local_day = timestamp.astimezone(ET).date()
                if local_day >= session:
                    continue
                close = Decimal(str(row["c"]))
                high = Decimal(str(row.get("h", row["c"])))
                completed.append((local_day.isoformat(), close, high))
            if not completed:
                raise ProviderError("alpaca", "historical_baseline_missing", False)
            last_day, last_close, _ = completed[-1]
            previous[symbol] = PreviousClose(
                symbol,
                last_close,
                datetime.fromisoformat(f"{last_day}T00:00:00+00:00"),
                "alpaca:stocks/bars:adjustment=split",
            )
            highs[symbol] = {day: high for day, _, high in completed[-252:]}
            completed_days = {day for day, _, _ in completed}
            by_day: dict[str, dict[int, int]] = {}
            for row in minute[symbol]:
                timestamp = datetime.fromisoformat(str(row["t"]))
                local = timestamp.astimezone(ET)
                if local.date().isoformat() not in completed_days or not (
                    datetime.min.time().replace(hour=9, minute=30)
                    <= local.time()
                    < datetime.min.time().replace(hour=16)
                ):
                    continue
                minute_index = (local.hour * 60 + local.minute) - 570
                by_day.setdefault(local.date().isoformat(), {})[minute_index] = int(row.get("v", 0))
            profile: dict[int, tuple[Decimal, ...]] = {}
            for minute_index in range(390):
                profile[minute_index] = tuple(
                    Decimal(sum(volumes.get(index, 0) for index in range(minute_index + 1)))
                    for volumes in by_day.values()
                )
            profiles[symbol] = profile
        return HistoricalBootstrap(
            session=session.isoformat(),
            previous_closes=previous,
            cumulative_profiles=profiles,
            completed_highs=highs,
            adjustment="split",
            source_provenance="alpaca:stocks/bars:completed-regular-sessions",
        )

    async def current_prices(self, symbols: Iterable[str]) -> dict[str, Decimal]:
        """Read current last-trade prices for moneyness selection."""
        requested = tuple(sorted({symbol.upper() for symbol in symbols}))
        headers = {
            "APCA-API-KEY-ID": self.key_id,
            "APCA-API-SECRET-KEY": self.secret_key,
        }
        async with httpx.AsyncClient(base_url=self.historical_url, timeout=self.timeout) as client:
            try:
                response = await client.get(
                    "/v2/stocks/snapshots",
                    headers=headers,
                    params={"symbols": ",".join(requested), "feed": self.url.rsplit("/", 1)[-1]},
                )
            except httpx.TimeoutException as exc:
                raise ProviderError("alpaca", "snapshot_timeout", True) from exc
        if response.status_code != 200:
            raise ProviderError("alpaca", f"snapshot_http_{response.status_code}", False)
        body: object = response.json()
        if not isinstance(body, dict):
            raise ProviderError("alpaca", "snapshot_malformed", False)
        prices: dict[str, Decimal] = {}
        for symbol, snapshot in body.items():
            if not isinstance(snapshot, dict):
                continue
            trade = snapshot.get("latestTrade") or snapshot.get("latest_trade")
            if isinstance(trade, dict) and trade.get("p") is not None:
                prices[str(symbol).upper()] = Decimal(str(trade["p"]))
        return prices

    async def health(self) -> dict[str, object]:
        return {
            "provider": "alpaca",
            "configured": bool(self.key_id and self.secret_key),
            "url": self.url,
        }
