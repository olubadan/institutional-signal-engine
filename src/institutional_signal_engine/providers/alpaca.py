"""Alpaca live equities market-data WebSocket adapter.

Authentication follows Alpaca's official Trading/Broker WebSocket contract:
the key pair is sent in an auth message within ten seconds of connection.
"""

import asyncio
import json
from collections.abc import AsyncIterator, Iterable
from datetime import UTC, datetime
from typing import Any
from uuid import NAMESPACE_URL, uuid5

import websockets

from ..schemas import CanonicalEvent, EventKind
from .common import ProviderError, reconnecting_stream


class AlpacaEquitiesProvider:
    def __init__(self, url: str, key_id: str, secret_key: str, timeout: float = 10.0) -> None:
        self.url, self.key_id, self.secret_key, self.timeout = url, key_id, secret_key, timeout
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

    async def health(self) -> dict[str, object]:
        return {
            "provider": "alpaca",
            "configured": bool(self.key_id and self.secret_key),
            "url": self.url,
        }
