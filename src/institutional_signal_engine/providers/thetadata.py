"""ThetaData v3 options adapter using the official Terminal event contract."""

import json
from collections.abc import AsyncIterator, Iterable
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import Any
from uuid import NAMESPACE_URL, uuid5
from zoneinfo import ZoneInfo

import websockets

from ..schemas import CanonicalEvent, EventKind
from .common import ProviderError, reconnecting_stream


class ThetaDataOptionsProvider:
    """Connects to one local Theta Terminal v3 ``/v1/events`` socket.

    Theta's current API authenticates the Terminal with ``THETADATA_API_KEY``;
    the socket itself accepts STREAM requests and emits JSON header/contract/data
    messages. The key is intentionally not included in socket payloads.
    """

    def __init__(self, events_url: str, api_key: str, timeout: float = 10.0) -> None:
        self.events_url, self.api_key, self.timeout = events_url, api_key, timeout
        self.connected = False
        self.subscription_acknowledged = False
        self.stream_status = "not_connected"

    async def _connection(self, symbols: Iterable[str]) -> AsyncIterator[CanonicalEvent]:
        try:
            async with websockets.connect(
                self.events_url, open_timeout=self.timeout, ping_interval=20, ping_timeout=10
            ) as ws:
                self.connected = True
                self.subscription_acknowledged = False
                self.stream_status = "connected"
                roots = {symbol.upper() for symbol in symbols}
                await ws.send(
                    json.dumps(
                        {
                            "msg_type": "STREAM_BULK",
                            "sec_type": "OPTION",
                            "req_type": "TRADE",
                            "add": True,
                            "id": 0,
                        }
                    )
                )
                async for raw in ws:
                    message = json.loads(raw)
                    header = message.get("header", {})
                    if header.get("status") in {"ERROR", "UNAUTHORIZED", "DENIED"}:
                        raise ProviderError("thetadata", "stream_rejected", False)
                    if self._observe_control(message):
                        continue
                    event = self._normalize(message)
                    if event is not None and (not roots or event.symbol in roots):
                        yield event
        except ProviderError:
            raise
        except websockets.ConnectionClosed as exc:
            raise ProviderError("thetadata", "connection_closed", True) from exc
        except (json.JSONDecodeError, KeyError, TypeError, ValueError) as exc:
            raise ProviderError("thetadata", "malformed_message", True) from exc

    def _observe_control(self, message: dict[str, Any]) -> bool:
        """Record sanitized stream control state without retaining payload data."""
        header = message.get("header", {})
        message_type = header.get("type")
        status = str(header.get("status", "unknown")).lower()
        if message_type == "REQ_RESPONSE":
            self.subscription_acknowledged = status == "connected"
            self.stream_status = status
            return True
        if message_type == "STATUS":
            self.stream_status = status
            return True
        return False

    def _normalize(self, message: dict[str, Any]) -> CanonicalEvent | None:
        if message.get("header", {}).get("type") not in {"TRADE", "QUOTE"}:
            return None
        contract = message["contract"]
        symbol = str(contract["root"]).upper()
        data = message.get("trade") or message.get("quote") or {}
        date = str(data["date"])
        seconds = int(data["ms_of_day"]) / 1000
        timestamp = (
            datetime.strptime(date, "%Y%m%d").replace(tzinfo=ZoneInfo("America/New_York"))
            + timedelta(seconds=seconds)
        ).astimezone(UTC)
        sequence = int(data.get("sequence", data.get("ms_of_day", 0))) & 0xFFFFFFFF
        size = int(data.get("size", 0))
        price = Decimal(str(data.get("price", 0)))
        payload = {
            "option_volume": size,
            "open_interest": int(data.get("open_interest", 0)),
            "call_premium": data.get("call_premium", price * size * 100),
            "distance_to_resistance": data.get("distance_to_resistance", 0),
        }
        return CanonicalEvent(
            event_id=uuid5(NAMESPACE_URL, f"thetadata:{symbol}:{timestamp.isoformat()}:{sequence}"),
            kind=EventKind.OPTIONS,
            symbol=symbol,
            source="thetadata",
            source_timestamp=timestamp,
            received_timestamp=datetime.now(UTC),
            normalized_timestamp=timestamp,
            sequence=sequence,
            payload=payload,
        )

    async def events(self, symbols: Iterable[str]) -> AsyncIterator[CanonicalEvent]:
        async for event in reconnecting_stream("thetadata", lambda: self._connection(symbols)):
            yield event

    async def health(self) -> dict[str, object]:
        return {
            "provider": "thetadata",
            "configured": bool(self.api_key),
            "events_url": self.events_url,
        }
