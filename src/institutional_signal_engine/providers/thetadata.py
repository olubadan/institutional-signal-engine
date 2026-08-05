"""ThetaData v3 options adapter using the official Terminal event contract."""

import json
from collections.abc import AsyncIterator, Iterable
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import Any
from uuid import NAMESPACE_URL, uuid5
from zoneinfo import ZoneInfo

import websockets

from ..schemas import CanonicalEvent, EventKind
from .common import ProviderError, reconnecting_stream


@dataclass(frozen=True)
class ThetaContract:
    root: str
    expiration: int
    strike: int
    right: str

    @classmethod
    def from_dollars(
        cls, root: str, expiration: int, strike: Decimal, right: str
    ) -> "ThetaContract":
        scaled = strike * Decimal(1000)
        if scaled != scaled.to_integral_value():
            raise ValueError("strike must convert exactly to tenths of a cent")
        return cls(root, expiration, int(scaled), right)

    def payload(self, request_id: int, add: bool = True) -> dict[str, object]:
        if self.right not in {"C", "P"} or self.strike <= 0:
            raise ValueError("invalid Theta contract")
        return {
            "msg_type": "STREAM",
            "sec_type": "OPTION",
            "req_type": "TRADE",
            "add": add,
            "id": request_id,
            "contract": {
                "root": self.root.upper(),
                "expiration": self.expiration,
                "strike": self.strike,
                "right": self.right,
            },
        }


SMOKE_AAPL_CONTRACT = ThetaContract("AAPL", 20260807, 310000, "C")


@dataclass(frozen=True)
class SubscriptionRequest:
    request_id: int
    contract: ThetaContract
    req_type: str
    add: bool
    generation: int


class ThetaDataOptionsProvider:
    """Connects to one local Theta Terminal v3 ``/v1/events`` socket.

    Theta's current API authenticates the Terminal with ``THETADATA_API_KEY``;
    the socket itself accepts STREAM requests and emits JSON header/contract/data
    messages. The key is intentionally not included in socket payloads.
    """

    def __init__(
        self,
        events_url: str,
        api_key: str,
        timeout: float = 10.0,
        contracts: Iterable[ThetaContract] = (),
    ) -> None:
        self.events_url, self.api_key, self.timeout = events_url, api_key, timeout
        self.contracts = tuple(contracts)
        self._next_request_id = 1
        self.subscription_ids: list[int] = []
        self.acknowledged_ids: set[int] = set()
        self.outstanding: dict[int, SubscriptionRequest] = {}
        self.diagnostics: list[str] = []
        self.connection_generation = 0
        self.connected = False
        self.subscription_acknowledged = False
        self.stream_status = "not_connected"

    async def _connection(self, symbols: Iterable[str]) -> AsyncIterator[CanonicalEvent]:
        try:
            async with websockets.connect(
                self.events_url, open_timeout=self.timeout, ping_interval=20, ping_timeout=10
            ) as ws:
                self.connected = True
                self.connection_generation += 1
                self.subscription_acknowledged = False
                self.stream_status = "connected"
                roots = {symbol.upper() for symbol in symbols}
                for request in self.subscription_payloads():
                    await ws.send(json.dumps(request))
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
        message_id = message.get("id", header.get("req_id"))
        if message_type == "REQ_RESPONSE":
            self.stream_status = status
            if not isinstance(message_id, int) or message_id not in self.outstanding:
                self.diagnostics.append("unmatched_request_response")
                return True
            request = self.outstanding[message_id]
            response_contract = message.get("contract")
            if (
                response_contract is not None
                and response_contract != request.contract.payload(message_id)["contract"]
            ):
                self.diagnostics.append("contradictory_request_response")
                return True
            if status in {"connected", "success", "ok"}:
                del self.outstanding[message_id]
                self.acknowledged_ids.add(message_id)
                self.subscription_acknowledged = True
            else:
                self.diagnostics.append("request_rejected")
            return True
        if message_type == "STATUS":
            self.stream_status = status
            return True
        return False

    def unsubscribe_payload(self, contract: ThetaContract) -> dict[str, object]:
        request_id = self._next_request_id
        self._next_request_id += 1
        payload = contract.payload(request_id, add=False)
        self.outstanding[request_id] = SubscriptionRequest(
            request_id, contract, "TRADE", False, self.connection_generation
        )
        return payload

    def subscription_payloads(self) -> tuple[dict[str, object], ...]:
        """Build Standard-compatible subscriptions on every connection/reconnect."""
        requests = []
        for contract in self.contracts:
            request_id = self._next_request_id
            self._next_request_id += 1
            self.subscription_ids.append(request_id)
            self.outstanding[request_id] = SubscriptionRequest(
                request_id, contract, "TRADE", True, self.connection_generation
            )
            requests.append(contract.payload(request_id))
        return tuple(requests)

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
            "trade_size": size,
            "trade_price": price,
            "condition_code": data.get("condition_code", data.get("condition")),
            "raw_exchange_condition": data.get("condition"),
            "quote_context": {"bid": data.get("bid"), "ask": data.get("ask")},
            "contract": {
                "root": symbol,
                "expiration": contract.get("expiration"),
                "strike": contract.get("strike"),
                "right": contract.get("right"),
            },
            "feature_reasons": ("requires_open_interest", "requires_resistance_definition"),
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
