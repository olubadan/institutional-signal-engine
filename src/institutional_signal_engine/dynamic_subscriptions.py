"""Incremental subscription management with owned WebSocket control channel.

The DynamicSubscriptionAdapter owns a live ThetaData WebSocket connection
and transmits actual STREAM add/remove payloads. It never manipulates
ThetaDataOptionsProvider private fields — it uses its own connection,
request registry, and acknowledgement state.
"""

from __future__ import annotations

import asyncio
import json
from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Any

import websockets

from .providers.common import ProviderError
from .providers.thetadata import SubscriptionRequest, ThetaContract

DEFAULT_ACK_TIMEOUT = 30.0
DEFAULT_CONNECT_TIMEOUT = 10.0


@dataclass
class SubscriptionAcknowledgement:
    """Result of an acknowledgement wait."""

    acknowledged: tuple[int, ...]
    rejected: tuple[dict[str, object], ...]
    timed_out: tuple[int, ...]
    partially_acknowledged: bool
    accepted: bool
    diagnostic: str

    @property
    def all_acknowledged(self) -> bool:
        return len(self.rejected) == 0 and len(self.timed_out) == 0


@dataclass
class DynamicSubscriptionAdapter:
    """Owns a live ThetaData WebSocket for incremental subscription control.

    This adapter maintains its own WebSocket connection, request registry,
    and acknowledgement state. It sends STREAM add/remove messages and
    correlates REQ_RESPONSE acknowledgements. It does not access
    ThetaDataOptionsProvider private fields.

    Events from unacknowledged or removed contracts are rejected.
    """

    events_url: str
    api_key: str
    request_types: tuple[str, ...] = ("TRADE", "QUOTE")
    connect_timeout: float = DEFAULT_CONNECT_TIMEOUT
    stage_callback: Callable[[dict[str, object]], None] | None = None

    # Internal mutable state
    _ws: Any = field(default=None, init=False)
    _connected: bool = field(default=False, init=False)
    _next_request_id: int = field(default=1, init=False)
    _connection_generation: int = field(default=0, init=False)
    _request_registry: dict[int, SubscriptionRequest] = field(default_factory=dict, init=False)
    _acknowledged_ids: set[int] = field(default_factory=set, init=False)
    _acknowledged_contracts: set[ThetaContract] = field(default_factory=set, init=False)
    _outstanding: dict[int, SubscriptionRequest] = field(default_factory=dict, init=False)
    _diagnostics: list[str] = field(default_factory=list, init=False)
    _pending_ack_event: asyncio.Event = field(default_factory=asyncio.Event, init=False)

    # ---- connection lifecycle ----

    async def connect(self) -> None:
        """Open the WebSocket and send initial subscription payloads."""
        if self._connected:
            return
        try:
            self._ws = await websockets.connect(
                self.events_url,
                open_timeout=self.connect_timeout,
                ping_interval=20,
                ping_timeout=10,
            )
        except (TimeoutError, OSError) as exc:
            raise ProviderError("thetadata", "connection_failed", True) from exc
        self._connected = True
        self._connection_generation += 1
        if self.stage_callback is not None:
            self.stage_callback({"stage": "websocket_connected"})

    async def disconnect(self) -> None:
        """Close the WebSocket."""
        if self._ws is not None:
            try:
                await self._ws.close()
            except (OSError, RuntimeError):
                self._diagnostics.append("disconnect_close_error")
        self._connected = False
        self._ws = None

    @property
    def connected(self) -> bool:
        return self._connected

    @property
    def connection_generation(self) -> int:
        return self._connection_generation

    @property
    def acknowledged_contracts(self) -> set[ThetaContract]:
        return self._acknowledged_contracts

    @property
    def acknowledged_ids(self) -> set[int]:
        return self._acknowledged_ids

    @property
    def request_registry(self) -> dict[int, SubscriptionRequest]:
        return dict(self._request_registry)

    @property
    def subscription_acknowledged(self) -> bool:
        return len(self._outstanding) == 0 and len(self._acknowledged_ids) > 0

    # ---- subscription commands ----

    async def add_subscriptions(self, contracts: tuple[ThetaContract, ...]) -> tuple[int, ...]:
        """Send paired TRADE+QUOTE STREAM add requests for every contract.

        Returns the request IDs for later acknowledgement correlation.
        """
        if not self._connected or self._ws is None:
            raise ProviderError("thetadata", "not_connected", True)

        request_ids: list[int] = []
        for contract in contracts:
            if contract in self._acknowledged_contracts:
                continue
            for req_type in self.request_types:
                rid = self._next_request_id
                self._next_request_id += 1
                payload = contract.payload(rid, add=True, req_type=req_type)
                request = SubscriptionRequest(
                    rid, contract, req_type, True, self._connection_generation
                )
                self._outstanding[rid] = request
                self._request_registry[rid] = request
                request_ids.append(rid)
                await self._ws.send(json.dumps(payload))
        return tuple(request_ids)

    async def remove_subscriptions(self, contracts: tuple[ThetaContract, ...]) -> tuple[int, ...]:
        """Send paired TRADE+QUOTE STREAM remove requests for every contract.

        Returns the request IDs for later acknowledgement correlation.
        """
        if not self._connected or self._ws is None:
            raise ProviderError("thetadata", "not_connected", True)

        request_ids: list[int] = []
        for contract in contracts:
            if contract not in self._acknowledged_contracts:
                continue
            for req_type in self.request_types:
                rid = self._next_request_id
                self._next_request_id += 1
                payload = contract.payload(rid, add=False, req_type=req_type)
                request = SubscriptionRequest(
                    rid, contract, req_type, False, self._connection_generation
                )
                self._outstanding[rid] = request
                self._request_registry[rid] = request
                request_ids.append(rid)
                await self._ws.send(json.dumps(payload))
        return tuple(request_ids)

    async def acknowledge(
        self, expected_request_ids: tuple[int, ...], timeout: float | None = None
    ) -> SubscriptionAcknowledgement:
        """Wait for REQ_RESPONSE acknowledgements on the given request IDs.

        Does not raise on timeout — callers check the result.
        """
        timeout = timeout or DEFAULT_ACK_TIMEOUT
        deadline = asyncio.get_event_loop().time() + timeout
        acknowledged: list[int] = []
        timed_out: list[int] = []
        rejected: list[dict[str, object]] = []

        while asyncio.get_event_loop().time() < deadline:
            all_done = True
            for rid in expected_request_ids:
                if rid in self._acknowledged_ids:
                    if rid not in acknowledged:
                        acknowledged.append(rid)
                        self._outstanding.pop(rid, None)
                elif rid in self._outstanding:
                    all_done = False
                elif rid in self._request_registry:
                    request = self._request_registry[rid]
                    rejected.append(
                        {
                            "request_id": rid,
                            "contract": {
                                "root": request.contract.root,
                                "expiration": request.contract.expiration,
                                "strike": request.contract.strike,
                                "right": request.contract.right,
                            },
                            "req_type": request.req_type,
                            "reason": "rejected_or_unmatched",
                        }
                    )
                    self._outstanding.pop(rid, None)
                else:
                    timed_out.append(rid)
            if all_done:
                break
            await asyncio.sleep(0.05)

        for rid in expected_request_ids:
            if rid not in acknowledged and rid not in [r["request_id"] for r in rejected]:
                if rid not in timed_out:
                    timed_out.append(rid)
                self._outstanding.pop(rid, None)

        partial = len(acknowledged) < len(expected_request_ids)
        accepted = not partial and len(rejected) == 0 and len(timed_out) == 0
        diagnostic = (
            "all_acknowledged"
            if accepted
            else f"partial: {len(acknowledged)}/{len(expected_request_ids)} ack, "
            f"{len(rejected)} rejected, {len(timed_out)} timed_out"
        )
        return SubscriptionAcknowledgement(
            acknowledged=tuple(acknowledged),
            rejected=tuple(rejected),
            timed_out=tuple(timed_out),
            partially_acknowledged=partial,
            accepted=accepted,
            diagnostic=diagnostic,
        )

    # ---- control message processing ----

    def observe_control(self, raw_message: str) -> bool:
        """Process a raw WebSocket message as a potential control frame.

        Returns True if the message was consumed as a control frame.
        Returns False if it should be treated as a data event.
        """
        try:
            message: dict[str, Any] = json.loads(raw_message)
        except (json.JSONDecodeError, TypeError):
            return True  # Consume unparseable messages

        header = message.get("header", {})
        message_type = header.get("type")
        status = str(header.get("status", "unknown")).lower()

        if message_type == "REQ_RESPONSE":
            return self._handle_req_response(header)
        if message_type == "STATUS":
            return True  # Consume keepalive status frames
        if message_type == "ERROR":
            self._diagnostics.append(f"stream_error:{status}")
            return True
        if header.get("status") in {"ERROR", "UNAUTHORIZED", "DENIED"}:
            self._diagnostics.append(f"stream_rejected:{status}")
            return True
        return False

    def _handle_req_response(self, header: dict[str, Any]) -> bool:
        response = str(header.get("response", "")).upper()
        req_id = header.get("req_id")
        if not isinstance(req_id, int) or req_id not in self._request_registry:
            self._diagnostics.append("unmatched_request_response")
            return True
        if req_id in self._acknowledged_ids:
            self._diagnostics.append("duplicate_request_response")
            return True
        request = self._request_registry[req_id]
        if response == "SUBSCRIBED":
            self._outstanding.pop(req_id, None)
            self._acknowledged_ids.add(req_id)
            self._acknowledged_contracts.add(request.contract)
        elif response in {"ERROR", "MAX_STREAMS_REACHED", "INVALID_PERMS"}:
            self._diagnostics.append(f"request_rejected:{response.lower()}")
        else:
            self._diagnostics.append("unknown_request_response")
        return True

    # ---- event acceptance ----

    def is_event_accepted(self, contract: ThetaContract) -> bool:
        """Check whether events for a contract should be accepted.

        Events are accepted only if the contract is in the acknowledged set.
        """
        return contract in self._acknowledged_contracts

    # ---- reconnect and restoration ----

    async def restore_epoch(self, desired: tuple[ThetaContract, ...]) -> None:
        """After reconnect, restore subscriptions to match the desired epoch.

        Reconnects, then re-subscribes all desired contracts.
        """
        if self._connected:
            await self.disconnect()
        await self.connect()
        await self.add_subscriptions(desired)

    def diagnostics(self) -> dict[str, object]:
        """Return diagnostic evidence for the current subscription state."""
        return {
            "connected": self._connected,
            "connection_generation": self._connection_generation,
            "acknowledged_contract_count": len(self._acknowledged_contracts),
            "total_requests": len(self._request_registry),
            "acknowledged_request_count": len(self._acknowledged_ids),
            "outstanding_count": len(self._outstanding),
            "subscription_acknowledged": self.subscription_acknowledged,
            "adapter_diagnostics": list(self._diagnostics),
        }
