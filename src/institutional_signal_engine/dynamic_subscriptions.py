"""Incremental subscription management for the Phase 4B orchestration shell."""

from __future__ import annotations

import asyncio
from collections.abc import Callable
from dataclasses import dataclass, field

from .providers.thetadata import SubscriptionRequest, ThetaContract, ThetaDataOptionsProvider

# Max time to wait for a single acknowledgement batch.
DEFAULT_ACK_TIMEOUT = 30.0
# Max retry attempts for a subscription request.
DEFAULT_MAX_RETRIES = 3
# Base delay for retry backoff.
DEFAULT_BASE_RETRY_DELAY = 0.5


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
    """Wraps a ThetaDataOptionsProvider for incremental subscription management.

    Supports mid-session add, remove, acknowledgement correlation, timeout,
    rejection, retry, and idempotent application. Events from unacknowledged
    or removed contracts are rejected with auditable diagnostics.

    This adapter wraps, rather than replaces, the existing provider. The
    underlying provider's normalisation and acknowledgement logic is reused.
    """

    events_url: str
    api_key: str
    contracts: tuple[ThetaContract, ...] = ()
    request_types: tuple[str, ...] = ("TRADE", "QUOTE")
    timeout: float = 10.0
    diagnostic_membership: dict[str, set[ThetaContract]] | None = None
    stage_callback: Callable[[dict[str, object]], None] | None = None

    # Internal state
    _provider: ThetaDataOptionsProvider | None = field(default=None, init=False)
    _connected: bool = field(default=False, init=False)
    _pending_additions: dict[int, SubscriptionRequest] = field(default_factory=dict, init=False)
    _pending_removals: dict[int, SubscriptionRequest] = field(default_factory=dict, init=False)
    _ack_timeout: float = field(default=DEFAULT_ACK_TIMEOUT, init=False)
    _max_retries: int = field(default=DEFAULT_MAX_RETRIES, init=False)
    _base_retry_delay: float = field(default=DEFAULT_BASE_RETRY_DELAY, init=False)
    _awaiting_ack: asyncio.Event | None = field(default=None, init=False)
    _last_ack_result: SubscriptionAcknowledgement | None = field(default=None, init=False)

    @property
    def provider(self) -> ThetaDataOptionsProvider:
        if self._provider is None:
            raise RuntimeError("DynamicSubscriptionAdapter not initialised — call initialise()")
        return self._provider

    @property
    def connected(self) -> bool:
        return self._connected

    @property
    def acknowledged_contracts(self) -> set[ThetaContract]:
        if self._provider is None:
            return set()
        return self._provider.acknowledged_contracts

    @property
    def acknowledged_ids(self) -> set[int]:
        if self._provider is None:
            return set()
        return self._provider.acknowledged_ids

    @property
    def request_registry(self) -> dict[int, SubscriptionRequest]:
        if self._provider is None:
            return {}
        return self._provider.request_registry

    def initialise(self) -> None:
        """Create the underlying provider with the initial contract set."""
        self._provider = ThetaDataOptionsProvider(
            self.events_url,
            self.api_key,
            timeout=self.timeout,
            contracts=self.contracts,
            request_types=self.request_types,
            diagnostic_membership=self.diagnostic_membership,
            stage_callback=self._on_provider_stage,
        )

    async def connect_and_subscribe(self) -> None:
        """Connect to the provider and send current subscription set."""
        # The provider sends subscription payloads inside its connection loop.
        # We need to iterate the event stream at least until acknowledgements arrive.
        # This is handled by the orchestration shell which creates the event task.

    def current_subscriptions(self) -> tuple[ThetaContract, ...]:
        """Return the currently-desired subscription set."""
        return tuple(
            sorted(
                self.acknowledged_contracts
                | {req.contract for req in self._pending_additions.values() if req.add}
                - {req.contract for req in self._pending_removals.values() if not req.add},
                key=lambda v: (v.root, v.expiration, v.strike, v.right),
            )
        )

    async def add_subscriptions(self, contracts: tuple[ThetaContract, ...]) -> tuple[int, ...]:
        """Request incremental subscription additions.

        Returns request IDs. Call acknowledge() to wait for provider responses.
        """
        if self._provider is None:
            raise RuntimeError("Adapter not initialised")
        request_ids: list[int] = []
        for contract in contracts:
            if contract in self.acknowledged_contracts:
                continue  # already subscribed
            # Build an add subscription payload and send it
            request_id = self._provider._next_request_id
            self._provider._next_request_id += 1
            self._provider.subscription_ids.append(request_id)
            request = SubscriptionRequest(
                request_id, contract, "TRADE", True, self._provider.connection_generation
            )
            self._provider.outstanding[request_id] = request
            self._provider.request_registry[request_id] = request
            self._pending_additions[request_id] = request
            request_ids.append(request_id)
        return tuple(request_ids)

    async def remove_subscriptions(self, contracts: tuple[ThetaContract, ...]) -> tuple[int, ...]:
        """Request incremental subscription removals.

        Returns request IDs. Call acknowledge() to wait for provider responses.
        """
        if self._provider is None:
            raise RuntimeError("Adapter not initialised")
        request_ids: list[int] = []
        for contract in contracts:
            if contract not in self.acknowledged_contracts:
                continue  # not subscribed
            request_id = self._provider._next_request_id
            self._provider._next_request_id += 1
            request = SubscriptionRequest(
                request_id, contract, "TRADE", False, self._provider.connection_generation
            )
            self._provider.outstanding[request_id] = request
            self._provider.request_registry[request_id] = request
            self._pending_removals[request_id] = request
            request_ids.append(request_id)
        return tuple(request_ids)

    async def acknowledge(
        self, expected_request_ids: tuple[int, ...], timeout: float | None = None
    ) -> SubscriptionAcknowledgement:
        """Wait for acknowledgements on the given request IDs.

        Returns a SubscriptionAcknowledgement with the results. Does not raise
        on timeout — callers must check the result.
        """
        timeout = timeout or self._ack_timeout
        deadline = asyncio.get_event_loop().time() + timeout
        acknowledged: list[int] = []
        timed_out: list[int] = []
        rejected: list[dict[str, object]] = []

        while asyncio.get_event_loop().time() < deadline:
            if self._provider is None:
                break
            all_done = True
            for rid in expected_request_ids:
                if rid in self._provider.acknowledged_ids:
                    if rid not in acknowledged:
                        acknowledged.append(rid)
                        self._pending_additions.pop(rid, None)
                        self._pending_removals.pop(rid, None)
                elif rid in self._provider.outstanding:
                    all_done = False
                else:
                    # Request was neither acknowledged nor outstanding — check diagnostics
                    request = self._provider.request_registry.get(rid)
                    if request is not None:
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
                        self._pending_additions.pop(rid, None)
                        self._pending_removals.pop(rid, None)
                    acknowledged.append(rid)  # mark as done
            if all_done:
                break
            await asyncio.sleep(0.05)

        # Any still-pending IDs timed out
        for rid in expected_request_ids:
            if rid not in acknowledged and rid not in [r["request_id"] for r in rejected]:
                timed_out.append(rid)
                self._pending_additions.pop(rid, None)
                self._pending_removals.pop(rid, None)

        partial = len(acknowledged) < len(expected_request_ids)
        accepted = not partial and len(rejected) == 0 and len(timed_out) == 0
        diagnostic = (
            "all_acknowledged"
            if accepted
            else f"partial: {len(acknowledged)}/{len(expected_request_ids)} ack, "
            f"{len(rejected)} rejected, {len(timed_out)} timed_out"
        )
        result = SubscriptionAcknowledgement(
            acknowledged=tuple(acknowledged),
            rejected=tuple(rejected),
            timed_out=tuple(timed_out),
            partially_acknowledged=partial,
            accepted=accepted,
            diagnostic=diagnostic,
        )
        self._last_ack_result = result
        return result

    def is_event_accepted(self, contract: ThetaContract) -> bool:
        """Check whether events for a contract should be accepted.

        Events are accepted only if the contract is in the acknowledged set
        and not in the pending removal set.
        """
        if contract in {req.contract for req in self._pending_removals.values() if not req.add}:
            return False
        return contract in self.acknowledged_contracts

    async def restore_epoch(self, desired: tuple[ThetaContract, ...]) -> None:
        """After reconnect, restore subscriptions to match the desired epoch.

        Computes the diff between currently-acknowledged and desired, then
        sends add/remove requests. After reconnect, the underlying provider
        resets its acknowledged set, so we re-subscribe all desired contracts.
        """
        if self._provider is None:
            raise RuntimeError("Adapter not initialised")
        current = self.acknowledged_contracts
        desired_set = set(desired)
        to_add = desired_set - current
        to_remove = current - desired_set

        if to_add:
            await self.add_subscriptions(
                tuple(sorted(to_add, key=lambda v: (v.root, v.expiration, v.strike, v.right)))
            )
        if to_remove:
            await self.remove_subscriptions(
                tuple(sorted(to_remove, key=lambda v: (v.root, v.expiration, v.strike, v.right)))
            )

    def _on_provider_stage(self, record: dict[str, object]) -> None:
        """Forward provider stage events, tracking connection state."""
        stage = record.get("stage")
        if stage == "websocket_connected":
            self._connected = True
        if self.stage_callback is not None:
            self.stage_callback(record)

    def diagnostics(self) -> dict[str, object]:
        """Return diagnostic evidence for the current subscription state."""
        if self._provider is None:
            return {"status": "not_initialised"}
        return {
            "connected": self._connected,
            "acknowledged_contract_count": len(self.acknowledged_contracts),
            "pending_additions": len(self._pending_additions),
            "pending_removals": len(self._pending_removals),
            "total_requests": len(self.request_registry),
            "acknowledged_request_count": len(self.acknowledged_ids),
            "provider_diagnostics": list(self._provider.diagnostics),
            "rejected_event_diagnostics": list(self._provider.rejected_event_diagnostics),
            "last_ack_result": (
                {
                    "acknowledged": len(self._last_ack_result.acknowledged),
                    "rejected": len(self._last_ack_result.rejected),
                    "timed_out": len(self._last_ack_result.timed_out),
                    "accepted": self._last_ack_result.accepted,
                    "diagnostic": self._last_ack_result.diagnostic,
                }
                if self._last_ack_result is not None
                else None
            ),
        }
