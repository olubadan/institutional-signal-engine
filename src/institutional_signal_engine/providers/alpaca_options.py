"""Alpaca option-contract discovery with complete pagination."""

from collections.abc import AsyncIterator, Iterable
from datetime import UTC, datetime

import httpx

from ..contract_mapping import AlpacaOptionContract
from .common import ProviderError


class AlpacaOptionsContractProvider:
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

    async def discover_active_calls(
        self,
        symbols: Iterable[str],
        limit: int = 100,
    ) -> AsyncIterator[AlpacaOptionContract]:
        requested = tuple(sorted({symbol.upper() for symbol in symbols}))
        params: dict[str, str | int] = {
            "underlying_symbols": ",".join(requested),
            "type": "call",
            "status": "active",
            "tradable": "true",
            "limit": limit,
        }
        headers = {
            "APCA-API-KEY-ID": self.key_id,
            "APCA-API-SECRET-KEY": self.secret_key,
        }
        token: str | None = None
        async with httpx.AsyncClient(base_url=self.api_base_url, timeout=self.timeout) as client:
            while True:
                if token is not None:
                    params["page_token"] = token
                try:
                    response = await client.get(
                        "/v2/options/contracts", headers=headers, params=params
                    )
                except httpx.HTTPError as exc:
                    raise ProviderError("alpaca", "contract_discovery_transport", True) from exc
                if response.status_code != 200:
                    raise ProviderError(
                        "alpaca", f"contract_discovery_http_{response.status_code}", False
                    )
                body: object = response.json()
                if not isinstance(body, dict) or not isinstance(body.get("option_contracts"), list):
                    raise ProviderError("alpaca", "contract_discovery_malformed_page", False)
                received_at = datetime.now(UTC)
                for raw in body["option_contracts"]:
                    if not isinstance(raw, dict):
                        raise ProviderError(
                            "alpaca", "contract_discovery_malformed_contract", False
                        )
                    try:
                        contract = AlpacaOptionContract.from_payload(raw, received_at)
                    except (TypeError, ValueError) as exc:
                        raise ProviderError(
                            "alpaca", "contract_discovery_incomplete_contract", False
                        ) from exc
                    if contract.underlying_symbol in requested:
                        yield contract
                next_token = body.get("next_page_token")
                token = str(next_token) if next_token else None
                if token is None:
                    return
