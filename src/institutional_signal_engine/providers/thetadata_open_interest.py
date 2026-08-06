"""Typed ThetaData dated open-interest snapshot evidence adapter."""

from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from typing import Any

import httpx

from .common import ProviderError
from .thetadata import ThetaContract


@dataclass(frozen=True)
class OpenInterestEvidence:
    contract: ThetaContract
    open_interest: int
    reported_at: datetime
    effective_date: date
    source: str = "THETADATA_OPRA_SNAPSHOT"
    verified_as_of: bool = True


def _observed_previous_session(value: date) -> date:
    candidate = value - timedelta(days=1)
    while candidate.weekday() >= 5 or candidate in _market_holidays(candidate.year):
        candidate -= timedelta(days=1)
    return candidate


def _market_holidays(year: int) -> set[date]:
    """Return the observed US market holidays needed for dated OI joins."""
    fixed = {date(year, 1, 1), date(year, 6, 19), date(year, 7, 4), date(year, 12, 25)}
    observed = set(fixed)
    for holiday in fixed:
        if holiday.weekday() == 5:
            observed.add(holiday - timedelta(days=1))
        elif holiday.weekday() == 6:
            observed.add(holiday + timedelta(days=1))

    def nth_weekday(month: int, weekday: int, ordinal: int) -> date:
        first = date(year, month, 1)
        return first + timedelta(days=(weekday - first.weekday()) % 7 + 7 * (ordinal - 1))

    def last_weekday(month: int, weekday: int) -> date:
        next_month = date(year + (month == 12), (month % 12) + 1, 1)
        return next_month - timedelta(days=(next_month.weekday() - weekday) % 7 + 1)

    observed.update(
        {
            nth_weekday(1, 0, 3),  # Martin Luther King Jr. Day
            nth_weekday(2, 0, 3),  # Washington's Birthday
            last_weekday(5, 0),  # Memorial Day
            nth_weekday(9, 0, 1),  # Labor Day
            nth_weekday(11, 3, 4),  # Thanksgiving
            # Good Friday is two days before Easter; use the Gregorian algorithm.
        }
    )
    a = year % 19
    b = year // 100
    c = year % 100
    d = b // 4
    e = b % 4
    f = (b + 8) // 25
    g = (b - f + 1) // 3
    h = (19 * a + b - d - g + 15) % 30
    i = c // 4
    k = c % 4
    l = (32 + 2 * e + 2 * i - h - k) % 7
    m = (a + 11 * h + 22 * l) // 451
    month = (h + l - 7 * m + 114) // 31
    day = ((h + l - 7 * m + 114) % 31) + 1
    observed.add(date(year, month, day) - timedelta(days=2))
    return observed


class ThetaDataOpenInterestProvider:
    def __init__(self, base_url: str = "http://127.0.0.1:25503/v3", timeout: float = 10.0) -> None:
        self.base_url = base_url.rstrip("/")
        self.timeout = timeout
        self.last_diagnostic: dict[str, object] = {}

    async def mdss_status(self) -> dict[str, object]:
        try:
            async with httpx.AsyncClient(timeout=self.timeout) as client:
                response = await client.get(f"{self.base_url}/terminal/mdds/status")
        except httpx.HTTPError as exc:
            raise ProviderError("thetadata", "mdss_status_transport", True) from exc
        return {
            "http_status": response.status_code,
            "content_type": response.headers.get("content-type", ""),
            "status": response.text.strip() if response.status_code == 200 else "unavailable",
        }

    async def snapshot(self, contract: ThetaContract) -> OpenInterestEvidence:
        params = {
            "symbol": contract.root,
            "expiration": f"{str(contract.expiration)[:4]}-{str(contract.expiration)[4:6]}-{str(contract.expiration)[6:]}",
            "strike": str(Decimal(contract.strike) / Decimal(1000)),
            "right": "call" if contract.right == "C" else "put",
            "format": "json",
        }
        try:
            async with httpx.AsyncClient(timeout=self.timeout) as client:
                response = await client.get(
                    f"{self.base_url}/option/snapshot/open_interest", params=params
                )
        except httpx.HTTPError as exc:
            raise ProviderError("thetadata", "open_interest_transport", True) from exc
        content_type = getattr(response, "headers", {}).get("content-type", "")
        try:
            diagnostic_body: object = response.json()
        except ValueError:
            diagnostic_body = None
        self.last_diagnostic = {
            "http_status": response.status_code,
            "content_type": content_type,
            "response_shape": type(diagnostic_body).__name__
            if diagnostic_body is not None
            else "text",
            "row_count": len(diagnostic_body) if isinstance(diagnostic_body, list) else 0,
        }
        if response.status_code != 200:
            raise ProviderError("thetadata", f"open_interest_http_{response.status_code}", False)
        try:
            body: Any = response.json()
        except ValueError as exc:
            raise ProviderError("thetadata", "open_interest_malformed", False) from exc
        rows = (
            body
            if isinstance(body, list)
            else body.get("response", [])
            if isinstance(body, dict)
            else []
        )
        if not isinstance(rows, list) or not rows:
            raise ProviderError("thetadata", "open_interest_missing", False)
        for row in rows:
            if not isinstance(row, dict):
                continue
            returned = ThetaContract(
                str(row.get("symbol", contract.root)).upper(),
                int(str(row["expiration"]).replace("-", "")),
                int(Decimal(str(row["strike"])) * Decimal(1000)),
                "C" if str(row.get("right", "")).lower() in {"call", "c"} else "P",
            )
            if returned != contract:
                raise ProviderError("thetadata", "open_interest_identity_mismatch", False)
            value = int(row.get("open_interest", 0))
            if value <= 0:
                raise ProviderError("thetadata", "open_interest_missing_or_zero", False)
            parsed_timestamp = datetime.fromisoformat(str(row["timestamp"]))
            reported_at = (
                parsed_timestamp.replace(tzinfo=UTC)
                if parsed_timestamp.tzinfo is None
                else parsed_timestamp.astimezone(UTC)
            )
            return OpenInterestEvidence(
                contract, value, reported_at, _observed_previous_session(reported_at.date())
            )
        raise ProviderError("thetadata", "open_interest_missing", False)
