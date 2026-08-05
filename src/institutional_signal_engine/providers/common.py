"""Bounded retry, heartbeat, staleness, and sanitized provider failures."""

import asyncio
import logging
import random
from collections.abc import AsyncIterator, Callable
from dataclasses import dataclass

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class ProviderError(Exception):
    provider: str
    category: str
    retryable: bool
    status: int | None = None

    def __str__(self) -> str:
        suffix = f" status={self.status}" if self.status is not None else ""
        return (
            f"provider={self.provider} category={self.category} retryable={self.retryable}{suffix}"
        )


def classify_http_failure(provider: str, status: int) -> ProviderError:
    """Return a secret-free, deterministic classification for provider status."""
    if status == 429:
        return ProviderError(provider, "rate_limited", True, status)
    if status in {408, 425} or status >= 500:
        return ProviderError(provider, "temporary_http_failure", True, status)
    if status in {401, 403}:
        return ProviderError(provider, "authentication_or_entitlement_failed", False, status)
    return ProviderError(provider, "provider_rejected_request", False, status)


async def reconnecting_stream[Item](
    provider: str,
    connect: Callable[[], AsyncIterator[Item]],
    *,
    max_attempts: int = 5,
    base_delay: float = 0.5,
) -> AsyncIterator[Item]:
    attempt = 0
    while attempt < max_attempts:
        try:
            stream = connect()
            attempt = 0
            async for item in stream:
                yield item
            return
        except ProviderError as exc:
            logger.warning("provider_failure", extra={"provider": provider, "error": str(exc)})
            if not exc.retryable:
                raise
            attempt += 1
        except (TimeoutError, OSError) as exc:
            logger.warning(
                "provider_transport_failure",
                extra={"provider": provider, "error": type(exc).__name__},
            )
            attempt += 1
        if attempt >= max_attempts:
            raise ProviderError(provider, "reconnect_exhausted", False)
        await asyncio.sleep(min(30.0, base_delay * (2 ** (attempt - 1)) + random.random() * 0.1))
