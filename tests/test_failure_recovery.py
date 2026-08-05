import asyncio

from institutional_signal_engine.providers.common import (
    classify_http_failure,
    reconnecting_stream,
)


def test_http_failures_are_classified_without_response_body():
    assert classify_http_failure("alpaca", 429).category == "rate_limited"
    assert not classify_http_failure("thetadata", 401).retryable
    assert classify_http_failure("alpaca", 503).retryable
    assert "response" not in str(classify_http_failure("alpaca", 429))


def test_reconnects_after_transport_failure():
    attempts = 0

    async def stream():
        nonlocal attempts
        attempts += 1
        if attempts == 1:
            raise OSError("fixture transport failure")
        yield "recovered"

    async def collect():
        result = []
        async for item in reconnecting_stream("fixture", stream, max_attempts=2, base_delay=0):
            result.append(item)
        return result

    assert asyncio.run(collect()) == ["recovered"]
    assert attempts == 2
