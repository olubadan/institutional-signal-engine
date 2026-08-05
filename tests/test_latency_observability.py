from datetime import UTC, datetime, timedelta
from uuid import uuid4

from institutional_signal_engine.config import Settings
from institutional_signal_engine.live_smoke import _distribution
from institutional_signal_engine.pipeline import EventTiming, SignalPipeline
from institutional_signal_engine.schemas import CanonicalEvent, EventKind


def test_distribution_reports_percentiles_without_cross_provider_aggregation():
    base = datetime(2026, 8, 5, 14, 31, tzinfo=UTC)
    timings = [
        EventTiming("alpaca", "trade", base, base, base, 1.0, 2.0, False, False),
        EventTiming("alpaca", "trade", base, base, base, 3.0, 4.0, False, False),
        EventTiming("thetadata", "trade", base, base, base, 100.0, 5.0, True, True),
    ]
    result = _distribution(timings)
    assert result["event_age_at_receipt_ms"]["sample_count"] == 3  # type: ignore[index]
    assert result["event_age_at_receipt_ms"]["minimum"] == 1.0  # type: ignore[index]
    assert result["event_age_at_receipt_ms"]["maximum"] == 100.0  # type: ignore[index]
    assert result["timestamp_conversions"] == {"precision_converted": 1, "timezone_converted": 1}


def test_pipeline_preserves_processing_timestamp_and_raw_event_age():
    source = datetime(2026, 8, 5, 14, 31, tzinfo=UTC)
    received = source + timedelta(seconds=2)
    processing = received + timedelta(seconds=1)
    pipeline = SignalPipeline(Settings(), now=lambda: processing)
    event = CanonicalEvent(
        event_id=uuid4(),
        kind=EventKind.EQUITY,
        symbol="AAPL",
        source="alpaca",
        source_timestamp=source,
        received_timestamp=received,
        normalized_timestamp=source,
        sequence=1,
        payload={
            "price": 100,
            "volume": 1,
            "provider_event_kind": "trade",
            "timestamp_conversion": {"precision_converted": False, "timezone_converted": True},
        },
    )
    pipeline.process(event)
    timing = (pipeline.metrics.timings or [])[0]
    assert timing.source_timestamp == source
    assert timing.received_timestamp == received
    assert timing.processing_timestamp == processing
    assert timing.event_age_at_receipt_ms == 2000
    assert timing.timezone_conversion_required is True
