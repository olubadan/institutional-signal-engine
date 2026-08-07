import asyncio
import json

import pytest

from institutional_signal_engine.startup import (
    STARTUP_STAGES,
    StageRecorder,
    StartupTimeout,
    bounded_startup,
)


def test_stage_recorder_flushes_sanitized_structured_progress(capsys: pytest.CaptureFixture[str]):
    recorder = StageRecorder()
    recorder.emit("configuration_loaded", api_key="do-not-log", completed_items=0)
    record = json.loads(capsys.readouterr().out)
    assert record["stage"] == "configuration_loaded"
    assert record["api_key"] == "[REDACTED]"
    assert record["completed_items"] == 0


def test_canonical_startup_stage_set_is_complete():
    assert set(STARTUP_STAGES) == {
        "configuration_loaded",
        "database_connected",
        "providers_authenticated",
        "universe_discovery_started",
        "universe_discovery_completed",
        "enrichment_started",
        "enrichment_completed",
        "selection_completed",
        "subscription_planning_completed",
        "websocket_connected",
        "subscriptions_acknowledged",
        "observation_started",
        "observation_completed",
        "persistence_drained",
        "report_emitted",
    }


def test_all_startup_stages_emit_through_the_recorder():
    emitted: list[str] = []
    recorder = StageRecorder(lambda record: emitted.append(str(record["stage"])))
    for stage in STARTUP_STAGES:
        recorder.emit(stage)
    assert emitted == list(STARTUP_STAGES)


def test_stage_record_adapter_accepts_a_structured_stage_once():
    emitted: list[str] = []
    recorder = StageRecorder(lambda record: emitted.append(str(record["stage"])))
    recorder.emit_record({"record_type": "phase4_stage", "stage": "websocket_connected"})
    assert emitted == ["websocket_connected"]


@pytest.mark.asyncio
async def test_blocking_startup_operation_times_out_with_sanitized_failure(
    capsys: pytest.CaptureFixture[str],
):
    recorder = StageRecorder()

    async def blocking() -> object:
        await asyncio.sleep(1)
        return None

    with pytest.raises(StartupTimeout) as raised:
        await bounded_startup(blocking(), recorder, "alpaca_contract_pagination", 0.001, 2, 8)
    assert raised.value.stage == "alpaca_contract_pagination"
    assert raised.value.report()["status"] == "startup_timeout"
    assert raised.value.report()["orders_constructed"] == 0
    output = capsys.readouterr().out
    records = [json.loads(line) for line in output.splitlines()]
    assert records[-1]["stage"] == "startup_timeout"
    assert records[-1]["failed_stage"] == "alpaca_contract_pagination"
    assert records[-1]["completed_items"] == 2
    assert records[-1]["remaining_items"] == 8
    assert "do-not-log" not in output


@pytest.mark.asyncio
async def test_successful_startup_precedes_observation_timer_and_drain():
    stages: list[str] = []
    recorder = StageRecorder(lambda record: stages.append(str(record["stage"])))
    await bounded_startup(asyncio.sleep(0), recorder, "alpaca_historical_bootstrap", 1)
    recorder.emit("observation_started", observation_seconds=600)
    recorder.emit("observation_completed")
    recorder.emit("persistence_drained")
    assert stages == ["observation_started", "observation_completed", "persistence_drained"]
