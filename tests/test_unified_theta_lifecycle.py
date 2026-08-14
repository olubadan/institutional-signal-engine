import asyncio
import json
from datetime import UTC, datetime, timedelta

import pytest

from institutional_signal_engine import live_session


class FakeWebSocket:
    def __init__(self, log: list[tuple[str, object]]) -> None:
        self.log = log
        self.messages: list[str] = []
        self.closed = False

    async def send(self, message: str) -> None:
        self.messages.append(message)
        self.log.append(("send", json.loads(message)))

    async def close(self) -> None:
        self.closed = True
        self.log.append(("close", self))

    async def wait_closed(self) -> None:
        self.log.append(("wait_closed", self))

    async def recv(self) -> str:
        while not self.closed:
            await asyncio.sleep(0)
        raise live_session.websockets.ConnectionClosed(None, None)


@pytest.mark.asyncio
async def test_unified_theta_session_has_one_process_owner(monkeypatch: pytest.MonkeyPatch) -> None:
    live_session.UnifiedThetaSession._active_owner = None
    live_session.UnifiedThetaSession._lifecycle_lock = None
    log: list[tuple[str, object]] = []
    sockets: list[FakeWebSocket] = []

    async def connect(*_args: object, **_kwargs: object) -> FakeWebSocket:
        socket = FakeWebSocket(log)
        sockets.append(socket)
        log.append(("open", socket))
        return socket

    monkeypatch.setattr(live_session.websockets, "connect", connect)
    first = live_session.UnifiedThetaSession("ws://theta", "key")
    second = live_session.UnifiedThetaSession("ws://theta", "key")
    await first.connect()
    with pytest.raises(live_session.LiveEvidenceFailure, match="CONNECTION_ALREADY_OWNED"):
        await second.connect()
    assert len(sockets) == 1
    await first.close()
    await second.connect()
    assert len(sockets) == 2
    await second.close()


@pytest.mark.asyncio
async def test_reconnect_sends_stop_and_closes_before_opening_replacement(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    live_session.UnifiedThetaSession._active_owner = None
    live_session.UnifiedThetaSession._lifecycle_lock = None
    log: list[tuple[str, object]] = []

    async def connect(*_args: object, **_kwargs: object) -> FakeWebSocket:
        socket = FakeWebSocket(log)
        log.append(("open", socket))
        return socket

    monkeypatch.setattr(live_session.websockets, "connect", connect)
    session = live_session.UnifiedThetaSession("ws://theta", "key")
    await session.connect()
    old = session._ws
    await session.reconnect()
    assert old is not session._ws
    assert log[0][0] == "open"
    assert log[1][0] == "send"
    assert log[1][1] == {"msg_type": "STOP"}
    assert [item[0] for item in log[2:]] == ["close", "wait_closed", "open"]
    await session.close()


@pytest.mark.asyncio
async def test_receive_until_normalizes_market_frame_without_extra_socket(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    live_session.UnifiedThetaSession._active_owner = None
    live_session.UnifiedThetaSession._lifecycle_lock = None
    session = live_session.UnifiedThetaSession("ws://theta", "key")

    class EventSocket:
        async def recv(self) -> str:
            return json.dumps(
                {
                    "header": {"type": "TRADE"},
                    "contract": {
                        "root": "AAPL",
                        "expiration": 20260821,
                        "strike": 307500,
                        "right": "C",
                    },
                    "trade": {"date": "20260814", "ms_of_day": 34200000, "size": 1, "price": 1.0},
                }
            )

    session._ws = EventSocket()
    session._reader_task = asyncio.create_task(session._read_loop(session._ws))
    frame = await session.receive_until(datetime.now(UTC).astimezone() + timedelta(seconds=1))
    assert frame.kind == "event"
    assert frame.event is not None
    assert frame.event.source == "thetadata"
    session._reader_task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await session._reader_task
