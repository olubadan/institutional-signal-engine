"""Keep the default pytest suite hermetic."""

import socket

import pytest


@pytest.fixture(autouse=True)
def prohibit_network(monkeypatch: pytest.MonkeyPatch, request: pytest.FixtureRequest) -> None:
    if request.node.get_closest_marker("integration") is not None:
        return

    real_socket = socket.socket

    def blocked(*args: object, **kwargs: object) -> socket.socket:
        family = args[0] if args else kwargs.get("family", socket.AF_INET)
        if family == socket.AF_UNIX:
            return real_socket(*args, **kwargs)
        raise AssertionError("unit tests may not open network sockets")

    monkeypatch.setattr(socket, "socket", blocked)
