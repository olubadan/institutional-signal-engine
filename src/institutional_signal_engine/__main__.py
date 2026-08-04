"""Signal-only application entry point; no execution route exists."""

import os

from .config import Settings
from .observability import StatusServer, configure_logging


def main() -> None:
    configure_logging()
    runtime_file = os.environ.get(
        "RUNTIME_ENV_FILE", "/etc/institutional-signal-engine/runtime.env"
    )
    settings = (
        Settings.from_env_file(runtime_file)
        if os.path.exists(runtime_file)
        else Settings.from_env()
    )
    if settings.trading_enabled:
        raise RuntimeError("signal-only phase refuses TRADING_ENABLED=true")
    server = StatusServer(
        {"status": "ready", "trading_enabled": False, "config_version": settings.config_version}
    )
    server.start()
    print("signal-only engine ready on loopback")


if __name__ == "__main__":
    main()
