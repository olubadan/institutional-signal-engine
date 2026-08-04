"""Signal-only application entry point; no execution route exists."""

from .config import Settings
from .observability import StatusServer, configure_logging


def main() -> None:
    configure_logging()
    settings = Settings.from_env()
    if settings.trading_enabled:
        raise RuntimeError("signal-only phase refuses TRADING_ENABLED=true")
    server = StatusServer(
        {"status": "ready", "trading_enabled": False, "config_version": settings.config_version}
    )
    server.start()
    print("signal-only engine ready on loopback")


if __name__ == "__main__":
    main()
