"""Secret-free structured logs and local-only status endpoint."""

import json
from http.server import BaseHTTPRequestHandler, HTTPServer
from threading import Thread
from typing import Any


def configure_logging() -> None:
    import logging

    logging.basicConfig(level=logging.INFO, format="%(message)s")


class StatusServer:
    def __init__(self, state: dict[str, Any], host: str = "127.0.0.1", port: int = 8765) -> None:
        self.state, self.host, self.port = state, host, port
        self.server: HTTPServer | None = None

    def start(self) -> None:
        state = self.state

        class Handler(BaseHTTPRequestHandler):
            def do_GET(self) -> None:
                if self.path not in {"/health", "/status"}:
                    self.send_error(404)
                    return
                body = json.dumps(state, sort_keys=True).encode()
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)

            def log_message(self, format: str, *args: object) -> None:
                return

        self.server = HTTPServer((self.host, self.port), Handler)
        Thread(target=self.server.serve_forever, daemon=True).start()

    def stop(self) -> None:
        if self.server is not None:
            self.server.shutdown()
