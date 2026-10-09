"""HTTP glue: receive intents / heartbeats from the live runner, report fills / status back to it."""

from __future__ import annotations

import asyncio
import json
import urllib.request
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer


class Reporter:
    """Posts to the live runner (`trade_jev.live`) web server. Failures are counted, never raised."""

    def __init__(self, live_url: str, timeout_s: float = 2.0):
        self.url, self.timeout_s = live_url.rstrip("/"), timeout_s
        self.errors = 0

    def _post(self, path: str, body: dict) -> None:
        req = urllib.request.Request(self.url + path, json.dumps(body, default=str).encode(),
                                     {"Content-Type": "application/json"})
        try:
            urllib.request.urlopen(req, timeout=self.timeout_s).read()
        except Exception:  # noqa: BLE001 — the live runner may be restarting
            self.errors += 1

    async def fill(self, body: dict) -> None:
        await asyncio.to_thread(self._post, "/api/live/fill", body)

    async def status(self, body: dict) -> None:
        await asyncio.to_thread(self._post, "/api/live/executor", body)


def make_server(executor, loop: asyncio.AbstractEventLoop, port: int) -> ThreadingHTTPServer:
    """POST /intent and /heartbeat are queued onto the executor's loop and acked at once
    (the live runner never waits on a broker action). GET /status returns the executor's state."""

    class Handler(BaseHTTPRequestHandler):
        def log_message(self, fmt, *args):
            pass

        def _json(self, obj, status=HTTPStatus.OK):
            body = json.dumps(obj, default=str).encode()
            self.send_response(status)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def do_POST(self):
            try:
                msg = json.loads(self.rfile.read(int(self.headers.get("Content-Length", 0))) or b"{}")
            except ValueError:
                return self._json({"error": "bad json"}, HTTPStatus.BAD_REQUEST)
            if self.path == "/intent":
                asyncio.run_coroutine_threadsafe(executor.on_intent(msg), loop)
            elif self.path == "/heartbeat":
                asyncio.run_coroutine_threadsafe(executor.on_heartbeat(msg), loop)
            else:
                return self._json({"error": "not found"}, HTTPStatus.NOT_FOUND)
            self._json({"queued": True})

        def do_GET(self):
            if self.path != "/status":
                return self._json({"error": "not found"}, HTTPStatus.NOT_FOUND)
            fut = asyncio.run_coroutine_threadsafe(executor.status(), loop)
            self._json(fut.result(timeout=5))

    srv = ThreadingHTTPServer(("127.0.0.1", port), Handler)
    srv.daemon_threads = True
    return srv
