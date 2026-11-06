"""The copy daemon: one process, one worker task per account, a loopback HTTP endpoint for the paper runner.

  POST /intent, /heartbeat   from the paper session (live.py --exec-url): stamp the receipt time, queue, ack at once
  POST /flatten-all          the Resonant tool: halt every account for the ET day and flatten (custom header)
  GET  /status               every account's status

Safety rules kept here: a flock on data/copy/.daemon.lock allows one daemon; the HTTP handler accepts only a
loopback Host and Content-Type application/json (a web page cannot send either cross-site; a same-user local
process still can, see the README); an account's crash, hang or dead token never touches the others; the kill
watcher (Resonant `paused`) flattens every account at once; SIGTERM stops without flattening. A daemon restart
in a position flattens it (startup rule).
"""

from __future__ import annotations

import asyncio
import fcntl
import json
import os
import sqlite3
import threading
import time
import urllib.request
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

from trade_jev.copytrade.config import Account, CopyConfig
from trade_jev.copytrade.executor import CopyExecutor
from trade_jev.copytrade.ledger import Ledger
from trade_jev.copytrade.tokens import AccountKeepalive

LOCK_NAME = ".daemon.lock"
FLATTEN_ALL_HEADER = "X-Trade-Jev-Copy"
KILL_POLL_S, PUBLISH_S, WORKER_TICK_S = 3.0, 5.0, 1.0
KILL_HTTP_TIMEOUT_S = 2.0
STARTUP_RETRY_S = 30.0
MAX_BODY = 64 * 1024


def _log(msg: str) -> None:
    print(f"[copy] {time.strftime('%F %T')} {msg}", flush=True)


class AlreadyRunning(RuntimeError):
    pass


def take_lock(directory: Path):
    """flock the daemon lock file; the returned file object must stay open. AlreadyRunning when held."""
    directory.mkdir(parents=True, exist_ok=True)
    fh = open(directory / LOCK_NAME, "a+")
    try:
        fcntl.flock(fh, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError:
        fh.close()
        raise AlreadyRunning("another copy daemon holds the lock") from None
    return fh


def lock_held(directory: Path) -> bool:
    """True when a daemon is running (the lock is taken); never keeps the lock."""
    try:
        fh = take_lock(directory)
    except AlreadyRunning:
        return True
    fh.close()
    return False


def http_kill_status(url: str) -> dict:
    """Resonant's /api/status: {"paused": bool, "autonomy": str, ...}. Raises on any failure."""
    with urllib.request.urlopen(url, timeout=KILL_HTTP_TIMEOUT_S) as r:
        d = json.loads(r.read())
    if not isinstance(d, dict):
        raise ValueError("status is not an object")
    return d


def db_kill_status(db_path: Path) -> dict:
    """`kv.paused` (a JSON boolean) from Resonant's DB opened read-only. Raises on any failure."""
    conn = sqlite3.connect(Path(db_path).resolve().as_uri() + "?mode=ro", uri=True, timeout=2)
    try:
        r = conn.execute("SELECT value FROM kv WHERE key='paused'").fetchone()
        a = conn.execute("SELECT value FROM kv WHERE key='autonomy'").fetchone()
    finally:
        conn.close()
    if r is None:
        raise ValueError("no paused flag")
    return {"paused": json.loads(r[0]), "autonomy": json.loads(a[0]) if a else None}


def kill_state(read_api, read_db) -> str:
    """clear | paused | autonomy_off | unknown. The API first; on failure the DB; both unavailable (or a
    non-boolean `paused`) is unknown: no new entries, hold."""
    for read in (read_api, read_db):
        try:
            d = read()
        except Exception:  # noqa: BLE001
            continue
        paused = d.get("paused")
        if not isinstance(paused, bool):
            continue
        if paused:
            return "paused"
        return "autonomy_off" if d.get("autonomy") == "off" else "clear"
    return "unknown"


@dataclass
class Worker:
    acct: Account
    ex: CopyExecutor
    keepalive: AccountKeepalive | None = None
    queue: asyncio.Queue = field(default_factory=asyncio.Queue)
    latest_hb: dict | None = None
    task: asyncio.Task | None = None
    ka_task: asyncio.Task | None = None
    startup_texted: bool = False
    crashes: int = 0


class Daemon:
    def __init__(self, cfg: CopyConfig, ledger: Ledger, make_broker, make_keepalive=None, *,
                 kill_api=None, kill_db=None, kill_poll_s=KILL_POLL_S, publish_s=PUBLISH_S,
                 tick_s=WORKER_TICK_S, now_ns=time.time_ns, mono=time.monotonic, executor_factory=None):
        self.cfg, self.ledger = cfg, ledger
        self.make_broker, self.make_keepalive = make_broker, make_keepalive
        self.kill_api = kill_api or (lambda: http_kill_status(cfg.kill_status_url))
        self.kill_db = kill_db or (lambda: (_ for _ in ()).throw(ValueError("no db configured")))
        self.kill_poll_s, self.publish_s, self.tick_s = kill_poll_s, publish_s, tick_s
        self.now_ns, self.mono = now_ns, mono
        self.executor_factory = executor_factory or (lambda a, b: CopyExecutor(a, b, ledger, now_ns=now_ns, mono=mono))
        self.workers: dict[str, Worker] = {}
        self.loop: asyncio.AbstractEventLoop | None = None
        self.kill = "clear"
        self._kill_texted = False
        self._stop = asyncio.Event()
        self.server: ThreadingHTTPServer | None = None
        self.pools: dict[str, ThreadPoolExecutor] = {}

    # ------------------------------------------------------------------ build

    def build(self) -> None:
        for a in self.cfg.enabled():
            ka = self.make_keepalive(a) if self.make_keepalive else None
            ex = self.executor_factory(a, self.make_broker(a, ka))
            self.workers[a.name] = Worker(a, ex, ka)

    # ------------------------------------------------------------------ HTTP-facing (any thread)

    def receive(self, kind: str, msg: dict) -> None:
        """Stamp the receipt time on every account NOW (the HTTP layer, not the worker), then queue."""
        for w in self.workers.values():
            w.ex.note_rx()
        if self.loop is not None:
            self.loop.call_soon_threadsafe(self._enqueue, kind, msg)

    def _enqueue(self, kind: str, msg: dict) -> None:
        for w in self.workers.values():
            if kind == "heartbeat":
                w.latest_hb = msg           # coalesced: only the latest matters
                w.queue.put_nowait(("hb", None))
            else:
                w.queue.put_nowait(("intent", msg))

    def status(self) -> dict:
        return {"kill": self.kill, "accounts": [w.ex.status_row() for w in self.workers.values()]}

    async def flatten_all(self) -> dict:
        res = await asyncio.gather(*(self._timed(w.ex.flatten_all()) for w in self.workers.values()),
                                   return_exceptions=True)
        return {n: (r if isinstance(r, str) else "timed out" if r is None else f"error: {type(r).__name__}")
                for n, r in zip(self.workers, res)}

    # ------------------------------------------------------------------ tasks

    async def _worker(self, w: Worker) -> None:
        """Startup, then the intent/heartbeat queue and the watchdog. Exceptions are caught per message."""
        ex = w.ex
        while not ex.clean and not self._stop.is_set():
            try:
                if w.keepalive is not None:
                    ex.token_ok, ex.token_expires = (h := w.keepalive.health())["ok"], h["expires_at"]
                if await ex.startup():
                    if w.startup_texted:
                        ex.event("startup_ok", "startup flatten/sweep succeeded", True)
                    break
                if not w.startup_texted:
                    w.startup_texted = True
                    ex.event("startup_retry", f"startup incomplete ({ex.last_error}); retrying", True)
            except asyncio.CancelledError:
                raise
            except Exception as e:  # noqa: BLE001
                _log(f"{w.acct.name}: startup error {type(e).__name__}")
            await asyncio.sleep(STARTUP_RETRY_S if self.tick_s >= 1 else self.tick_s)   # tests run fast ticks
        if self.kill == "paused":
            await self._safe(w, ex.kill_flatten("paused"))
        last_tick = 0.0
        while not self._stop.is_set():
            try:
                kind, msg = await asyncio.wait_for(w.queue.get(), self.tick_s)
            except asyncio.TimeoutError:
                kind, msg = None, None
            if kind == "intent":
                await self._safe(w, ex.on_intent(msg))
            elif kind == "hb":
                hb, w.latest_hb = w.latest_hb, None
                if hb is not None:
                    await self._safe(w, ex.on_heartbeat(hb))
            if self.mono() - last_tick >= self.tick_s:
                last_tick = self.mono()
                await self._safe(w, ex.watchdog_tick())

    async def _safe(self, w: Worker, coro) -> None:
        """One account's failure never leaves the account: it halts that account and the worker carries on."""
        try:
            await coro
        except asyncio.CancelledError:
            raise
        except Exception as e:  # noqa: BLE001
            w.crashes += 1
            w.ex.errors += 1
            w.ex.last_error = f"worker crash: {type(e).__name__}"
            _log(f"{w.acct.name}: worker crash {type(e).__name__}: {e}")
            try:
                await w.ex.halt_day("day", f"worker crashed: {type(e).__name__}")
            except Exception:  # noqa: BLE001
                pass

    async def _keepalive(self, w: Worker) -> None:
        await w.keepalive.run()

    async def kill_once(self) -> str:
        new = await asyncio.get_running_loop().run_in_executor(None, kill_state, self.kill_api, self.kill_db)
        old, self.kill = self.kill, new
        for w in self.workers.values():
            if new != "paused":
                w.ex.set_kill(new)
        if new == "unknown" and not self._kill_texted:
            self._kill_texted = True
            for w in self.workers.values():
                w.ex.event("kill_unknown", "cannot read Resonant's kill state: no new entries, holding", True)
        if new != "unknown":
            self._kill_texted = False
        if new == "paused" and (old != "paused" or any(w.ex.kill != "paused" for w in self.workers.values())):
            await asyncio.gather(*(self._timed(w.ex.kill_flatten("paused")) for w in self.workers.values()
                                   if w.ex.clean), return_exceptions=True)
            for w in self.workers.values():
                w.ex.kill = "paused"
        return new

    @staticmethod
    async def _timed(coro, s: float = 20.0):
        """A hung account must not hold up the others' kill flatten."""
        try:
            return await asyncio.wait_for(coro, s)
        except asyncio.TimeoutError:
            return None

    async def _kill_loop(self) -> None:
        while not self._stop.is_set():
            try:
                await self.kill_once()
            except asyncio.CancelledError:
                raise
            except Exception as e:  # noqa: BLE001
                _log(f"kill watcher error {type(e).__name__}")
            await asyncio.sleep(self.kill_poll_s)

    def publish_once(self) -> None:
        for w in self.workers.values():
            try:
                if w.keepalive is not None:
                    h = w.keepalive.health()
                    w.ex.token_ok, w.ex.token_expires = h["ok"], h["expires_at"]
                w.ex._refresh_day()
                self.ledger.status(w.ex.status_row())
            except Exception as e:  # noqa: BLE001
                _log(f"{w.acct.name}: publish error {type(e).__name__}")

    async def _publish_loop(self) -> None:
        while not self._stop.is_set():
            self.publish_once()
            await asyncio.sleep(self.publish_s)

    # ------------------------------------------------------------------ run

    async def start(self, port: int | None = None) -> None:
        self.loop = asyncio.get_running_loop()
        if not self.workers:
            self.build()
        self._tasks = [asyncio.create_task(self._kill_loop()), asyncio.create_task(self._publish_loop())]
        for w in self.workers.values():
            w.task = asyncio.create_task(self._worker(w))
            self._tasks.append(w.task)
            if w.keepalive is not None:
                w.ka_task = asyncio.create_task(self._keepalive(w))
                self._tasks.append(w.ka_task)
        if port is not None:
            self.server = make_server(self, port)
            threading.Thread(target=self.server.serve_forever, daemon=True).start()

    async def stop(self) -> None:
        """SIGTERM path: stop everything, flatten nothing."""
        self._stop.set()
        if self.server:
            self.server.shutdown()
            self.server.server_close()
        for t in getattr(self, "_tasks", []):
            t.cancel()
        await asyncio.gather(*getattr(self, "_tasks", []), return_exceptions=True)

    async def run_forever(self, port: int) -> None:
        await self.start(port)
        await self._stop.wait()


def make_server(daemon: Daemon, port: int) -> ThreadingHTTPServer:
    loopback = {f"127.0.0.1:{port}", f"localhost:{port}", f"[::1]:{port}"}

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

        def _host_ok(self) -> bool:
            h = (self.headers.get("Host") or "").lower()
            return h in loopback or h in ("127.0.0.1", "localhost")

        def do_POST(self):
            if not self._host_ok():
                return self._json({"error": "bad host"}, HTTPStatus.FORBIDDEN)
            ctype = (self.headers.get("Content-Type") or "").split(";")[0].strip().lower()
            if ctype != "application/json":
                return self._json({"error": "content-type must be application/json"}, HTTPStatus.UNSUPPORTED_MEDIA_TYPE)
            try:
                n = int(self.headers.get("Content-Length", 0))
                if n > MAX_BODY:
                    return self._json({"error": "too large"}, HTTPStatus.REQUEST_ENTITY_TOO_LARGE)
                msg = json.loads(self.rfile.read(n) or b"{}")
                if not isinstance(msg, dict):
                    raise ValueError
            except ValueError:
                return self._json({"error": "bad json"}, HTTPStatus.BAD_REQUEST)
            if self.path in ("/intent", "/heartbeat"):
                daemon.receive("intent" if self.path == "/intent" else "heartbeat", msg)
                return self._json({"queued": True})
            if self.path == "/flatten-all":
                if self.headers.get(FLATTEN_ALL_HEADER) != "flatten-all":
                    return self._json({"error": "missing header"}, HTTPStatus.FORBIDDEN)
                fut = asyncio.run_coroutine_threadsafe(daemon.flatten_all(), daemon.loop)
                try:
                    return self._json({"ok": True, "accounts": fut.result(timeout=25)})
                except Exception:  # noqa: BLE001
                    return self._json({"ok": False, "error": "timed out"}, HTTPStatus.GATEWAY_TIMEOUT)
            return self._json({"error": "not found"}, HTTPStatus.NOT_FOUND)

        def do_GET(self):
            if not self._host_ok():
                return self._json({"error": "bad host"}, HTTPStatus.FORBIDDEN)
            if self.path != "/status":
                return self._json({"error": "not found"}, HTTPStatus.NOT_FOUND)
            self._json(daemon.status())

    srv = ThreadingHTTPServer(("127.0.0.1", port), Handler)
    srv.daemon_threads = True
    return srv
