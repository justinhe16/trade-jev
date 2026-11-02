"""Tradovate live source: L2 DOM + trade ticks from the Tradovate market-data websocket.

Paper only. This module calls exactly: the auth endpoints (accesstokenrequest, renewaccesstoken),
GET contract/find (read-only reference data) and the md/* websocket endpoints (subscribeDOM,
unsubscribeDOM, getChart, cancelChart). It never touches an order, position or account endpoint.

Secrets (TRADOVATE_PASSWORD, TRADOVATE_SEC, access tokens) are redacted from every log line and
exception text, and `repr()` of the config and auth objects never shows them.

Rows. Each DOM snapshot (the full visible depth, best-first top 10 per side) becomes one book row with
delta 0. Each trade tick (after the chart's end-of-history marker) becomes one row carrying the latest
book and delta = +size (buyer lifted the ask) or -size (seller hit the bid). Row timestamps come from the
event times and are forced non-decreasing. Trades before the first book are dropped (counted).

Aggressor rule, per trade tick:
  1. the tick carries the ask and price >= ask  -> buy
  2. the tick carries the bid and price <= bid  -> sell
  3. else the same test against the latest DOM best ask / bid
  4. else the tick rule vs the previous trade price: up -> buy, down -> sell, unchanged -> the previous
     trade's side; the very first trade with no information -> unknown (delta 0)

Tokens and hosts (some facts are undocumented and are tried in order, see README "Tradovate source"):
the md socket wants either `mdAccessToken` or `accessToken`: mdAccessToken is tried first, then
accessToken. The only documented md host is md.tradovateapi.com (both environments); for TRADOVATE_ENV=demo
md-demo.tradovateapi.com is the one fallback. TRADOVATE_MD_URL overrides everything (no fallback).
"""

from __future__ import annotations

import asyncio
import json
import os
import re
import time
import urllib.error
import urllib.request
from collections import deque
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from statistics import median

from trade_jev.data import LEVELS, TICK

REQUIRED_ENV = ("TRADOVATE_USERNAME", "TRADOVATE_PASSWORD", "TRADOVATE_CID", "TRADOVATE_SEC",
                "TRADOVATE_DEVICE_ID")
REST = {"live": "https://live.tradovateapi.com/v1", "demo": "https://demo.tradovateapi.com/v1"}
MD_URL = "wss://md.tradovateapi.com/v1/websocket"
MD_DEMO_URL = "wss://md-demo.tradovateapi.com/v1/websocket"  # third-party sources only: fallback for demo
HEARTBEAT_S = 2.5        # the client must send "[]" this often
IDLE_S = 15.0            # no frame (the server heartbeats too) for this long -> the connection is dead
MAX_PENALTY_S = 120     # a login penalty longer than this is reported, not slept through
HEALTHY_S = 60.0        # a connection must deliver data this long before the reconnect backoff resets
MAX_REFUSALS = 5        # consecutive md refusals with no book data ever received -> give up (crashed)
RENEW_MARGIN_S = 15 * 60  # renew the access token this long before it expires (tokens live 90 min)
HINT_NO_API = ("API Access requires a Tradovate API subscription; prop-firm evaluation accounts may not "
               "allow it: ask the firm / Tradovate.")


def _log(msg: str) -> None:
    print(f"[tradovate] {msg}", flush=True)


# ---------------------------------------------------------------- errors, redaction

class TradovateError(Exception):
    pass


class AuthError(TradovateError):
    pass


class BadCredentials(AuthError):
    """Wrong username / password."""


class ApiKeyRejected(AuthError):
    """cid / sec (the API key) rejected: the account has no API access."""
    hint = HINT_NO_API


class CaptchaRequired(AuthError):
    pass


class RequestFailed(TradovateError):
    pass


class MdRefused(TradovateError):
    """The md socket authorized or subscribed with a non-200 answer (not a network drop)."""


def redact(text: str, secrets) -> str:
    """Replace every secret value (longest first) in `text` with ***."""
    text = str(text)
    for s in sorted({s for s in secrets if s}, key=len, reverse=True):
        text = text.replace(s, "***")
    return text


# ---------------------------------------------------------------- config

def missing_credentials(env=os.environ) -> list[str]:
    """Names (never values) of the required TRADOVATE_* credentials that are unset or blank."""
    return [n for n in REQUIRED_ENV if not (env.get(n) or "").strip()]


@dataclass(frozen=True)
class TradovateConfig:
    username: str = field(repr=False)
    password: str = field(repr=False)
    cid: str = field(repr=False)
    sec: str = field(repr=False)
    device_id: str = field(repr=False)
    env: str = "live"
    app_id: str = "trade-jev"
    app_version: str = "1.0"
    md_url: str | None = None

    @classmethod
    def from_env(cls, env=os.environ) -> "TradovateConfig":
        missing = missing_credentials(env)
        if missing:
            raise TradovateError("tradovate credentials missing: " + ", ".join(missing))
        which = (env.get("TRADOVATE_ENV") or "live").strip().lower()
        if which not in REST:
            raise TradovateError(f"TRADOVATE_ENV must be live or demo, not {which!r}")
        g = lambda k, d="": (env.get(k) or d).strip()  # noqa: E731
        return cls(g("TRADOVATE_USERNAME"), env["TRADOVATE_PASSWORD"], g("TRADOVATE_CID"), env["TRADOVATE_SEC"],
                   g("TRADOVATE_DEVICE_ID"), which, g("TRADOVATE_APP_ID", "trade-jev"),
                   g("TRADOVATE_APP_VERSION", "1.0"), g("TRADOVATE_MD_URL") or None)

    @property
    def base(self) -> str:
        return REST[self.env]

    def md_candidates(self) -> list[str]:
        if self.md_url:
            return [self.md_url]
        return [MD_URL, MD_DEMO_URL] if self.env == "demo" else [MD_URL]

    def secrets(self) -> list[str]:
        return [self.password, self.sec]


# ---------------------------------------------------------------- auth (REST)

async def default_http(method: str, url: str, headers: dict, body: dict | None) -> tuple[int, dict]:
    """(status, parsed JSON) over urllib in a thread. Network failures raise TradovateError."""
    def call() -> tuple[int, bytes]:
        data = json.dumps(body).encode() if body is not None else None
        req = urllib.request.Request(url, data=data, method=method,
                                     headers={"Content-Type": "application/json", "Accept": "application/json",
                                              **headers})
        try:
            with urllib.request.urlopen(req, timeout=15) as r:
                return r.status, r.read()
        except urllib.error.HTTPError as e:
            return e.code, e.read()
        except (urllib.error.URLError, TimeoutError, OSError) as e:
            raise TradovateError(f"{type(e).__name__}: {e}") from None
    status, raw = await asyncio.to_thread(call)
    try:
        d = json.loads(raw)
    except ValueError:
        d = {"errorText": raw.decode("utf-8", "replace")[:200]}
    return status, d if isinstance(d, dict) else {"d": d}


APP_RE = re.compile(r"\b(app|appid|application|cid|sec|secret|api[ _-]?key|client[ _]?id)\b", re.I)
CRED_RE = re.compile(r"password|user\s?name|credential|login", re.I)


def _parse_iso(s: str) -> float | None:
    try:
        return datetime.fromisoformat(s.replace("Z", "+00:00")).astimezone(timezone.utc).timestamp()
    except (ValueError, AttributeError):
        return None


class TradovateAuth:
    """Login, renew and contract lookup. `http` and `sleep` are injectable for tests."""

    def __init__(self, cfg: TradovateConfig, http=None, sleep=asyncio.sleep, clock=time.time):
        self.cfg, self.sleep, self.clock = cfg, sleep, clock
        self.http = http or default_http  # looked up now, so tests can replace the module's default
        self.access_token: str | None = None
        self.md_token: str | None = None
        self.expires_at = 0.0
        self.user_status: str | None = None
        self.has_market_data: bool | None = None
        self.user_id = None
        self._renew_task: asyncio.Task | None = None
        self._lock = asyncio.Lock()  # login / renew never run concurrently
        self._old_tokens: deque = deque(maxlen=4)  # kept in the redaction set after a renew

    def __repr__(self) -> str:
        return f"TradovateAuth(env={self.cfg.env!r}, logged_in={bool(self.access_token)})"

    def secrets(self) -> list[str]:
        return [*self.cfg.secrets(), self.access_token or "", self.md_token or "", *self._old_tokens]

    def redact(self, text) -> str:
        return redact(text, self.secrets())

    # -- requests

    def _login_body(self) -> dict:
        c = self.cfg
        return {"name": c.username, "password": c.password, "appId": c.app_id, "appVersion": c.app_version,
                "cid": int(c.cid) if c.cid.isdigit() else c.cid, "sec": c.sec, "deviceId": c.device_id}

    async def _post_login(self, body: dict) -> dict:
        try:
            status, d = await self.http("POST", self.cfg.base + "/auth/accesstokenrequest", {}, body)
        except TradovateError as e:
            raise AuthError(f"login request failed: {self.redact(e)}") from None
        if "p-ticket" in d or "p-time" in d:
            return d
        err = str(d.get("errorText") or "").strip()
        if err or status >= 400:
            self._raise_login_failure(status, err)
        return d

    def _raise_login_failure(self, status: int, err: str):
        text = self.redact(err)
        if err and APP_RE.search(text):
            raise ApiKeyRejected(f"this account has no API key: cid/sec rejected ({text})")
        if err and CRED_RE.search(text):
            raise BadCredentials(f"bad credentials: Tradovate rejected the username/password ({text})")
        if not err:
            raise AuthError(f"login rejected (HTTP {status}, no error text)")
        raise AuthError(f"login failed: {text}")

    async def login(self) -> None:
        async with self._lock:
            await self._login()

    def _penalty_wait(self, d: dict) -> float:
        wait = float(d.get("p-time") or 0)
        if wait > MAX_PENALTY_S:
            raise AuthError(f"Tradovate asks to wait {wait:g}s before another login (more than {MAX_PENALTY_S}s): "
                            "retry later")
        return wait

    async def _login(self) -> None:
        body = self._login_body()
        d = await self._post_login(body)
        if "p-ticket" in d or "p-time" in d:
            if d.get("p-captcha"):
                raise CaptchaRequired("Tradovate wants a captcha: log in once by hand in the Tradovate web app "
                                      "(same device), wait ~1h, then retry")
            wait = self._penalty_wait(d)
            _log(f"login rate-limited: waiting {wait:g}s, then retrying once")
            await self.sleep(wait)
            d = await self._post_login({**body, "p-ticket": d.get("p-ticket")})
            if "p-ticket" in d or "p-time" in d:
                if d.get("p-captcha"):
                    raise CaptchaRequired("Tradovate wants a captcha: log in once by hand in the Tradovate web "
                                          "app (same device), wait ~1h, then retry")
                raise AuthError("Tradovate penalised the login twice; wait and retry later")
        self._apply(d, full=True)
        _log(f"login ok ({self.cfg.env}); token expires {time.strftime('%H:%M:%S', time.localtime(self.expires_at))}")

    def _apply(self, d: dict, full: bool) -> None:
        tok = d.get("accessToken")
        if not tok:
            raise AuthError("response had no accessToken")
        self._old_tokens.extend(t for t in (self.access_token, self.md_token) if t and t not in (tok, d.get("mdAccessToken")))
        self.access_token = tok
        if d.get("mdAccessToken"):
            self.md_token = d["mdAccessToken"]
        elif full:
            self.md_token = None
        exp = _parse_iso(d.get("expirationTime")) if d.get("expirationTime") else None
        self.expires_at = exp if exp else self.clock() + 80 * 60
        if full:
            self.user_status, self.user_id = d.get("userStatus"), d.get("userId")
            self.has_market_data = d.get("hasMarketData")

    async def renew(self) -> None:
        try:
            status, d = await self.http("GET", self.cfg.base + "/auth/renewaccesstoken",
                                        {"Authorization": f"Bearer {self.access_token}"}, None)
        except TradovateError as e:
            raise AuthError(f"renew failed: {self.redact(e)}") from None
        if status >= 400 or d.get("errorText"):
            raise AuthError(f"renew failed: {self.redact(d.get('errorText') or 'HTTP ' + str(status))}")
        self._apply(d, full=False)

    async def _renew_or_login(self) -> None:
        try:
            await self.renew()
            _log("token renewed")
        except Exception as e:  # noqa: BLE001 — any renewal failure -> a full login
            _log(f"renew failed ({self.redact(e)}); logging in again")
            await self._login()

    async def renew_or_login(self) -> None:
        async with self._lock:
            await self._renew_or_login()

    async def ensure_fresh(self) -> None:
        async with self._lock:
            if not self.access_token:
                await self._login()
            elif self.clock() >= self.expires_at - RENEW_MARGIN_S:
                await self._renew_or_login()

    def start_renew_loop(self) -> None:
        if self._renew_task is None:
            self._renew_task = asyncio.create_task(self._renew_loop())

    async def stop_renew_loop(self) -> None:
        t, self._renew_task = self._renew_task, None
        if t:
            t.cancel()
            await asyncio.gather(t, return_exceptions=True)

    async def _renew_loop(self) -> None:
        fails = 0
        while True:
            await self.sleep(max(30.0, self.expires_at - RENEW_MARGIN_S - self.clock()))
            try:
                await self.renew_or_login()
            except asyncio.CancelledError:
                raise
            except Exception as e:  # noqa: BLE001
                fails += 1
                wait = min(60 * 2 ** (fails - 1), 900)  # 1, 2, 4 ... 15 min: no login hammering in an outage
                _log(f"token refresh failed ({fails}): {self.redact(e)}; retrying in {wait}s")
                self.expires_at = self.clock() + RENEW_MARGIN_S + wait
            else:
                fails = 0

    async def contract_id(self, symbol: str) -> int:
        """GET contract/find (read-only reference data)."""
        try:
            status, d = await self.http("GET", f"{self.cfg.base}/contract/find?name={symbol}",
                                        {"Authorization": f"Bearer {self.access_token}"}, None)
        except TradovateError as e:
            raise TradovateError(f"contract/find failed: {self.redact(e)}") from None
        if status >= 400 or d.get("errorText") or not isinstance(d.get("id"), int):
            raise TradovateError(f"contract/find {symbol}: {self.redact(d.get('errorText') or status)}")
        return d["id"]


# ---------------------------------------------------------------- frames

def encode_request(endpoint: str, id: int, body=None, query: str = "") -> str:
    return f"{endpoint}\n{id}\n{query}\n{json.dumps(body) if body is not None else ''}"


def decode_frame(text: str) -> tuple[str, object]:
    """Server frame -> (kind, payload): open | heartbeat | messages (list) | close ([code, reason]) | unknown."""
    if not text:
        return "unknown", text
    k, rest = text[0], text[1:]
    if k == "o":
        return "open", None
    if k == "h":
        return "heartbeat", None
    if k in "ac":
        try:
            p = json.loads(rest)
        except ValueError:
            return "unknown", text
        if k == "a":
            return ("messages", p) if isinstance(p, list) else ("unknown", text)
        return "close", p
    return "unknown", text


# ---------------------------------------------------------------- DOM parsing

def _ticks(price) -> int:
    return int(round(float(price) / TICK))


def _level(x) -> tuple[int, int] | None:
    if isinstance(x, dict):
        p, s = x.get("price", x.get("p")), x.get("size", x.get("s"))
    elif isinstance(x, (list, tuple)) and len(x) >= 2:
        p, s = x[0], x[1]
    else:
        return None
    if not isinstance(p, (int, float)) or not isinstance(s, (int, float)):
        return None
    return _ticks(p), int(round(s))


_EPOCH = datetime(1970, 1, 1, tzinfo=timezone.utc)


def _ts_ns(t) -> int | None:
    """ISO string or epoch milliseconds (seconds if small) -> ns, with integer arithmetic."""
    if isinstance(t, str):
        try:
            dt = datetime.fromisoformat(t.replace("Z", "+00:00"))
        except ValueError:
            return None
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return (dt - _EPOCH) // timedelta(microseconds=1) * 1000
    if isinstance(t, (int, float)) and not isinstance(t, bool):
        return int(t) * 1_000_000 if abs(t) > 1e11 else int(t) * 1_000_000_000
    return None


def book_row(bids: list, asks: list) -> tuple[list, list, list, list] | None:
    """Raw levels [(price_ticks, size)] -> (bid_px, bid_sz, ask_px, ask_sz), best-first top 10 per side,
    padded like mbp10_row (one tick further out, size 0). A missing side -> None."""
    def side(levels, sign):
        d = {p: s for p, s in levels if s > 0}
        best = sorted(d, reverse=sign < 0)[:LEVELS]  # bids: descending, asks: ascending
        return best, d
    bp, bd = side(bids, -1)
    ap, ad = side(asks, 1)
    if not bp or not ap or bp[0] >= ap[0]:  # an empty side, or a crossed / locked book: unusable
        return None
    bs = [bd[p] for p in bp]
    as_ = [ad[p] for p in ap]
    while len(bp) < LEVELS:
        bp.append(bp[-1] - 1)
        bs.append(0)
    while len(ap) < LEVELS:
        ap.append(ap[-1] + 1)
        as_.append(0)
    return bp, bs, ap, as_


def parse_dom(d) -> list[tuple[int | None, int | None, tuple | None]] | None:
    """The `d` of an md event -> [(contract_id, ts_ns, book|None)], or None if it is not a DOM message.
    Handles the documented form {"doms":[{contractId,timestamp,bids,offers}]} and the compact form
    {"d":[{id,t,b,a}]}. Each entry is a full snapshot; book is None when a side is empty."""
    if not isinstance(d, dict):
        return None
    out = []
    if isinstance(d.get("doms"), list):
        for x in d["doms"]:
            if not isinstance(x, dict):
                continue
            bids = [l for l in map(_level, x.get("bids") or []) if l]
            asks = [l for l in map(_level, x.get("offers") or []) if l]
            out.append((x.get("contractId"), _ts_ns(x.get("timestamp")), book_row(bids, asks)))
        return out
    if isinstance(d.get("d"), list):
        for x in d["d"]:
            if not isinstance(x, dict):
                continue
            bids = [l for l in map(_level, x.get("b") or []) if l]
            asks = [l for l in map(_level, x.get("a") or []) if l]
            out.append((x.get("id"), _ts_ns(x.get("t")), book_row(bids, asks)))
        return out
    return None


# ---------------------------------------------------------------- ticks

@dataclass
class Trade:
    ts_ns: int
    price: int               # ticks
    size: int
    bid: int | None = None   # ticks, when the tick carried it
    ask: int | None = None
    id: int | None = None


class Aggressor:
    """Classifies a trade as buy (+1) / sell (-1) / unknown (0); see the module docstring for the rule."""

    def __init__(self):
        self.bid: int | None = None
        self.ask: int | None = None
        self.last_px: int | None = None
        self.last_side = 0

    def book(self, bid: int, ask: int) -> None:
        self.bid, self.ask = bid, ask

    def classify(self, price: int, bid: int | None = None, ask: int | None = None) -> int:
        side = 0
        if ask is not None and price >= ask:
            side = 1
        elif bid is not None and price <= bid:
            side = -1
        elif self.ask is not None and price >= self.ask:
            side = 1
        elif self.bid is not None and price <= self.bid:
            side = -1
        elif self.last_px is not None:
            side = 1 if price > self.last_px else -1 if price < self.last_px else self.last_side
        self.last_px = price
        if side:
            self.last_side = side
        return side


class TickDecoder:
    """Chart events of one md/getChart subscription -> Trades. Everything before the end-of-history
    marker (`eoh`) is history and ignored; ticks are sorted by (time, id) per packet and deduped by id."""

    def __init__(self, keep_ids: int = 50_000):
        self.ids: set = set()      # realtimeId / historicalId; empty = accept any (one subscription)
        self.eoh = False
        self.seen: set = set()
        self.order: deque = deque()
        self.keep_ids = keep_ids

    def feed(self, d) -> list[Trade]:
        out: list[Trade] = []
        charts = d.get("charts") if isinstance(d, dict) else None
        for c in charts if isinstance(charts, list) else []:
            if not isinstance(c, dict) or (self.ids and c.get("id") not in self.ids):
                continue
            if c.get("eoh"):
                self.eoh = True
                continue  # the marker closes the history; its own ticks (if any) are history
            tks = c.get("tks")
            if not isinstance(tks, list) or not self.eoh:
                continue
            bp, bt = c.get("bp") or 0, c.get("bt") or 0
            if isinstance(bt, str):  # an ISO base time instead of epoch ms
                n = _ts_ns(bt)
                bt = n // 1_000_000 if n is not None else 0
            ts = c.get("ts") or TICK
            px = lambda v: int(round((bp + v) * ts / TICK))  # noqa: E731
            batch = []
            for t in tks:
                if not isinstance(t, dict) or "p" not in t or "t" not in t:
                    continue
                batch.append((bt + t["t"], t.get("id") if t.get("id") is not None else 0, t))
            for tm, tid, t in sorted(batch, key=lambda x: (x[0], x[1])):
                if t.get("id") is not None:
                    if t["id"] in self.seen:
                        continue
                    self.seen.add(t["id"])
                    self.order.append(t["id"])
                    if len(self.order) > self.keep_ids:
                        self.seen.discard(self.order.popleft())
                size = int(round(t.get("s") or 0))
                if size <= 0:
                    continue
                out.append(Trade(int(tm) * 1_000_000, px(t["p"]), size,
                                 px(t["b"]) if "bs" in t and "b" in t else None,
                                 px(t["a"]) if "as" in t and "a" in t else None, t.get("id")))
        return out


# ---------------------------------------------------------------- md socket

class MdSocket:
    """One authorized md websocket: heartbeat, request/response by id, event dispatch."""

    def __init__(self, ws, on_event):
        self.ws, self.on_event = ws, on_event
        self.next_id = 1
        self.pending: dict[int, asyncio.Future] = {}
        self.dropped = asyncio.Event()
        self.drop_reason = ""
        self.event_errors = 0
        self.tasks: list[asyncio.Task] = []

    async def wait_open(self, timeout: float = 10.0) -> None:
        async with asyncio.timeout(timeout):
            while True:
                kind, _ = decode_frame(await self.ws.recv())
                if kind == "open":
                    return
                if kind == "close":
                    raise TradovateError("closed before open")

    def start(self) -> None:
        self.tasks = [asyncio.create_task(self._reader()), asyncio.create_task(self._beat())]

    def _drop(self, why: str) -> None:
        if not self.dropped.is_set():
            self.drop_reason = why
            self.dropped.set()
        for f in self.pending.values():
            if not f.done():
                f.set_exception(TradovateError(f"connection lost ({why})"))

    async def _beat(self) -> None:
        try:
            while True:
                await asyncio.sleep(HEARTBEAT_S)
                await self.ws.send("[]")
        except Exception as e:  # noqa: BLE001
            self._drop(f"heartbeat: {type(e).__name__}")

    async def _reader(self) -> None:
        try:
            while True:
                try:
                    text = await asyncio.wait_for(self.ws.recv(), IDLE_S)
                except TimeoutError:
                    self._drop("idle")
                    return
                kind, p = decode_frame(text)
                if kind == "close":
                    self._drop(f"server close {p}")
                    return
                if kind != "messages":
                    continue
                for m in p:
                    if not isinstance(m, dict):
                        continue
                    if "i" in m and "s" in m and m["i"] in self.pending:
                        f = self.pending.pop(m["i"])
                        if not f.done():
                            f.set_result(m)
                    elif m.get("e") == "shutdown":
                        self._drop(f"shutdown {m.get('d')}")
                        return
                    elif "e" in m:
                        try:
                            self.on_event(m)
                        except Exception as e:  # noqa: BLE001 — one bad event must not drop the socket
                            self.event_errors += 1
                            if self.event_errors <= 3:
                                _log(f"event skipped ({type(e).__name__})")
        except asyncio.CancelledError:
            raise
        except Exception as e:  # noqa: BLE001 — ConnectionClosed and friends
            self._drop(type(e).__name__)

    async def request(self, endpoint: str, body=None, timeout: float = 10.0) -> dict:
        i, self.next_id = self.next_id, self.next_id + 1
        fut = asyncio.get_running_loop().create_future()
        self.pending[i] = fut
        await self.ws.send(encode_request(endpoint, i, body))
        try:
            m = await asyncio.wait_for(fut, timeout)
        finally:
            self.pending.pop(i, None)
        if m.get("s") != 200:
            raise RequestFailed(f"{endpoint}: HTTP {m.get('s')} {m.get('d')}")
        return m

    async def authorize(self, token: str) -> None:
        """Bare token, per the docs: authorize\\n<id>\\n\\n<token>."""
        i, self.next_id = self.next_id, self.next_id + 1
        fut = asyncio.get_running_loop().create_future()
        self.pending[i] = fut
        await self.ws.send(f"authorize\n{i}\n\n{token}")
        try:
            m = await asyncio.wait_for(fut, 10.0)
        finally:
            self.pending.pop(i, None)
        if m.get("s") != 200:
            raise RequestFailed(f"authorize: HTTP {m.get('s')}")

    async def close(self) -> None:
        for t in self.tasks:
            t.cancel()
        await asyncio.gather(*self.tasks, return_exceptions=True)
        try:
            await asyncio.wait_for(self.ws.close(), 3.0)
        except Exception:  # noqa: BLE001
            pass


# ---------------------------------------------------------------- source

class TradovateSource:
    """Same interface as DatabentoSource: `.live`, `async batches()`. Needs credentials and network;
    tests inject `config`, `auth` and `md_urls`."""

    def __init__(self, symbol: str, stop_ns: int, flush_s: float = 0.1, config: TradovateConfig | None = None,
                 auth: TradovateAuth | None = None, md_urls: list[str] | None = None,
                 backoff_s: float = 1.0, backoff_max_s: float = 30.0, connect_ws=None):
        self.symbol, self.stop_ns, self.flush_s = symbol, stop_ns, flush_s
        if config is None:
            config = auth.cfg if auth is not None else TradovateConfig.from_env()
        self.cfg = config
        self.auth = auth or TradovateAuth(self.cfg)
        self.md_urls = md_urls or self.cfg.md_candidates()
        self.backoff_s, self.backoff_max_s = backoff_s, backoff_max_s
        self.connect_ws = connect_ws
        self.live = True
        # counters (smoke test, logs)
        self.dom_updates = self.ticks = self.buys = self.sells = self.unknown = self.reconnects = 0
        self.buy_volume = self.sell_volume = self.unknown_volume = 0
        self.dropped_trades = self.dom_empty = self.unknown_events = 0  # dom_empty: empty side or crossed book
        self._refusals = 0
        self._first_data_t: float | None = None
        self.max_bid_levels = self.max_ask_levels = 0
        self.bid_levels: list[int] = []
        self.ask_levels: list[int] = []
        self.contract_id: int | None = None
        self.md_token_kind: str | None = None
        self.md_host: str | None = None
        self.first_dom_ns: int | None = None
        self.last_dom_ns: int | None = None
        # state
        self._good: tuple[str, str] | None = None  # (url, token kind) that worked
        self._rows: asyncio.Queue = asyncio.Queue()
        self._stop = asyncio.Event()
        self._agg = Aggressor()
        self._book: tuple | None = None
        self._last_ts = 0
        self._decoder = TickDecoder()
        self._healthy = False

    # -- event handling (called from the reader task; synchronous)

    def _emit(self, ts: int, book: tuple, delta: int) -> None:
        ts = max(ts, self._last_ts)
        self._last_ts = ts
        bp, bs, ap, as_ = book
        self._rows.put_nowait((ts, bp, bs, ap, as_, delta))

    def _on_event(self, m: dict) -> None:
        e, d = m.get("e"), m.get("d")
        if e == "md":
            doms = parse_dom(d)
            if doms is None:
                if not (isinstance(d, dict) and any(k in d for k in ("q", "quotes", "quote"))):
                    self.unknown_events += 1
                return
            for cid, ts, book in doms:
                self._on_dom(cid, ts, book)
        elif e == "chart":
            for t in self._decoder.feed(d):
                self._on_trade(t)
        elif e != "props":
            self.unknown_events += 1

    def _on_dom(self, cid, ts: int | None, book: tuple | None) -> None:
        if self.contract_id is None and isinstance(cid, int):
            self.contract_id = cid
            _log(f"contract id taken from the first DOM event: {cid}")
        elif isinstance(cid, int) and cid != self.contract_id:
            return
        self.dom_updates += 1
        self._healthy = True
        if self._first_data_t is None:
            self._first_data_t = time.monotonic()
        if book is None:
            self.dom_empty += 1
            return
        ts = ts if ts is not None else time.time_ns()
        self.first_dom_ns = self.first_dom_ns or ts
        self.last_dom_ns = ts
        bp, bs, ap, as_ = book
        self.bid_levels.append(sum(1 for s in bs if s > 0))
        self.ask_levels.append(sum(1 for s in as_ if s > 0))
        self.max_bid_levels = max(self.max_bid_levels, self.bid_levels[-1])
        self.max_ask_levels = max(self.max_ask_levels, self.ask_levels[-1])
        if len(self.bid_levels) > 20_000:
            del self.bid_levels[:10_000], self.ask_levels[:10_000]
        self._agg.book(bp[0], ap[0])
        if self._book == book:
            return
        self._book = book
        self._emit(ts, book, 0)

    def _on_trade(self, t: Trade) -> None:
        side = self._agg.classify(t.price, t.bid, t.ask)
        self.ticks += 1
        if side > 0:
            self.buys += 1
            self.buy_volume += t.size
        elif side < 0:
            self.sells += 1
            self.sell_volume += t.size
        else:
            self.unknown += 1
            self.unknown_volume += t.size
        if self._book is None:
            self.dropped_trades += 1
            return
        self._emit(t.ts_ns, self._book, side * t.size)

    # -- connection

    def _token_kinds(self) -> list[tuple[str, str]]:
        kinds = []
        if self.auth.md_token:
            kinds.append(("mdAccessToken", self.auth.md_token))
        if self.auth.access_token and self.auth.access_token != self.auth.md_token:
            kinds.append(("accessToken", self.auth.access_token))
        return kinds

    async def _open(self, url: str):
        if self.connect_ws:
            return await self.connect_ws(url)
        from websockets.asyncio.client import connect
        return await connect(url, open_timeout=10, ping_interval=None, close_timeout=3)

    @staticmethod
    def _host(url: str) -> str:
        return re.sub(r"^\w+://([^/]+).*$", r"\1", url)

    async def _connect(self) -> MdSocket:
        """Open + authorize. Tries the remembered (url, token) first, then every url x token kind."""
        kinds = self._token_kinds()
        attempts = [(u, k, t) for u in self.md_urls for k, t in kinds]
        if self._good:
            attempts.sort(key=lambda a: (a[0], a[1]) != self._good)
        last = "no tokens"
        refused_only = bool(attempts)  # every failure so far was a non-200 authorize answer
        dead_urls: set[str] = set()
        for url, kind, token in attempts:
            if url in dead_urls:
                continue
            try:
                ws = await self._open(url)
            except Exception as e:  # noqa: BLE001
                last = f"{self._host(url)}: connect failed ({type(e).__name__})"
                _log(last)
                refused_only = False
                dead_urls.add(url)
                continue
            sock = MdSocket(ws, self._on_event)
            try:
                await sock.wait_open()
                sock.start()
                await sock.authorize(token)
            except BaseException as e:  # noqa: BLE001
                if not isinstance(e, Exception):
                    await sock.close()
                    raise
                refused_only = refused_only and isinstance(e, RequestFailed)
                last = f"{self._host(url)}: authorize with {kind} failed ({self.auth.redact(f'{type(e).__name__}: {e}')})"
                _log(last)
                await sock.close()
                continue
            if self._good != (url, kind):
                _log(f"md socket authorized: host {self._host(url)}, token {kind}")
            self._good, self.md_host, self.md_token_kind = (url, kind), self._host(url), kind
            return sock
        raise (MdRefused if refused_only else TradovateError)(f"md connect failed: {last}")

    async def _subscribe(self, sock: MdSocket) -> int | None:
        self._decoder = TickDecoder()  # new chart ids; history replayed after a reconnect is ignored (eoh gate)
        await sock.request("md/subscribeDOM", {"symbol": self.symbol})
        r = await sock.request("md/getChart", {
            "symbol": self.symbol,
            "chartDescription": {"underlyingType": "Tick", "elementSize": 1,
                                 "elementSizeUnit": "UnderlyingUnits", "withHistogram": False},
            "timeRange": {"asMuchAsElements": 1}})
        d = r.get("d") or {}
        self._decoder.ids = {i for i in (d.get("historicalId"), d.get("realtimeId")) if i is not None}
        return d.get("realtimeId")

    async def _unsubscribe(self, sock: MdSocket, realtime_id) -> None:
        for endpoint, body in (("md/cancelChart", {"subscriptionId": realtime_id} if realtime_id is not None else None),
                               ("md/unsubscribeDOM", {"symbol": self.symbol})):
            if body is None:
                continue
            try:
                await sock.request(endpoint, body, timeout=2.0)
            except Exception:  # noqa: BLE001 — best effort on the way out
                pass

    async def _session(self) -> None:
        """One connection from authorize to drop. Raises on a connect failure; returns on a drop."""
        await self.auth.ensure_fresh()
        sock = await self._connect()
        realtime_id = None
        self._healthy = False
        self._first_data_t = None
        t0 = time.monotonic()
        try:
            try:
                realtime_id = await self._subscribe(sock)
            except RequestFailed as e:
                raise MdRefused(self.auth.redact(e)) from None
            waiter = asyncio.create_task(sock.dropped.wait())
            stopper = asyncio.create_task(self._stop.wait())
            try:
                await asyncio.wait({waiter, stopper}, return_when=asyncio.FIRST_COMPLETED)
            finally:
                waiter.cancel()
                stopper.cancel()
            if sock.dropped.is_set():
                _log(f"connection dropped ({sock.drop_reason}) after {time.monotonic() - t0:.0f}s")
        finally:
            if not sock.dropped.is_set():
                await self._unsubscribe(sock, realtime_id)
            await sock.close()

    async def _supervisor(self) -> None:
        delay = self.backoff_s
        try:
            if not self.auth.access_token:  # tradovate_check.py logs in first to report it
                await self.auth.login()
            self.auth.start_renew_loop()
            try:
                self.contract_id = await self.auth.contract_id(self.symbol)
                _log(f"contract {self.symbol} id {self.contract_id}")
            except Exception as e:  # noqa: BLE001
                _log(f"contract lookup failed ({self.auth.redact(e)}); will use the first DOM contractId")
            while not self._stop.is_set():
                try:
                    await self._session()
                except (AuthError,) as e:
                    if isinstance(e, (ApiKeyRejected, BadCredentials, CaptchaRequired)):
                        raise
                    _log(f"auth problem: {self.auth.redact(e)}")
                except asyncio.CancelledError:
                    raise
                except MdRefused as e:
                    self._refusals += 1
                    _log(f"md refused ({self._refusals}): {self.auth.redact(e)}")
                    if self.dom_updates == 0 and self._refusals >= MAX_REFUSALS:
                        raise MdRefused(f"Tradovate market data refused: {self.auth.redact(e)} - likely no CME "
                                        "market-data entitlement for API access") from None
                except Exception as e:  # noqa: BLE001
                    _log(f"connection failed: {self.auth.redact(f'{type(e).__name__}: {e}')}")
                if self._stop.is_set():
                    break
                if self._first_data_t is not None and time.monotonic() - self._first_data_t >= HEALTHY_S:
                    delay, self._refusals = self.backoff_s, 0  # healthy long enough: a fresh backoff
                self.reconnects += 1
                _log(f"reconnecting in {delay:g}s")
                try:
                    await asyncio.wait_for(self._stop.wait(), delay)
                except TimeoutError:
                    pass
                delay = min(delay * 2, self.backoff_max_s)
        except asyncio.CancelledError:
            raise
        except Exception as e:  # noqa: BLE001 — fatal: surfaces from batches()
            exc = type(e) if isinstance(e, TradovateError) else TradovateError
            self._rows.put_nowait(exc(self.auth.redact(e if exc is type(e) else f"{type(e).__name__}: {e}")))
        finally:
            await self.auth.stop_renew_loop()

    async def batches(self):
        from trade_jev.live import rows_to_batch
        task = asyncio.create_task(self._supervisor())
        rows: list[tuple] = []
        last_flush = time.monotonic()
        done = False
        try:
            while not done:
                try:
                    item = await asyncio.wait_for(self._rows.get(), self.flush_s)
                except TimeoutError:
                    item = None
                if isinstance(item, Exception):
                    if rows:
                        yield rows_to_batch(rows)
                        rows = []
                    raise item
                if item is not None:
                    rows.append(item)
                    if self.stop_ns and item[0] > self.stop_ns:
                        done = True
                elif task.done() and self._rows.empty():
                    done = True
                if rows and (done or len(rows) >= 5000 or time.monotonic() - last_flush >= self.flush_s):
                    yield rows_to_batch(rows)
                    rows, last_flush = [], time.monotonic()
        finally:
            self._stop.set()
            try:
                await asyncio.wait_for(task, 8.0)
            except (TimeoutError, Exception):  # noqa: BLE001
                task.cancel()
                await asyncio.gather(task, return_exceptions=True)

    # -- reporting

    def stats(self) -> dict:
        return {"dom_updates": self.dom_updates, "ticks": self.ticks, "buys": self.buys, "sells": self.sells,
                "unknown": self.unknown, "reconnects": self.reconnects, "contract_id": self.contract_id,
                "md_token_kind": self.md_token_kind, "md_host": self.md_host,
                "max_bid_levels": self.max_bid_levels, "max_ask_levels": self.max_ask_levels,
                "median_bid_levels": median(self.bid_levels) if self.bid_levels else 0,
                "median_ask_levels": median(self.ask_levels) if self.ask_levels else 0,
                "dropped_trades": self.dropped_trades, "dom_unusable": self.dom_empty, "unknown_events": self.unknown_events}
