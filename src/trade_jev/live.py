"""Live: stream NQ L10 (Databento or Tradovate), ask Jev every 15s, apply the filters, paper-trade, serve a live view.

  uv run python -m trade_jev.live                                   # live session → http://localhost:8765/live
  uv run python -m trade_jev.live --from-file 2026-06-23 --speed 60 # rehearsal from a recorded day
  uv run python -m trade_jev.live --from-file 2026-06-23 --speed 0 --stored-answers runs/<id>  # no Jev calls

The backtest code runs unchanged on a growing `LiveDay`: same decision grid (09:30:00 ET + k·15s),
same encoder, same `Gated` filters, same harness `Book` (fills `latency_ms` after the order,
stops/targets on every book update). The engine consumes book rows strictly in time order and
pauses while Jev answers, so a recorded day replays to exactly the backtest's trades.

One live-only difference: the order is sent when Jev answers (t + Jev latency), not at t.
Both are recorded per decision (`t_ns`, `send_ns`, `jev_ms`).

No orders are placed. A human trades from the screen.

Unattended daily paper trading (deploy/launchd + scripts/run_paper_day.sh):
  uv run python -m trade_jev.live --exit-at-close --no-open     # exits 0 after the session's outputs
Weekends and CME full-closure holidays are skipped (exit 0, a `holiday` row). Every live session is
also recorded in the paper DB (trade_jev.paperdb: sessions, trades as they close, Jev decisions,
summary), which the viewer's /paper tab reads.

Feed: `--source databento|tradovate` (default $TRADE_JEV_SOURCE, else databento). TRADE_JEV_SOURCE=off, or
`tradovate` with any TRADOVATE_* credential missing (or, in token mode, with the token expired and no fresh
tokens in .env: reason "tradovate token expired"), skips the day: a `skipped` row with the reason, exit 0,
no feed started. Databento replays the session so far after a late start. Tradovate cannot: a start after
the 09:28 ET open instead warms up for 60s on live data (every decision HOLD, no Jev call) and skips the
grid points it missed. See src/trade_jev/tradovate.py and README "Tradovate source".
"""

from __future__ import annotations

import argparse
import asyncio
import dataclasses
import heapq
import json
import os
import queue
import signal
import sys
import threading
import time
import traceback
import webbrowser
from dataclasses import asdict, fields
from datetime import date, datetime, timedelta
from http.server import ThreadingHTTPServer
from pathlib import Path

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq

from trade_jev import ROOT, paperdb, tradingdays
from trade_jev.tradovate import TradovateConfig, TradovateError, TradovateSource, missing_credentials
from trade_jev.tradovate_token import auth_mode, token_skip_reason
from trade_jev.data import (ET, LEVELS, LIVE_DIR, TICK, Day, decision_grid, et_to_ns, list_days,
                            ns_to_et, secs)
from trade_jev.encode import LOOKBACKS, Context
from trade_jev.harness import Book, Config, DayResult
from trade_jev.metrics import JEV_USD_PER_TOKEN
from trade_jev.outputs import print_results, write_config, write_equity, write_results
from trade_jev.policies import BASELINES, Decision, Gated, JevPolicy, JsonlCache, RateLimiter
from trade_jev.settings import DEFAULT, Settings

STALE_S = 5.0          # no feed records for this long during the session → STALE
CATCH_UP_S = 30.0      # a decision older than this (wall clock) is catch-up: shown, never alerted
WARMUP_S = 60          # Tradovate late start: no catch-up replay exists, so decide HOLD for this long
SOURCES = ("databento", "tradovate")


# ---------------------------------------------------------------- growing day

class LiveDay:
    """`data.Day` that grows: same attributes and methods, so Context / encoders / Book work unchanged.
    Full L10 books are kept only for decision rows (`store_book`), like `load_day`."""

    row_at = Day.row_at
    book = Day.book
    mid = Day.mid

    def __init__(self, day: str, symbol: str, cap: int = 1 << 16):
        self.day, self.symbol = day, symbol
        self.n = 0
        self._ts = np.empty(cap, np.int64)
        self._bid = np.empty(cap, np.int32)
        self._ask = np.empty(cap, np.int32)
        self._cd = np.empty(cap, np.int64)
        self._rows: list[int] = []
        self._books: list[np.ndarray] = []
        self.last_book = np.zeros((4, LEVELS), np.int64)  # book of the last appended row
        self._views()

    def _views(self) -> None:
        n = self.n
        self.ts, self.bid, self.ask, self.cum_delta = self._ts[:n], self._bid[:n], self._ask[:n], self._cd[:n]

    @property
    def book_rows(self) -> np.ndarray:
        return np.asarray(self._rows, np.int64)

    @property
    def books(self) -> np.ndarray:
        return np.stack(self._books) if self._books else np.zeros((0, 4, LEVELS), np.int64)

    def append(self, b: "Batch") -> None:
        k = len(b.ts)
        if not k:
            return
        need = self.n + k
        if need > len(self._ts):
            cap = max(need, 2 * len(self._ts))
            for name in ("_ts", "_bid", "_ask", "_cd"):
                old = getattr(self, name)
                new = np.empty(cap, old.dtype)
                new[:self.n] = old[:self.n]
                setattr(self, name, new)
        s = slice(self.n, need)
        self._ts[s] = b.ts
        self._bid[s] = b.bid_px[:, 0]
        self._ask[s] = b.ask_px[:, 0]
        base = self._cd[self.n - 1] if self.n else 0
        self._cd[s] = base + np.cumsum(b.delta)
        self.last_book = np.stack([b.bid_px[-1], b.bid_sz[-1], b.ask_px[-1], b.ask_sz[-1]]).astype(np.int64)
        self.n = need
        self._views()

    def store_book(self) -> int:
        """Keep the full book of the last row (a decision row). Returns its index."""
        row = self.n - 1
        if row >= 0 and (not self._rows or self._rows[-1] != row):
            self._rows.append(row)
            self._books.append(self.last_book.copy())
        return row


@dataclasses.dataclass
class Batch:
    """Normalized book rows, the columns of the day Parquet files (prices in ticks)."""
    ts: np.ndarray      # int64 ns
    bid_px: np.ndarray  # (n, 10) int32 ticks
    bid_sz: np.ndarray  # (n, 10) int64
    ask_px: np.ndarray
    ask_sz: np.ndarray
    delta: np.ndarray   # int64 trade_delta

    def __getitem__(self, s: slice) -> "Batch":
        return Batch(self.ts[s], self.bid_px[s], self.bid_sz[s], self.ask_px[s], self.ask_sz[s], self.delta[s])

    def __len__(self) -> int:
        return len(self.ts)


# ---------------------------------------------------------------- sources

class ParquetSource:
    """Stream a recorded day file. speed=0: as fast as possible; speed=60: 1 min of data per second."""

    def __init__(self, path: Path, speed: float = 0, chunk: int = 2000):
        self.path, self.speed, self.chunk = path, speed, chunk
        self.live = False

    async def batches(self):
        pf = pq.ParquetFile(self.path)
        t_data0 = t_wall0 = None
        for g in range(pf.num_row_groups):
            t = pf.read_row_group(g, columns=["ts_event", "bid_px", "bid_sz", "ask_px", "ask_sz", "trade_delta"])
            n = t.num_rows
            lv = {c: t.column(c).combine_chunks().values.to_numpy(zero_copy_only=False).reshape(n, LEVELS)
                  for c in ("bid_px", "bid_sz", "ask_px", "ask_sz")}
            b = Batch(t.column("ts_event").combine_chunks().cast(pa.int64()).to_numpy(),
                      np.rint(lv["bid_px"] / TICK).astype(np.int32), lv["bid_sz"].astype(np.int64),
                      np.rint(lv["ask_px"] / TICK).astype(np.int32), lv["ask_sz"].astype(np.int64),
                      t.column("trade_delta").combine_chunks().to_numpy().astype(np.int64))
            step = self.chunk if self.speed else 200_000
            for i in range(0, n, step):
                part = b[i:i + step]
                if self.speed:
                    if t_data0 is None:
                        t_data0, t_wall0 = int(part.ts[0]), time.monotonic()
                    ahead = (int(part.ts[-1]) - t_data0) / 1e9 / self.speed - (time.monotonic() - t_wall0)
                    if ahead > 0:
                        await asyncio.sleep(ahead)
                yield part
            await asyncio.sleep(0)


UNDEF = 2**63 - 1
PX_PER_TICK = int(TICK * 1e9)  # Databento prices are fixed-point 1e-9


def mbp10_row(rec) -> tuple | None:
    """One Databento MBP10Msg → (ts, bid_px[10], bid_sz[10], ask_px[10], ask_sz[10], delta), prices in ticks.
    Empty levels are padded (one tick further out, size 0) so every row has 10 levels.
    Trade delta: +size if a buyer hit the ask, −size if a seller hit the bid (as data/DATA.md)."""
    lv = rec.levels
    bp, bs, ap, as_ = [0] * LEVELS, [0] * LEVELS, [0] * LEVELS, [0] * LEVELS
    for i, l in enumerate(lv[:LEVELS]):
        if l.bid_px != UNDEF:
            bp[i], bs[i] = l.bid_px // PX_PER_TICK, l.bid_sz
        elif i == 0:
            return None
        else:
            bp[i], bs[i] = bp[i - 1] - 1, 0
        if l.ask_px != UNDEF:
            ap[i], as_[i] = l.ask_px // PX_PER_TICK, l.ask_sz
        elif i == 0:
            return None
        else:
            ap[i], as_[i] = ap[i - 1] + 1, 0
    delta = 0
    if str(rec.action) in ("T", "Action.TRADE"):
        side = str(rec.side)
        delta = rec.size if side in ("B", "Side.BID") else -rec.size if side in ("A", "Side.ASK") else 0
    return rec.ts_event, bp, bs, ap, as_, delta


def rows_to_batch(rows: list[tuple]) -> Batch:
    ts, bp, bs, ap, as_, d = zip(*rows)
    return Batch(np.array(ts, np.int64), np.array(bp, np.int32), np.array(bs, np.int64),
                 np.array(ap, np.int32), np.array(as_, np.int64), np.array(d, np.int64))


class DatabentoSource:
    """Databento Live, GLBX.MDP3 mbp-10. Starting late? Intraday replay from `start_ns` fills the gap."""

    def __init__(self, symbol: str, start_ns: int | None, stop_ns: int, flush_s: float = 0.1):
        self.symbol, self.start_ns, self.stop_ns, self.flush_s = symbol, start_ns, stop_ns, flush_s
        self.live = True
        self.instrument_ids: set[int] = set()

    async def batches(self):
        import databento as db
        client = db.Live(key=os.environ.get("DATABENTO_API_KEY"), reconnect_policy="reconnect")
        client.subscribe(dataset="GLBX.MDP3", schema="mbp-10", symbols=[self.symbol],
                         stype_in="raw_symbol", start=self.start_ns)
        client.start()
        rows: list[tuple] = []
        last_flush = time.monotonic()
        try:
            async for rec in client:
                if isinstance(rec, db.ErrorMsg):
                    print(f"[databento] error: {rec.err}", flush=True)
                    continue
                if not isinstance(rec, db.MBP10Msg):
                    continue
                r = mbp10_row(rec)
                if r is not None:
                    rows.append(r)
                    self.instrument_ids.add(rec.instrument_id)
                if rows and (len(rows) >= 5000 or time.monotonic() - last_flush >= self.flush_s):
                    yield rows_to_batch(rows)
                    rows, last_flush = [], time.monotonic()
                    if self.stop_ns and rec.ts_event > self.stop_ns:
                        break
            if rows:
                yield rows_to_batch(rows)
        finally:
            client.stop()


class Recorder:
    """Writes the live feed to data/live/ in the day-file format, so it loads with `load_day`."""

    SCHEMA = pa.schema([
        ("ts_event", pa.timestamp("ns", tz="UTC")), ("instrument_id", pa.int64()),
        ("bid_px", pa.list_(pa.float64())), ("bid_sz", pa.list_(pa.int64())),
        ("ask_px", pa.list_(pa.float64())), ("ask_sz", pa.list_(pa.int64())),
        ("trade_delta", pa.int64()),
    ])

    def __init__(self, day: str, symbol: str, out_dir: Path = LIVE_DIR, rows_per_group: int = 100_000,
                 feed: str = "databento"):
        self.feed = feed
        out_dir.mkdir(parents=True, exist_ok=True)
        self.path = out_dir / f"GLBX.MDP3__{symbol}__mbp-10__rth__{day}__live.parquet"
        self.day, self.symbol, self.rows_per_group = day, symbol, rows_per_group
        self.writer = pq.ParquetWriter(self.path, self.SCHEMA)
        self.pending: list[Batch] = []
        self.n_pending = self.rows = 0

    def add(self, b: Batch) -> None:
        self.pending.append(b)
        self.n_pending += len(b)
        if self.n_pending >= self.rows_per_group:
            self.flush()

    def flush(self) -> None:
        if not self.pending:
            return
        b = Batch(*(np.concatenate([getattr(p, f.name) for p in self.pending]) for f in fields(Batch)))
        n = len(b)

        def lists(a, px):
            vals = a.astype(np.float64).ravel() * TICK if px else a.astype(np.int64).ravel()
            return pa.ListArray.from_arrays(pa.array(np.arange(0, n * LEVELS + 1, LEVELS, dtype=np.int32)),
                                            pa.array(vals))
        tbl = pa.table({
            "ts_event": pa.array(b.ts, pa.timestamp("ns", tz="UTC")),
            "instrument_id": pa.array(np.zeros(n, np.int64)),
            "bid_px": lists(b.bid_px, True), "bid_sz": lists(b.bid_sz, False),
            "ask_px": lists(b.ask_px, True), "ask_sz": lists(b.ask_sz, False),
            "trade_delta": pa.array(b.delta),
        }, schema=self.SCHEMA)
        self.writer.write_table(tbl)
        self.rows += n
        self.pending, self.n_pending = [], 0

    def close(self) -> None:
        self.flush()
        self.writer.close()
        self.path.with_suffix(".json").write_text(json.dumps({
            "dataset": "GLBX.MDP3", "symbol": self.symbol, "schema": "mbp-10", "day": self.day,
            "session": "rth", "rows": self.rows, "writer": "trade_jev.live", "feed": self.feed}, indent=2))


# ---------------------------------------------------------------- policies

class Guarded:
    """Times out / catches Jev errors. A failed call is a no-confidence HOLD, so `Gated` breaks the streak."""

    def __init__(self, inner, timeout_s: float):
        self.inner, self.timeout_s = inner, timeout_s
        self.name = inner.name
        self.errors = 0
        self.last_ms = 0.0
        self.last_error: str | None = None

    async def __call__(self, ctx: Context) -> Decision:
        t0 = time.monotonic()
        try:
            d = await asyncio.wait_for(self.inner(ctx), self.timeout_s)
            self.last_error = None
        except Exception as e:  # noqa: BLE001 — any failure is a HOLD, logged
            self.errors += 1
            self.last_error = f"{type(e).__name__}: {e}"[:200] or "timeout"
            d = Decision("HOLD", {}, None)
        self.last_ms = (time.monotonic() - t0) * 1000
        return d


class StoredAnswers:
    """Plays back a run's stored Jev answers (before gating). For rehearsals and parity, no API calls."""

    def __init__(self, answers: dict[int, dict], name: str = "jev[raw_l10]"):
        self.answers, self.name = answers, name

    async def __call__(self, ctx: Context) -> Decision:
        a = self.answers.get(ctx.t_ns)
        if a is None:
            return Decision("HOLD", {})
        return Decision(a.get("raw_action") or a["action"], a["probs"], a.get("state"), a.get("tokens", 0))


# ---------------------------------------------------------------- engine

class Engine:
    """Consumes book rows in time order and fires decision / fill / session-end events on the backtest's
    clock. An event at time t fires once the first row with ts > t arrives, so rows ≤ t are final."""

    def __init__(self, day: str, symbol: str, cfg: Config, gate: Settings, jev, baselines: list[str],
                 answer_latency: bool = False, on_change=None, warmup_s: float = 0):
        self.cfg, self.gate = cfg, gate
        self.warmup_s = warmup_s  # late first row (after open_ns): HOLD for this long, no policy calls
        self.open_ns = et_to_ns(day, cfg.start_et) - secs(max(LOOKBACKS) + 60)
        self.warmup_until_ns = 0
        self.first_row_seen = False
        self.day = LiveDay(day, symbol)
        self.grid = decision_grid(day, cfg.start_et, cfg.end_et, cfg.cadence_s)
        self.end_ns = int(self.grid[-1]) + secs(cfg.cadence_s)
        self.gi = 0
        self.answer_latency = answer_latency  # live: order goes out when Jev answers
        self.on_change = on_change or (lambda kind, **kw: None)
        self.policies: dict[str, object] = {}
        self.jev_name: str | None = None
        if jev is not None:
            self.guard = Guarded(jev, getattr(jev, "timeout_s", 8.0))
            self.real_jev = isinstance(jev, JevPolicy)
            g = Gated(self.guard, gate.min_conf, gate.agree, gate.min_hold_s)
            self.policies[g.name] = g
            self.jev_name = g.name
        for b in baselines:
            p = BASELINES[b]()
            self.policies[p.name] = p
        self.results = {n: DayResult(day, n) for n in self.policies}
        self.books = {n: Book(self.day, cfg, self.results[n]) for n in self.policies}
        self.fills: list[tuple[int, int, str, int, int]] = []  # heap: (fill_t, seq, policy, side, send_ns)
        self.seq = 0
        self.decided: list[int] = []
        self.closed = False
        self.jev_tokens = 0
        self.jev_calls = 0
        self.jev_ms: list[float] = []

    # -- clock

    def next_event(self) -> int:
        t = self.end_ns
        if self.gi < len(self.grid):
            t = min(t, int(self.grid[self.gi]))
        if self.fills:
            t = min(t, self.fills[0][0])
        return t

    async def feed(self, b: Batch) -> None:
        i, n = 0, len(b)
        if n and not self.first_row_seen:
            self.first_row_seen = True
            first = int(b.ts[0])
            if self.warmup_s and first > self.open_ns:  # late start with no replay: skip the missed grid, warm up
                self.warmup_until_ns = first + secs(self.warmup_s)
                self.gi = int(np.searchsorted(self.grid, first, side="left"))
                self.on_change("warmup", until_ns=self.warmup_until_ns)
        while i < n and not self.closed:
            t = self.next_event()
            k = i + int(np.searchsorted(b.ts[i:], t, side="right"))
            if k > i:
                self.day.append(b[i:k])
                self._check_exits()
            if k == n:
                break
            await self._fire(t)
            i = k

    async def finish(self) -> None:
        """Data ended (file end, early close, Ctrl-C): fire what the data covers, then flatten."""
        if self.closed or self.day.n == 0:
            self.closed = True
            return
        last_ts = int(self.day.ts[-1])
        while not self.closed and self.next_event() <= last_ts and self.next_event() < self.end_ns:
            await self._fire(self.next_event())
        while self.fills:  # in-flight orders fill at the last book
            await self._fire(self.fills[0][0])
        self._close_session(self.day.n - 1)

    async def _fire(self, t: int) -> None:
        if self.fills and self.fills[0][0] == t:
            while self.fills and self.fills[0][0] == t:
                _, _, name, side, send_ns = heapq.heappop(self.fills)
                before = len(self.results[name].trades)
                self.books[name].go(side, send_ns)
                self.on_change("fill", policy=name, side=side, closed=len(self.results[name].trades) > before)
            return
        if self.gi < len(self.grid) and int(self.grid[self.gi]) == t:
            self.gi += 1
            await self._decide(t)
            return
        if t == self.end_ns:
            self._close_session(int(self.day.row_at(t)))

    def _check_exits(self) -> None:
        last = self.day.n - 1
        for name, bk in self.books.items():
            before = len(self.results[name].trades)
            bk.check_exits(last)
            if len(self.results[name].trades) > before:
                self.on_change("exit", policy=name, trade=self.results[name].trades[-1])

    async def _decide(self, t: int) -> None:
        row = self.day.store_book()
        if row < 0:
            return
        self.decided.append(t)
        prev = {lb: int(self.day.row_at(t - secs(lb))) for lb in LOOKBACKS}
        warm = t < self.warmup_until_ns
        for name, p in self.policies.items():
            bk = self.books[name]
            before = bk.pos.side
            ctx = Context(self.day, t, row, prev, bk.pos)
            d = Decision("HOLD", {}, None, raw_action="WARMUP") if warm else await p(ctx)
            rec = {"day": self.day.day, "t_ns": t, "time_et": ns_to_et(t), "mid": ctx.mid,
                   "position": before, "action": d.action, "raw_action": d.raw_action, "probs": d.probs,
                   "tokens": d.tokens, "cached": d.cached, "state": d.state}
            send_ns = t
            if name == self.jev_name:
                ms = 0.0 if warm else self.guard.last_ms
                if self.real_jev and not d.cached and d.tokens:
                    self.jev_calls += 1
                    self.jev_tokens += d.tokens
                    self.jev_ms.append(ms)
                if self.answer_latency:
                    send_ns = t + int(ms * 1e6)
                rec.update(jev_ms=round(ms, 1), send_ns=send_ns, error=None if warm else self.guard.last_error,
                           streak=self.streak())
            self.results[name].decisions.append(rec)
            side = {"BUY": 1, "SELL": -1}.get(d.action, 0)
            if side and side != bk.pos.side:  # as Book.go: already on that side → no order
                heapq.heappush(self.fills, (send_ns + secs(self.cfg.latency_ms / 1000), self.seq, name, side, send_ns))
                self.seq += 1
                self.on_change("order", policy=name, side=side, t=t, send_ns=send_ns)
        self.on_change("decision", t=t)

    def _close_session(self, last: int) -> None:
        if self.closed:
            return
        self.closed = True
        for name, bk in self.books.items():
            bk.check_exits(last)
            if bk.pos.side != 0:
                px = float(self.day.bid[last] if bk.pos.side > 0 else self.day.ask[last]) * TICK
                bk._close(last, px, "eod")
        self.on_change("closed")

    def order_pending(self, name: str) -> bool:
        return any(f[2] == name for f in self.fills)

    def streak(self) -> dict:
        """Trailing run of confident same-side answers, capped at `agree`."""
        g = self.policies.get(self.jev_name)
        hist = g.hist.get(self.day.day, []) if g else []
        side, k = None, 0
        for a in reversed(hist):
            if a is None or (side and a != side):
                break
            side, k = a, k + 1
        return {"side": side, "k": min(k, self.gate.agree), "need": self.gate.agree}


# ---------------------------------------------------------------- session (engine + state + outputs)

class Bus:
    """Fan-out of server-sent events to browser tabs (thread-safe)."""

    def __init__(self):
        self.subs: list[queue.Queue] = []
        self.lock = threading.Lock()

    def subscribe(self) -> queue.Queue:
        q: queue.Queue = queue.Queue(maxsize=1000)
        with self.lock:
            self.subs.append(q)
        return q

    def unsubscribe(self, q: queue.Queue) -> None:
        with self.lock:
            if q in self.subs:
                self.subs.remove(q)

    def publish(self, event: str, data: dict) -> None:
        msg = f"event: {event}\ndata: {json.dumps(data)}\n\n".encode()
        with self.lock:
            for q in self.subs:
                try:
                    q.put_nowait(msg)
                except queue.Full:
                    pass


class IntentLink:
    """Sends order intents and heartbeats to the executor (POST JSON, in order, never blocking the engine)."""

    def __init__(self, url: str, heartbeat, every_s: float = 2.0, timeout_s: float = 1.0):
        self.url, self.heartbeat, self.every_s, self.timeout_s = url.rstrip("/"), heartbeat, every_s, timeout_s
        self.q: queue.Queue = queue.Queue(maxsize=200)
        self.sent = self.errors = 0
        self.last_error: str | None = None
        threading.Thread(target=self._worker, daemon=True).start()
        threading.Thread(target=self._beat, daemon=True).start()

    def send(self, path: str, msg: dict) -> None:
        try:
            self.q.put_nowait((path, msg))
        except queue.Full:
            self.errors += 1
            self.last_error = "queue full: executor not keeping up"

    def beat_now(self) -> None:
        self.send("/heartbeat", self.heartbeat())

    def _beat(self) -> None:
        while True:
            time.sleep(self.every_s)
            self.beat_now()

    def _worker(self) -> None:
        import urllib.request
        while True:
            path, msg = self.q.get()
            req = urllib.request.Request(self.url + path, json.dumps(msg).encode(),
                                         {"Content-Type": "application/json"})
            try:
                urllib.request.urlopen(req, timeout=self.timeout_s).read()
                self.sent += 1
                self.last_error = None
            except Exception as e:  # noqa: BLE001 — executor down or slow; logged, shown on the page
                self.errors += 1
                self.last_error = f"{type(e).__name__}: {e}"[:200]
                if path == "/intent":
                    print(f"[exec] intent {msg.get('seq')} not delivered: {self.last_error}", flush=True)

    def stats(self) -> dict:
        return {"url": self.url, "sent": self.sent, "errors": self.errors, "last_error": self.last_error}


class Session:
    def __init__(self, args, cfg: Config, gate: Settings, day: str, symbol: str, source, jev,
                 baselines: list[str], run_dir: Path | None, recorder: Recorder | None,
                 exec_url: str | None = None, paper: paperdb.Recorder | None = None,
                 deadline_ns: int | None = None, warmup_s: float = 0):
        self.args, self.cfg, self.gate, self.source = args, cfg, gate, source
        self.paper_settings: dict | None = None  # the settings_json written at start; updated on a warm-up
        self.run_dir, self.recorder = run_dir, recorder
        self.paper = paper              # paper DB writer (trades as they close, Jev decisions)
        self.deadline_ns = deadline_ns  # wall clock: stop reading the feed after this (early halts)
        self.bus = Bus()
        self.engine = Engine(day, symbol, cfg, gate, jev, baselines,
                             answer_latency=source.live, on_change=self._changed, warmup_s=warmup_s)
        self.mode = "live" if source.live else "replay"
        self.engine.real_jev = getattr(self.engine, "real_jev", False)
        self.status = "WAITING"
        self.quote = {"ts": 0, "bid": None, "ask": None}
        self.track: list[list[int]] = []  # [epoch_s, bid_ticks, ask_ticks] per second
        self.last_rx = time.monotonic()
        self.manual: list[dict] = []
        self.signal = {"side": 0, "action": "FLAT", "reason": None, "t_ns": None, "entry_px": None,
                       "catch_up": False}
        self.snapshot: dict = {}
        self.dirty = False
        self.last_tick = 0.0
        self.written = False
        self.t_start = time.time()
        # executor link (Phase 2): intents for the Jev paper position, arm state, executor status
        self.armed = False
        self.target = 0
        self.intent_seq = 0
        self.intents: list[dict] = []
        self.executor: dict | None = None
        self.executor_rx = 0.0
        self.disarm_reason: str | None = None
        self.link = IntentLink(exec_url, self.heartbeat) if exec_url else None
        self.dec_fh = None
        if run_dir and self.engine.jev_name:
            p = run_dir / self.engine.jev_name / "decisions.jsonl"
            p.parent.mkdir(parents=True, exist_ok=True)
            self.dec_fh = p.open("w")
        self.paper_sent = {n: 0 for n in self.engine.policies}  # trades already written, per policy
        # chart-only paper rows (paper_bars, paper_open): fed from the quotes and the Jev book, never read back
        self.paper_bars = paperdb.MinuteBars()
        self.paper_open: tuple[int, int] = (0, 0)  # the Jev (side, entry_ns) last written to paper_open
        self._rebuild()

    # -- feed side

    def on_batch(self, b: Batch) -> None:
        self.last_rx = time.monotonic()
        if self.recorder:
            self.recorder.add(b)
        if self.paper:
            self._paper_safe(self._paper_minute_bars, b)
        ts, bid, ask = int(b.ts[-1]), int(b.bid_px[-1, 0]), int(b.ask_px[-1, 0])
        sec = ts // 1_000_000_000
        if self.track and self.track[-1][0] == sec:
            self.track[-1][1:] = [bid, ask]
        else:
            if self.track:  # carry the last quote across empty seconds
                last = self.track[-1]
                for s in range(last[0] + 1, min(sec, last[0] + 600)):
                    self.track.append([s, last[1], last[2]])
            self.track.append([sec, bid, ask])
        self.quote = {"ts": ts, "bid": bid, "ask": ask}
        if time.monotonic() - self.last_tick >= 0.25:
            self.last_tick = time.monotonic()
            self.bus.publish("tick", self.tick())

    @property
    def status_now(self) -> str:
        e = self.engine
        if self.status == "LIVE" and e.decided and (time.time_ns() - e.decided[-1]) / 1e9 > CATCH_UP_S:
            return "CATCH-UP"
        return self.status

    def tick(self) -> dict:
        lag = (time.time_ns() - self.quote["ts"]) / 1e6 if self.source.live and self.quote["ts"] else None
        return {**self.quote, "lag_ms": round(lag) if lag is not None else None, "status": self.status_now}

    async def _flusher(self) -> None:
        while True:
            await asyncio.sleep(0.25)
            if self.armed and self.executor_rx and time.monotonic() - self.executor_rx > 10:
                self.set_armed(False, "executor stopped reporting")
            if self.armed and self.status_now == "STALE":
                self.set_armed(False, "feed stale")
            if self.dirty:
                self._rebuild()
                self.bus.publish("update", {"status": self.status_now})

    async def run(self) -> None:
        watchdog = asyncio.create_task(self._watchdog()) if self.source.live else None
        flusher = asyncio.create_task(self._flusher())
        timeout = max(0.0, (self.deadline_ns - time.time_ns()) / 1e9) if self.deadline_ns else None
        batches = self.source.batches()
        try:
            async with asyncio.timeout(timeout) as deadline:  # a quiet feed (13:00 ET halt) can't hang us
                async for b in batches:
                    self.on_batch(b)
                    await self.engine.feed(b)
                    if self.engine.closed and not self.source.live:
                        break
                    if self.status == "WAITING" and self.engine.decided:
                        self.status = "REPLAY" if not self.source.live else "LIVE"
        except TimeoutError:
            if not deadline.expired():
                raise  # a timeout inside the feed is a failure, not the session end
            print("feed past the session end with no more data; closing the session", flush=True)
        finally:
            await batches.aclose()
            if watchdog:
                watchdog.cancel()
            flusher.cancel()
            await self.engine.finish()
            self.status = "CLOSED"
            self.set_armed(False, "session closed")
            self._rebuild()
            self.bus.publish("update", {"status": self.status})

    async def _watchdog(self) -> None:
        while True:
            await asyncio.sleep(1)
            now_ns = time.time_ns()
            in_session = int(self.engine.grid[0]) <= now_ns <= self.engine.end_ns
            if self.engine.closed:
                continue
            if in_session and time.monotonic() - self.last_rx > STALE_S:
                if self.status != "STALE":
                    self.status = "STALE"
                    self.bus.publish("update", {"status": self.status})
            elif self.status == "STALE":
                self.status = "LIVE"
                self.bus.publish("update", {"status": self.status})
            self.bus.publish("tick", self.tick())

    # -- engine side

    def _changed(self, kind: str, **kw) -> None:
        e = self.engine
        jn = e.jev_name or "imbalance"
        if kind == "warmup":
            until = ns_to_et(kw["until_ns"])
            print(f"late start: warming up {e.warmup_s:g}s on live data (no catch-up replay on Tradovate); "
                  f"decisions until {until} ET are HOLD", flush=True)
            if self.paper and self.paper_settings is not None:
                self.paper_settings["warmup_until_et"] = until
                self._paper_safe(self.paper.set_settings, self.paper_settings)
            return
        if kind == "decision" and e.results.get(e.jev_name) and e.results[e.jev_name].decisions:
            last = e.results[e.jev_name].decisions[-1]
            if self.dec_fh:
                self.dec_fh.write(json.dumps(last) + "\n")
                self.dec_fh.flush()
            if self.paper:
                self._paper_safe(self.paper.decision, e.jev_name, last)
        if self.paper:
            self._paper_safe(self._paper_trades)
            self._paper_safe(self._paper_open_position)
        if kind in ("fill", "exit", "closed") and (kw.get("policy") in (None, jn)):
            self._update_signal(kind, kw)
        if e.jev_name and kw.get("policy") in (None, e.jev_name):
            self._intent(kind, kw)
        self.dirty = True

    @staticmethod
    def _paper_safe(fn, *a) -> None:
        """The paper record must never stop the session: a bad row is logged and skipped."""
        try:
            fn(*a)
        except Exception as e:  # noqa: BLE001
            print(f"[paperdb] not recorded: {type(e).__name__}: {e}", flush=True)

    def _paper_trades(self) -> None:
        """Queue every newly closed trade (stop, target, reversal, session end) of every policy."""
        for name, r in self.engine.results.items():
            for t in r.trades[self.paper_sent[name]:]:
                self.paper.trade(name, t)
            self.paper_sent[name] = len(r.trades)

    def _paper_minute_bars(self, b: Batch) -> None:
        """Queue each 1-minute mid bar the batch completes (chart only; see paperdb.MinuteBars)."""
        for bar in self.paper_bars.add(b.ts, b.bid_px[:, 0], b.ask_px[:, 0]):
            self.paper.bar(*bar)

    def _paper_open_position(self) -> None:
        """Write the Jev paper position when it changes (chart only: an open trade on the price chart)."""
        e = self.engine
        if not e.jev_name:
            return
        pos = e.books[e.jev_name].pos
        key = (pos.side, pos.entry_ns if pos.side else 0)
        if key != self.paper_open:
            self.paper.open_position(e.jev_name, pos.side, pos.entry_ns, pos.entry_px)
            self.paper_open = key

    def paper_flush(self) -> None:
        """Session end: the minute in progress, and the final Jev position."""
        for bar in self.paper_bars.flush():
            self.paper.bar(*bar)
        self._paper_open_position()

    def paper_summary(self) -> list[dict]:
        """paper_summary rows: one per policy, from the closed trades so far."""
        e = self.engine
        out = []
        for name, r in e.results.items():
            pnls = [t.pnl for t in r.trades]
            jev = name == e.jev_name and e.real_jev
            out.append({"policy": name, "trades": len(pnls), "wins": sum(1 for p in pnls if p > 0),
                        "pnl": round(sum(pnls), 2), "max_drawdown": paperdb.max_drawdown(pnls),
                        "jev_calls": e.jev_calls if jev else 0,
                        "jev_cost_usd": round(e.jev_tokens * JEV_USD_PER_TOKEN, 4) if jev else 0.0})
        return out

    # -- executor link

    def _intent(self, kind: str, kw: dict) -> None:
        """Turn Jev paper-position changes into intents: an order queued → ±1; a stop/target exit or the
        session end → 0 (unless a new order is already queued, which will land on its own side)."""
        e = self.engine
        if kind == "order":
            target, reason, t = kw["side"], "jev", kw["t"]
        elif kind == "exit" and not e.order_pending(e.jev_name):
            target, reason, t = 0, kw["trade"].reason, kw["trade"].exit_ns
        elif kind == "closed":
            target, reason, t = 0, "eod", int(e.day.ts[-1]) if e.day.n else 0
        elif kind == "fill" and self.intents and self.intents[-1]["target"] == kw["side"]:
            bk = e.books[e.jev_name]
            if bk.pos.side == kw["side"]:
                self.intents[-1]["paper_fill_px"] = bk.pos.entry_px
            return
        else:
            return
        if target == self.target:
            return
        self.target = target
        self.intent_seq += 1
        q = self.quote
        catch_up = bool(self.source.live and t and (time.time_ns() - t) / 1e9 > CATCH_UP_S)
        msg = {"type": "intent", "seq": self.intent_seq, "day": e.day.day, "symbol": e.day.symbol,
               "mode": self.mode, "t_ns": int(t), "send_ns": int(kw.get("send_ns") or t), "sent_wall_ns": time.time_ns(),
               "target": target, "reason": reason,
               "ref": {"bid": q["bid"] * TICK if q["bid"] is not None else None,
                       "ask": q["ask"] * TICK if q["ask"] is not None else None},
               "stop_ticks": self.cfg.stop_ticks, "target_ticks": self.cfg.target_ticks,
               "armed": self.armed, "catch_up": catch_up, "paper_fill_px": None}
        self.intents.append(msg)
        if self.link:
            self.link.send("/intent", msg)

    def heartbeat(self) -> dict:
        return {"type": "heartbeat", "seq_last": self.intent_seq, "target": self.target, "armed": self.armed,
                "status": self.status_now, "mode": self.mode, "sent_wall_ns": time.time_ns()}

    def set_armed(self, armed: bool, reason: str | None = None) -> dict:
        armed = bool(armed)
        if armed and (self.link is None or not self.executor_rx or self.status in ("CLOSED", "STALE")):
            raise ValueError("can't arm: needs a connected executor and an open session")
        changed = armed != self.armed
        self.armed = armed
        self.disarm_reason = None if armed else reason
        if changed:
            print(f"[exec] {'ARMED' if armed else 'disarmed'}{f' ({reason})' if reason and not armed else ''}", flush=True)
            if self.link:
                self.link.beat_now()
            self.dirty = True
            self.bus.publish("update", {"kind": "arm"})
        return {"armed": self.armed, "reason": self.disarm_reason}

    def on_executor_status(self, body: dict) -> dict:
        self.executor = {k: body.get(k) for k in (
            "broker", "armed_ack", "halted", "position", "working_orders", "last_action", "last_latency_ms",
            "day_pnl", "orders_today", "errors", "alert", "preflight")}
        self.executor_rx = time.monotonic()
        self.dirty = True
        return {"ok": True, "armed": self.armed}

    def _update_signal(self, kind: str, kw: dict) -> None:
        e = self.engine
        name = e.jev_name or "imbalance"
        if name not in e.books:
            return
        bk = e.books[name]
        prev, side = self.signal["side"], bk.pos.side
        if side == prev:
            return
        last_trade = e.results[name].trades[-1] if e.results[name].trades else None
        if side == 0:
            reason = last_trade.reason if last_trade else "flat"
            action = {"stop": "STOP HIT: FLAT", "target": "TARGET HIT: FLAT",
                      "eod": "SESSION END: FLAT"}.get(reason, "FLAT")
        else:
            word = "LONG" if side > 0 else "SHORT"
            action = f"REVERSE TO {word}" if prev and prev != side else f"ENTER {word}"
            reason = "jev"
        t_ns = int(e.day.ts[-1]) if e.day.n else None
        catch_up = bool(self.source.live and t_ns and (time.time_ns() - t_ns) / 1e9 > CATCH_UP_S)
        self.signal = {"side": side, "action": action, "reason": reason, "t_ns": t_ns,
                       "et": ns_to_et(t_ns) if t_ns else None,
                       "entry_px": bk.pos.entry_px if side else None, "catch_up": catch_up}

    def _rebuild(self) -> None:
        """JSON-able state for the page, swapped in whole (safe to read from server threads)."""
        self.dirty = False
        e, c, g = self.engine, self.cfg, self.gate
        pols = {}
        for name, bk in e.books.items():
            r = e.results[name]
            pols[name] = {
                "side": bk.pos.side, "entry_px": bk.pos.entry_px if bk.pos.side else None,
                "entry_et": ns_to_et(bk.pos.entry_ns) if bk.pos.side else None,
                "realized": round(sum(t.pnl for t in r.trades), 2), "n_trades": len(r.trades),
                "trades": [{**asdict(t), "entry_et": ns_to_et(t.entry_ns), "exit_et": ns_to_et(t.exit_ns)}
                           for t in r.trades[-200:]],
            }
        decs = []
        if e.jev_name:
            for i, d in enumerate(e.results[e.jev_name].decisions):
                decs.append({"i": i, "et": d["time_et"], "t_ns": d["t_ns"], "mid": d["mid"],
                             "raw": d.get("raw_action") or d["action"], "action": d["action"],
                             "p": d["probs"], "pos": d["position"], "jev_ms": d.get("jev_ms"),
                             "err": d.get("error"), "streak": d.get("streak")})
        self.snapshot = {
            "active": True, "mode": self.mode, "status": self.status, "day": e.day.day, "symbol": e.day.symbol,
            "run_dir": str(self.run_dir.relative_to(ROOT)) if self.run_dir else None,
            "speed": getattr(self.source, "speed", None),
            "settings": {**asdict(g), "cadence_s": c.cadence_s, "latency_ms": c.latency_ms,
                         "commission": c.commission, "start_et": c.start_et, "end_et": c.end_et},
            "tick": TICK, "point_value": 20.0,
            "jev": None if not e.jev_name else {
                "name": e.jev_name, "stored": not e.real_jev, "calls": e.jev_calls, "errors": e.guard.errors,
                "avg_ms": round(float(np.mean(e.jev_ms)), 0) if e.jev_ms else None,
                "last_ms": round(e.guard.last_ms, 0), "last_error": e.guard.last_error,
                "cost_usd": round(e.jev_tokens * JEV_USD_PER_TOKEN, 4)},
            "streak": e.streak() if e.jev_name else None,
            "signal": self.signal,
            "signal_policy": e.jev_name or "imbalance",
            "policies": pols,
            "decisions": decs,
            "decided": len(e.decided), "grid": len(e.grid),
            "manual": self.manual,
            "exec": None if self.link is None else {
                "armed": self.armed, "disarm_reason": self.disarm_reason, "target": self.target,
                "link": self.link.stats(), "status": self.executor,
                "intents": self.intents[-50:]},
        }

    def state(self, window_s: int = 3600) -> dict:
        st = {**self.snapshot, "status": self.status_now, "quote": self.tick(), "track": self.track[-window_s:]}
        if st.get("exec"):
            age = time.monotonic() - self.executor_rx if self.executor_rx else None
            st["exec"] = {**st["exec"], "armed": self.armed, "disarm_reason": self.disarm_reason,
                          "status_age_s": round(age, 1) if age is not None else None}
        return st

    def decision_state(self, i: int) -> dict | None:
        e = self.engine
        decs = e.results[e.jev_name].decisions if e.jev_name else []
        return decs[i] if 0 <= i < len(decs) else None

    def add_manual_fill(self, body: dict) -> dict:
        rec = {"action": str(body.get("action", "")).upper()[:10], "price": float(body["price"]),
               "contracts": int(body.get("contracts", 1)), "note": str(body.get("note", ""))[:200],
               "t_ns": time.time_ns(), "time_et": datetime.now(ET).strftime("%H:%M:%S"),
               "paper_side": self.signal["side"], "paper_entry_px": self.signal["entry_px"],
               "source": "executor" if body.get("source") == "executor" else "manual"}
        if rec["source"] == "executor":
            seq = int(body.get("seq") or 0)
            intent = next((i for i in self.intents if i["seq"] == seq), None)
            rec.update(seq=seq, ack_ms=body.get("ack_ms"), latency_ms=body.get("latency_ms"),
                       paper_entry_px=intent.get("paper_fill_px") if intent else None,
                       paper_side=intent["target"] if intent else None)
        self.manual.append(rec)
        if self.run_dir:
            with (self.run_dir / "manual_fills.jsonl").open("a") as fh:
                fh.write(json.dumps(rec) + "\n")
        self._rebuild()
        self.bus.publish("update", {"kind": "manual"})
        return rec

    def write_outputs(self) -> None:
        if self.written:
            return
        self.written = True
        if self.dec_fh:
            self.dec_fh.close()
        if self.recorder:
            self.recorder.close()
            print(f"recorded feed → {self.recorder.path} ({self.recorder.rows:,} rows)")
        if not self.run_dir or not self.engine.decided:
            return
        e = self.engine
        grid = np.asarray(e.decided, np.int64)
        write_equity(self.run_dir, e.day, grid, list(e.results.values()))
        jev = [e.guard.inner] if e.jev_name and e.real_jev else []
        rows = int(e.day.row_at(grid[-1]) - e.day.row_at(grid[0]) + 1)
        summary, record = write_results(
            self.run_dir, self.run_dir.name, {n: [r] for n, r in e.results.items()}, [e.day.day], self.cfg,
            wall_s=round(time.time() - self.t_start, 1), snapshots=len(grid), book_rows=rows,
            jev_policies=jev, encoder=self.args.encoder, model=self.args.model,
            gate={"min_conf": self.gate.min_conf, "agree": self.gate.agree, "min_hold_s": self.gate.min_hold_s},
            live=self.mode == "live", source=self.mode,
            jev_avg_ms=round(float(np.mean(e.jev_ms)), 1) if e.jev_ms else None,
            manual_fills=len(self.manual))
        print_results(self.run_dir, record, summary)


# ---------------------------------------------------------------- server

def serve(session: Session | None, port: int, open_browser: bool) -> ThreadingHTTPServer | None:
    """The live view in this process. A taken port is a warning, not a crash: the session matters more."""
    from trade_jev.view import Handler
    try:
        srv = ThreadingHTTPServer(("127.0.0.1", port), Handler)
    except OSError as e:
        print(f"WARNING: no live view (port {port}: {e}); the session runs without it", flush=True)
        return None
    srv.daemon_threads = True
    srv.live = session
    threading.Thread(target=srv.serve_forever, daemon=True).start()
    url = f"http://localhost:{port}/live"
    print(f"live view → {url}   (replay: http://localhost:{port}/  docs: http://localhost:{port}/docs)", flush=True)
    if open_browser:
        webbrowser.open(url)
    return srv


# ---------------------------------------------------------------- main

def front_month(today: datetime) -> str:
    """NQ quarterly front month (H M U Z), rolling 8 days before the 3rd-Friday expiry."""
    codes = {3: "H", 6: "M", 9: "U", 12: "Z"}
    d = today.date()
    for y, m in [(d.year, 3), (d.year, 6), (d.year, 9), (d.year, 12), (d.year + 1, 3)]:
        first = date(y, m, 1)
        third_fri = date(y, m, 1 + (4 - first.weekday()) % 7 + 14)
        if d < third_fri - timedelta(days=8):
            return f"NQ{codes[m]}{y % 10}"
    raise AssertionError("unreachable")


def new_run_dir(prefix: str, day: str) -> Path:
    base = ROOT / "runs" / f"{prefix}-{day}"
    return base if not base.exists() else base.with_name(f"{base.name}-{datetime.now():%H%M%S}")


def now_et() -> datetime:
    return datetime.now(ET)


async def heartbeat(paper: paperdb.Recorder, every_s: float = 30.0) -> None:
    """Runs on the event loop, so a hung loop stops the heartbeat and the session shows as dead."""
    while True:
        await asyncio.sleep(every_s)
        paper.heartbeat()


def error_text(e: BaseException) -> str:
    return (f"{type(e).__name__}: {e}\n" + traceback.format_exc(limit=8))[-4000:]


def source_from_env(env=os.environ) -> str:
    """TRADE_JEV_SOURCE: databento (default) | tradovate | off. Returns the value, lower-cased."""
    return (env.get("TRADE_JEV_SOURCE") or "databento").strip().lower()


def skip_reason(source: str, env=os.environ, explicit: bool = False) -> str | None:
    """Why today's live run must not start a feed (names only, never secret values), or None.
    TRADE_JEV_SOURCE=off is only a default: an explicit --source (`explicit`) wins over it."""
    if source_from_env(env) == "off" and not explicit:
        return "TRADE_JEV_SOURCE=off"
    if source == "tradovate":
        if auth_mode(env) == "token":  # tokens, not a password: dead store and no fresh .env tokens -> skip
            return token_skip_reason(env)
        missing = missing_credentials(env)
        if missing:
            return "tradovate credentials missing: " + ", ".join(missing)
    return None


async def amain(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description="Live (or rehearsed) Jev signals on NQ with a local view.")
    ap.add_argument("--source", choices=SOURCES, default=None,
                    help="live feed: databento or tradovate (default: $TRADE_JEV_SOURCE, else databento; "
                         "TRADE_JEV_SOURCE=off skips the day)")
    ap.add_argument("--symbol", default=None,
                    help="raw symbol for the chosen feed (e.g. NQZ6 on both), default: NQ front month")
    ap.add_argument("--from-file", metavar="DAY", default=None, help="rehearse on a recorded day (no subscription)")
    ap.add_argument("--speed", type=float, default=60.0, help="--from-file: x real time (0 = as fast as possible)")
    ap.add_argument("--stored-answers", metavar="RUN_DIR", default=None, help="play back a run's Jev answers")
    ap.add_argument("--no-jev", action="store_true", help="baselines only, no Jev calls")
    ap.add_argument("--no-catch-up", action="store_true",
                    help="live, started after 09:30: skip replaying the session so far")
    ap.add_argument("--baselines", default="hold,random,imbalance")
    ap.add_argument("--encoder", default="raw_l10")
    ap.add_argument("--model", default="jev-latest")
    ap.add_argument("--jev-timeout", type=float, default=8.0, help="seconds; a timeout counts as HOLD")
    ap.add_argument("--min-conf", type=float, default=DEFAULT.min_conf)
    ap.add_argument("--agree", type=int, default=DEFAULT.agree)
    ap.add_argument("--min-hold", type=float, default=DEFAULT.min_hold_s)
    for f in fields(Config):
        ap.add_argument(f"--{f.name.replace('_', '-')}", type=type(f.default), default=f.default)
    ap.add_argument("--port", type=int, default=8765)
    ap.add_argument("--no-open", action="store_true")
    ap.add_argument("--no-save", action="store_true", help="don't write runs/ or data/live/")
    ap.add_argument("--exec-url", default=None,
                    help="send order intents to an executor, e.g. http://127.0.0.1:8766 (arm it on the Live page)")
    ap.add_argument("--exit-at-close", action="store_true",
                    help="exit 0 once the session's outputs are written (and when there is no session today)")
    ap.add_argument("--paper-db", action=argparse.BooleanOptionalAction, default=None,
                    help="record the session in the paper DB (default: on for live sessions that save)")
    args = ap.parse_args(argv)
    explicit_source = args.source is not None
    env_source = source_from_env()
    if env_source not in (*SOURCES, "off"):
        ap.error(f"TRADE_JEV_SOURCE={env_source!r}: use databento, tradovate or off")
    if args.source is None:
        args.source = "databento" if env_source == "off" else env_source

    cfg = Config(**{f.name: getattr(args, f.name) for f in fields(Config)})
    gate = Settings(args.min_conf, args.agree, args.min_hold, cfg.stop_ticks, cfg.target_ticks)
    backtest = Config()
    if (cfg.cadence_s, cfg.start_et, cfg.end_et, cfg.latency_ms) != \
            (backtest.cadence_s, backtest.start_et, backtest.end_et, backtest.latency_ms):
        print("WARNING: the timing differs from the backtest (15s grid from 09:30:00 ET, 250ms latency); "
              "the registered settings were chosen on that clock.", flush=True)
    use_paper = args.paper_db if args.paper_db is not None else (not args.from_file and not args.no_save)

    recorder, deadline_ns = None, None
    warmup_s, late_start = 0, False
    if args.from_file:
        day = args.from_file
        days = list_days()
        if day not in days:
            ap.error(f"no recorded day {day}; have {sorted(days)}")
        symbol, path = days[day]
        source = ParquetSource(path, args.speed)
        run_dir = None if args.no_save else new_run_dir("rehearsal", day)
    else:
        now = now_et()
        now_ns = int(now.timestamp() * 1e9)
        day = now.date().isoformat()
        symbol = args.symbol or front_month(now)
        if not tradingdays.covered(day):
            print(f"WARNING: no CME holiday list for {now.year} (trade_jev/tradingdays.py); "
                  "only weekends are skipped", flush=True)
        if (closed := tradingdays.closed_reason(day)) is not None:
            print(f"{day}: no NQ session ({closed}); nothing to do", flush=True)
            if use_paper:
                paperdb.record_holiday(f"live-{day}", day, symbol, closed)
            return 0
        if (why := skip_reason(args.source, explicit=explicit_source)) is not None:
            print(f"{day}: skipped ({why}); no feed started", flush=True)
            if use_paper:
                paperdb.record_skip(f"live-{day}", day, symbol, "skipped", why,
                                    source="off" if why == "TRADE_JEV_SOURCE=off" else args.source)
            return 0
        open_ns = et_to_ns(day, cfg.start_et) - secs(max(LOOKBACKS) + 60)
        start_ns = None
        if now_ns > open_ns:  # late start: intraday replay so the 60s look-back and streaks exist
            start_ns = now_ns - secs(max(LOOKBACKS) + 60) if args.no_catch_up else open_ns
        stop_ns = et_to_ns(day, cfg.end_et) + secs(300)
        if now_ns > stop_ns:
            if args.exit_at_close:
                print(f"the {day} session is over (ends {cfg.end_et} ET); nothing to do", flush=True)
                return 0
            ap.error(f"the {day} session is over (ends {cfg.end_et} ET); use --from-file to rehearse")
        if args.source == "tradovate":
            try:
                TradovateConfig.from_env()  # validates TRADOVATE_ENV; credentials were checked by skip_reason
            except TradovateError as e:
                ap.error(str(e))
            source = TradovateSource(symbol, stop_ns)
            late_start = now_ns > open_ns
            warmup_s = WARMUP_S
            how = ("now" if not late_start else f"now (late start: {WARMUP_S}s warm-up, no catch-up replay)")
        else:
            if not os.environ.get("DATABENTO_API_KEY"):
                ap.error("set DATABENTO_API_KEY in .env (or use --from-file)")
            source = DatabentoSource(symbol, start_ns, stop_ns)
            how = "now" if start_ns is None else ns_to_et(start_ns) + " ET (intraday replay)"
        deadline_ns = stop_ns + secs(120)
        run_dir = None if args.no_save else new_run_dir("live", day)
        if not args.no_save:
            recorder = Recorder(day, symbol, feed=args.source)
        print(f"{symbol} live ({args.source}) from {how}; "
              f"decisions {cfg.start_et}–{cfg.end_et} ET every {cfg.cadence_s:g}s", flush=True)

    jev, client = None, None
    if args.stored_answers:
        from trade_jev.replay import load_runs
        runs = load_runs([Path(args.stored_answers)])
        if day not in runs:
            ap.error(f"{args.stored_answers} has no answers for {day}")
        jev = StoredAnswers(runs[day].answers)
    elif not args.no_jev:
        from typesafe_sdk import AsyncTypeSafeClient
        client = AsyncTypeSafeClient()
        jev = JevPolicy(client, JsonlCache(ROOT / "cache" / "jev.jsonl"), RateLimiter(15),
                        encoder=args.encoder, model=args.model)
    if jev is not None:
        jev.timeout_s = args.jev_timeout

    paper = None
    if use_paper:
        session_id = run_dir.name if run_dir else f"{'live' if source.live else 'rehearsal'}-{day}"
        paper = paperdb.Recorder(session_id)
        print(f"paper record → {paper.path} (session {session_id})", flush=True)

    baselines = [b for b in args.baselines.split(",") if b]
    session = Session(args, cfg, gate, day, symbol, source, jev, baselines, run_dir, recorder, args.exec_url,
                      paper=paper, deadline_ns=deadline_ns, warmup_s=warmup_s)
    if run_dir:
        write_config(run_dir, cfg, [day], list(session.engine.policies), args.encoder, args.model,
                     {"min_conf": gate.min_conf, "agree": gate.agree, "min_hold_s": gate.min_hold_s},
                     live=source.live, symbol=symbol)
    hb = None
    if paper:
        model = None if jev is None else f"stored:{args.stored_answers}" if args.stored_answers else args.model
        settings = {**session.snapshot["settings"], "encoder": args.encoder,
                    "policies": list(session.engine.policies)}
        if late_start:
            settings["late_start_warmup_s"] = WARMUP_S
        session.paper_settings = settings
        paper.start(day, symbol, settings, model, session.snapshot["run_dir"],
                    source=None if args.from_file else args.source)
        hb = asyncio.create_task(heartbeat(paper))
    sigterm = []  # launchd / the extension stop jobs with SIGTERM: close the session like Ctrl-C, exit 0
    main_task = asyncio.current_task()

    def on_sigterm() -> None:
        sigterm.append(True)
        main_task.cancel()
    try:
        asyncio.get_running_loop().add_signal_handler(signal.SIGTERM, on_sigterm)
    except (NotImplementedError, RuntimeError):
        pass
    serve(session, args.port, not args.no_open)
    print(f"settings: {gate.label()} · Ctrl-C to stop", flush=True)

    def end_paper(status: str, error: str | None = None) -> None:
        nonlocal paper
        if paper is None:
            return
        if hb:
            hb.cancel()
        session._paper_safe(session.paper_flush)
        try:
            session._paper_trades()
            summary = session.paper_summary()
        except Exception as e:  # noqa: BLE001 — best effort on the way down
            summary, error = [], (error or "") + f"\nsummary failed: {e}"
        paper.finish(status, summary, error)
        paper.close()
        paper = None

    try:
        await session.run()
        if not args.from_file and args.source == "tradovate" and session.engine.day.n == 0:
            raise RuntimeError("no Tradovate book data received all session: likely no CME market-data "
                               "entitlement for API access yet (run scripts/tradovate_check.py)")
        session.write_outputs()
        end_paper("done")
        if args.exit_at_close:
            print("session closed; outputs written. Exiting (--exit-at-close).", flush=True)
            return 0
        print("session closed; outputs written. The view stays up — Ctrl-C to quit.", flush=True)
        while True:
            await asyncio.sleep(3600)
    except asyncio.CancelledError:  # SIGTERM (launchctl kill, the extension's stop) exits 0; Ctrl-C 130
        end_paper("aborted", "stopped before the session end (" + ("SIGTERM)" if sigterm else "Ctrl-C)"))
        return 0 if sigterm else 130
    except Exception as e:
        end_paper("crashed", error_text(e))
        raise
    finally:
        try:
            await session.engine.finish()
            session.write_outputs()
        finally:
            end_paper("crashed", "exited without a final status")
            if client is not None:
                await client.aclose()


def main() -> None:
    try:
        code = asyncio.run(amain())
    except KeyboardInterrupt:
        print("\nstopped.")
        code = 130
    except Exception:  # noqa: BLE001 — already recorded as `crashed`; print and exit 1
        traceback.print_exc()
        code = 1
    # Everything is written by now (outputs, feed recording, paper DB joined). Exit hard so a stray
    # non-daemon thread (feed client, SDK) can't keep an unattended run alive past the session.
    sys.stdout.flush()
    sys.stderr.flush()
    os._exit(code)


if __name__ == "__main__":
    main()
