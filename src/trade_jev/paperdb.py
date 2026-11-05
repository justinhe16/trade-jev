"""Live paper-trading record: one SQLite file, written by the live runner, read by the viewer.

  data/paper.db (gitignored), or $TRADE_JEV_PAPER_DB. WAL mode, created on demand.

Tables (other tools read these; don't rename columns):
  paper_sessions   one row per session (session_id = the run dir name, e.g. live-2026-11-02)
                   status: running | done | crashed | holiday | aborted | skipped
                   (skipped: the runner chose not to trade, e.g. TRADE_JEV_SOURCE=off or missing
                   Tradovate credentials; the reason is in `error`)
                   source: the feed, databento | tradovate (NULL on older rows). Added to old DBs by connect().
                   timestamps ISO-8601 UTC; day = ET trading date. heartbeat_at is refreshed at least
                   every 60s while running: `running` with a stale heartbeat means the process died.
  paper_trades     one row per closed trade, every policy, written as it closes. side: long | short
  paper_decisions  the Jev decision log (one row per 15s decision)
  paper_summary    per policy, written at session end (and best effort on a crash)
  paper_bars       1-minute OHLC of the top-of-book mid (t_s = the minute's start, unix seconds), from every
                   quote, written as each minute completes (the last one at session end). For charts only:
                   nothing in the engine reads it. Backfill a recorded day with `python -m trade_jev.paperdb bars`.
  paper_open       the Jev policy's open paper position (one row while open, deleted when flat). Chart only.

The runner writes through `Recorder`, which queues statements to one writer thread, so the event
loop never waits on the disk. The viewer opens the file read-only (`connect_ro`).
"""

from __future__ import annotations

import json
import os
import queue
import sqlite3
import threading
from datetime import datetime, timezone
from pathlib import Path

import numpy as np

from trade_jev import ROOT
TICK = 0.25  # as trade_jev.data.TICK (not imported: that pulls pyarrow into every reader)

STALE_S = 180  # a running session whose heartbeat is older than this is presumed dead

SCHEMA = """
CREATE TABLE IF NOT EXISTS paper_sessions (
    session_id TEXT PRIMARY KEY, day TEXT, symbol TEXT, status TEXT,
    started_at TEXT, ended_at TEXT, heartbeat_at TEXT,
    settings_json TEXT, jev_model TEXT, run_dir TEXT, error TEXT
);
CREATE TABLE IF NOT EXISTS paper_trades (
    id INTEGER PRIMARY KEY, session_id TEXT, policy TEXT, side TEXT,
    entry_ns INTEGER, entry_px REAL, exit_ns INTEGER, exit_px REAL,
    reason TEXT, points REAL, pnl REAL
);
CREATE TABLE IF NOT EXISTS paper_decisions (
    session_id TEXT, policy TEXT, t_ns INTEGER, time_et TEXT, mid REAL, position INTEGER,
    action TEXT, raw_action TEXT, probs_json TEXT, jev_ms REAL, error TEXT
);
CREATE TABLE IF NOT EXISTS paper_summary (
    session_id TEXT, policy TEXT, trades INTEGER, wins INTEGER, pnl REAL, max_drawdown REAL,
    jev_calls INTEGER, jev_cost_usd REAL,
    PRIMARY KEY (session_id, policy)
);
CREATE TABLE IF NOT EXISTS paper_bars (
    session_id TEXT, t_s INTEGER, o REAL, h REAL, l REAL, c REAL,
    PRIMARY KEY (session_id, t_s)
);
CREATE TABLE IF NOT EXISTS paper_open (
    session_id TEXT, policy TEXT, side TEXT, entry_ns INTEGER, entry_px REAL,
    PRIMARY KEY (session_id, policy)
);
CREATE INDEX IF NOT EXISTS paper_sessions_day ON paper_sessions(day);
CREATE INDEX IF NOT EXISTS paper_trades_session ON paper_trades(session_id, exit_ns);
CREATE INDEX IF NOT EXISTS paper_decisions_session ON paper_decisions(session_id, t_ns);
"""


def db_path() -> Path:
    return Path(os.environ.get("TRADE_JEV_PAPER_DB") or ROOT / "data" / "paper.db").expanduser()


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def connect(path: Path | None = None) -> sqlite3.Connection:
    """Read-write connection; creates the file and schema if needed."""
    path = path or db_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    con = sqlite3.connect(path, timeout=10, check_same_thread=False)
    con.execute("PRAGMA journal_mode=WAL")
    con.execute("PRAGMA synchronous=NORMAL")
    con.executescript(SCHEMA)
    if "source" not in {r[1] for r in con.execute("PRAGMA table_info(paper_sessions)")}:
        try:
            con.execute("ALTER TABLE paper_sessions ADD COLUMN source TEXT")  # older DBs; idempotent
            con.commit()
        except sqlite3.OperationalError:  # another process added it first
            pass
    return con


def connect_ro(path: Path | None = None) -> sqlite3.Connection | None:
    """Read-only connection, or None if there is no database yet."""
    path = path or db_path()
    if not path.exists():
        return None
    con = sqlite3.connect(f"{path.resolve().as_uri()}?mode=ro", uri=True, timeout=5)
    con.row_factory = sqlite3.Row
    return con


def max_drawdown(pnls: list[float]) -> float:
    """Largest peak-to-trough drop of the cumulative closed-trade P&L (starting at 0), as metrics.py."""
    peak = cum = dd = 0.0
    for p in pnls:
        cum += p
        peak = max(peak, cum)
        dd = max(dd, peak - cum)
    return round(dd, 2)


def side_word(side: int) -> str:
    return "long" if side > 0 else "short"


# ---------------------------------------------------------------- minute bars

Bar = tuple[int, float, float, float, float]  # (minute start in unix s, o, h, l, c) in points


class MinuteBars:
    """1-minute OHLC of the top-of-book mid from every quote (prices in ticks, as the feed's Batch).
    `add` returns the minutes it completed; `flush` returns the one in progress. A row with an empty or
    crossed top of book is skipped, and so is a minute older than the current one (out-of-order data)."""

    def __init__(self) -> None:
        self.cur: list[int] | None = None  # [minute_s, o, h, l, c] in half-ticks (bid + ask)

    def add(self, ts: np.ndarray, bid: np.ndarray, ask: np.ndarray) -> list[Bar]:
        ok = (bid > 0) & (ask >= bid)
        ts = np.asarray(ts, np.int64)[ok]
        if not len(ts):
            return []
        two_mid = bid[ok].astype(np.int64) + ask[ok].astype(np.int64)
        minute = ts // 60_000_000_000 * 60
        starts = np.flatnonzero(np.r_[True, minute[1:] != minute[:-1]])
        done: list[Bar] = []
        for a, z in zip(starts, np.r_[starts[1:], len(ts)]):
            t, seg = int(minute[a]), two_mid[a:z]
            o, h, lo, c = int(seg[0]), int(seg.max()), int(seg.min()), int(seg[-1])
            cur = self.cur
            if cur is None or t > cur[0]:
                if cur is not None:
                    done.append(self._bar(cur))
                self.cur = [t, o, h, lo, c]
            elif t == cur[0]:
                cur[2], cur[3], cur[4] = max(cur[2], h), min(cur[3], lo), c
        return done

    def flush(self) -> list[Bar]:
        cur, self.cur = self.cur, None
        return [self._bar(cur)] if cur is not None else []

    @staticmethod
    def _bar(b: list[int]) -> Bar:
        return (b[0], *(v * TICK / 2 for v in b[1:]))  # type: ignore[return-value]


# ---------------------------------------------------------------- writer

class Recorder:
    """Writes one session. Every call only queues; a daemon thread executes and commits.
    A failing write is printed and dropped: the paper record must never stop the session."""

    def __init__(self, session_id: str, path: Path | None = None):
        self.session_id, self.path = session_id, path or db_path()
        self.q: queue.Queue = queue.Queue()
        self.errors = 0
        self.con = connect(self.path)  # create the schema now, so a bad path fails at startup
        self.thread = threading.Thread(target=self._worker, daemon=True)
        self.thread.start()

    def _put(self, sql: str, params: tuple = ()) -> None:
        self.q.put((sql, params))

    def _worker(self) -> None:
        while True:
            items = [self.q.get()]
            while True:
                try:
                    items.append(self.q.get_nowait())
                except queue.Empty:
                    break
            stop = None in items
            for it in items:  # one bad statement loses only itself; nothing may kill this thread
                if it is None:
                    continue
                try:
                    self.con.execute(*it)
                except Exception as e:  # noqa: BLE001
                    self.errors += 1
                    print(f"[paperdb] write failed: {type(e).__name__}: {e}", flush=True)
            try:
                self.con.commit()
            except Exception as e:  # noqa: BLE001
                self.errors += 1
                print(f"[paperdb] commit failed: {type(e).__name__}: {e}", flush=True)
            if stop:
                self.con.close()
                return

    # -- session lifecycle

    def start(self, day: str, symbol: str, settings: dict, jev_model: str | None, run_dir: str | None,
              source: str | None = None) -> None:
        now = utc_now()
        self._put("""INSERT INTO paper_sessions
                     (session_id, day, symbol, status, started_at, ended_at, heartbeat_at,
                      settings_json, jev_model, run_dir, error, source)
                     VALUES (?, ?, ?, 'running', ?, NULL, ?, ?, ?, ?, NULL, ?)
                     ON CONFLICT(session_id) DO UPDATE SET
                       day=excluded.day, symbol=excluded.symbol, status='running',
                       started_at=excluded.started_at, ended_at=NULL, heartbeat_at=excluded.heartbeat_at,
                       settings_json=excluded.settings_json, jev_model=excluded.jev_model,
                       run_dir=excluded.run_dir, error=NULL, source=excluded.source""",
                  (self.session_id, day, symbol, now, now, json.dumps(settings), jev_model, run_dir, source))

    def set_settings(self, settings: dict) -> None:
        self._put("UPDATE paper_sessions SET settings_json=? WHERE session_id=?",
                  (json.dumps(settings), self.session_id))

    def heartbeat(self) -> None:
        self._put("UPDATE paper_sessions SET heartbeat_at=? WHERE session_id=?", (utc_now(), self.session_id))

    def trade(self, policy: str, t) -> None:
        """`t` is a harness.Trade."""
        self._put("""INSERT INTO paper_trades
                     (session_id, policy, side, entry_ns, entry_px, exit_ns, exit_px, reason, points, pnl)
                     VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                  (self.session_id, policy, side_word(t.side), int(t.entry_ns), float(t.entry_px),
                   int(t.exit_ns), float(t.exit_px), t.reason, float(t.points), float(t.pnl)))

    def decision(self, policy: str, d: dict) -> None:
        """`d` is a decision record as the engine logs it (decisions.jsonl)."""
        self._put("""INSERT INTO paper_decisions
                     (session_id, policy, t_ns, time_et, mid, position, action, raw_action,
                      probs_json, jev_ms, error)
                     VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                  (self.session_id, policy, int(d["t_ns"]), d["time_et"], float(d["mid"]), int(d["position"]),
                   d["action"], d.get("raw_action"), json.dumps(d.get("probs") or {}),
                   d.get("jev_ms"), d.get("error")))

    def bar(self, t_s: int, o: float, h: float, lo: float, c: float) -> None:
        """One completed 1-minute mid bar (see `MinuteBars`)."""
        self._put("INSERT OR REPLACE INTO paper_bars (session_id, t_s, o, h, l, c) VALUES (?, ?, ?, ?, ?, ?)",
                  (self.session_id, int(t_s), float(o), float(h), float(lo), float(c)))

    def open_position(self, policy: str, side: int, entry_ns: int = 0, entry_px: float = 0.0) -> None:
        """The policy's open position now: a row while long/short, none while flat."""
        if side:
            self._put("""INSERT OR REPLACE INTO paper_open (session_id, policy, side, entry_ns, entry_px)
                         VALUES (?, ?, ?, ?, ?)""",
                      (self.session_id, policy, side_word(side), int(entry_ns), float(entry_px)))
        else:
            self._put("DELETE FROM paper_open WHERE session_id=? AND policy=?", (self.session_id, policy))

    def finish(self, status: str, summary: list[dict], error: str | None = None) -> None:
        """End the session: summary rows (one per policy) and the final status."""
        for s in summary:
            self._put("""INSERT OR REPLACE INTO paper_summary
                         (session_id, policy, trades, wins, pnl, max_drawdown, jev_calls, jev_cost_usd)
                         VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
                      (self.session_id, s["policy"], s["trades"], s["wins"], s["pnl"], s["max_drawdown"],
                       s.get("jev_calls", 0), s.get("jev_cost_usd", 0.0)))
        now = utc_now()
        self._put("UPDATE paper_sessions SET status=?, ended_at=?, heartbeat_at=?, error=? WHERE session_id=?",
                  (status, now, now, error, self.session_id))

    def close(self, timeout: float = 10.0) -> None:
        """Drain the queue and stop the writer (blocks; call once, at exit)."""
        if self.thread.is_alive():
            self.q.put(None)
            self.thread.join(timeout)


def record_skip(session_id: str, day: str, symbol: str | None, status: str, reason: str,
                source: str | None = None, path: Path | None = None) -> None:
    """A day the runner chose not to trade (`holiday` or `skipped`): one row with the reason in `error`, so a
    watcher knows the runner ran. Never overwrites a real session: only holiday/skipped rows are replaced."""
    now = utc_now()
    con = connect(path)
    try:
        with con:
            con.execute("""INSERT INTO paper_sessions
                           (session_id, day, symbol, status, started_at, ended_at, heartbeat_at, error, source)
                           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                           ON CONFLICT(session_id) DO UPDATE SET status=excluded.status,
                             ended_at=excluded.ended_at, heartbeat_at=excluded.heartbeat_at,
                             error=excluded.error, source=excluded.source
                           WHERE paper_sessions.status IN ('holiday', 'skipped')""",
                        (session_id, day, symbol, status, now, now, now, reason, source))
    finally:
        con.close()


def record_holiday(session_id: str, day: str, symbol: str | None, reason: str, path: Path | None = None) -> None:
    """A holiday / weekend: one `holiday` row (see `record_skip`)."""
    record_skip(session_id, day, symbol, "holiday", reason, path=path)


# ---------------------------------------------------------------- readers (viewer)

def _stale(row: dict, now: datetime | None = None) -> bool:
    if row["status"] != "running" or not row["heartbeat_at"]:
        return False
    now = now or datetime.now(timezone.utc)
    return (now - datetime.fromisoformat(row["heartbeat_at"])).total_seconds() > STALE_S


def _live_summary(con: sqlite3.Connection, session_id: str) -> dict[str, dict]:
    """Per-policy totals from the trades so far (for sessions without a summary yet)."""
    out: dict[str, dict] = {}
    for r in con.execute("SELECT policy, pnl FROM paper_trades WHERE session_id=? ORDER BY exit_ns, id",
                         (session_id,)):
        out.setdefault(r["policy"], []).append(r["pnl"])
    return {p: {"trades": len(v), "wins": sum(1 for x in v if x > 0), "pnl": round(sum(v), 2),
                "max_drawdown": max_drawdown(v), "jev_calls": None, "jev_cost_usd": None}
            for p, v in out.items()}


def _policies(con: sqlite3.Connection, session_id: str) -> tuple[dict[str, dict], bool]:
    """(per-policy totals, final?) — the summary if written, else live totals from the trades."""
    rows = con.execute("SELECT * FROM paper_summary WHERE session_id=?", (session_id,)).fetchall()
    if rows:
        return {r["policy"]: {k: r[k] for k in ("trades", "wins", "pnl", "max_drawdown", "jev_calls",
                                                 "jev_cost_usd")} for r in rows}, True
    return _live_summary(con, session_id), False


def list_sessions(path: Path | None = None, limit: int = 500) -> dict:
    con = connect_ro(path)
    if con is None:
        return {"db": str(path or db_path()), "exists": False, "sessions": []}
    try:
        out = []
        for r in con.execute("SELECT * FROM paper_sessions ORDER BY day DESC, started_at DESC LIMIT ?", (limit,)):
            row = dict(r)
            row.pop("settings_json")
            row["stale"] = _stale(row)
            row["policies"], row["final"] = _policies(con, row["session_id"])
            out.append(row)
        return {"db": str(path or db_path()), "exists": True, "sessions": out}
    finally:
        con.close()


def get_session(session_id: str, path: Path | None = None) -> dict | None:
    con = connect_ro(path)
    if con is None:
        return None
    try:
        r = con.execute("SELECT * FROM paper_sessions WHERE session_id=?", (session_id,)).fetchone()
        if r is None:
            return None
        s = dict(r)
        s["settings"] = json.loads(s.pop("settings_json") or "null")
        s["stale"] = _stale(s)
        s["policies"], s["final"] = _policies(con, session_id)
        s["trades"] = [dict(t) for t in con.execute(
            "SELECT * FROM paper_trades WHERE session_id=? ORDER BY exit_ns, id", (session_id,))]
        s["decisions"] = []
        for d in con.execute("SELECT * FROM paper_decisions WHERE session_id=? ORDER BY t_ns", (session_id,)):
            d = dict(d)
            d["probs"] = json.loads(d.pop("probs_json") or "{}")
            s["decisions"].append(d)
        return s
    finally:
        con.close()


# ---------------------------------------------------------------- backfill (CLI)

def backfill_bars(session_id: str, parquet: Path, path: Path | None = None, chunk: int = 200_000) -> int:
    """Write `paper_bars` for a session from its recorded feed (a day Parquet file, as live.Recorder writes):
    the same `MinuteBars` the live runner uses. Idempotent (rows are replaced). Returns the bar count."""
    import pyarrow.compute as pc
    import pyarrow.parquet as pq

    bars, out = MinuteBars(), []
    for b in pq.ParquetFile(parquet).iter_batches(chunk, columns=["ts_event", "bid_px", "ask_px"]):
        ts = b.column("ts_event").cast("int64").to_numpy()
        top = [np.rint(pc.list_element(b.column(c), 0).to_numpy(zero_copy_only=False) / TICK).astype(np.int64)
               for c in ("bid_px", "ask_px")]
        out += bars.add(ts, *top)
    out += bars.flush()
    con = connect(path)
    try:
        with con:
            con.executemany("INSERT OR REPLACE INTO paper_bars (session_id, t_s, o, h, l, c) VALUES (?, ?, ?, ?, ?, ?)",
                            [(session_id, *bar) for bar in out])
    finally:
        con.close()
    return len(out)


def main(argv: list[str] | None = None) -> None:
    import argparse

    ap = argparse.ArgumentParser(prog="python -m trade_jev.paperdb", description=__doc__.splitlines()[0])
    sub = ap.add_subparsers(dest="cmd", required=True)
    b = sub.add_parser("bars", help="backfill paper_bars for a recorded day (the chart's 1-min candles)")
    b.add_argument("--day", required=True, help="YYYY-MM-DD")
    b.add_argument("--session", help="session id (default live-DAY)")
    b.add_argument("--file", type=Path, help="the day's Parquet recording (default: the one in data/live)")
    args = ap.parse_args(argv)
    from trade_jev.data import LIVE_DIR

    f = args.file or next(iter(sorted(LIVE_DIR.glob(f"*__{args.day}__live.parquet"))), None)
    if f is None or not f.exists():
        ap.error(f"no recording for {args.day} in {LIVE_DIR}")
    sid = args.session or f"live-{args.day}"
    print(f"{sid}: {backfill_bars(sid, f)} bars from {f.name} → {db_path()}")


if __name__ == "__main__":
    main()
