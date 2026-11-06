"""data/copy.db: the copy daemon's SQLite ledger (the daemon is the only writer; `clear-halts` is the one
exception). WAL. Order rows are written BEFORE the POST, so a crash mid-placement leaves a `sending` row that
startup treats as unknown. The Resonant extension reads this file read-only for the `copy` panel and job."""

from __future__ import annotations

import sqlite3
import threading
import time
from datetime import datetime
from pathlib import Path

from trade_jev.data import ET

SCHEMA = """
CREATE TABLE IF NOT EXISTS copy_status(name TEXT PRIMARY KEY, day TEXT, updated_at TEXT, mode TEXT, env TEXT,
  root TEXT, qty INTEGER, contract TEXT, armed INTEGER, gate_reason TEXT, pos_side INTEGER, pos_qty INTEGER,
  avg_px REAL, realized REAL, open_pnl REAL, halted INTEGER, halt_kind TEXT, token_ok INTEGER,
  token_expires_at TEXT, entries INTEGER, errors INTEGER, last_error TEXT);
CREATE TABLE IF NOT EXISTS copy_signals(name TEXT, boot TEXT, seq INTEGER, day TEXT, target INTEGER, reason TEXT,
  t_ns INTEGER, outcome TEXT, detail TEXT, PRIMARY KEY(name, boot, seq));
CREATE TABLE IF NOT EXISTS copy_orders(id INTEGER PRIMARY KEY, name TEXT, day TEXT, boot TEXT, seq INTEGER,
  role TEXT, cl_ord_id TEXT UNIQUE, broker_order_id INTEGER, parent_broker_id INTEGER, contract TEXT,
  action TEXT, qty INTEGER, order_type TEXT, limit_px REAL, stop_px REAL, tif TEXT, state TEXT,
  failure_reason TEXT, failure_text TEXT, sent_ns INTEGER, ack_ms REAL);
CREATE TABLE IF NOT EXISTS copy_fills(name TEXT, broker_fill_id INTEGER, day TEXT, broker_order_id INTEGER,
  role TEXT, contract TEXT, action TEXT, qty INTEGER, px REAL, t_ns INTEGER, PRIMARY KEY(name, broker_fill_id));
CREATE TABLE IF NOT EXISTS copy_trades(id INTEGER PRIMARY KEY, name TEXT, day TEXT, boot TEXT, seq INTEGER,
  contract TEXT, side TEXT, qty INTEGER, entry_ns INTEGER, entry_px REAL, tp_px REAL, sl_px REAL, exit_ns INTEGER,
  exit_px REAL, exit_reason TEXT, points REAL, gross REAL, commission REAL, pnl REAL, estimated INTEGER);
CREATE TABLE IF NOT EXISTS copy_events(id INTEGER PRIMARY KEY, name TEXT, day TEXT, t_ns INTEGER, kind TEXT,
  seq INTEGER, detail TEXT, notify INTEGER);
CREATE TABLE IF NOT EXISTS copy_halts(name TEXT, day TEXT, kind TEXT, reason TEXT, t_ns INTEGER, cleared_ns INTEGER);
CREATE INDEX IF NOT EXISTS copy_orders_day ON copy_orders(day, name);
CREATE INDEX IF NOT EXISTS copy_fills_day ON copy_fills(day, name);
CREATE INDEX IF NOT EXISTS copy_trades_day ON copy_trades(day, name);
CREATE INDEX IF NOT EXISTS copy_events_day ON copy_events(day, name);
"""
STATUS_COLS = ("name", "day", "updated_at", "mode", "env", "root", "qty", "contract", "armed", "gate_reason",
               "pos_side", "pos_qty", "avg_px", "realized", "open_pnl", "halted", "halt_kind", "token_ok",
               "token_expires_at", "entries", "errors", "last_error")


def et_day(t_ns: int) -> str:
    return datetime.fromtimestamp(t_ns / 1e9, ET).date().isoformat()


class Ledger:
    def __init__(self, path: Path | str, now_ns=time.time_ns):
        self.path, self.now_ns = Path(path), now_ns
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.conn = sqlite3.connect(self.path, check_same_thread=False, timeout=10)
        self.conn.row_factory = sqlite3.Row
        self.lock = threading.RLock()
        with self.lock:
            self.conn.execute("PRAGMA journal_mode=WAL")
            self.conn.executescript(SCHEMA)
            self.conn.commit()

    def close(self) -> None:
        with self.lock:
            self.conn.close()

    def _x(self, sql: str, args=()) -> sqlite3.Cursor:
        with self.lock:
            cur = self.conn.execute(sql, args)
            self.conn.commit()
            return cur

    def _q(self, sql: str, args=()) -> list[sqlite3.Row]:
        with self.lock:
            return self.conn.execute(sql, args).fetchall()

    # -- events

    def event(self, name: str, kind: str, detail: str = "", *, seq: int | None = None, notify: bool = False,
              day: str | None = None) -> None:
        t = self.now_ns()
        self._x("INSERT INTO copy_events(name, day, t_ns, kind, seq, detail, notify) VALUES (?,?,?,?,?,?,?)",
                (name, day or et_day(t), t, kind, seq, str(detail)[:300], 1 if notify else 0))

    # -- signals

    def signal_seen(self, name: str, boot: str, seq: int) -> bool:
        return bool(self._q("SELECT 1 FROM copy_signals WHERE name=? AND boot=? AND seq=?", (name, boot, seq)))

    def signal(self, name: str, boot: str, seq: int, day: str, target: int, reason, t_ns, outcome: str,
               detail: str = "") -> None:
        self._x("INSERT OR REPLACE INTO copy_signals VALUES (?,?,?,?,?,?,?,?,?)",
                (name, boot, seq, day, target, reason, t_ns, outcome, str(detail)[:300]))

    def signal_outcome(self, name: str, boot: str, seq: int, outcome: str, detail: str = "") -> None:
        self._x("UPDATE copy_signals SET outcome=?, detail=? WHERE name=? AND boot=? AND seq=?",
                (outcome, str(detail)[:300], name, boot, seq))

    # -- orders (written before the POST)

    def order_new(self, **f) -> int:
        cols = ("name", "day", "boot", "seq", "role", "cl_ord_id", "broker_order_id", "parent_broker_id",
                "contract", "action", "qty", "order_type", "limit_px", "stop_px", "tif", "state",
                "failure_reason", "failure_text", "sent_ns", "ack_ms")
        row = {c: f.get(c) for c in cols}
        row["state"] = row["state"] or "sending"
        row["sent_ns"] = row["sent_ns"] or self.now_ns()
        return self._x(f"INSERT OR IGNORE INTO copy_orders({','.join(cols)}) VALUES ({','.join('?' * len(cols))})",
                       tuple(row[c] for c in cols)).lastrowid

    def order_update(self, cl_ord_id: str, **f) -> None:
        if f:
            sets = ", ".join(f"{k}=?" for k in f)
            self._x(f"UPDATE copy_orders SET {sets} WHERE cl_ord_id=?", (*f.values(), cl_ord_id))

    def orders_in_state(self, name: str, state: str) -> list[sqlite3.Row]:
        return self._q("SELECT * FROM copy_orders WHERE name=? AND state=?", (name, state))

    def fill(self, name: str, fid: int, day: str, order_id, role, contract, action, qty, px, t_ns) -> None:
        self._x("INSERT OR IGNORE INTO copy_fills VALUES (?,?,?,?,?,?,?,?,?,?)",
                (name, fid, day, order_id, role, contract, action, qty, px, t_ns))

    # -- trades

    def trade_open(self, name, day, boot, seq, contract, side, qty, entry_ns, entry_px, tp_px, sl_px) -> int:
        return self._x("INSERT INTO copy_trades(name, day, boot, seq, contract, side, qty, entry_ns, entry_px, tp_px, "
                       "sl_px) VALUES (?,?,?,?,?,?,?,?,?,?,?)",
                       (name, day, boot, seq, contract, side, qty, entry_ns, entry_px, tp_px, sl_px)).lastrowid

    def trade_close(self, tid: int, exit_ns, exit_px, reason, points, gross, commission, pnl,
                    estimated: bool = False) -> None:
        """`estimated`: the exit price is a mark or the entry price, not a real fill."""
        self._x("UPDATE copy_trades SET exit_ns=?, exit_px=?, exit_reason=?, points=?, gross=?, commission=?, pnl=?, "
                "estimated=? WHERE id=?", (exit_ns, exit_px, reason, points, gross, commission, pnl,
                                           1 if estimated else 0, tid))

    def open_trades(self, name: str) -> list[sqlite3.Row]:
        return self._q("SELECT * FROM copy_trades WHERE name=? AND exit_ns IS NULL", (name,))

    def day_pnl(self, name: str, day: str) -> float:
        r = self._q("SELECT COALESCE(SUM(pnl), 0) AS p FROM copy_trades WHERE name=? AND day=? AND exit_ns IS NOT NULL",
                    (name, day))
        return float(r[0]["p"])

    def day_entries(self, name: str, day: str) -> int:
        r = self._q("SELECT COUNT(*) AS n FROM copy_orders WHERE name=? AND day=? AND role='entry' AND state<>'dry'",
                    (name, day))
        return int(r[0]["n"])

    # -- halts

    def halt(self, name: str, day: str, kind: str, reason: str) -> None:
        self._x("INSERT INTO copy_halts VALUES (?,?,?,?,?,NULL)", (name, day, kind, str(reason)[:300], self.now_ns()))

    def active_halt(self, name: str, day: str) -> sqlite3.Row | None:
        r = self._q("SELECT * FROM copy_halts WHERE name=? AND day=? AND cleared_ns IS NULL ORDER BY t_ns LIMIT 1",
                    (name, day))
        return r[0] if r else None

    def clear_halts(self, name: str, day: str) -> int:
        return self._x("UPDATE copy_halts SET cleared_ns=? WHERE name=? AND day=? AND cleared_ns IS NULL",
                       (self.now_ns(), name, day)).rowcount

    # -- status

    def status(self, row: dict) -> None:
        vals = tuple(row.get(c) for c in STATUS_COLS)
        self._x(f"INSERT OR REPLACE INTO copy_status({','.join(STATUS_COLS)}) VALUES ({','.join('?' * len(STATUS_COLS))})",
                vals)

    def statuses(self) -> list[dict]:
        return [dict(r) for r in self._q("SELECT * FROM copy_status ORDER BY name")]
