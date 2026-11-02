"""Fixtures for the Resonant extension tests: a temp paper DB with the runner's schema, a data
dir whose config.json points at it, and fakes for report() and launchctl. No network, and
launchctl is never actually run."""

from __future__ import annotations

import json
import sqlite3
import subprocess
import sys
from datetime import datetime
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

import pytest

EXT_DIR = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(EXT_DIR))

import cli  # noqa: E402

ET = ZoneInfo("America/New_York")

SCHEMA = """
CREATE TABLE paper_sessions(session_id TEXT PRIMARY KEY, day TEXT, symbol TEXT, status TEXT,
  started_at TEXT, ended_at TEXT, heartbeat_at TEXT, settings_json TEXT, jev_model TEXT,
  run_dir TEXT, error TEXT);
CREATE TABLE paper_trades(id INTEGER PRIMARY KEY, session_id TEXT, policy TEXT, side TEXT,
  entry_ns INTEGER, entry_px REAL, exit_ns INTEGER, exit_px REAL, reason TEXT, points REAL,
  pnl REAL);
CREATE TABLE paper_decisions(session_id TEXT, policy TEXT, t_ns INTEGER, time_et TEXT,
  mid REAL, position INTEGER, action TEXT, raw_action TEXT, probs_json TEXT, jev_ms REAL,
  error TEXT);
CREATE TABLE paper_summary(session_id TEXT, policy TEXT, trades INTEGER, wins INTEGER,
  pnl REAL, max_drawdown REAL, jev_calls INTEGER, jev_cost_usd REAL,
  PRIMARY KEY(session_id, policy));
"""


def et(day: str, hm: str) -> datetime:
    """An aware datetime at ``day`` ``HH:MM`` ET."""
    return datetime.fromisoformat(f"{day}T{hm}:00").replace(tzinfo=ET)


def ns(day: str, hm: str) -> int:
    return int(et(day, hm).timestamp() * 1e9)


def utc_iso(dt: datetime) -> str:
    return dt.astimezone(ZoneInfo("UTC")).isoformat().replace("+00:00", "Z")


class PaperDB:
    """A writable handle on the temp DB for building scenarios."""

    def __init__(self, path: Path) -> None:
        self.path = path
        self.conn = sqlite3.connect(path)
        self.conn.executescript(SCHEMA)

    def session(
        self,
        sid: str,
        day: str,
        status: str = "done",
        *,
        started: str = "09:25",
        heartbeat: datetime | None = None,
        error: str | None = None,
        symbol: str = "NQ",
    ) -> None:
        start = et(day, started)
        self.conn.execute(
            "INSERT INTO paper_sessions VALUES (?,?,?,?,?,?,?,?,?,?,?)",
            (sid, day, symbol, status, utc_iso(start), None,
             utc_iso(heartbeat or start), "{}", "jev-1", f"runs/{sid}", error),
        )  # fmt: skip
        self.conn.commit()

    def trade(
        self, sid: str, policy: str, day: str, t_in: str, t_out: str | None, pnl: float,
        side: str = "long", reason: str = "jev",
    ) -> None:  # fmt: skip
        self.conn.execute(
            "INSERT INTO paper_trades(session_id, policy, side, entry_ns, entry_px, exit_ns, "
            "exit_px, reason, points, pnl) VALUES (?,?,?,?,?,?,?,?,?,?)",
            (sid, policy, side, ns(day, t_in), 21000.0, ns(day, t_out) if t_out else None,
             21000.0 + pnl / 20, reason, pnl / 20, pnl),
        )  # fmt: skip
        self.conn.commit()

    def decision(self, sid: str, policy: str, day: str, hm: str, position: int) -> None:
        self.conn.execute(
            "INSERT INTO paper_decisions VALUES (?,?,?,?,?,?,?,?,?,?,?)",
            (sid, policy, ns(day, hm), hm, 21000.0, position, "BUY", "BUY", "{}", 900.0, None),
        )
        self.conn.commit()

    def summary(
        self, sid: str, policy: str, trades: int, wins: int, pnl: float, dd: float,
        calls: int | None = None, cost: float | None = None,
    ) -> None:  # fmt: skip
        self.conn.execute(
            "INSERT INTO paper_summary VALUES (?,?,?,?,?,?,?,?)",
            (sid, policy, trades, wins, pnl, dd, calls, cost),
        )
        self.conn.commit()


@pytest.fixture
def data_dir(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    d = tmp_path / "ext-data"
    d.mkdir()
    monkeypatch.setenv("RESONANT_EXT_DATA", str(d))
    monkeypatch.setenv("HOME", str(d))
    monkeypatch.delenv("RESONANT_JOB_DUE_AT", raising=False)
    (d / "config.json").write_text(json.dumps({"db_path": str(tmp_path / "paper.db")}))
    return d


@pytest.fixture
def db(tmp_path: Path, data_dir: Path) -> PaperDB:
    return PaperDB(tmp_path / "paper.db")


@pytest.fixture
def reports(monkeypatch: pytest.MonkeyPatch) -> list[dict[str, Any]]:
    """Every report() call, instead of writing to fd 3."""
    got: list[dict[str, Any]] = []

    def fake(key: str, title: str, **kw: Any) -> bool:
        got.append({"key": key, "title": title, **kw})
        return True

    monkeypatch.setattr(cli, "report", fake)
    return got


class FakeLaunchctl:
    """Autouse, so no test ever runs the real launchctl. Answers ``launchctl`` calls from a script and records each argv."""

    def __init__(self) -> None:
        self.calls: list[tuple[str, ...]] = []
        self.loaded = True
        self.state = "running"
        self.fail: dict[str, int] = {}
        self.raise_os = False
        self.timeout: set[str] = set()  # verbs that time out

    def __call__(self, *args: str) -> subprocess.CompletedProcess[str]:
        self.calls.append(args)
        if self.raise_os:
            raise OSError("no launchctl")
        verb = args[0]
        if verb in self.timeout:
            raise subprocess.TimeoutExpired([cli.LAUNCHCTL, *args], cli.LAUNCHCTL_TIMEOUT_S)
        if verb == "print":
            if not self.loaded:
                return subprocess.CompletedProcess(args, 113, "", "Could not find service")
            body = f"gui/501/com.tradejev.paper = {{\n\tstate = {self.state}\n\tpid = 4242\n}}\n"
            return subprocess.CompletedProcess(args, 0, body, "")
        code = self.fail.get(verb, 0)
        return subprocess.CompletedProcess(args, code, "", "boom" if code else "")


@pytest.fixture(autouse=True)
def launchd(monkeypatch: pytest.MonkeyPatch) -> FakeLaunchctl:
    fake = FakeLaunchctl()
    monkeypatch.setattr(cli, "launchctl", fake)
    return fake
