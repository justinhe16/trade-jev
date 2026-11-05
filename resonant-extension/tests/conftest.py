"""Fixtures for the Resonant extension tests: a temp paper DB with the runner's schema, a data
dir whose config.json points at it, and fakes for report() and launchctl. No network, and
launchctl is never actually run."""

from __future__ import annotations

import json
import os
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
RESONANT_REPO = Path(os.environ.get("RESONANT_REPO", "/Users/justinhe/resonant"))


def sdk_manifest(tmp_path: Path) -> Path:
    """resonant.yaml as the Resonant checkout's SDK parser can read it. A checkout from before the
    `recover:` hook (PER-284) rejects that key as unknown, so a copy without that one line is
    returned; everything else is still validated. Once the SDK knows `recover`, the real file."""
    src = EXT_DIR / "resonant.yaml"
    sdk = RESONANT_REPO / "packages" / "resonant-sdk" / "src" / "resonant_sdk" / "manifest.py"
    try:
        if "recover" in sdk.read_text():
            return src
    except OSError:
        return src  # no checkout: the callers skip
    lines = src.read_text().splitlines(keepends=True)
    out = tmp_path / "resonant.sdk.yaml"
    out.write_text("".join(ln for ln in lines if not ln.startswith("recover:")))
    return out

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
        jev_model: str | None = "jev-1",
    ) -> None:
        start = et(day, started)
        self.conn.execute(
            "INSERT INTO paper_sessions VALUES (?,?,?,?,?,?,?,?,?,?,?)",
            (sid, day, symbol, status, utc_iso(start), None,
             utc_iso(heartbeat or start), "{}", jev_model, f"runs/{sid}", error),
        )  # fmt: skip
        self.conn.commit()

    def set_source(self, sid: str, source: str) -> None:
        """The runner's `source` column (absent in the base schema: an older DB)."""
        if "source" not in {r[1] for r in self.conn.execute("PRAGMA table_info(paper_sessions)")}:
            self.conn.execute("ALTER TABLE paper_sessions ADD COLUMN source TEXT")
        self.conn.execute("UPDATE paper_sessions SET source=? WHERE session_id=?", (source, sid))
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

    def decision(
        self, sid: str, policy: str, day: str, hm: str, position: int, *, action: str = "BUY",
        raw: str | None = "BUY", ms: float | None = 900.0, error: str | None = None,
        probs: str = "{}", sec: int = 0,
    ) -> None:  # fmt: skip
        """One Jev decision as live.py logs it (``jev_ms`` = the policy call's wall time)."""
        self.conn.execute(
            "INSERT INTO paper_decisions VALUES (?,?,?,?,?,?,?,?,?,?,?)",
            (sid, policy, ns(day, hm) + sec * 10**9, f"{hm}:{sec:02d}", 21000.0, position, action,
             raw, probs, ms, error),
        )  # fmt: skip
        self.conn.commit()

    def chart_tables(self) -> None:
        """The runner's chart tables (PER-283); absent on a DB from before them."""
        self.conn.executescript(
            "CREATE TABLE IF NOT EXISTS paper_bars(session_id TEXT, t_s INTEGER, o REAL, h REAL,"
            " l REAL, c REAL, PRIMARY KEY(session_id, t_s));"
            "CREATE TABLE IF NOT EXISTS paper_open(session_id TEXT, policy TEXT, side TEXT,"
            " entry_ns INTEGER, entry_px REAL, PRIMARY KEY(session_id, policy));"
        )

    def bars(self, sid: str, day: str, start: str, n: int, base: float = 21000.0, seed: int = 0) -> None:
        """``n`` 1-minute mid bars from ``start`` ET: a seeded random walk in 0.125 steps."""
        import random

        self.chart_tables()
        rng, px, t0 = random.Random(seed), base, int(et(day, start).timestamp())
        rows = []
        for i in range(n):
            o = px
            path = [o + 0.125 * rng.randint(-12, 12) for _ in range(4)]
            px = path[-1]
            rows.append((sid, t0 + 60 * i, o, max(o, *path), min(o, *path), px))
        self.conn.executemany("INSERT OR REPLACE INTO paper_bars VALUES (?,?,?,?,?,?)", rows)
        self.conn.commit()

    def open_position(self, sid: str, policy: str, side: str, hm: str, px: float, day: str) -> None:
        self.chart_tables()
        self.conn.execute("INSERT OR REPLACE INTO paper_open VALUES (?,?,?,?,?)",
                          (sid, policy, side, ns(day, hm), px))  # fmt: skip
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


@pytest.fixture(autouse=True)
def token_status(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    """The keep-alive's status file: a temp path (absent until a test writes it), never the real one."""
    path = tmp_path / "tradovate_token_status.json"
    monkeypatch.setattr(cli, "DEFAULT_TOKEN_STATUS", str(path))
    return path


def write_token_status(path: Path, **fields: Any) -> None:
    path.write_text(json.dumps(fields))


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
