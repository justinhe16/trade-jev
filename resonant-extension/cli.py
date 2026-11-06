#!/usr/bin/env python3
"""trade-jev Resonant extension: paper status, P&L, trades, Jev decisions, start/stop, two jobs,
and the `paper` and `chart` dashboard panels.

Stdlib only. Resonant runs this as ``python -I -S cli.py <cmd>`` with ``PATH=/usr/bin:/bin``
and ``HOME``/``RESONANT_EXT_DATA`` set to the extension's data dir (never the real home).
Every command prints exactly one JSON object on stdout and exits 0; a failure prints one
line to stderr and exits 1, and a failed run changed nothing (``recover`` excepted: a failed
kickstart after its SIGTERM leaves the job stopped). ``health`` exits 2 while today's session is
down (crashed, stale, missing or stalled); ``recover`` restarts it then.

Data comes from the paper runner's SQLite DB, opened read-only (``mode=ro``). The default
path is ``DEFAULT_DB``; ``$RESONANT_EXT_DATA/config.json`` may override it (see README).
A missing DB is not an error: it means no paper session has run yet.
"""

from __future__ import annotations

import argparse
import json
import os
import sqlite3
import subprocess
import sys
import time as clock
from collections.abc import Iterable
from contextlib import closing
from dataclasses import dataclass, field, replace
from datetime import date, datetime, time, timedelta, timezone
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

# emit.py is the SDK's stdlib helper, copied next to this file. ``-I`` keeps this directory
# off sys.path, so add it before importing.
sys.path.insert(0, str(Path(__file__).resolve().parent))
import panel as pw  # noqa: E402 - the SDK's panel builders, copied like emit.py
from emit import report  # noqa: E402

DEFAULT_DB = "/Users/justinhe/trade-jev/data/paper.db"
DEFAULT_TOKEN_STATUS = "/Users/justinhe/trade-jev/data/tradovate_token_status.json"
DEFAULT_LABEL = "com.tradejev.paper"
LAUNCHCTL = "/bin/launchctl"
LAUNCHCTL_TIMEOUT_S = 10
ET = ZoneInfo("America/New_York")

STALE_S = 180  # a running session whose heartbeat is older than this is likely dead
OPEN = time(9, 30)
CLOSE = time(16, 0)
MISSING_AFTER = time(9, 35)  # no session row by this time on a trading day = down
# health/recover judge the session only inside this window on trading days (the first minute after
# OPEN and the run-up to the runner's 15:55 EOD flatten are left alone). recover never acts after EOD.
CHECK_FROM = time(9, 31)
CHECK_UNTIL = time(15, 50)
EOD = time(15, 55)
STALL_S = 120  # a running session whose last decision is older than this is stalled (normally every 15 s)
GRACE_S = 360  # a new session (or the open) gets this long to log its first decision (3m42s seen live)
CATCH_UP_MAX_S = 2700  # a late start's catch-up replay longer than this (45 min) is judged stalled
RECOVER_WAIT_S = 20  # recover: how long the old process gets to exit after SIGTERM
RECOVER_COOLDOWN_S = 600  # recover: at most one restart per 10 min (the new session needs its grace)
RECOVER_MAX = 3  # recover: restarts per ET day; past that it's the owner's (health stays unhealthy)
RECOVER_STATE = "recover_state.json"  # {"day", "count", "last_at"}: recover's attempts today
UNHEALTHY = 2  # health's exit code when the session is down (1 stays "the command failed")
NO_SESSIONS = "no paper sessions yet"

DEFAULT_TRADES = 20
MAX_TRADES = 50
DEFAULT_DECISIONS = 20
MAX_DECISIONS = 50
# A Jev decision whose jev_ms (the wall time of the policy call, `Guarded.last_ms` in live.py) is at
# least this long went over the network. Stored answers (--stored-answers) and jsonl cache hits
# answer from memory in well under 1 ms; a real Jev API call takes ~90 ms or more. Warm-up rows: 0.
CALL_MS = 20.0
RECENT_DECISIONS = 20  # calls_mode judges the latest 20 decisions (5 min at 15 s): "right now"
RESULT_BUDGET = 3800  # bytes of JSON; the model sees at most 4 KB of a result
PNL_BUCKET = 50  # close summary state: P&L rounded to this, so tiny revisions aren't news
DOWN_KEY = "trade-jev/down"
WATCH_STATE = "watch_state.json"
TOKEN_KEY = "trade-jev/token"
TOKEN_TITLE = (
    "Tradovate token expired — log into trader.tradovate.com and paste a fresh access token "
    "+ md token into ~/trade-jev/.env"
)
TOKEN_WARN_S = 600  # a token that expires within this long is an alert (the renewer should have run)
TOKEN_STALE_S = 1200  # the keep-alive writes checked_at every 60 s; older than this = the renewer stopped
TOKEN_FAILS = 2  # consecutive failed renewals before "renewal failing" is an alert
STOPPED = "stopped_day"  # written by `stop`: watch stays quiet for that ET day

# NYSE full-day closures. A weekday listed here never raises "no session"; the runner's own
# `holiday` rows cover the rest, and config.json may add days ("holidays": [...]).
NYSE_HOLIDAYS = frozenset(
    {
        "2026-01-01", "2026-01-19", "2026-02-16", "2026-04-03", "2026-05-25",
        "2026-06-19", "2026-07-03", "2026-09-07", "2026-11-26", "2026-12-25",
        "2027-01-01", "2027-01-18", "2027-02-15", "2027-03-26", "2027-05-31",
        "2027-06-18", "2027-07-05", "2027-09-06", "2027-11-25", "2027-12-24",
    }
)  # fmt: skip


class Fail(Exception):
    """A clean failure: its message is the one stderr line, and the exit code is 1."""


# --- config and DB -------------------------------------------------------------------------


@dataclass(frozen=True)
class Config:
    db_path: Path = Path(DEFAULT_DB)
    label: str = DEFAULT_LABEL
    holidays: frozenset[str] = field(default_factory=frozenset)
    # looked up at call time (tests point the default at a temp file)
    token_status_path: Path = field(default_factory=lambda: Path(DEFAULT_TOKEN_STATUS))


def data_dir() -> Path:
    return Path(os.environ.get("RESONANT_EXT_DATA") or os.environ.get("HOME") or ".")


def load_config() -> Config:
    """``config.json`` in the data dir: ``{"db_path", "launchd_label", "holidays", "token_status_path"}``, all
    optional. A malformed file is an error rather than a silent fallback to the default."""
    path = data_dir() / "config.json"
    if not path.exists():
        return Config()
    try:
        raw = json.loads(path.read_text())
    except (OSError, ValueError) as e:
        raise Fail(f"config.json unreadable: {type(e).__name__}") from None
    if not isinstance(raw, dict):
        raise Fail("config.json must be a JSON object")
    db = raw.get("db_path", DEFAULT_DB)
    label = raw.get("launchd_label", DEFAULT_LABEL)
    holidays = raw.get("holidays", [])
    tok = raw.get("token_status_path", DEFAULT_TOKEN_STATUS)
    if not isinstance(tok, str) or not tok:
        raise Fail("config.json: token_status_path must be a non-empty string")
    if not isinstance(db, str) or not db:
        raise Fail("config.json: db_path must be a non-empty string")
    if not isinstance(label, str) or not label.replace(".", "").replace("-", "").isalnum():
        raise Fail("config.json: launchd_label must look like com.tradejev.paper")
    if not isinstance(holidays, list) or not all(isinstance(h, str) for h in holidays):
        raise Fail("config.json: holidays must be a list of YYYY-MM-DD strings")
    return Config(Path(db).expanduser(), label, frozenset(holidays), Path(tok).expanduser())


def connect(cfg: Config) -> sqlite3.Connection | None:
    """The DB read-only, or None when it doesn't exist yet (or its tables don't)."""
    if not cfg.db_path.exists():
        return None
    uri = cfg.db_path.resolve().as_uri() + "?mode=ro"
    conn = sqlite3.connect(uri, uri=True, timeout=5)
    conn.row_factory = sqlite3.Row
    names = {r[0] for r in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")}
    if "paper_sessions" not in names:
        conn.close()
        return None
    return conn


def has_table(conn: sqlite3.Connection, name: str) -> bool:
    row = conn.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (name,))
    return row.fetchone() is not None


# --- small helpers ---------------------------------------------------------------------------


def is_jev(policy: str) -> bool:
    """The one place that says which policy is Jev: its name contains "jev" (any case).
    Everything else (hold, random, imbalance, ...) is a baseline."""
    return "jev" in policy.lower()


def pick_jev(policies: list[str]) -> str | None:
    """The Jev policy among ``policies`` (the first by name if several match)."""
    jevs = sorted(p for p in policies if is_jev(p))
    return jevs[0] if jevs else None


def money(x: float | None) -> str:
    """``+$420`` / ``−$110`` (a real minus sign, as in the owner's example)."""
    if x is None:
        return "n/a"
    v = round(x)
    return f"{'−' if v < 0 else '+'}${abs(v):,}"


def parse_ts(s: str | None) -> datetime | None:
    """An ISO timestamp as an aware UTC datetime; naive means UTC. None if unparseable."""
    if not s:
        return None
    try:
        dt = datetime.fromisoformat(s.strip().replace("Z", "+00:00"))
    except ValueError:
        return None
    return dt.replace(tzinfo=timezone.utc) if dt.tzinfo is None else dt.astimezone(timezone.utc)


def ns_to_et(ns: int | None) -> str | None:
    if ns is None:
        return None
    return datetime.fromtimestamp(ns / 1e9, ET).strftime("%H:%M:%S")


def clip(s: Any, n: int = 160) -> str | None:
    if s is None:
        return None
    s = " ".join(str(s).split())
    return s if len(s) <= n else s[: n - 1] + "…"


def num(x: Any) -> float | None:
    return None if x is None else round(float(x), 2)


def fit(out: dict[str, Any], list_key: str) -> dict[str, Any]:
    """Drop the oldest entries of ``out[list_key]`` until the JSON fits the result budget."""
    items = out.get(list_key)
    base = out.get("omitted", 0)
    dropped = 0
    while isinstance(items, list) and items and len(json.dumps(out)) > RESULT_BUDGET:
        items.pop(0)
        dropped += 1
        out["omitted"] = base + dropped  # counted in the size check that follows
    return out


def now_utc() -> datetime:
    return datetime.now(timezone.utc)


def job_now() -> datetime:
    """A job's due time (so a caught-up run reports the day it was due for), else now."""
    due = parse_ts(os.environ.get("RESONANT_JOB_DUE_AT"))
    return due or now_utc()


def is_trading_day(d: date, cfg: Config) -> bool:
    iso = d.isoformat()
    return d.weekday() < 5 and iso not in NYSE_HOLIDAYS and iso not in cfg.holidays


# --- sessions and P&L -----------------------------------------------------------------------


NO_TRADE = ("holiday", "skipped")  # the runner ran and chose not to trade: no P&L, not a "last trading day"


def source_of(session: sqlite3.Row) -> str | None:
    """The feed of a session; None on a DB written before the `source` column existed."""
    return session["source"] if "source" in session.keys() else None  # noqa: SIM118 — sqlite3.Row


def sessions_on(conn: sqlite3.Connection, days: list[str]) -> list[sqlite3.Row]:
    marks = ",".join("?" * len(days))
    return conn.execute(
        f"SELECT * FROM paper_sessions WHERE day IN ({marks}) ORDER BY started_at",  # noqa: S608
        days,
    ).fetchall()


def latest_session(conn: sqlite3.Connection, day: str) -> sqlite3.Row | None:
    return conn.execute(
        "SELECT * FROM paper_sessions WHERE day=? ORDER BY started_at DESC, rowid DESC LIMIT 1",
        (day,),
    ).fetchone()


def latest_trading_day(conn: sqlite3.Connection) -> str | None:
    row = conn.execute(
        "SELECT day FROM paper_sessions WHERE status NOT IN ('holiday', 'skipped') "
        "ORDER BY day DESC LIMIT 1"
    ).fetchone()
    return row["day"] if row else None


def health_of(session: sqlite3.Row, now: datetime) -> tuple[str, int | None]:
    """(health, heartbeat age in s). Health is the status, except a ``running`` session
    whose heartbeat is older than ``STALE_S`` (or missing) is ``stale``."""
    status = session["status"]
    hb = parse_ts(session["heartbeat_at"]) or parse_ts(session["started_at"])
    age = int((now - hb).total_seconds()) if hb else None
    if status == "running" and (age is None or age > STALE_S):
        return "stale", age
    return status, age


def policy_stats(conn: sqlite3.Connection, session_ids: list[str]) -> dict[str, dict[str, Any]]:
    """Per policy over the sessions: pnl/trades/wins from paper_trades (always current, even
    mid-session); drawdown, Jev calls and cost from paper_summary when it has a row. A policy
    with summary rows but no trade rows takes its pnl/trades/wins from the summary."""
    if not session_ids:
        return {}
    marks = ",".join("?" * len(session_ids))
    stats: dict[str, dict[str, Any]] = {}

    def entry(policy: str) -> dict[str, Any]:
        return stats.setdefault(
            policy,
            {"pnl": 0.0, "trades": 0, "wins": 0, "max_drawdown": None, "jev_calls": None,
             "jev_cost_usd": None, "_traded": False, "_summary": None},
        )  # fmt: skip

    if has_table(conn, "paper_trades"):
        for r in conn.execute(
            f"SELECT policy, COUNT(*) n, COALESCE(SUM(pnl), 0) pnl, "  # noqa: S608
            f"SUM(CASE WHEN pnl > 0 THEN 1 ELSE 0 END) wins "
            f"FROM paper_trades WHERE session_id IN ({marks}) AND exit_ns IS NOT NULL "
            f"GROUP BY policy",
            session_ids,
        ):
            e = entry(r["policy"])
            e.update(pnl=float(r["pnl"]), trades=int(r["n"]), wins=int(r["wins"] or 0))
            e["_traded"] = True
    summarized: dict[str, set[str]] = {}  # policy -> sessions with a paper_summary row
    if has_table(conn, "paper_summary"):
        for r in conn.execute(
            f"SELECT * FROM paper_summary WHERE session_id IN ({marks})",  # noqa: S608
            session_ids,
        ):
            summarized.setdefault(r["policy"], set()).add(r["session_id"])
            e = entry(r["policy"])
            s = e["_summary"] = e["_summary"] or {"pnl": 0.0, "trades": 0, "wins": 0}
            s["pnl"] += float(r["pnl"] or 0)
            s["trades"] += int(r["trades"] or 0)
            s["wins"] += int(r["wins"] or 0)
            if r["max_drawdown"] is not None:
                dd = float(r["max_drawdown"])
                cur = e["max_drawdown"]
                e["max_drawdown"] = dd if cur is None or abs(dd) > abs(cur) else cur
            if r["jev_calls"] is not None:
                e["jev_calls"] = (e["jev_calls"] or 0) + int(r["jev_calls"])
            if r["jev_cost_usd"] is not None:
                e["jev_cost_usd"] = (e["jev_cost_usd"] or 0.0) + float(r["jev_cost_usd"])
    # Jev before its first closed trade has no row yet: seed it from the decision log, so pnl
    # answers "are calls going out?" from the first decision.
    if not any(is_jev(p) for p in stats) and has_table(conn, "paper_decisions"):
        logged = [r["policy"] for r in conn.execute(
            f"SELECT DISTINCT policy FROM paper_decisions WHERE session_id IN ({marks})",  # noqa: S608
            session_ids)]  # fmt: skip
        if (jp := pick_jev(logged)) is not None:
            entry(jp)
    # paper_summary is written at session end, so a running session has no jev_calls yet: count the
    # live model calls in its decision log instead (jev_cost_usd stays null: the log has no tokens).
    for p, e in stats.items():
        live = [sid for sid in session_ids if sid not in summarized.get(p, set())]
        if is_jev(p) and live and (n := live_call_count(conn, live, p)) is not None:
            e["jev_calls"] = (e["jev_calls"] or 0) + n
            only_live = len(live) == len(session_ids)
            e["jev_calls_from"] = "decisions" if only_live else "summary+decisions"
    for e in stats.values():
        summary = e.pop("_summary")
        if not e.pop("_traded") and summary:
            e.update(summary)
        e["pnl"] = num(e["pnl"])
        e["max_drawdown"] = num(e["max_drawdown"])
        e["jev_cost_usd"] = None if e["jev_cost_usd"] is None else round(e["jev_cost_usd"], 4)
    return stats


def jev_position(conn: sqlite3.Connection, session_id: str, policy: str) -> Any:
    if not has_table(conn, "paper_decisions"):
        return None
    row = conn.execute(
        "SELECT position FROM paper_decisions WHERE session_id=? AND policy=? "
        "ORDER BY t_ns DESC LIMIT 1",
        (session_id, policy),
    ).fetchone()
    return row["position"] if row else None


def live_call_count(conn: sqlite3.Connection, session_ids: list[str], policy: str) -> int | None:
    """Answered model calls (``jev_ms >= CALL_MS``) in the decision log; None without a log."""
    if not has_table(conn, "paper_decisions"):
        return None
    marks = ",".join("?" * len(session_ids))
    return int(conn.execute(
        f"SELECT COUNT(*) FROM paper_decisions WHERE session_id IN ({marks}) AND policy=? "  # noqa: S608
        f"AND jev_ms >= ? AND error IS NULL",
        [*session_ids, policy, CALL_MS],
    ).fetchone()[0])  # fmt: skip


def calls_mode(jev_model: str | None, recent: list[sqlite3.Row]) -> str:
    """Whether Jev is making real model calls right now, from the latest decisions:

    - ``replay/no calls``: the session plays stored answers (``jev_model`` is ``stored:<run>``), or
      every recent decision answered in under ``CALL_MS`` (a stored or cached answer)
    - ``live model calls``: at least one recent decision took a network round trip and answered
    - ``calls failing``: every recent decision recorded an error (a failed call is a HOLD)
    - ``warming up`` / ``no decisions yet``
    """
    if jev_model and jev_model.startswith("stored:"):
        return "replay/no calls"
    if not recent:
        return "no decisions yet"
    real = [r for r in recent if r["raw_action"] != "WARMUP"]
    if not real:
        return "warming up"
    if any(r["error"] is None and (r["jev_ms"] or 0) >= CALL_MS for r in real):
        return "live model calls"
    if all(r["error"] is not None for r in real):
        return "calls failing"
    return "replay/no calls"


def jev_live(
    conn: sqlite3.Connection, session: sqlite3.Row, policy: str | None, now: datetime
) -> dict[str, Any] | None:
    """Live Jev stats for one session from ``paper_decisions`` (written as each decision is made;
    ``paper_summary`` only appears at session end): decisions, model calls, errors, latency, the
    last decision and the action mix before (raw) and after (gated) the filters."""
    if not has_table(conn, "paper_decisions"):
        return None
    if policy is None:  # no Jev decision or trade yet (the first decision is at 09:30:00 ET)
        return {"policy": None, "calls_mode": "no decisions yet", "model": session["jev_model"],
                "decisions": 0}  # fmt: skip
    sid = session["session_id"]
    agg = conn.execute(
        "SELECT COUNT(*) n, SUM(raw_action = 'WARMUP') warm, "
        "SUM(error IS NULL AND jev_ms >= ?) calls, SUM(error IS NOT NULL) errors, MAX(t_ns) last_t "
        "FROM paper_decisions WHERE session_id=? AND policy=?",
        (CALL_MS, sid, policy),
    ).fetchone()
    n = int(agg["n"] or 0)
    ms = [r[0] for r in conn.execute(
        "SELECT jev_ms FROM paper_decisions WHERE session_id=? AND policy=? AND error IS NULL "
        "AND jev_ms >= ? ORDER BY jev_ms", (sid, policy, CALL_MS))]  # fmt: skip
    recent = conn.execute(
        "SELECT raw_action, jev_ms, error FROM paper_decisions WHERE session_id=? AND policy=? "
        "ORDER BY t_ns DESC LIMIT ?", (sid, policy, RECENT_DECISIONS)).fetchall()  # fmt: skip
    last_err = conn.execute(
        "SELECT time_et, error FROM paper_decisions WHERE session_id=? AND policy=? "
        "AND error IS NOT NULL ORDER BY t_ns DESC LIMIT 1", (sid, policy)).fetchone()  # fmt: skip
    raw: dict[str, int] = {}
    gated: dict[str, int] = {}
    for r in conn.execute(
        "SELECT COALESCE(raw_action, action) raw, action, COUNT(*) c FROM paper_decisions "
        "WHERE session_id=? AND policy=? GROUP BY 1, 2", (sid, policy)):  # fmt: skip
        raw[r["raw"]] = raw.get(r["raw"], 0) + r["c"]
        gated[r["action"]] = gated.get(r["action"], 0) + r["c"]
    last_t = agg["last_t"]
    return {
        "policy": policy,
        "calls_mode": calls_mode(session["jev_model"], recent),
        "model": session["jev_model"],
        "decisions": n,
        "warmup": int(agg["warm"] or 0),
        "model_calls": int(agg["calls"] or 0),
        "errors": int(agg["errors"] or 0),
        "last_error": None
        if last_err is None
        else f"{last_err['time_et']} {clip(last_err['error'], 100)}",
        "avg_ms": round(sum(ms) / len(ms)) if ms else None,
        "p95_ms": round(ms[-(-95 * len(ms) // 100) - 1]) if ms else None,  # nearest rank
        "last_decision_et": ns_to_et(last_t),
        "last_decision_age_s": None if last_t is None else int(now.timestamp() - last_t / 1e9),
        "raw": raw,
        "gated": gated,
    }


def jev_live_line(j: dict[str, Any] | None) -> str:
    """``Jev: live model calls (1224 calls, 0 errors, avg 123 ms, last 14:36:45 ET)``."""
    if not j:
        return ""
    if not j["decisions"]:
        return f"; Jev: {j['calls_mode']}"
    bits = [f"{j['model_calls']} calls", f"{j['errors']} errors"]
    if j["avg_ms"] is not None:
        bits.append(f"avg {j['avg_ms']} ms")
    if j["last_decision_et"]:
        bits.append(f"last {j['last_decision_et']} ET")
    return f"; Jev: {j['calls_mode']} ({', '.join(bits)})"


def resolve_days(arg: str, conn: sqlite3.Connection, today: date) -> tuple[str, list[str]]:
    """``date`` arg → (label, ET days). ``""``/``latest``: the latest session day; ``today``;
    ``yesterday``; ``YYYY-MM-DD``; ``week`` (Monday..today); ``month`` (the 1st..today);
    ``YYYY-MM-DD..YYYY-MM-DD`` (inclusive, at most 366 days)."""
    a = (arg or "").strip().lower()
    if a in ("", "latest", "last"):
        day = latest_trading_day(conn)
        return ("latest", [day] if day else [])
    if a == "today":
        return ("today", [today.isoformat()])
    if a == "yesterday":
        return ("yesterday", [(today - timedelta(days=1)).isoformat()])
    if a in ("week", "this week"):
        start = today - timedelta(days=today.weekday())
    elif a in ("month", "this month"):
        start = today.replace(day=1)
    elif ".." in a:
        lo, _, hi = a.partition("..")
        start, today = _parse_day(lo), _parse_day(hi)
        if start > today or (today - start).days > 366:
            raise Fail("date range must be ascending and at most 366 days")
        a = f"{start}..{today}"
    else:
        return (a, [_parse_day(a).isoformat()])
    days = [(start + timedelta(days=i)).isoformat() for i in range((today - start).days + 1)]
    return (a, days)


def _parse_day(s: str) -> date:
    try:
        return date.fromisoformat(s.strip())
    except ValueError:
        raise Fail(
            f"bad date {clip(s, 40)!r}: use YYYY-MM-DD, latest, today, week, month or A..B"
        ) from None


# --- launchd ----------------------------------------------------------------------------------


def launchctl(*args: str) -> subprocess.CompletedProcess[str]:
    """``/bin/launchctl <args>`` (absolute path: nothing is looked up on PATH)."""
    return subprocess.run(  # noqa: S603 - fixed binary, argv built from validated config
        [LAUNCHCTL, *args],
        capture_output=True,
        text=True,
        timeout=LAUNCHCTL_TIMEOUT_S,
        stdin=subprocess.DEVNULL,
        check=False,
    )


def launchctl_checked(*args: str) -> None:
    """``launchctl <args>`` that must succeed: ``Fail`` (exit 1) when it can't run or exits non-zero.
    ``subprocess.TimeoutExpired`` propagates: the caller decides what an unknown outcome means."""
    try:
        proc = launchctl(*args)
    except subprocess.TimeoutExpired:
        raise
    except (OSError, subprocess.SubprocessError) as e:
        raise Fail(f"launchctl {args[0]} failed: {type(e).__name__}") from None
    if proc.returncode != 0:
        raise Fail(f"launchctl {args[0]} exited {proc.returncode}: {clip(proc.stderr, 120)}")


def service(cfg: Config) -> str:
    return f"gui/{os.getuid()}/{cfg.label}"


def launchd_state(cfg: Config) -> dict[str, Any]:
    """Best effort: {"loaded": bool|None, "state": "running"|"not running"|..., "pid"}."""
    try:
        proc = launchctl("print", service(cfg))
    except (OSError, subprocess.SubprocessError):
        return {"loaded": None, "label": cfg.label}
    if proc.returncode != 0:
        return {"loaded": False, "label": cfg.label}
    out: dict[str, Any] = {"loaded": True, "label": cfg.label}
    for line in proc.stdout.splitlines():
        key, sep, val = line.strip().partition(" = ")
        if not sep:
            continue
        if key == "state" and "state" not in out:
            out["state"] = val.strip()
        elif key == "pid" and "pid" not in out and val.strip().isdigit():
            out["pid"] = int(val.strip())
    return out


# --- Tradovate token (written by trade_jev.tradovate_keepalive; never holds a token) ----------


def read_token_status(cfg: Config) -> dict[str, Any] | None:
    """The keep-alive's status file: None when it doesn't exist (token mode isn't in use, or the
    keep-alive has never run), ``{"unreadable": True}`` when it exists but isn't a JSON object."""
    path = cfg.token_status_path
    if not path.exists():
        return None
    try:
        raw = json.loads(path.read_text())
    except (OSError, ValueError):
        return {"unreadable": True}
    return raw if isinstance(raw, dict) else {"unreadable": True}


def _token_expiry_s(st: dict[str, Any], now: datetime) -> float | None:
    exp = parse_ts(st.get("expires_at")) if isinstance(st.get("expires_at"), str) else None
    return None if exp is None else (exp - now).total_seconds()


def token_problem(st: dict[str, Any] | None, now: datetime) -> str | None:
    """Why the Tradovate token needs the owner (``unreadable``, ``expired``, ``expiring`` or
    ``renewal failing``), or None when it is healthy or there is no status yet."""
    if st is None:
        return None
    if st.get("unreadable"):
        return "unreadable"
    left = _token_expiry_s(st, now)
    if left is not None and left <= 0:
        return "expired"
    if left is not None and left <= TOKEN_WARN_S:
        return "expiring"
    fails = st.get("consecutive_failures")
    if isinstance(fails, int) and fails >= TOKEN_FAILS:
        return "renewal failing"
    checked = parse_ts(st.get("checked_at")) if isinstance(st.get("checked_at"), str) else None
    if checked is not None and (now - checked).total_seconds() > TOKEN_STALE_S:
        return "renewer stopped"
    return None


def token_summary(st: dict[str, Any] | None, now: datetime) -> dict[str, Any]:
    """The ``token`` block of trade_jev_status (no secrets: the status file has none)."""
    if st is None:
        return {"known": False, "note": "no token status file (token mode not in use yet)"}
    problem = token_problem(st, now)
    left = None if st.get("unreadable") else _token_expiry_s(st, now)
    return {
        "known": not st.get("unreadable", False),
        "ok": problem is None,
        "problem": problem,
        "expires_in_min": None if left is None else round(left / 60),
        "expires_at": st.get("expires_at"),
        "last_ok_at": st.get("last_ok_at"),
        "last_error": clip(st.get("last_error"), 120),
        "consecutive_failures": st.get("consecutive_failures"),
        "checked_at": st.get("checked_at"),
    }


# --- tools ------------------------------------------------------------------------------------


def no_sessions(**extra: Any) -> dict[str, Any]:
    return {"sessions": 0, "summary": NO_SESSIONS, **extra}


def cmd_status(cfg: Config, now: datetime) -> dict[str, Any]:
    today = now.astimezone(ET).date().isoformat()
    launchd = launchd_state(cfg)
    tstatus = read_token_status(cfg)
    token = token_summary(tstatus, now)
    problem = token_problem(tstatus, now)
    note = f" | Tradovate token: {problem}" if problem else ""
    conn = connect(cfg)
    if conn is None:
        return no_sessions(day=today, running=False, launchd=launchd, token=token) | (
            {"summary": NO_SESSIONS + note} if note else {})
    with closing(conn):
        s = latest_session(conn, today)
        if s is None:
            last = latest_trading_day(conn)
            return {
                "day": today,
                "running": False,
                "session": None,
                "last_session_day": last,
                "launchd": launchd,
                "token": token,
                "summary": f"no session today ({today}); last session {last or 'never'}{note}",
            }
        health, age = health_of(s, now)
        stats = policy_stats(conn, [s["session_id"]])
        jev = pick_jev(list(stats)) or pick_jev(_decision_policies(conn, s["session_id"]))
        j = stats.get(jev or "", {})
        position = jev_position(conn, s["session_id"], jev) if jev else None
        live = jev_live(conn, s, jev, now)
    out = {
        "day": today,
        "running": health == "running",
        "session": {
            "id": s["session_id"],
            "symbol": s["symbol"],
            "status": s["status"],
            "source": source_of(s),
            "health": health,
            "started_at": s["started_at"],
            "ended_at": s["ended_at"],
            "heartbeat_age_s": age,
            "error": clip(s["error"]),
        },
        "jev": {
            "policy": jev,
            "position": position,
            "pnl": j.get("pnl", 0.0),
            "trades": j.get("trades", 0),
            "wins": j.get("wins", 0),
        },
        "jev_live": live,
        "launchd": launchd,
        "token": token,
    }
    label = "stale/likely dead" if health == "stale" else health
    if health == "skipped":
        out["summary"] = f"skipped: {clip(s['error'], 160) or 'no reason recorded'}" + (
            f" (source {source_of(s)})" if source_of(s) else "") + note
        return out
    src = f" [{source_of(s)}]" if source_of(s) else ""
    out["summary"] = (
        f"{s['symbol']}{src} paper session {label}; Jev {money(out['jev']['pnl'])} "
        f"({out['jev']['trades']} trades), position {position if position is not None else '?'}"
        f"{jev_live_line(live)}{note}"
    )
    return out


def _decision_policies(conn: sqlite3.Connection, session_id: str) -> list[str]:
    if not has_table(conn, "paper_decisions"):
        return []
    rows = conn.execute(
        "SELECT DISTINCT policy FROM paper_decisions WHERE session_id=?", (session_id,)
    )
    return [r["policy"] for r in rows]


def cmd_pnl(cfg: Config, now: datetime, date_arg: str) -> dict[str, Any]:
    conn = connect(cfg)
    if conn is None:
        return no_sessions()
    with closing(conn):
        label, days = resolve_days(date_arg, conn, now.astimezone(ET).date())
        found = sessions_on(conn, days) if days else []
        sessions = [s for s in found if s["status"] not in NO_TRADE]
        if not sessions:
            skipped = [s for s in found if s["status"] == "skipped"]
            if skipped:
                why = clip(skipped[-1]["error"], 160) or "no reason recorded"
                return {"range": label, "days": days[:1] + days[-1:], "sessions": 0,
                        "skipped": [{"day": s["day"], "reason": clip(s["error"], 160)} for s in skipped],
                        "summary": f"skipped: {why}"}  # fmt: skip
            return {"range": label, "days": days[:1] + days[-1:], "sessions": 0,
                    "summary": f"no paper sessions for {label}"}  # fmt: skip
        stats = policy_stats(conn, [s["session_id"] for s in sessions])
        by_day: dict[str, list[str]] = {}
        for s in sessions:
            by_day.setdefault(s["day"], []).append(s["session_id"])
        daily = []
        if len(by_day) > 1:
            for d, ids in sorted(by_day.items()):
                ds = policy_stats(conn, ids)
                jp = pick_jev(list(ds))
                statuses = sorted({s["status"] for s in sessions if s["day"] == d})
                daily.append({
                    "day": d,
                    "jev_pnl": ds[jp]["pnl"] if jp else None,
                    "jev_trades": ds[jp]["trades"] if jp else 0,
                    "status": "/".join(statuses),
                })  # fmt: skip
    jev = pick_jev(list(stats))
    policies = [
        {"policy": p, "jev": is_jev(p), **v}
        for p, v in sorted(stats.items(), key=lambda kv: (not is_jev(kv[0]), kv[0]))
    ]
    out: dict[str, Any] = {
        "range": label,
        "from": min(by_day),
        "to": max(by_day),
        "trading_days": len(by_day),
        "sessions": len(sessions),
        "jev_policy": jev,
        "policies": policies,
    }
    if jev:
        jp = stats[jev]["pnl"] or 0.0
        out["jev_minus_baseline"] = {
            p: num(jp - (v["pnl"] or 0.0)) for p, v in stats.items() if not is_jev(p)
        }
    if len(by_day) == 1:
        out["status"] = "/".join(sorted({s["status"] for s in sessions}))
    out["summary"] = summary_line(sessions[-1]["symbol"], stats, len(by_day), min(by_day))
    last = sessions[-1]
    if len(by_day) == 1 and last["status"] == "crashed":  # don't let a crash read as a flat day
        err = short_error(last["error"])
        out["summary"] += " (crashed" + (f": {err})" if err else ")")
    if daily:
        out["daily"] = daily
        fit(out, "daily")
    return out


def day_label(day: str) -> str:
    """``2026-11-02`` → ``Mon 11/2``: every one-day line names its day, so yesterday's result is
    never read as today's (the 08:00 briefing lists still-open items verbatim)."""
    d = date.fromisoformat(day)
    return f"{d:%a} {d.month}/{d.day}"


def pnl_parts(stats: dict[str, dict[str, Any]]) -> str:
    """``Jev +$420 (6 trades), hold −$110, random +$30``."""
    jev = pick_jev(list(stats))
    parts = []
    if jev:
        parts.append(f"Jev {money(stats[jev]['pnl'])} ({stats[jev]['trades']} trades)")
    parts += [f"{p} {money(v['pnl'])}" for p, v in sorted(stats.items()) if not is_jev(p)]
    return ", ".join(parts) or "no trades"


def summary_line(
    symbol: str | None, stats: dict[str, dict[str, Any]], ndays: int = 1, day: str | None = None
) -> str:
    """``Paper NQ Mon 11/2: Jev +$420 (6 trades), hold −$110, random +$30``."""
    head = f"Paper {symbol or '?'}"
    if ndays > 1:
        head += f" ({ndays} days)"
    elif day:
        head += f" {day_label(day)}"
    return f"{head}: {pnl_parts(stats)}"


def short_error(err: str | None, n: int = 90) -> str | None:
    """The first line of a session error (the exception, not its traceback), clipped."""
    first = next((ln for ln in (err or "").splitlines() if ln.strip()), "")
    return clip(first, n) or None


def cmd_trades(
    cfg: Config, now: datetime, date_arg: str, policy_arg: str, limit: int
) -> dict[str, Any]:
    conn = connect(cfg)
    if conn is None:
        return no_sessions(trades=[])
    limit = DEFAULT_TRADES if limit <= 0 else min(limit, MAX_TRADES)
    with closing(conn):
        label, days = resolve_days(date_arg, conn, now.astimezone(ET).date())
        sessions = [s for s in sessions_on(conn, days) if s["status"] not in NO_TRADE] if days else []
        if not sessions or not has_table(conn, "paper_trades"):
            return {"range": label, "trades": [], "summary": f"no paper sessions for {label}"}
        ids = [s["session_id"] for s in sessions]
        marks = ",".join("?" * len(ids))
        known = [
            r["policy"]
            for r in conn.execute(
                f"SELECT DISTINCT policy FROM paper_trades WHERE session_id IN ({marks})",  # noqa: S608
                ids,
            )
        ]
        want = (policy_arg or "").strip()
        if want.lower() in ("", "jev"):
            policy = pick_jev(known)
        else:
            policy = next((p for p in known if p.lower() == want.lower()), None)
        if policy is None:
            return {"range": label, "policy": clip(want, 40) or "jev", "trades": [],
                    "policies": known[:20],
                    "summary": f"no trades for policy {clip(want, 40) or 'jev'} in {label}"}  # fmt: skip
        total = conn.execute(
            f"SELECT COUNT(*) FROM paper_trades WHERE session_id IN ({marks}) AND policy=?",  # noqa: S608
            [*ids, policy],
        ).fetchone()[0]
        rows = conn.execute(
            f"SELECT * FROM paper_trades WHERE session_id IN ({marks}) AND policy=? "  # noqa: S608
            f"ORDER BY entry_ns DESC LIMIT ?",
            [*ids, policy, limit],
        ).fetchall()
    multi_day = len({s["day"] for s in sessions}) > 1
    day_of = {s["session_id"]: s["day"] for s in sessions}
    trades = []
    for r in reversed(rows):
        t: dict[str, Any] = {
            "in": ns_to_et(r["entry_ns"]),
            "out": ns_to_et(r["exit_ns"]),
            "side": r["side"],
            "entry": r["entry_px"],
            "exit": r["exit_px"],
            "why": r["reason"],
            "pts": num(r["points"]),
            "pnl": num(r["pnl"]),
        }
        if multi_day:
            t = {"day": day_of.get(r["session_id"]), **t}
        trades.append(t)
    out: dict[str, Any] = {
        "range": label,
        "policy": policy,
        "total": total,
        "shown": len(trades),
        "times": "ET",
        "trades": trades,
    }
    fit(out, "trades")
    out["shown"] = len(out["trades"])
    out["summary"] = f"{policy}: {total} trades in {label}, showing the last {out['shown']}"
    return out


def _probs(raw: str | None) -> dict[str, float]:
    try:
        probs = json.loads(raw or "{}")
    except ValueError:
        return {}
    if not isinstance(probs, dict):
        return {}
    return {str(k): round(float(v), 2) for k, v in probs.items() if isinstance(v, (int, float))}


def cmd_decisions(cfg: Config, now: datetime, date_arg: str, limit: int) -> dict[str, Any]:
    """The latest ``limit`` Jev decisions (oldest first, ET): raw answer, gated action, position,
    probabilities, latency and error, plus the live stats (``jev_live``) of the latest session."""
    conn = connect(cfg)
    if conn is None:
        return no_sessions(decisions=[])
    limit = DEFAULT_DECISIONS if limit <= 0 else min(limit, MAX_DECISIONS)
    with closing(conn):
        label, days = resolve_days(date_arg, conn, now.astimezone(ET).date())
        found = sessions_on(conn, days) if days else []
        sessions = [s for s in found if s["status"] not in NO_TRADE]
        if not sessions or not has_table(conn, "paper_decisions"):
            return {"range": label, "decisions": [], "summary": f"no paper sessions for {label}"}
        ids = [s["session_id"] for s in sessions]
        marks = ",".join("?" * len(ids))
        known = [r["policy"] for r in conn.execute(
            f"SELECT DISTINCT policy FROM paper_decisions WHERE session_id IN ({marks})",  # noqa: S608
            ids)]  # fmt: skip
        policy = pick_jev(known)
        if policy is None:
            return {"range": label, "decisions": [],
                    "summary": f"no Jev decisions in {label}"}  # fmt: skip
        total = conn.execute(
            f"SELECT COUNT(*) FROM paper_decisions WHERE session_id IN ({marks}) AND policy=?",  # noqa: S608
            [*ids, policy],
        ).fetchone()[0]
        rows = conn.execute(
            f"SELECT * FROM paper_decisions WHERE session_id IN ({marks}) AND policy=? "  # noqa: S608
            f"ORDER BY t_ns DESC LIMIT ?",
            [*ids, policy, limit],
        ).fetchall()
        live = jev_live(conn, sessions[-1], policy, now)
    multi_day = len({s["day"] for s in sessions}) > 1
    day_of = {s["session_id"]: s["day"] for s in sessions}
    decisions = []
    for r in reversed(rows):
        d: dict[str, Any] = {
            "t": r["time_et"],
            "raw": r["raw_action"],
            "act": r["action"],
            "pos": r["position"],
            "p": _probs(r["probs_json"]),
            "ms": r["jev_ms"],
            "err": clip(r["error"], 80),
        }
        if multi_day:
            d = {"day": day_of.get(r["session_id"]), **d}
        decisions.append(d)
    out: dict[str, Any] = {
        "range": label,
        "policy": policy,
        "total": total,
        "shown": len(decisions),
        "times": "ET",
        "live": live,
        "decisions": decisions,
    }
    def said() -> str:
        shown = f"showing the last {out['shown']}"
        return f"{policy}: {total} decisions in {label}, {shown}{jev_live_line(live)}"

    out["summary"] = said()  # counted in the budget: fit() trims with it in place
    fit(out, "decisions")
    out["shown"] = len(out["decisions"])
    out["summary"] = said()
    return out


def cmd_start(cfg: Config, now: datetime) -> dict[str, Any]:
    """Kickstart the launchd job. Refuses (exit 1, nothing done) when the job isn't loaded;
    reports without acting when today's session is already running and healthy."""
    st = launchd_state(cfg)
    if st["loaded"] is False:
        raise Fail(f"launchd job {cfg.label} isn't loaded; load its plist first")
    if st.get("state") == "running":
        return {"started": False, "reason": "already running", "launchd": st,
                "summary": f"{cfg.label} is already running"}  # fmt: skip
    try:
        launchctl_checked("kickstart", service(cfg))
    except subprocess.TimeoutExpired:
        # It may have gone through: never exit 1 (that promises nothing happened).
        _clear_stopped()
        return {"started": "unknown", "reason": "launchctl timed out",
                "summary": f"kickstart of {cfg.label} timed out; check trade_jev_status"}  # fmt: skip
    _clear_stopped()
    return {"started": True, "service": service(cfg),
            "summary": f"kickstarted {cfg.label}; status will show the session once it's up"}  # fmt: skip


def cmd_stop(cfg: Config, now: datetime) -> dict[str, Any]:
    """SIGTERM the launchd job's process. Nothing to do (exit 0) when it isn't running."""
    st = launchd_state(cfg)
    if st["loaded"] is False:
        return {"stopped": False, "reason": "job not loaded", "launchd": st,
                "summary": f"{cfg.label} isn't loaded; nothing to stop"}  # fmt: skip
    if st["loaded"] and st.get("state") not in (None, "running"):
        return {"stopped": False, "reason": f"not running ({st.get('state')})", "launchd": st,
                "summary": f"{cfg.label} isn't running; nothing to stop"}  # fmt: skip
    day = now.astimezone(ET).date().isoformat()
    try:
        launchctl_checked("kill", "SIGTERM", service(cfg))
    except subprocess.TimeoutExpired:
        # It may have gone through: never exit 1 (that promises nothing happened).
        _mark_stopped(day)
        return {"stopped": "unknown", "reason": "launchctl timed out",
                "summary": f"SIGTERM to {cfg.label} timed out; check trade_jev_status"}  # fmt: skip
    _mark_stopped(day)
    return {"stopped": True, "service": service(cfg),
            "summary": f"sent SIGTERM to {cfg.label}"}  # fmt: skip


def _mark_stopped(day: str) -> None:
    """Remember that the owner stopped today's session, so watch doesn't call it dead.
    Best effort: it runs after the signal went out, so a failed write must not exit 1."""
    path = data_dir() / STOPPED
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(day)
    except OSError:
        pass


def _clear_stopped() -> None:
    try:
        (data_dir() / STOPPED).unlink(missing_ok=True)
    except OSError:
        pass


def _stopped_today(day: str) -> bool:
    try:
        return (data_dir() / STOPPED).read_text().strip() == day
    except OSError:
        return False


CANT_TELL = "can't tell: paper.db won't open read-only"


def _cant_open(e: sqlite3.Error) -> bool:
    """A read-only open of a WAL DB fails like this when its -wal/-shm side files are missing and
    can't be created (sqlite says "unable to open database file", or "attempt to write a readonly
    database" when the directory is read-only): health can't tell, which is no reason to restart."""
    msg = str(e)
    return isinstance(e, sqlite3.OperationalError) and (
        "unable to open database file" in msg or "attempt to write a readonly database" in msg
    )


def session_check(cfg: Config, now: datetime) -> dict[str, Any]:
    """Is today's paper session down right now? ``{"problem": str | None, "why": str}``: ``problem``
    is set only on a trading day inside CHECK_FROM..CHECK_UNTIL ET, unless the owner stopped it on
    purpose today, when the session is crashed, stale or missing (``down_reason``, as ``watch``) or
    running with a fresh heartbeat but no decision for STALL_S (``stall_reason``). ``why`` is the
    one-line reason it is fine (or can't be judged) otherwise. Shared by ``health`` and ``recover``."""
    et = now.astimezone(ET)
    day = et.date().isoformat()
    if not is_trading_day(et.date(), cfg):
        return {"problem": None, "why": "not a trading day"}
    if not CHECK_FROM <= et.time() <= CHECK_UNTIL:
        return {"problem": None, "why": "outside the session window (09:31-15:50 ET)"}
    if _stopped_today(day):
        return {"problem": None, "why": "stopped on purpose today (trade_jev_stop)"}
    try:
        conn = connect(cfg)
    except sqlite3.Error as e:
        if _cant_open(e):
            return {"problem": None, "why": CANT_TELL}
        raise
    try:
        reason = down_reason(conn, now)
        if reason or conn is None:
            return {"problem": reason, "why": "no session yet" if not reason else ""}
        s = latest_session(conn, day)
        if s is None:  # before MISSING_AFTER
            return {"problem": None, "why": "no session yet"}
        health, _ = health_of(s, now)
        if health == "aborted" and _aborted_by_recover(s, day):
            # recover's SIGTERM ended it but no new session followed (a failed kickstart): retry
            return {"problem": "session aborted by recover, no new session", "why": ""}
        if health != "running":
            return {"problem": None, "why": f"session {health}"}
        stalled, why = stall_reason(conn, s, now)
        return {"problem": stalled, "why": why}
    except sqlite3.Error as e:
        if _cant_open(e):
            return {"problem": None, "why": CANT_TELL}
        raise
    finally:
        if conn is not None:
            conn.close()


def stall_reason(conn: sqlite3.Connection, s: sqlite3.Row, now: datetime) -> tuple[str | None, str]:
    """(problem, why) for a running session with a fresh heartbeat. Stalled when its last decision is
    older than STALL_S, or none came within GRACE_S of the later of its start and the open. A decision
    from before the session started is a late start's intraday catch-up replay (decisions' ``t_ns``
    is market time), not a stall, for up to CATCH_UP_MAX_S. ``paper_decisions`` is Jev's log only
    (baselines don't write it), so the latest row is Jev's latest decision."""
    if not has_table(conn, "paper_decisions"):
        return None, "can't tell: no decisions table"
    last = conn.execute(
        "SELECT MAX(t_ns) FROM paper_decisions WHERE session_id=?", (s["session_id"],)
    ).fetchone()[0]
    started = parse_ts(s["started_at"])
    opened = datetime.combine(now.astimezone(ET).date(), OPEN, ET)
    if last is None:
        waited = int((now - max(started or opened, opened)).total_seconds())
        if waited > GRACE_S:
            return f"no decisions for {age_text(waited)}", ""
        return None, "session starting, no decision yet"
    last_at = datetime.fromtimestamp(last / 1e9, timezone.utc)
    if started is not None and last_at < started:
        replay = f"{last_at.astimezone(ET):%H:%M} ET"
        if (now - started).total_seconds() > CATCH_UP_MAX_S:
            return f"still catching up after {age_text(CATCH_UP_MAX_S)} (replay at {replay})", ""
        return None, f"session catching up (replay at {replay})"
    age = int((now - last_at).total_seconds())
    if age > STALL_S:
        return f"no decisions for {age_text(age)}", ""
    return None, f"session running, last decision {age_text(max(age, 0))} ago"


def age_text(secs: int) -> str:
    """4m12s, 45s, 1h05m."""
    if secs < 60:
        return f"{secs}s"
    if secs < 3600:
        return f"{secs // 60}m{secs % 60:02d}s"
    return f"{secs // 3600}h{secs % 3600 // 60:02d}m"


def cmd_health(cfg: Config, now: datetime) -> dict[str, Any]:
    """Unhealthy (``ok: false``, exit UNHEALTHY) only when ``session_check`` finds today's paper
    session down. Otherwise healthy when the config parses and the DB, if present, opens read-only
    and answers a query. A missing DB is healthy outside the session window: no session has run yet.
    A DB that won't open read-only for want of its WAL side files is "can't tell": healthy."""
    chk = session_check(cfg, now)
    out: dict[str, Any] = {"ok": chk["problem"] is None}
    try:
        conn = connect(cfg)
        if conn is None:
            out["db"] = "absent"
        else:
            with closing(conn):
                out["db"] = "ok"
                out["sessions"] = conn.execute("SELECT COUNT(*) FROM paper_sessions").fetchone()[0]
    except sqlite3.Error as e:
        if not _cant_open(e):
            raise
        out["db"] = "unopenable"
    if chk["problem"]:
        out["problem"] = chk["problem"]
        out["summary"] = f"paper session down: {chk['problem']}"
    elif out["db"] == "absent":
        out["summary"] = f"ok: {NO_SESSIONS}"
    elif out["db"] == "unopenable":
        out["summary"] = f"ok: {CANT_TELL}"
    else:
        out["summary"] = f"ok: {chk['why']}"
    return out


def cmd_recover(cfg: Config, now: datetime) -> dict[str, Any]:
    """Restart the paper session when ``session_check`` finds it down (crashed, stale, missing or
    stalled) in the session window: SIGTERM the launchd job if it is running, wait up to
    RECOVER_WAIT_S for it to exit, then kickstart it (the runner writes a new session row, so nothing
    is counted twice). At most RECOVER_MAX restarts per ET day, RECOVER_COOLDOWN_S apart (a session
    that keeps dying is the owner's: health stays unhealthy). An attempt is recorded before acting, so
    a session its SIGTERM aborted without a successful kickstart is retried after the cooldown.
    Otherwise ``nothing to do: <reason>``. Exit 1 only when launchctl fails.
    Paper only: it starts and stops the paper runner, never anything that places orders."""
    chk = session_check(cfg, now)
    problem = chk["problem"]
    if not problem:
        return {"restarted": False, "summary": f"nothing to do: {chk['why']}"}
    if now.astimezone(ET).time() >= EOD:  # session_check's window already ends before EOD
        return {"restarted": False, "summary": "nothing to do: after EOD"}
    day = now.astimezone(ET).date().isoformat()
    done = _recover_state(day)
    if done["count"] >= RECOVER_MAX:
        return {"restarted": False, "problem": problem,
                "summary": f"nothing to do: already restarted {done['count']} times today, needs the owner ({problem})"}  # fmt: skip
    last = parse_ts(done.get("last_at"))
    if last is not None and (now - last).total_seconds() < RECOVER_COOLDOWN_S:
        ago = age_text(int((now - last).total_seconds()))
        return {"restarted": False, "problem": problem,
                "summary": f"nothing to do: restarted {ago} ago, waiting for it ({problem})"}  # fmt: skip
    st = launchd_state(cfg)
    if st["loaded"] is False:
        raise Fail(f"launchd job {cfg.label} isn't loaded; can't restart ({problem})")
    # Recorded before acting: a SIGTERM that went out counts even if the kickstart then fails.
    _write_state(data_dir() / RECOVER_STATE,
                 {"day": day, "count": done["count"] + 1, "last_at": now.astimezone(timezone.utc).isoformat()})  # fmt: skip
    try:
        if st.get("state") == "running":
            launchctl_checked("kill", "SIGTERM", service(cfg))
            if not _wait_stopped(cfg):
                raise Fail(f"{cfg.label} not confirmed stopped {RECOVER_WAIT_S}s after SIGTERM; not kickstarted")
        launchctl_checked("kickstart", service(cfg))
    except subprocess.TimeoutExpired as e:
        raise Fail(f"launchctl {e.cmd[1]} timed out; check trade_jev_status") from None
    return {"restarted": True, "problem": problem, "service": service(cfg),
            "summary": f"restarted paper session ({problem})"}  # fmt: skip


def _recover_state(day: str) -> dict[str, Any]:
    """recover's attempts today: ``{"count", "last_at"}`` (zero on another day or no file)."""
    st = _read_state(data_dir() / RECOVER_STATE)
    if st.get("day") != day or not isinstance(st.get("count"), int):
        return {"count": 0, "last_at": None}
    return {"count": st["count"], "last_at": st.get("last_at")}


def _aborted_by_recover(s: sqlite3.Row, day: str) -> bool:
    """The session started before recover's last attempt today, so that SIGTERM ended it."""
    last = parse_ts(_recover_state(day)["last_at"])
    started = parse_ts(s["started_at"])
    return last is not None and started is not None and started <= last


def _wait_stopped(cfg: Config) -> bool:
    """Poll launchd once a second until the job is known not to be running (True), for up to
    RECOVER_WAIT_S. An unreadable state (``launchctl print`` failed) is not "stopped"."""
    deadline = clock.monotonic() + RECOVER_WAIT_S
    while True:
        st = launchd_state(cfg)
        if st["loaded"] is False or (st["loaded"] and st.get("state") != "running"):
            return True
        if clock.monotonic() >= deadline:
            return False
        clock.sleep(1)


# --- jobs -------------------------------------------------------------------------------------


CLOSE_WORDS = {"stale": "stale (likely dead)", "running": "still running"}


def close_title(
    session: sqlite3.Row, health: str, stats: dict[str, dict[str, Any]], day: str
) -> str:
    """``Paper NQ Mon 11/2: Jev +$420 (6 trades), hold −$110`` for a finished day. Any other end
    leads with it: ``Paper NQZ6 Mon 11/2: crashed — BentoError: A live data license is required…``,
    then the P&L only if something traded (a crash before the first trade has nothing to add)."""
    head = f"Paper {session['symbol'] or '?'} {day_label(day)}"
    if health == "done":
        return f"{head}: {pnl_parts(stats)}"
    title = f"{head}: {CLOSE_WORDS.get(health, health)}"
    if health == "crashed" and (err := short_error(session["error"])):
        title += f" — {err}"
    if any(v["trades"] for v in stats.values()):
        title += f"; {pnl_parts(stats)}"
    return title


def next_open_ttl(day: str, now: datetime, cfg: Config) -> str:
    """The report's ttl: until the next trading day's 09:30 ET open, when that day's own session
    takes over (status and watch report it). So a day's result can be in the next morning's
    briefing, labelled with its day, but is never left open as if it were the current session."""
    d = date.fromisoformat(day) + timedelta(days=1)
    while not is_trading_day(d, cfg):
        d += timedelta(days=1)
    # both sides in UTC: an aware ET `now` would otherwise subtract as wall clock across DST
    left = (datetime.combine(d, OPEN, ET).astimezone(timezone.utc) - now.astimezone(timezone.utc))
    left = left.total_seconds()
    return f"{min(168, max(1, -(-int(left) // 3600)))}h"


def cmd_close_summary(cfg: Config, now: datetime) -> dict[str, Any]:
    """After the close: one normal-urgency report per trading day, keyed by the day, with
    the Jev and baseline P&L. Nothing on a holiday or a day with no session. The title names
    the day and the report expires at the next open (``next_open_ttl``)."""
    day = now.astimezone(ET).date().isoformat()
    conn = connect(cfg)
    if conn is None:
        return {"reported": False, "day": day, "summary": NO_SESSIONS}
    with closing(conn):
        sessions = [s for s in sessions_on(conn, [day]) if s["status"] not in NO_TRADE]
        if not sessions:
            return {"reported": False, "day": day, "summary": f"no paper session on {day}"}
        stats = policy_stats(conn, [s["session_id"] for s in sessions])
    last = sessions[-1]
    health, _ = health_of(last, now)
    title = close_title(last, health, stats, day)
    jev = pick_jev(list(stats))
    jpnl = stats[jev]["pnl"] if jev else None
    state = {
        "day": day,
        "status": last["status"],
        "jev_bucket": None if jpnl is None else round(jpnl / PNL_BUCKET) * PNL_BUCKET,
        "jev_trades": stats[jev]["trades"] if jev else 0,
    }
    ttl = next_open_ttl(day, now, cfg)
    report(f"trade-jev/pnl/{day}", title, urgency="normal", state=state, ttl=ttl)
    return {"reported": True, "day": day, "title": title, "state": state, "ttl": ttl}


def down_reason(conn: sqlite3.Connection | None, now: datetime) -> str | None:
    """Why paper trading is down right now, or None when it's fine (or not expected up)."""
    et = now.astimezone(ET)
    day = et.date().isoformat()
    s = latest_session(conn, day) if conn is not None else None
    if s is None:
        if et.time() >= MISSING_AFTER:
            return "no session started"
        return None
    health, age = health_of(s, now)  # holiday, skipped, done and aborted (stopped on purpose) are fine
    if health == "crashed":
        err = clip(s["error"], 100)
        return "session crashed" + (f": {err}" if err else "")
    if health == "stale":
        mins = f"{age // 60} min" if age is not None else "ever"
        return f"session stale (no heartbeat for {mins}), likely dead"
    return None


def cmd_watch(cfg: Config, now: datetime) -> dict[str, Any]:
    """Two checks. The Tradovate token runs on every call, any hour of any day (tokens die over a
    weekend too): an urgent ``trade-jev/token`` while renewal is failing or the token expires within
    10 min, cleared once healthy. The paper-session check keeps its market-hours gate: an urgent
    ``trade-jev/down`` while today's session is crashed or stale, or missing by 09:35. Their remembered
    states are independent (both live in ``watch_state.json``)."""
    out: dict[str, Any] = {}
    # Token first, and each check isolated: a crash in one must never suppress the other's alert.
    try:
        token = _watch_token(cfg, now)
    except Exception as e:  # noqa: BLE001
        token = {"error": type(e).__name__}
    try:
        out = _watch_down(cfg, now)
    except Exception as e:  # noqa: BLE001
        out = {"checked": False, "error": type(e).__name__}
    out["token"] = token
    return out


def _watch_token(cfg: Config, now: datetime) -> dict[str, Any]:
    state_path = data_dir() / WATCH_STATE
    status = read_token_status(cfg)
    problem = token_problem(status, now)
    was = _read_state(state_path).get("token")
    if problem:
        report(TOKEN_KEY, TOKEN_TITLE, urgency="urgent", state={"reason": problem}, ttl="2h")
        _write_state(state_path, {"token": {"reason": problem}})
        return {"problem": problem}
    if was and status is not None:  # a vanished status file is not "healthy": no clear
        report(TOKEN_KEY, "Tradovate token healthy again", urgency="urgent", clear=True,
               state={"reason": "ok"})  # fmt: skip
        _write_state(state_path, {"token": None})
        return {"problem": None, "cleared": True}
    return {"problem": None}


def _watch_down(cfg: Config, now: datetime) -> dict[str, Any]:
    """Every 15 min in market hours: an urgent ``trade-jev/down`` while today's session is
    crashed or stale, or missing by 09:35; a clear once it's healthy again. A clear is sent
    only after a down report (remembered in the data dir), so a healthy day says nothing."""
    et = now.astimezone(ET)
    day = et.date().isoformat()
    if not is_trading_day(et.date(), cfg) or not OPEN <= et.time() <= CLOSE:
        return {"checked": False, "summary": "outside market hours"}
    conn = connect(cfg)
    try:
        # Stopped on purpose today (trade_jev_stop): a stale or missing session isn't news.
        reason = None if _stopped_today(day) else down_reason(conn, now)
    finally:
        if conn is not None:
            conn.close()
    state_path = data_dir() / WATCH_STATE
    was_down = _read_state(state_path).get("down")
    if reason:
        kind = reason.split(":")[0].split(" (")[0]
        report(DOWN_KEY, f"Paper trading down: {reason}", urgency="urgent",
               state={"day": day, "reason": kind}, ttl="12h")  # fmt: skip
        _write_state(state_path, {"down": {"day": day, "reason": kind}})
        return {"checked": True, "down": True, "reason": reason}
    if was_down:
        report(DOWN_KEY, "Paper trading healthy again", urgency="urgent", clear=True,
               state={"day": day, "reason": "ok"})  # fmt: skip
        _write_state(state_path, {"down": None})
        return {"checked": True, "down": False, "cleared": True}
    return {"checked": True, "down": False}


def _read_state(path: Path) -> dict[str, Any]:
    try:
        raw = json.loads(path.read_text())
    except (OSError, ValueError):
        return {}
    return raw if isinstance(raw, dict) else {}


def _write_state(path: Path, updates: dict[str, Any]) -> None:
    """Merge ``updates`` into the remembered state (read-modify-write), so the down and token
    states coexist."""
    state = {**_read_state(path), **updates}
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(".tmp")
    tmp.write_text(json.dumps(state))
    tmp.replace(path)


# --- dashboard panel (Resonant panels, PER-280) ----------------------------------------------
#
# `./cli.py panel paper` prints one object in Resonant's panel format (built with panel.py, the
# SDK's builders copied next to this file). It draws what the tools already compute: cmd_status
# (session, jev_live, token, launchd), cmd_pnl (per-policy P&L) and policy_stats. Read-only.

PANEL_LOCAL_URL = "http://127.0.0.1:8765/paper"
PANEL_LIVE_URL = "http://127.0.0.1:8767/live"  # the running session's own view
# Core averages a time axis over 240 points into 240 buckets; a running total sampled at 240 of its
# own points stays exact instead, and keeps the output small.
PANEL_POINTS = 240
PANEL_TRADES = 20
PANEL_DAYS = 30  # History: the last 30 trading days
PANEL_SESSIONS = 30
PANEL_BASELINES = ("imbalance", "random")  # charted next to Jev; other baselines fill up to 4 lines
PANEL_LINES = 4
ALWAYS_ON = ("com.tradejev.view", "com.tradejev.tvtoken")  # KeepAlive agents: always running
SESSION_TONE = {"running": "ok", "done": "ok", "crashed": "bad", "stale": "bad", "aborted": "warn"}
TOKEN_BAD = ("expired", "renewal failing", "unreadable")


def chart_policies(policies: Iterable[str]) -> list[str]:
    """Jev first, then imbalance and random, then any other baselines by name: at most 4."""
    names = set(policies)
    jev = pick_jev(list(names))
    rest = [p for p in PANEL_BASELINES if p in names]
    rest += sorted(p for p in names if not is_jev(p) and p not in PANEL_BASELINES)
    return ([jev] if jev else []) + rest[: PANEL_LINES - (1 if jev else 0)]


def closed_trades(conn: sqlite3.Connection, session_ids: list[str]) -> list[sqlite3.Row]:
    """Every closed trade of the sessions, in exit order, all policies. (``trades`` answers one
    policy and at most 50 rows; the cumulative curve needs them all.)"""
    if not session_ids or not has_table(conn, "paper_trades"):
        return []
    marks = ",".join("?" * len(session_ids))
    return conn.execute(
        f"SELECT policy, side, entry_ns, entry_px, exit_ns, exit_px, points, pnl, reason "  # noqa: S608
        f"FROM paper_trades "
        f"WHERE session_id IN ({marks}) AND exit_ns IS NOT NULL ORDER BY exit_ns, id",
        session_ids,
    ).fetchall()


def cumulative(
    trades: list[sqlite3.Row], policies: list[str], day: str
) -> tuple[list[int], dict[str, list[float]]]:
    """Each policy's running realized P&L: 0 at the 09:30 open, then a point per exit second
    (unix seconds), thinned to ``PANEL_POINTS`` evenly spaced points (exact where sampled)."""
    total = dict.fromkeys(policies, 0.0)
    xs = [int(datetime.combine(date.fromisoformat(day), OPEN, ET).timestamp())]
    ys: dict[str, list[float]] = {p: [0.0] for p in policies}
    for r in trades:
        if r["policy"] not in total:
            continue
        total[r["policy"]] += float(r["pnl"] or 0)
        t = int(r["exit_ns"] // 10**9)
        if t > xs[-1]:
            xs.append(t)
            for p in policies:
                ys[p].append(0.0)
        for p in policies:  # same second (or before the open): folds into the last point
            ys[p][-1] = round(total[p], 2)
    n = len(xs)
    if n > PANEL_POINTS:
        keep = sorted({round(i * (n - 1) / (PANEL_POINTS - 1)) for i in range(PANEL_POINTS)})
        xs = [xs[i] for i in keep]
        ys = {p: [v[i] for i in keep] for p, v in ys.items()}
    return xs, ys


def _expect_paper_running(cfg: Config, now: datetime, session: dict[str, Any] | None) -> bool:
    """Should com.tradejev.paper be running right now (a trading day's session hours, not
    stopped by the owner, and today's session not already over)?"""
    et = now.astimezone(ET)
    if not is_trading_day(et.date(), cfg) or not OPEN <= et.time() <= CLOSE:
        return False
    if _stopped_today(et.date().isoformat()):
        return False
    return session is None or session["status"] not in ("done", "aborted", *NO_TRADE)


def agent_item(label: str, st: dict[str, Any], expected: bool) -> dict[str, Any]:
    """A launchd agent as a status item: down when not loaded, warn when it should be running
    and isn't (the always-on viewer and keep-alive, or the paper job during its session)."""
    if st.get("loaded") is None:
        return {"name": label, "state": "unknown", "detail": "launchctl unavailable"}
    if st["loaded"] is False:
        return {"name": label, "state": "down", "detail": "not loaded"}
    state = st.get("state") or "loaded"
    if state == "running":
        pid = st.get("pid")
        return {"name": label, "state": "up", "detail": f"running (pid {pid})" if pid else "running"}
    ok = not (expected or label in ALWAYS_ON)
    return {"name": label, "state": "up" if ok else "warn", "detail": f"loaded, {state}"}


def feed_item(session: dict[str, Any] | None) -> dict[str, Any]:
    """The live data feed, judged by the session heartbeat (written at least every 60 s)."""
    if session is None:
        return {"name": "data feed", "state": "unknown", "detail": "no session today"}
    name = f"data feed ({session['source']})" if session.get("source") else "data feed"
    age, health = session["heartbeat_age_s"], session["health"]
    if health == "running":
        return {"name": name, "state": "up", "detail": f"heartbeat {age}s ago"}
    if health == "stale":
        gone = f"{age // 60} min" if age is not None else "ever"
        return {"name": name, "state": "down", "detail": f"no heartbeat for {gone}"}
    return {"name": name, "state": "unknown", "detail": f"session {health}"}


def session_stat(st: dict[str, Any], cfg: Config, raw_error: str | None) -> dict[str, Any]:
    """Today's session state; ``raw_error`` is the row's own error (status clips it to one line)."""
    s, today = st.get("session"), date.fromisoformat(st["day"])
    if s is None:
        last = st.get("last_session_day")
        why = "market closed" if not is_trading_day(today, cfg) else "not started"
        hint = f"{why}; last {day_label(last)}" if last else why
        return pw.stat("Session", "no session", hint=hint)
    health = s["health"]
    if health == "running":
        hint = f"{day_label(st['day'])}, heartbeat {s['heartbeat_age_s']}s ago"
    elif health in ("crashed", "skipped", "stale"):
        hint = short_error(raw_error, 78) or day_label(st["day"])
    else:
        hint = day_label(st["day"])
    return pw.stat("Session", health, health=SESSION_TONE.get(health), hint=hint)


def token_stat(tok: dict[str, Any]) -> dict[str, Any]:
    if tok.get("problem") == "unreadable":
        return pw.stat("Tradovate token", "unreadable", health="bad", hint="status file unreadable")
    if not tok.get("known"):
        return pw.stat("Tradovate token", "n/a", hint="no status file (token mode off)")
    problem, mins = tok.get("problem"), tok.get("expires_in_min")
    tone = "ok" if problem is None else "bad" if problem in TOKEN_BAD else "warn"
    value = None if mins is None else max(0, mins) * 60
    return pw.stat("Token expires in", value, format="duration", health=tone,
                   hint=problem or "keep-alive ok")  # fmt: skip


def call_stats(st: dict[str, Any], jev: dict[str, Any] | None) -> list[dict[str, Any]]:
    """Model calls, errors and calls_mode: today's ``jev_live``, else the shown day's call count."""
    live = st.get("jev_live")
    if not live or not live.get("decisions"):
        calls = jev.get("jev_calls") if jev else None
        mode = live["calls_mode"] if live else "no session today"
        return [pw.stat("Model calls", calls, format="integer"),
                pw.stat("Call errors", None, format="integer"),
                pw.stat("Calls mode", mode)]  # fmt: skip
    ms = f"avg {live['avg_ms']} ms, p95 {live['p95_ms']} ms" if live["avg_ms"] is not None else None
    errors, mode = live["errors"], live["calls_mode"]
    err_tone = "ok" if not errors else "bad" if mode == "calls failing" else "warn"
    mode_tone = {"live model calls": "ok", "calls failing": "bad"}.get(mode)
    return [
        pw.stat("Model calls", live["model_calls"], format="integer", hint=ms),
        pw.stat("Call errors", errors, format="integer", health=err_tone,
                hint=clip(live["last_error"], 80)),
        pw.stat("Calls mode", mode, health=mode_tone,
                hint=f"last {live['last_decision_et']} ET" if live["last_decision_et"] else None),
    ]


def panel_health(st: dict[str, Any], items: list[dict[str, Any]], expected: bool) -> str:
    """bad: the session crashed, stalled or is missing during its hours, Jev's calls all fail,
    or the token is dead; warn: the token needs attention soon, or an agent or Jev call is off."""
    s, tok, live = st.get("session"), st["token"], st.get("jev_live") or {}
    if s and s["health"] in ("crashed", "stale"):
        return "bad"
    if (s is None and expected) or tok.get("problem") in TOKEN_BAD:
        return "bad"
    if live.get("calls_mode") == "calls failing" and s and s["health"] == "running":
        return "bad"
    if tok.get("problem") or any(i["state"] in ("down", "warn") for i in items):
        return "warn"
    if live.get("errors") and s and s["health"] == "running":
        return "warn"
    return "ok"


def pnl_widgets(
    conn: sqlite3.Connection | None, day: str | None, policies: list[str], when: str | None
) -> list[dict[str, Any]]:
    """The day's cumulative P&L per charted policy and its latest closed trades (all policies).
    ``when`` names the day in titles: "today", or a label like "Tue 11/3"."""
    widgets: list[dict[str, Any]] = []
    trades: list[sqlite3.Row] = []
    if conn is not None and day:
        ids = [s["session_id"] for s in sessions_on(conn, [day]) if s["status"] not in NO_TRADE]
        trades = closed_trades(conn, ids)
    if trades and policies:
        xs, ys = cumulative(trades, policies, day)
        widgets.append(pw.series("line", xs, ys, format="money", id="cum-pnl",
                                 title=f"Cumulative P&L, {when}"))  # fmt: skip
    else:
        widgets.append(pw.text(f"No closed trades {'yet ' if when == 'today' else ''}"
                               f"{'today' if when in (None, 'today') else 'on ' + when}.",
                               id="cum-pnl", title="Cumulative P&L"))  # fmt: skip
    recent = trades[-PANEL_TRADES:][::-1]
    if recent:
        widgets.append(pw.table(
            [pw.column("t", "Time ET"), pw.column("policy", "Policy"), pw.column("side", "Side"),
             pw.column("pts", "Pts", "number"), pw.column("pnl", "$", "money"),
             pw.column("why", "Why")],
            [{"t": (ns_to_et(r["exit_ns"]) or "")[:5], "policy": r["policy"], "side": r["side"],
              "pts": num(r["points"]), "pnl": num(r["pnl"]), "why": r["reason"]} for r in recent],
            id="trades", title=f"Recent trades, {when} ({len(trades)} closed)",
        ))  # fmt: skip
    return widgets


def today_view(
    cfg: Config, now: datetime, st: dict[str, Any], conn: sqlite3.Connection | None
) -> tuple[dict[str, Any], str]:
    """Today's tiles, cumulative P&L, recent trades and agents (with no session today, the P&L
    widgets show the latest trading day, named in their titles). Returns (view, health)."""
    pnl = cmd_pnl(cfg, now, "today")
    if not pnl.get("sessions"):
        pnl = cmd_pnl(cfg, now, "")
    day = pnl.get("from")
    jev = next((p for p in pnl.get("policies", []) if p["jev"]), None)
    row = latest_session(conn, st["day"]) if conn is not None and st.get("session") else None
    raw_error = row["error"] if row is not None else None
    when = "today" if day == st["day"] else (day_label(day) if day else None)
    pnl_hint = None if when is None else when if day == st["day"] else f"last session {when}"
    widgets = [
        pw.stat("Jev P&L", jev["pnl"] if jev else None, format="money",
                health=None if not jev else "ok" if (jev["pnl"] or 0) >= 0 else "bad",
                hint=pnl_hint),
        pw.stat("Jev trades / wins", f"{jev['trades']} / {jev['wins']}" if jev else "0 / 0",
                hint=pnl_hint),
        *call_stats(st, jev),
        session_stat(st, cfg, raw_error),
        token_stat(st["token"]),
    ]
    policies = chart_policies(p["policy"] for p in pnl.get("policies", []))
    widgets += pnl_widgets(conn, day, policies, when)
    expected = _expect_paper_running(cfg, now, st.get("session"))
    items = []
    for label in (cfg.label, *ALWAYS_ON):
        ls = st["launchd"] if label == cfg.label else launchd_state(replace(cfg, label=label))
        items.append(agent_item(label, ls, expected and label == cfg.label))
    items.append(feed_item(st.get("session")))
    widgets.append(pw.status(items, id="agents", title="Agents and feed"))
    return pw.view("today", "Today", widgets), panel_health(st, items, expected)


def history_view(conn: sqlite3.Connection | None, now: datetime, until: str = "9999-12-31") -> dict[str, Any]:
    """Daily P&L per policy over the last 30 trading days, and the latest sessions; both end at
    ``until`` (an archived day's view: nothing after that day)."""
    if conn is None:
        return pw.view("history", "History", [pw.text(NO_SESSIONS + ".", id="none")])
    days = sorted(r["day"] for r in conn.execute(
        "SELECT DISTINCT day FROM paper_sessions WHERE status NOT IN ('holiday', 'skipped') "
        "AND day<=? ORDER BY day DESC LIMIT ?", (until, PANEL_DAYS)))  # fmt: skip
    stats: dict[str, dict[str, dict[str, Any]]] = {}
    per_day: dict[str, dict[str, float]] = {d: {} for d in days}
    for s in sessions_on(conn, days) if days else []:
        if s["status"] in NO_TRADE:
            continue
        stats[s["session_id"]] = st = policy_stats(conn, [s["session_id"]])
        for p, v in st.items():
            per_day[s["day"]][p] = round(per_day[s["day"]].get(p, 0.0) + (v["pnl"] or 0.0), 2)
    widgets: list[dict[str, Any]] = []
    policies = chart_policies({p for d in per_day.values() for p in d})
    if days and policies:
        widgets.append(pw.series(
            "bar", [day_label(d) for d in days],
            {p: [per_day[d].get(p) for d in days] for p in policies},
            x_type="category", format="money", id="daily-pnl",
            title=f"Daily P&L, last {len(days)} trading days",
        ))  # fmt: skip
    rows = []
    for s in conn.execute(
        "SELECT * FROM paper_sessions WHERE day<=? ORDER BY day DESC, started_at DESC, rowid DESC LIMIT ?",
        (until, PANEL_SESSIONS),
    ):
        sid = s["session_id"]
        if sid not in stats and s["status"] not in NO_TRADE:
            stats[sid] = policy_stats(conn, [sid])
        st = stats.get(sid, {})
        jp = pick_jev(list(st))
        j = st.get(jp or "", {})
        health, _ = health_of(s, now)
        rows.append({
            "day": day_label(s["day"]),
            "status": pw.pill(health, SESSION_TONE.get(health, "neutral")),
            "jev": j.get("pnl"),
            "trades": j.get("trades"),
            "calls": j.get("jev_calls"),
            "error": short_error(s["error"], 100),
        })  # fmt: skip
    if rows:
        widgets.append(pw.table(
            [pw.column("day", "Day"), pw.column("status", "Status", "pill"),
             pw.column("jev", "Jev $", "money"), pw.column("trades", "Trades", "number"),
             pw.column("calls", "Calls", "number"), pw.column("error", "Error")],
            rows, id="sessions", title="Sessions",
        ))  # fmt: skip
    return pw.view("history", "History", widgets or [pw.text(NO_SESSIONS + ".", id="none")])


def cmd_panel(cfg: Config, now: datetime, date_arg: str | None = None) -> dict[str, Any]:
    """The ``paper`` dashboard panel: views Today and History (see README). With ``date_arg``
    (``--date YYYY-MM-DD``, a past ET day) that day's archived view instead (``day_panel``)."""
    day = archive_day(date_arg, now)
    if day is not None:
        return day_panel(cfg, now, day)
    st = cmd_status(cfg, now)
    conn = panel_connect(cfg)
    try:
        today, health = today_view(cfg, now, st, conn)
        history = history_view(conn, now)
    finally:
        if conn is not None:
            conn.close()
    running = bool(st.get("session")) and st["session"]["health"] == "running"
    return pw.panel([today, history], updated_at=now, health=health,
                    local_url=PANEL_LIVE_URL if running else PANEL_LOCAL_URL)  # fmt: skip


# --- the `chart` panel (PER-283) ---------------------------------------------------------------

CHART_TRADES = 100  # the price widget's cap: the newest are kept
CHART_DAYS = 10  # how far back to look for the last session with data
WHY = {"jev": "reversal"}  # paper_trades.reason: Jev's own order closed it; target, stop, eod as is


def _window(day: str) -> tuple[int, int]:
    """09:30 and 16:00 ET of ``day`` in unix seconds: the candles are [open, close)."""
    d = date.fromisoformat(day)
    return tuple(int(datetime.combine(d, t, ET).timestamp()) for t in (OPEN, CLOSE))  # type: ignore[return-value]


def chart_bars(conn: sqlite3.Connection, day: str, ids: list[str]) -> dict[int, list[float]]:
    """The day's 1-minute mid bars (``paper_bars``, written by the runner) keyed by minute start; a
    minute two sessions both saw (a restart) merges in session order."""
    if not ids or not has_table(conn, "paper_bars"):
        return {}
    lo, hi = _window(day)
    out: dict[int, list[float]] = {}
    for sid in ids:
        for t, o, h, lw, c in conn.execute(
            "SELECT t_s, o, h, l, c FROM paper_bars WHERE session_id=? AND t_s>=? AND t_s<? ORDER BY t_s",
            (sid, lo, hi),
        ):
            b = out.get(t)
            out[t] = [o, h, lw, c] if b is None else [b[0], max(b[1], h), min(b[2], lw), c]
    return out


def chart_mids(conn: sqlite3.Connection, day: str, ids: list[str]) -> dict[int, float]:
    """Fallback for a session from before ``paper_bars`` existed: the last 15 s decision mid of each
    minute. Too coarse for candles (their high/low miss most of the range), so it is drawn as a line."""
    if not ids or not has_table(conn, "paper_decisions"):
        return {}
    lo, hi = _window(day)
    marks = ",".join("?" * len(ids))
    out: dict[int, float] = {}
    for t_ns, mid in conn.execute(
        f"SELECT t_ns, mid FROM paper_decisions WHERE session_id IN ({marks}) "  # noqa: S608
        f"AND t_ns>=? AND t_ns<? AND mid IS NOT NULL ORDER BY t_ns",
        (*ids, lo * 10**9, hi * 10**9),
    ):
        out[t_ns // 10**9 // 60 * 60] = float(mid)
    return out


def chart_trades(conn: sqlite3.Connection, ids: list[str], running: str | None) -> list[dict[str, Any]]:
    """Jev's trades on the chart: the closed ones and, while ``running`` (that session's id), the
    open position from ``paper_open`` as an open trade (last, so it survives the cap). At most the
    newest ``CHART_TRADES``."""
    out = [
        pw.trade(r["side"], r["entry_ns"] // 10**9, r["entry_px"], r["exit_ns"] // 10**9, r["exit_px"],
                 pnl=num(r["pnl"]), currency="USD", label="jev", why=WHY.get(r["reason"], r["reason"]))
        for r in closed_trades(conn, ids) if is_jev(r["policy"])
    ]  # fmt: skip
    if running and has_table(conn, "paper_open"):
        for r in conn.execute(
            "SELECT policy, side, entry_ns, entry_px FROM paper_open WHERE session_id=?", (running,)
        ):
            if is_jev(r["policy"]) and r["side"] in ("long", "short"):
                out.append(pw.trade(r["side"], r["entry_ns"] // 10**9, r["entry_px"],
                                    currency="USD", label="jev"))  # fmt: skip
    return out[-CHART_TRADES:]


def chart_view(conn: sqlite3.Connection | None, now: datetime) -> tuple[list[dict[str, Any]], str | None, bool]:
    """The chart's widgets, health and whether the charted session is running. The day is today once
    its session has market data or has ended; otherwise (weekend, holiday, before 09:30) the latest
    earlier session, named in the title."""
    if conn is None:
        return [pw.text(NO_SESSIONS + ".", id="none")], None, False
    today = now.astimezone(ET).date().isoformat()
    days = [r["day"] for r in conn.execute(
        "SELECT DISTINCT day FROM paper_sessions WHERE status NOT IN ('holiday', 'skipped') AND day<=? "
        "ORDER BY day DESC LIMIT ?", (today, CHART_DAYS))]  # fmt: skip
    for day in days:
        sess = [s for s in sessions_on(conn, [day]) if s["status"] not in NO_TRADE]
        ids = [s["session_id"] for s in sess]
        bars = chart_bars(conn, day, ids)
        mids = {} if bars else chart_mids(conn, day, ids)
        last = sess[-1]
        if bars or mids or last["status"] != "running" or day == days[-1]:
            break
    else:
        return [pw.text(NO_SESSIONS + ".", id="none")], None, False
    health = health_of(last, now)[0]
    running = health == "running"
    when = "today" if day == today else f"{day_label(day)} (last session)"
    widgets = chart_widgets(conn, last, ids, bars, mids, when, running)
    # health is about today: an older session's crash still shows its text, but doesn't stay red
    return widgets, "bad" if day == today and health in ("crashed", "stale") else None, running


def chart_widgets(
    conn: sqlite3.Connection, last: sqlite3.Row, ids: list[str], bars: dict[int, list[float]],
    mids: dict[int, float], when: str, running: bool, past: bool = False,
) -> list[dict[str, Any]]:  # fmt: skip
    """One day's chart: candles (or the decision-mid line) with Jev's trades; with no market data,
    Jev's trades as a table; the crash line when ``last`` (the day's last session) crashed."""
    symbol = last["symbol"] or "NQ"
    trades = chart_trades(conn, ids, last["session_id"] if running else None)
    widgets: list[dict[str, Any]] = []
    if bars:
        xs = sorted(bars)
        widgets.append(pw.price(xs, candles=[bars[t] for t in xs], trades=trades, format="number",
                                id="price", title=f"{symbol} 1-min mid, {when}"))  # fmt: skip
    elif mids:
        xs = sorted(mids)
        widgets.append(pw.price(xs, line=[mids[t] for t in xs], trades=trades, format="number", id="price",
                                title=f"{symbol} mid (15 s samples; no 1-min bars), {when}"))  # fmt: skip
    else:
        widgets.append(pw.text(f"No market data recorded for {when}{'' if trades or past else ' yet'}."
                               + (" Jev's trades:" if trades else ""), id="none"))  # fmt: skip
        if trades:
            widgets.append(trades_table(trades))
    if last["status"] == "crashed":
        widgets.append(pw.text(f"Session crashed: {short_error(last['error'], 160) or 'no error recorded'}",
                               id="crash", title="Crashed"))  # fmt: skip
    return widgets


def trades_table(trades: list[dict[str, Any]]) -> dict[str, Any]:
    """Jev's chart trades as rows, newest first: for a day with trades but no market data."""
    def hm(t: int | None) -> str | None:
        return None if t is None else datetime.fromtimestamp(t, ET).strftime("%H:%M")

    return pw.table(
        [pw.column("in", "In ET"), pw.column("out", "Out ET"), pw.column("side", "Side"),
         pw.column("entry", "Entry", "number"), pw.column("exit", "Exit", "number"),
         pw.column("pnl", "$", "money"), pw.column("why", "Why")],
        [{"in": hm(t["entry_t"]), "out": hm(t.get("exit_t")), "side": t["side"], "entry": num(t["entry_px"]),
          "exit": num(t.get("exit_px")), "pnl": t.get("pnl"), "why": t.get("why")} for t in trades[::-1]],
        id="trades", title=f"Jev trades ({len(trades)})",
    )  # fmt: skip


def cmd_chart(cfg: Config, now: datetime, date_arg: str | None = None) -> dict[str, Any]:
    """The ``chart`` dashboard panel: NQ 1-minute candles with Jev's trades (see README). With
    ``date_arg`` (``--date YYYY-MM-DD``, a past ET day) that day's chart (``chart_day_panel``)."""
    day = archive_day(date_arg, now)
    if day is not None:
        return chart_day_panel(cfg, now, day)
    conn = panel_connect(cfg)
    try:
        widgets, health, running = chart_view(conn, now)
    finally:
        if conn is not None:
            conn.close()
    return pw.panel([pw.view("chart", "Jev trades", widgets)], updated_at=now, health=health,
                    local_url=PANEL_LIVE_URL if running else PANEL_LOCAL_URL)  # fmt: skip


# --- daily history: `panel paper|chart --date YYYY-MM-DD` (PER-287) ------------------------------
#
# Resonant archives each panel once per ended day (manifest `history: daily`) by running its command
# with `--date D`. A day no session ran (weekend, holiday, before the first session) prints NONE, so
# it is recorded as such and left out of the day picker. Live-only parts (token, launchd, agents)
# are left out: they describe now, not D.

NONE = {"schema": pw.SCHEMA, "none": True}
CANT_READ = "paper.db won't open read-only (WAL side files missing?); try again later"


def archive_day(arg: str | None, now: datetime) -> str | None:
    """``--date`` as the ET day to archive: None renders the live panel (no ``--date``, or today's
    date: today renders as now). A future day is returned too; it has no session, so it is NONE."""
    if arg is None:
        return None
    try:
        d = date.fromisoformat(arg.strip())
    except ValueError:
        raise Fail(f"bad --date {clip(arg, 40)!r}: use YYYY-MM-DD") from None
    return None if d == now.astimezone(ET).date() else d.isoformat()


def panel_connect(cfg: Config) -> sqlite3.Connection | None:
    """``connect`` for the panels: a WAL DB that won't open read-only (its -wal/-shm side files
    missing, as ``health`` tolerates) is a clean one-line failure, so an archive run is retried
    later instead of recording the day from the main file alone (the WAL may still hold it)."""
    try:
        return connect(cfg)
    except sqlite3.Error as e:
        if _cant_open(e):
            raise Fail(CANT_READ) from None
        raise


def ran_on(conn: sqlite3.Connection | None, day: str, now: datetime) -> list[sqlite3.Row]:
    """The day's session rows without the runner's ``holiday`` rows; [] when no session ran (no DB,
    a future day, no row, or only holiday rows). A ``skipped`` day did run (it says why)."""
    if conn is None or day > now.astimezone(ET).date().isoformat():
        return []
    return [r for r in sessions_on(conn, [day]) if r["status"] != "holiday"]


def data_time(sessions: list[sqlite3.Row], now: datetime) -> datetime:
    """An archived day's ``updated_at``: when its last session ended (or last beat), never after now."""
    last = sessions[-1]
    t = parse_ts(last["ended_at"]) or parse_ts(last["heartbeat_at"]) or parse_ts(last["started_at"])
    return min(t, now) if t else now


def day_calls(conn: sqlite3.Connection, sessions: list[sqlite3.Row], policy: str | None) -> dict[str, Any] | None:
    """``jev_live`` over all of a day's sessions (a restart day has two), for ``call_stats``."""
    ids = [s["session_id"] for s in sessions]
    if not ids or policy is None or not has_table(conn, "paper_decisions"):
        return None
    marks = ",".join("?" * len(ids))
    where = f"session_id IN ({marks}) AND policy=?"  # noqa: S608 - placeholders only
    agg = conn.execute(
        f"SELECT COUNT(*) n, SUM(error IS NULL AND jev_ms >= ?) calls, SUM(error IS NOT NULL) errors, "  # noqa: S608
        f"MAX(t_ns) last_t FROM paper_decisions WHERE {where}", (CALL_MS, *ids, policy)).fetchone()  # fmt: skip
    ms = [r[0] for r in conn.execute(
        f"SELECT jev_ms FROM paper_decisions WHERE {where} AND error IS NULL AND jev_ms >= ? "  # noqa: S608
        f"ORDER BY jev_ms", (*ids, policy, CALL_MS))]  # fmt: skip
    recent = conn.execute(
        f"SELECT raw_action, jev_ms, error FROM paper_decisions WHERE {where} "  # noqa: S608
        f"ORDER BY t_ns DESC LIMIT ?", (*ids, policy, RECENT_DECISIONS)).fetchall()  # fmt: skip
    last_err = conn.execute(
        f"SELECT time_et, error FROM paper_decisions WHERE {where} AND error IS NOT NULL "  # noqa: S608
        f"ORDER BY t_ns DESC LIMIT 1", (*ids, policy)).fetchone()  # fmt: skip
    return {
        "decisions": int(agg["n"] or 0),
        "model_calls": int(agg["calls"] or 0),
        "errors": int(agg["errors"] or 0),
        "calls_mode": calls_mode(sessions[-1]["jev_model"], recent),
        "last_error": None if last_err is None else f"{last_err['time_et']} {clip(last_err['error'], 100)}",
        "avg_ms": round(sum(ms) / len(ms)) if ms else None,
        "p95_ms": round(ms[-(-95 * len(ms) // 100) - 1]) if ms else None,
        "last_decision_et": ns_to_et(agg["last_t"]),
    }


def day_sessions_table(
    conn: sqlite3.Connection, sessions: list[sqlite3.Row], now: datetime
) -> dict[str, Any]:
    """Every session of the day (a restart or crash is its own row): times, status, Jev, error."""
    rows = []
    for s in sessions:
        st = {} if s["status"] in NO_TRADE else policy_stats(conn, [s["session_id"]])
        j = st.get(pick_jev(list(st)) or "", {})
        health, _ = health_of(s, now)
        start, end = parse_ts(s["started_at"]), parse_ts(s["ended_at"])
        rows.append({
            "start": start.astimezone(ET).strftime("%H:%M") if start else None,
            "end": end.astimezone(ET).strftime("%H:%M") if end else None,
            "status": pw.pill(health, SESSION_TONE.get(health, "neutral")),
            "jev": j.get("pnl"),
            "trades": j.get("trades"),
            "calls": j.get("jev_calls"),
            "error": short_error(s["error"], 100),
        })  # fmt: skip
    n = len(sessions)
    return pw.table(
        [pw.column("start", "Start ET"), pw.column("end", "End ET"), pw.column("status", "Status", "pill"),
         pw.column("jev", "Jev $", "money"), pw.column("trades", "Trades", "number"),
         pw.column("calls", "Calls", "number"), pw.column("error", "Error")],
        rows, id="day-sessions", title=f"Sessions ({n}{', 1 restart' if n == 2 else f', {n - 1} restarts' if n > 2 else ''})",
    )  # fmt: skip


def day_view(conn: sqlite3.Connection, sessions: list[sqlite3.Row], day: str, now: datetime) -> tuple[dict[str, Any], str]:
    """A past day's tiles (Jev P&L, trades, model calls, session), Jev vs the baselines, the
    cumulative curve, recent trades, the day's sessions and the crash line. Returns (view, health)."""
    label = day_label(day)
    trading = [s for s in sessions if s["status"] not in NO_TRADE]
    ids = [s["session_id"] for s in trading]
    stats = policy_stats(conn, ids)
    jp = pick_jev(list(stats)) or pick_jev([p for i in ids for p in _decision_policies(conn, i)])
    jev = stats.get(jp or "")
    last = sessions[-1]
    health, _ = health_of(last, now)
    if health in ("crashed", "skipped", "stale"):
        hint = short_error(last["error"], 78) or label
    else:
        hint = f"{label}, {len(sessions)} sessions" if len(sessions) > 1 else label
    widgets = [
        pw.stat("Jev P&L", jev["pnl"] if jev else None, format="money",
                health=None if not jev else "ok" if (jev["pnl"] or 0) >= 0 else "bad", hint=label),
        pw.stat("Jev trades / wins", f"{jev['trades']} / {jev['wins']}" if jev else "0 / 0", hint=label),
        *call_stats({"jev_live": day_calls(conn, trading, jp)}, jev),
        pw.stat("Session", health, health=SESSION_TONE.get(health), hint=hint),
    ]  # fmt: skip
    if stats:
        order = sorted(stats, key=lambda p: (not is_jev(p), p))
        widgets.append(pw.table(
            [pw.column("policy", "Policy"), pw.column("pnl", "$", "money"),
             pw.column("trades", "Trades", "number"), pw.column("wins", "Wins", "number"),
             pw.column("dd", "Max DD", "money")],
            [{"policy": p, "pnl": stats[p]["pnl"], "trades": stats[p]["trades"], "wins": stats[p]["wins"],
              "dd": stats[p].get("max_drawdown")} for p in order],
            id="policies", title=f"Jev vs baselines, {label}",
        ))  # fmt: skip
    widgets += pnl_widgets(conn, day, chart_policies(stats), label)
    widgets.append(day_sessions_table(conn, sessions, now))
    crashed = [s for s in sessions if health_of(s, now)[0] in ("crashed", "stale")]
    if crashed:
        widgets.append(pw.text("; ".join(
            f"{health_of(s, now)[0].capitalize()}: {short_error(s['error'], 160) or 'no error recorded'}"
            for s in crashed), id="crash", title="Crashed"))  # fmt: skip
    tone = "bad" if health in ("crashed", "stale") else "warn" if crashed else "ok"
    return pw.view("today", label, widgets), tone


def day_panel(cfg: Config, now: datetime, day: str) -> dict[str, Any]:
    """``panel paper --date D``: D's view (titled like "Tue 11/3") and the History tab ending at
    D; NONE when no session ran on D."""
    conn = panel_connect(cfg)
    try:
        sessions = ran_on(conn, day, now)
        if conn is None or not sessions:
            return NONE
        view, health = day_view(conn, sessions, day, now)
        history = history_view(conn, now, until=day)
    finally:
        if conn is not None:
            conn.close()
    return pw.panel([view, history], updated_at=data_time(sessions, now), health=health,
                    local_url=PANEL_LOCAL_URL)  # fmt: skip


def chart_day_panel(cfg: Config, now: datetime, day: str) -> dict[str, Any]:
    """``panel chart --date D``: D's 1-minute candles (or decision mids) with Jev's trades, or the
    trades as a table when D has no market data; NONE when no session ran on D."""
    conn = panel_connect(cfg)
    try:
        sessions = ran_on(conn, day, now)
        if conn is None or not sessions:
            return NONE
        trading = [s for s in sessions if s["status"] not in NO_TRADE]
        last = sessions[-1]
        if trading:
            ids = [s["session_id"] for s in trading]
            bars = chart_bars(conn, day, ids)
            mids = {} if bars else chart_mids(conn, day, ids)
            widgets = chart_widgets(conn, trading[-1], ids, bars, mids, day_label(day), False, past=True)
        else:  # only `skipped` rows: the runner ran and chose not to trade
            widgets = [pw.text(f"Session skipped on {day_label(day)}: "
                               f"{short_error(last['error'], 160) or 'no reason recorded'}", id="none")]  # fmt: skip
        health = "bad" if health_of(last, now)[0] in ("crashed", "stale") else None
    finally:
        if conn is not None:
            conn.close()
    return pw.panel([pw.view("chart", f"Jev trades, {day_label(day)}", widgets)],
                    updated_at=data_time(sessions, now), health=health, local_url=PANEL_LOCAL_URL)  # fmt: skip


# --- main -------------------------------------------------------------------------------------


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(prog="cli.py")
    sub = p.add_subparsers(dest="cmd", required=True)
    for name in ("status", "start", "stop", "health", "recover", "close-summary", "watch"):
        sub.add_parser(name)
    pnl = sub.add_parser("pnl")
    pnl.add_argument("--date", default="")
    tr = sub.add_parser("trades")
    tr.add_argument("--date", default="")
    tr.add_argument("--policy", default="")
    tr.add_argument("--limit", type=int, default=0)
    dec = sub.add_parser("decisions")
    dec.add_argument("--date", default="")
    dec.add_argument("--limit", type=int, default=0)
    pan = sub.add_parser("panel")
    pan.add_argument("name", choices=["paper", "chart"])
    pan.add_argument("--date", default=None)  # PER-287: a past ET day's archived view
    return p


def run(argv: list[str], now: datetime | None = None) -> dict[str, Any]:
    args = build_parser().parse_args(argv)
    cfg = load_config()
    if args.cmd == "panel":
        return (cmd_chart if args.name == "chart" else cmd_panel)(cfg, now or now_utc(), args.date)
    if args.cmd == "pnl":
        return cmd_pnl(cfg, now or now_utc(), args.date)
    if args.cmd == "trades":
        return cmd_trades(cfg, now or now_utc(), args.date, args.policy, args.limit)
    if args.cmd == "decisions":
        return cmd_decisions(cfg, now or now_utc(), args.date, args.limit)
    if args.cmd in ("close-summary", "watch"):
        fn = cmd_close_summary if args.cmd == "close-summary" else cmd_watch
        # close-summary reports the day it was due for; watch is about right now.
        return fn(cfg, now or (job_now() if args.cmd == "close-summary" else now_utc()))
    fn = {"status": cmd_status, "start": cmd_start, "stop": cmd_stop, "health": cmd_health,
          "recover": cmd_recover}[args.cmd]  # fmt: skip
    return fn(cfg, now or now_utc())


def main(argv: list[str] | None = None) -> int:
    argv = sys.argv[1:] if argv is None else argv
    try:
        out = run(argv)
    except Fail as e:
        print(f"trade-jev: {e}", file=sys.stderr)
        return 1
    except sqlite3.Error as e:
        print(f"trade-jev: database error: {type(e).__name__}: {clip(e, 120)}", file=sys.stderr)
        return 1
    print(json.dumps(out, separators=(",", ":"), default=str))
    # health's verdict is its exit code (Resonant reads only that): non-zero while the session is down.
    return UNHEALTHY if argv[:1] == ["health"] and out.get("ok") is False else 0


if __name__ == "__main__":
    sys.exit(main())
