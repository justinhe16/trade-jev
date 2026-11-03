#!/usr/bin/env python3
"""trade-jev Resonant extension: paper status, P&L, trades, Jev decisions, start/stop, two jobs.

Stdlib only. Resonant runs this as ``python -I -S cli.py <cmd>`` with ``PATH=/usr/bin:/bin``
and ``HOME``/``RESONANT_EXT_DATA`` set to the extension's data dir (never the real home).
Every command prints exactly one JSON object on stdout and exits 0; a failure prints one
line to stderr and exits 1, and a failed run changed nothing.

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
from contextlib import closing
from dataclasses import dataclass, field
from datetime import date, datetime, time, timedelta, timezone
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

# emit.py is the SDK's stdlib helper, copied next to this file. ``-I`` keeps this directory
# off sys.path, so add it before importing.
sys.path.insert(0, str(Path(__file__).resolve().parent))
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
        proc = launchctl("kickstart", service(cfg))
    except subprocess.TimeoutExpired:
        # It may have gone through: never exit 1 (that promises nothing happened).
        _clear_stopped()
        return {"started": "unknown", "reason": "launchctl timed out",
                "summary": f"kickstart of {cfg.label} timed out; check trade_jev_status"}  # fmt: skip
    except (OSError, subprocess.SubprocessError) as e:
        raise Fail(f"launchctl kickstart failed: {type(e).__name__}") from None
    if proc.returncode != 0:
        raise Fail(f"launchctl kickstart exited {proc.returncode}: {clip(proc.stderr, 120)}")
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
        proc = launchctl("kill", "SIGTERM", service(cfg))
    except subprocess.TimeoutExpired:
        # It may have gone through: never exit 1 (that promises nothing happened).
        _mark_stopped(day)
        return {"stopped": "unknown", "reason": "launchctl timed out",
                "summary": f"SIGTERM to {cfg.label} timed out; check trade_jev_status"}  # fmt: skip
    except (OSError, subprocess.SubprocessError) as e:
        raise Fail(f"launchctl kill failed: {type(e).__name__}") from None
    if proc.returncode != 0:
        raise Fail(f"launchctl kill exited {proc.returncode}: {clip(proc.stderr, 120)}")
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


def cmd_health(cfg: Config, now: datetime) -> dict[str, Any]:
    """Healthy (exit 0) when the config parses and the DB, if present, opens read-only and
    answers a query. A missing DB is healthy: no session has run yet."""
    conn = connect(cfg)
    if conn is None:
        return {"ok": True, "db": "absent", "summary": NO_SESSIONS}
    with closing(conn):
        n = conn.execute("SELECT COUNT(*) FROM paper_sessions").fetchone()[0]
    return {"ok": True, "db": "ok", "sessions": n}


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


# --- main -------------------------------------------------------------------------------------


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(prog="cli.py")
    sub = p.add_subparsers(dest="cmd", required=True)
    for name in ("status", "start", "stop", "health", "close-summary", "watch"):
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
    return p


def run(argv: list[str], now: datetime | None = None) -> dict[str, Any]:
    args = build_parser().parse_args(argv)
    cfg = load_config()
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
    fn = {"status": cmd_status, "start": cmd_start, "stop": cmd_stop, "health": cmd_health}[
        args.cmd
    ]
    return fn(cfg, now or now_utc())


def main(argv: list[str] | None = None) -> int:
    try:
        out = run(sys.argv[1:] if argv is None else argv)
    except Fail as e:
        print(f"trade-jev: {e}", file=sys.stderr)
        return 1
    except sqlite3.Error as e:
        print(f"trade-jev: database error: {type(e).__name__}: {clip(e, 120)}", file=sys.stderr)
        return 1
    print(json.dumps(out, separators=(",", ":"), default=str))
    return 0


if __name__ == "__main__":
    sys.exit(main())
