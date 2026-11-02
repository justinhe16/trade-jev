"""Live paper trading: paper DB, trading days, --exit-at-close, crash/holiday rows, /paper routes.
No network, no keys: recorded synthetic days, fake sources and a temp DB."""

import asyncio
import json
import sqlite3
import threading
import urllib.error
import urllib.request
from datetime import date, datetime, timedelta, timezone
from http.server import ThreadingHTTPServer

import pytest

from trade_jev import live, outputs, paperdb, tradingdays
from trade_jev.data import ET
from trade_jev.harness import Config, Trade
from trade_jev.live import Recorder, Session
from trade_jev.settings import Settings
from tests.test_live import DAY, MemSource, Script, synthetic

SPEC = {
    "paper_sessions": ["session_id", "day", "symbol", "status", "started_at", "ended_at", "heartbeat_at",
                       "settings_json", "jev_model", "run_dir", "error", "source"],
    "paper_trades": ["id", "session_id", "policy", "side", "entry_ns", "entry_px", "exit_ns", "exit_px",
                     "reason", "points", "pnl"],
    "paper_decisions": ["session_id", "policy", "t_ns", "time_et", "mid", "position", "action", "raw_action",
                        "probs_json", "jev_ms", "error"],
    "paper_summary": ["session_id", "policy", "trades", "wins", "pnl", "max_drawdown", "jev_calls",
                      "jev_cost_usd"],
}


@pytest.fixture
def db(tmp_path, monkeypatch):
    path = tmp_path / "paper.db"
    monkeypatch.setenv("TRADE_JEV_PAPER_DB", str(path))
    return path


def rows(path, sql, *params):
    con = sqlite3.connect(path)
    con.row_factory = sqlite3.Row
    try:
        return [dict(r) for r in con.execute(sql, params)]
    finally:
        con.close()


@pytest.fixture
def sandbox(tmp_path, monkeypatch, db):
    """runs/ and data/live/ under tmp_path, with DAY recorded there; nothing touches the repo."""
    monkeypatch.setattr(live, "ROOT", tmp_path)
    monkeypatch.setattr(outputs, "ROOT", tmp_path)
    import trade_jev.data as data
    monkeypatch.setattr(data, "LIVE_DIR", tmp_path / "live")
    monkeypatch.setattr(data, "DATA_DIR", tmp_path / "none")
    rec = Recorder(DAY, "NQU6", out_dir=tmp_path / "live")
    rec.add(synthetic())
    rec.close()
    return tmp_path


# ---------------------------------------------------------------- trading days

def test_trading_days_skip_weekends_and_full_closures():
    assert tradingdays.closed_reason("2026-11-07") == "weekend"           # Saturday
    assert tradingdays.closed_reason("2026-11-08") == "weekend"           # Sunday
    assert tradingdays.closed_reason("2026-12-25") == "Christmas Day"
    assert tradingdays.closed_reason("2027-03-26") == "Good Friday"
    assert tradingdays.closed_reason("2027-12-24") == "Christmas Day (observed)"
    assert tradingdays.is_trading_day("2026-11-02")                       # Monday
    assert tradingdays.is_trading_day(date(2026, 11, 26))                 # Thanksgiving: CME trades till 13:00 ET
    assert all(date.fromisoformat(d).weekday() < 5 for d in tradingdays.FULL_CLOSURES)
    assert {date.fromisoformat(d).year for d in tradingdays.FULL_CLOSURES} == tradingdays.COVERED_YEARS
    assert tradingdays.covered("2027-06-01") and not tradingdays.covered("2028-01-03")


# ---------------------------------------------------------------- paper DB

def test_schema_is_exact_and_wal(db):
    con = paperdb.connect()
    try:
        for table, cols in SPEC.items():
            assert [r[1] for r in con.execute(f"PRAGMA table_info({table})")] == cols, table
        assert con.execute("PRAGMA journal_mode").fetchone()[0] == "wal"
        idx = {r[0] for r in con.execute("SELECT name FROM sqlite_master WHERE type='index'")}
        assert {"paper_sessions_day", "paper_trades_session", "paper_decisions_session"} <= idx
    finally:
        con.close()
    assert db.exists() and paperdb.db_path() == db


def test_recorder_writes_session_trades_decisions_summary(db):
    rec = paperdb.Recorder("live-2026-11-02")
    rec.start("2026-11-02", "NQZ6", {"min_conf": 0.7}, "jev-latest", "runs/live-2026-11-02")
    rec.trade("jev[raw_l10]", Trade("2026-11-02", 1, 10, 100.0, 20, 101.0, "target", 1.0, 15.0))
    rec.trade("random", Trade("2026-11-02", -1, 10, 100.0, 30, 101.0, "jev", -1.0, -25.0))
    rec.decision("jev[raw_l10]", {"t_ns": 10, "time_et": "09:30:00", "mid": 100.125, "position": 0,
                                  "action": "BUY", "raw_action": "BUY", "probs": {"BUY": 0.8}, "jev_ms": 412.0,
                                  "error": None})
    rec.close()  # drains the writer
    s = rows(db, "SELECT * FROM paper_sessions")[0]
    assert s["status"] == "running" and s["day"] == "2026-11-02" and s["heartbeat_at"] and s["ended_at"] is None
    assert json.loads(s["settings_json"]) == {"min_conf": 0.7}
    t = rows(db, "SELECT * FROM paper_trades ORDER BY id")
    assert [(r["policy"], r["side"], r["pnl"]) for r in t] == [("jev[raw_l10]", "long", 15.0), ("random", "short", -25.0)]
    d = rows(db, "SELECT * FROM paper_decisions")[0]
    assert json.loads(d["probs_json"]) == {"BUY": 0.8} and d["jev_ms"] == 412.0

    # a running session: the viewer computes totals from the trades so far
    got = paperdb.list_sessions()["sessions"][0]
    assert not got["final"] and got["policies"]["random"]["pnl"] == -25.0 and not got["stale"]

    rec = paperdb.Recorder("live-2026-11-02")
    rec.finish("done", [{"policy": "jev[raw_l10]", "trades": 1, "wins": 1, "pnl": 15.0, "max_drawdown": 0.0,
                         "jev_calls": 3, "jev_cost_usd": 0.001}])
    rec.close()
    s = paperdb.get_session("live-2026-11-02")
    assert s["status"] == "done" and s["ended_at"] and s["final"]
    assert s["policies"]["jev[raw_l10]"]["jev_calls"] == 3
    assert len(s["trades"]) == 2 and s["decisions"][0]["probs"] == {"BUY": 0.8}


def test_stale_heartbeat_marks_a_running_session_dead(db):
    rec = paperdb.Recorder("live-2026-11-03")
    rec.start("2026-11-03", "NQZ6", {}, None, None)
    rec.close()
    assert not paperdb.list_sessions()["sessions"][0]["stale"]
    old = (datetime.now(timezone.utc) - timedelta(seconds=paperdb.STALE_S + 5)).isoformat(timespec="seconds")
    with sqlite3.connect(db) as con:
        con.execute("UPDATE paper_sessions SET heartbeat_at=?", (old,))
    assert paperdb.list_sessions()["sessions"][0]["stale"]


def test_heartbeat_task_beats_on_the_event_loop():
    class Count:
        n = 0

        def heartbeat(self):
            self.n += 1

    c = Count()

    async def go():
        task = asyncio.create_task(live.heartbeat(c, every_s=0.01))
        await asyncio.sleep(0.1)
        task.cancel()
    asyncio.run(go())
    assert c.n >= 3


def test_max_drawdown_matches_metrics():
    assert paperdb.max_drawdown([]) == 0
    assert paperdb.max_drawdown([100, -50, -80, 200, -10]) == 130


def test_session_writes_every_closed_trade_and_the_jev_log(db):
    b = synthetic()
    cfg = Config(cadence_s=5, start_et="09:30:00", end_et="09:35:00", stop_ticks=6, target_ticks=8)
    rec = paperdb.Recorder("rehearsal-x")
    s = Session(None, cfg, Settings(0.6, 2, 0, 6, 8), DAY, "NQU6", MemSource(b), Script(), ["random", "imbalance"],
                None, None, paper=rec)
    asyncio.run(s.run())
    rec.finish("done", s.paper_summary())
    rec.close()
    e = s.engine
    for name, r in e.results.items():
        got = rows(db, "SELECT * FROM paper_trades WHERE policy=? ORDER BY id", name)
        assert [(g["entry_ns"], g["exit_ns"], g["reason"], g["pnl"]) for g in got] == \
               [(t.entry_ns, t.exit_ns, t.reason, t.pnl) for t in r.trades], name
    assert sum(len(r.trades) for r in e.results.values()) > 3
    decs = rows(db, "SELECT * FROM paper_decisions ORDER BY t_ns")
    assert [d["action"] for d in decs] == [d["action"] for d in e.results[e.jev_name].decisions]
    assert {d["policy"] for d in decs} == {e.jev_name}
    summ = {r["policy"]: r for r in rows(db, "SELECT * FROM paper_summary")}
    assert set(summ) == set(e.results)
    for name, r in e.results.items():
        assert summ[name]["pnl"] == pytest.approx(sum(t.pnl for t in r.trades))
        assert summ[name]["wins"] == sum(1 for t in r.trades if t.pnl > 0)


# ---------------------------------------------------------------- amain: exit-at-close, holidays, crashes

def test_exit_at_close_returns_0_and_records_a_done_session(sandbox, db):
    code = asyncio.run(live.amain(["--from-file", DAY, "--speed", "0", "--no-jev", "--exit-at-close",
                                   "--no-open", "--port", "0", "--paper-db"]))
    assert code == 0
    s = rows(db, "SELECT * FROM paper_sessions")
    assert len(s) == 1 and s[0]["status"] == "done" and s[0]["session_id"] == f"rehearsal-{DAY}"
    assert s[0]["run_dir"] == f"runs/rehearsal-{DAY}" and s[0]["ended_at"]
    summary = json.loads((sandbox / "runs" / f"rehearsal-{DAY}" / "summary.json").read_text())
    got = {r["policy"]: r for r in rows(db, "SELECT * FROM paper_summary")}
    assert set(got) == {"hold", "random", "imbalance"}
    for name, m in summary.items():
        assert got[name]["pnl"] == pytest.approx(m["all"]["net_pnl"])
        assert got[name]["trades"] == m["all"]["trades"]
        assert len(rows(db, "SELECT id FROM paper_trades WHERE policy=?", name)) == m["all"]["trades"]


def test_rehearsals_stay_out_of_the_paper_db_by_default(sandbox, db):
    assert asyncio.run(live.amain(["--from-file", DAY, "--speed", "0", "--no-jev", "--exit-at-close",
                                   "--no-open", "--port", "0"])) == 0
    assert not db.exists()


def test_crash_marks_the_session_crashed_with_the_error(sandbox, db, monkeypatch):
    class Broken:
        live, speed = False, 0

        def __init__(self, path, speed):
            self.b = synthetic()

        async def batches(self):
            yield self.b[:len(self.b) // 2]
            raise RuntimeError("feed broke")

    monkeypatch.setattr(live, "ParquetSource", Broken)
    with pytest.raises(RuntimeError, match="feed broke"):
        asyncio.run(live.amain(["--from-file", DAY, "--no-jev", "--exit-at-close", "--no-open", "--port", "0",
                                "--paper-db"]))
    s = rows(db, "SELECT * FROM paper_sessions")[0]
    assert s["status"] == "crashed" and "RuntimeError: feed broke" in s["error"] and s["ended_at"]
    assert {r["policy"] for r in rows(db, "SELECT policy FROM paper_summary")} == {"hold", "random", "imbalance"}


@pytest.mark.parametrize("now, reason", [(datetime(2026, 11, 7, 9, 15, tzinfo=ET), "weekend"),
                                         (datetime(2026, 12, 25, 9, 15, tzinfo=ET), "Christmas Day")])
def test_no_session_day_exits_0_with_a_holiday_row(sandbox, db, monkeypatch, now, reason):
    monkeypatch.setattr(live, "now_et", lambda: now)
    monkeypatch.delenv("DATABENTO_API_KEY", raising=False)  # never reaches the feed
    assert asyncio.run(live.amain(["--exit-at-close", "--no-open"])) == 0
    s = rows(db, "SELECT * FROM paper_sessions")
    day = now.date().isoformat()
    assert [(r["session_id"], r["day"], r["status"], r["error"]) for r in s] == [(f"live-{day}", day, "holiday", reason)]
    assert asyncio.run(live.amain(["--exit-at-close", "--no-open"])) == 0  # idempotent
    assert len(rows(db, "SELECT * FROM paper_sessions")) == 1
    assert not (sandbox / "runs").exists()


def test_after_the_session_exit_at_close_is_a_clean_no_op(sandbox, db, monkeypatch):
    monkeypatch.setattr(live, "now_et", lambda: datetime(2026, 11, 2, 17, 0, tzinfo=ET))
    assert asyncio.run(live.amain(["--exit-at-close", "--no-open"])) == 0
    assert not db.exists()
    with pytest.raises(SystemExit) as e:  # interactive: still an error
        asyncio.run(live.amain(["--no-open"]))
    assert e.value.code == 2


def test_holiday_row_never_overwrites_a_real_session(db):
    rec = paperdb.Recorder("live-2026-11-02")
    rec.start("2026-11-02", "NQZ6", {}, None, None)
    rec.close()
    paperdb.record_holiday("live-2026-11-02", "2026-11-02", "NQZ6", "weekend")
    assert rows(db, "SELECT status FROM paper_sessions")[0]["status"] == "running"


# ---------------------------------------------------------------- viewer routes

@pytest.fixture
def server():
    from trade_jev.view import Handler
    srv = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    srv.daemon_threads = True
    threading.Thread(target=srv.serve_forever, daemon=True).start()
    yield f"http://127.0.0.1:{srv.server_address[1]}"
    srv.shutdown()
    srv.server_close()


def get(url):
    with urllib.request.urlopen(url, timeout=5) as r:
        return r.status, r.headers.get("Content-Type"), r.read()


def test_paper_routes_without_a_db_or_a_run(server, db):
    status, ctype, body = get(server + "/paper")
    assert status == 200 and ctype.startswith("text/html") and b"Jev Paper" in body and b"/paper.js" in body
    assert get(server + "/paper.js")[1].startswith("text/javascript")
    data = json.loads(get(server + "/api/paper/sessions")[2])
    assert data == {"db": str(db), "exists": False, "sessions": []}
    with pytest.raises(urllib.error.HTTPError) as e:
        get(server + "/api/paper/session?id=nope")
    assert e.value.code == 404
    assert not db.exists()  # read-only: the viewer never creates the DB


def test_paper_routes_list_and_drill_down(server, db):
    rec = paperdb.Recorder("live-2026-11-02")
    rec.start("2026-11-02", "NQZ6", {"min_conf": 0.7}, "jev-latest", "runs/live-2026-11-02")
    rec.trade("jev[raw_l10]", Trade("2026-11-02", 1, 10, 100.0, 20, 101.0, "target", 1.0, 15.0))
    rec.decision("jev[raw_l10]", {"t_ns": 10, "time_et": "09:30:00", "mid": 100.0, "position": 0,
                                  "action": "HOLD", "raw_action": "BUY", "probs": {"BUY": 0.6}})
    rec.close()
    paperdb.record_holiday("live-2026-11-07", "2026-11-07", "NQZ6", "weekend")
    data = json.loads(get(server + "/api/paper/sessions")[2])
    assert [s["session_id"] for s in data["sessions"]] == ["live-2026-11-07", "live-2026-11-02"]
    running = data["sessions"][1]
    assert running["status"] == "running" and running["policies"]["jev[raw_l10]"]["pnl"] == 15.0
    s = json.loads(get(server + "/api/paper/session?id=live-2026-11-02")[2])
    assert s["settings"] == {"min_conf": 0.7} and len(s["trades"]) == 1
    assert s["decisions"][0]["raw_action"] == "BUY" and s["decisions"][0]["probs"] == {"BUY": 0.6}


def test_writer_survives_a_bad_statement(db):
    rec = paperdb.Recorder("live-2026-11-04")
    rec._put("INSERT INTO no_such_table VALUES (1)")
    rec.trade("hold", Trade("2026-11-04", 1, 10, 100.0, 20, 101.0, "eod", 1.0, 15.0))
    rec.close()
    assert rec.errors == 1
    assert len(rows(db, "SELECT * FROM paper_trades")) == 1


def test_sigterm_aborts_with_a_summary_and_exits_0(sandbox, db):
    import os
    import signal
    threading.Timer(1.0, os.kill, (os.getpid(), signal.SIGTERM)).start()
    code = asyncio.run(live.amain(["--from-file", DAY, "--speed", "1", "--no-jev", "--exit-at-close",
                                   "--no-open", "--port", "0", "--paper-db"]))
    assert code == 0
    s = rows(db, "SELECT * FROM paper_sessions")[0]
    assert s["status"] == "aborted" and s["ended_at"]
    assert {r["policy"] for r in rows(db, "SELECT policy FROM paper_summary")} == {"hold", "random", "imbalance"}
