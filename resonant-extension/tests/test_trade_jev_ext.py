"""Tests for the trade-jev Resonant extension (resonant-extension/cli.py): every tool, both
jobs' report logic, and the CLI contract (one JSON object, exit codes, python -I -S)."""

from __future__ import annotations

import json
import os
import shutil
import subprocess
import sys
from datetime import datetime, timedelta
from pathlib import Path

import cli
import pytest
from conftest import EXT_DIR, PaperDB, et, sdk_manifest, utc_iso, write_token_status

MON = "2026-11-02"  # a Monday
TUE = "2026-11-03"
WED = "2026-11-04"
THANKSGIVING = "2026-11-26"


def seed_day(db: PaperDB, sid: str, day: str, status: str = "done", jev: float = 420.0) -> None:
    """A finished day: Jev 6 trades (4 winners) totalling ``jev``; hold −110; random +30."""
    db.session(sid, day, status)
    per = [jev / 2, jev / 2, 100.0, 50.0, -50.0, -100.0]
    for i, pnl in enumerate(per):
        db.trade(sid, "Jev+filters", day, f"10:{i:02d}", f"10:{i:02d}", pnl)
    db.trade(sid, "hold", day, "09:30", "16:00", -110.0, reason="eod")
    db.trade(sid, "random", day, "11:00", "11:30", 30.0)
    db.summary(sid, "Jev+filters", 6, 4, jev, -180.0, calls=300, cost=1.25)
    db.summary(sid, "hold", 1, 0, -110.0, -400.0)
    db.summary(sid, "random", 1, 1, 30.0, -60.0)


# --- helpers ------------------------------------------------------------------------------


def test_is_jev_is_the_one_rule() -> None:
    assert cli.is_jev("Jev+filters") and cli.is_jev("jev") and cli.is_jev("JEV_raw")
    assert not any(cli.is_jev(p) for p in ("hold", "random", "imbalance"))
    assert cli.pick_jev(["hold", "jev_raw", "Jev+filters"]) == "Jev+filters"
    assert cli.pick_jev(["hold"]) is None


def test_money_uses_a_real_minus() -> None:
    assert cli.money(420.4) == "+$420"
    assert cli.money(-110) == "−$110"
    assert cli.money(1234.6) == "+$1,235"


def test_config_override_and_bad_config(data_dir: Path) -> None:
    (data_dir / "config.json").write_text(json.dumps({"db_path": "/x/y.db", "holidays": ["2026-11-27"]}))
    cfg = cli.load_config()
    assert cfg.db_path == Path("/x/y.db") and "2026-11-27" in cfg.holidays
    (data_dir / "config.json").write_text("{not json")
    with pytest.raises(cli.Fail):
        cli.load_config()
    (data_dir / "config.json").write_text(json.dumps({"launchd_label": "x; rm -rf /"}))
    with pytest.raises(cli.Fail):
        cli.load_config()


def test_default_db_path_without_config(data_dir: Path) -> None:
    (data_dir / "config.json").unlink()
    assert cli.load_config().db_path == Path("/Users/justinhe/trade-jev/data/paper.db")


# --- missing DB ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "argv", [["status"], ["pnl", "--date", ""], ["trades", "--date", "", "--policy", "", "--limit", "0"]]
)
def test_missing_db_is_a_clear_result_not_an_error(data_dir: Path, argv: list[str]) -> None:
    out = cli.run(argv, now=et(MON, "10:00"))
    assert out["summary"] == "no paper sessions yet"
    assert out["sessions"] == 0


def test_db_without_tables_counts_as_missing(tmp_path: Path, data_dir: Path) -> None:
    (tmp_path / "paper.db").write_bytes(b"")
    assert cli.run(["pnl", "--date", ""], now=et(MON, "10:00"))["summary"] == "no paper sessions yet"


def test_db_is_opened_read_only(db: PaperDB) -> None:
    seed_day(db, "s1", MON)
    conn = cli.connect(cli.load_config())
    assert conn is not None
    with pytest.raises(Exception, match="readonly"):
        conn.execute("DELETE FROM paper_sessions")
    conn.close()


# --- status -------------------------------------------------------------------------------


def test_status_running_with_position_and_pnl(db: PaperDB, launchd) -> None:
    now = et(MON, "11:00")
    db.session("s1", MON, "running", heartbeat=now - timedelta(seconds=30))
    db.trade("s1", "Jev+filters", MON, "09:40", "09:50", 125.0)
    db.trade("s1", "Jev+filters", MON, "10:00", "10:20", -25.0)
    db.trade("s1", "Jev+filters", MON, "10:30", None, 0.0)  # still open: not counted
    db.trade("s1", "hold", MON, "09:30", None, 0.0)
    db.decision("s1", "Jev+filters", MON, "10:59", 1)
    db.decision("s1", "Jev+filters", MON, "10:58", -1)
    out = cli.run(["status"], now=now)
    assert out["running"] is True
    assert out["session"]["health"] == "running"
    assert out["session"]["heartbeat_age_s"] == 30
    assert out["jev"] == {"policy": "Jev+filters", "position": 1, "pnl": 100.0, "trades": 2, "wins": 1}
    assert out["launchd"] == {"loaded": True, "label": "com.tradejev.paper", "state": "running", "pid": 4242}
    assert launchd.calls == [("print", f"gui/{os.getuid()}/com.tradejev.paper")]
    assert "running" in out["summary"] and "+$100" in out["summary"]


def test_status_stale_when_heartbeat_old(db: PaperDB) -> None:
    now = et(MON, "11:00")
    db.session("s1", MON, "running", heartbeat=now - timedelta(minutes=4))
    out = cli.run(["status"], now=now)
    assert out["running"] is False
    assert out["session"]["health"] == "stale"
    assert "stale/likely dead" in out["summary"]


def test_status_crashed_carries_the_error(db: PaperDB) -> None:
    db.session("s1", MON, "crashed", error="Traceback ... ConnectionResetError")
    out = cli.run(["status"], now=et(MON, "11:00"))
    assert out["session"]["health"] == "crashed"
    assert "ConnectionResetError" in out["session"]["error"]


def test_status_uses_latest_session_of_the_day(db: PaperDB) -> None:
    now = et(MON, "11:00")
    db.session("s1", MON, "crashed", started="09:25")
    db.session("s2", MON, "running", started="10:05", heartbeat=now)
    assert cli.run(["status"], now=now)["session"]["id"] == "s2"


def test_status_no_session_today(db: PaperDB) -> None:
    seed_day(db, "s1", MON)
    out = cli.run(["status"], now=et(TUE, "09:00"))
    assert out["session"] is None and out["last_session_day"] == MON


@pytest.mark.parametrize(
    ("loaded", "raise_os", "expect"),
    [(False, False, {"loaded": False, "label": "com.tradejev.paper"}),
     (True, True, {"loaded": None, "label": "com.tradejev.paper"})],
)  # fmt: skip
def test_status_launchd_best_effort(db: PaperDB, launchd, loaded, raise_os, expect) -> None:
    launchd.loaded, launchd.raise_os = loaded, raise_os
    db.session("s1", MON, "done")
    assert cli.run(["status"], now=et(MON, "17:00"))["launchd"] == expect


# --- pnl ----------------------------------------------------------------------------------


def test_pnl_latest_session_vs_baselines(db: PaperDB) -> None:
    seed_day(db, "s1", MON)
    seed_day(db, "s2", TUE, jev=-200.0)
    db.session("h", WED, "holiday")
    out = cli.run(["pnl", "--date", ""], now=et(WED, "12:00"))
    assert out["from"] == out["to"] == TUE  # holiday rows are skipped
    jev = out["policies"][0]
    assert jev["policy"] == "Jev+filters" and jev["jev"] is True
    assert jev["pnl"] == -200.0 and jev["trades"] == 6 and jev["wins"] == 2
    assert jev["max_drawdown"] == -180.0 and jev["jev_calls"] == 300 and jev["jev_cost_usd"] == 1.25
    assert out["jev_minus_baseline"] == {"hold": -90.0, "random": -230.0}
    assert out["summary"] == "Paper NQ Tue 11/3: Jev −$200 (6 trades), hold −$110, random +$30"


def test_pnl_specific_date_and_empty_date(db: PaperDB) -> None:
    seed_day(db, "s1", MON)
    out = cli.run(["pnl", "--date", MON], now=et(WED, "12:00"))
    assert out["policies"][0]["pnl"] == 420.0
    out = cli.run(["pnl", "--date", WED], now=et(WED, "12:00"))
    assert out["sessions"] == 0 and "no paper sessions" in out["summary"]


def test_pnl_week_totals_and_daily(db: PaperDB) -> None:
    seed_day(db, "s1", MON, jev=420.0)
    seed_day(db, "s2", TUE, jev=-200.0)
    seed_day(db, "old", "2026-10-30", jev=999.0)  # last week: excluded
    out = cli.run(["pnl", "--date", "week"], now=et(WED, "12:00"))
    assert out["trading_days"] == 2 and out["from"] == MON and out["to"] == TUE
    jev = out["policies"][0]
    assert jev["pnl"] == 220.0 and jev["trades"] == 12 and jev["jev_cost_usd"] == 2.5
    assert [d["jev_pnl"] for d in out["daily"]] == [420.0, -200.0]
    assert out["summary"].startswith("Paper NQ (2 days): Jev +$220 (12 trades), hold −$220")


def test_pnl_month_and_range(db: PaperDB) -> None:
    seed_day(db, "s1", MON)
    seed_day(db, "old", "2026-10-30", jev=100.0)
    month = cli.run(["pnl", "--date", "month"], now=et(WED, "12:00"))
    assert month["trading_days"] == 1
    rng = cli.run(["pnl", "--date", "2026-10-01..2026-11-30"], now=et(WED, "12:00"))
    assert rng["trading_days"] == 2 and rng["policies"][0]["pnl"] == 520.0


def test_pnl_bad_date_fails_cleanly(db: PaperDB, capsys) -> None:
    seed_day(db, "s1", MON)
    assert cli.main(["pnl", "--date", "tomorrowish"]) == 1
    assert "bad date" in capsys.readouterr().err


def test_pnl_falls_back_to_summary_when_no_trade_rows(db: PaperDB) -> None:
    db.session("s1", MON, "done")
    db.summary("s1", "jev", 3, 2, 75.0, -20.0)
    out = cli.run(["pnl", "--date", MON], now=et(MON, "17:00"))
    assert out["policies"][0]["pnl"] == 75.0 and out["policies"][0]["trades"] == 3


def test_pnl_month_result_fits_4kb(db: PaperDB) -> None:
    for i in range(1, 31):
        day = f"2026-11-{i:02d}"
        seed_day(db, f"s{i}", day)
    out = cli.run(["pnl", "--date", "month"], now=et("2026-11-30", "17:00"))
    assert len(json.dumps(out)) <= 4096


# --- trades -------------------------------------------------------------------------------


def test_trades_default_jev_latest(db: PaperDB) -> None:
    seed_day(db, "s1", MON)
    out = cli.run(["trades", "--date", "", "--policy", "", "--limit", "0"], now=et(MON, "17:00"))
    assert out["policy"] == "Jev+filters" and out["total"] == 6 and out["shown"] == 6
    first = out["trades"][0]
    assert first["in"] == "10:00:00" and first["side"] == "long" and first["pnl"] == 210.0
    assert out["trades"][-1]["in"] == "10:05:00"


def test_trades_policy_and_limit(db: PaperDB) -> None:
    seed_day(db, "s1", MON)
    out = cli.run(["trades", "--date", MON, "--policy", "HOLD", "--limit", "5"], now=et(MON, "17:00"))
    assert out["policy"] == "hold" and out["trades"][0]["why"] == "eod"
    out = cli.run(["trades", "--date", MON, "--policy", "jev", "--limit", "2"], now=et(MON, "17:00"))
    assert [t["in"] for t in out["trades"]] == ["10:04:00", "10:05:00"]  # the latest two
    out = cli.run(["trades", "--date", MON, "--policy", "nope", "--limit", "2"], now=et(MON, "17:00"))
    assert out["trades"] == [] and "hold" in out["policies"]


def test_trades_capped_and_fit(db: PaperDB) -> None:
    db.session("s1", MON, "done")
    for i in range(200):
        db.trade("s1", "jev", MON, f"{10 + i // 60:02d}:{i % 60:02d}", f"{10 + i // 60:02d}:{i % 60:02d}", 1.0)
    out = cli.run(["trades", "--date", MON, "--policy", "", "--limit", "500"], now=et(MON, "17:00"))
    assert out["total"] == 200 and out["shown"] <= 50
    assert len(json.dumps(out)) <= 4096


def test_trades_week_adds_day(db: PaperDB) -> None:
    seed_day(db, "s1", MON)
    seed_day(db, "s2", TUE)
    out = cli.run(["trades", "--date", "week", "--policy", "", "--limit", "3"], now=et(WED, "12:00"))
    assert all(t["day"] == TUE for t in out["trades"]) and out["total"] == 12


# --- start / stop -------------------------------------------------------------------------


def svc() -> str:
    return f"gui/{os.getuid()}/com.tradejev.paper"


def test_start_kickstarts(db: PaperDB, launchd) -> None:
    launchd.state = "not running"
    out = cli.run(["start"], now=et(MON, "09:20"))
    assert out["started"] is True
    assert launchd.calls[-1] == ("kickstart", svc())


def test_start_already_running_does_nothing(db: PaperDB, launchd) -> None:
    out = cli.run(["start"], now=et(MON, "10:00"))
    assert out["started"] is False and all(c[0] == "print" for c in launchd.calls)


def test_start_not_loaded_fails_without_acting(data_dir: Path, launchd, capsys) -> None:
    launchd.loaded = False
    assert cli.main(["start"]) == 1
    assert "isn't loaded" in capsys.readouterr().err
    assert all(c[0] == "print" for c in launchd.calls)


def test_start_kickstart_failure_exits_1(data_dir: Path, launchd) -> None:
    launchd.state, launchd.fail = "not running", {"kickstart": 5}
    assert cli.main(["start"]) == 1


def test_stop_sends_sigterm(data_dir: Path, launchd) -> None:
    out = cli.run(["stop"], now=et(MON, "12:00"))
    assert out["stopped"] is True
    assert launchd.calls[-1] == ("kill", "SIGTERM", svc())


@pytest.mark.parametrize(("loaded", "state"), [(False, "running"), (True, "not running")])
def test_stop_nothing_to_stop(data_dir: Path, launchd, loaded, state) -> None:
    launchd.loaded, launchd.state = loaded, state
    out = cli.run(["stop"], now=et(MON, "12:00"))
    assert out["stopped"] is False and all(c[0] == "print" for c in launchd.calls)


def test_stop_failure_exits_1(data_dir: Path, launchd) -> None:
    launchd.fail = {"kill": 3}
    assert cli.main(["stop"]) == 1


def test_launchd_label_from_config(data_dir: Path, launchd) -> None:
    (data_dir / "config.json").write_text(json.dumps({"launchd_label": "com.example.paper"}))
    cli.run(["stop"], now=et(MON, "12:00"))
    assert launchd.calls[-1] == ("kill", "SIGTERM", f"gui/{os.getuid()}/com.example.paper")


# --- health -------------------------------------------------------------------------------


def test_health(data_dir: Path, db: PaperDB) -> None:
    assert cli.run(["health"], now=et(MON, "17:00"))["db"] == "ok"


def test_health_without_db(data_dir: Path) -> None:
    assert cli.run(["health"], now=et(MON, "17:00")) == {
        "ok": True, "db": "absent", "summary": "ok: no paper sessions yet"}  # fmt: skip


# --- close-summary job --------------------------------------------------------------------


def test_close_summary_reports_the_day(db: PaperDB, reports) -> None:
    seed_day(db, "s1", MON)
    out = cli.run(["close-summary"], now=et(MON, "16:10"))
    assert out["reported"] is True
    (r,) = reports
    assert r["key"] == f"trade-jev/pnl/{MON}"
    assert r["title"] == "Paper NQ Mon 11/2: Jev +$420 (6 trades), hold −$110, random +$30"
    assert r["urgency"] == "normal"
    assert r["state"] == {"day": MON, "status": "done", "jev_bucket": 400, "jev_trades": 6}


def test_close_summary_state_is_coarse(db: PaperDB, reports) -> None:
    seed_day(db, "s1", MON, jev=410.0)
    cli.run(["close-summary"], now=et(MON, "16:10"))
    db.trade("s1", "hold", MON, "15:00", "15:01", 5.0)  # a baseline revision: same state
    cli.run(["close-summary"], now=et(MON, "16:40"))
    assert reports[0]["state"] == reports[1]["state"]


def test_close_summary_flags_a_crashed_day(db: PaperDB, reports) -> None:
    seed_day(db, "s1", MON, status="crashed")
    cli.run(["close-summary"], now=et(MON, "16:10"))
    assert reports[0]["title"].startswith("Paper NQ Mon 11/2: crashed; Jev +$420 (6 trades)")
    assert reports[0]["state"]["status"] == "crashed"


@pytest.mark.parametrize("setup", ["none", "holiday", "nodb"])
def test_close_summary_silent_without_session(tmp_path, data_dir, reports, setup) -> None:
    if setup != "nodb":
        db = PaperDB(tmp_path / "paper.db")
        seed_day(db, "old", "2026-11-25")
        if setup == "holiday":
            db.session("h", THANKSGIVING, "holiday")
    out = cli.run(["close-summary"], now=et(THANKSGIVING, "16:10"))
    assert out["reported"] is False and reports == []


def test_close_summary_uses_due_time_on_catchup(db: PaperDB, reports, monkeypatch) -> None:
    seed_day(db, "s1", MON)
    monkeypatch.setenv("RESONANT_JOB_DUE_AT", et(MON, "16:10").isoformat())
    cli.run(["close-summary"])  # real now is later; the due day wins
    assert reports[0]["key"] == f"trade-jev/pnl/{MON}"


# --- watch job ----------------------------------------------------------------------------


def test_watch_quiet_when_healthy(db: PaperDB, reports) -> None:
    now = et(MON, "11:00")
    db.session("s1", MON, "running", heartbeat=now - timedelta(seconds=20))
    assert cli.run(["watch"], now=now)["down"] is False
    assert reports == []  # no clear without a prior down


def test_watch_crashed_is_urgent_then_clears(db: PaperDB, reports, data_dir: Path) -> None:
    db.session("s1", MON, "crashed", error="boom")
    out = cli.run(["watch"], now=et(MON, "11:00"))
    assert out["down"] is True
    r = reports[-1]
    assert r["key"] == "trade-jev/down" and r["urgency"] == "urgent"
    assert r["state"] == {"day": MON, "reason": "session crashed"}
    assert "crashed: boom" in r["title"]
    # repeat runs send the same state (Resonant treats them as not news)
    cli.run(["watch"], now=et(MON, "11:15"))
    assert reports[-1]["state"] == r["state"] and not reports[-1].get("clear")
    # restarted and healthy: one clear
    now = et(MON, "11:30")
    db.session("s2", MON, "running", started="11:20", heartbeat=now)
    out = cli.run(["watch"], now=now)
    assert out["cleared"] is True and reports[-1]["clear"] is True
    n = len(reports)
    db.conn.execute("UPDATE paper_sessions SET heartbeat_at=? WHERE session_id='s2'", (et(MON, "11:45").isoformat(),))
    db.conn.commit()
    cli.run(["watch"], now=et(MON, "11:45"))
    assert len(reports) == n  # healthy again: nothing more


def test_watch_stale(db: PaperDB, reports) -> None:
    now = et(MON, "13:00")
    db.session("s1", MON, "running", heartbeat=now - timedelta(minutes=10))
    cli.run(["watch"], now=now)
    assert reports[-1]["state"]["reason"] == "session stale"
    assert "10 min" in reports[-1]["title"]


def test_watch_missing_session_after_0935(db: PaperDB, reports) -> None:
    seed_day(db, "old", "2026-10-30")
    assert cli.run(["watch"], now=et(MON, "09:30"))["down"] is False  # grace until 09:35
    cli.run(["watch"], now=et(MON, "09:45"))
    assert reports[-1]["state"] == {"day": MON, "reason": "no session started"}


def test_watch_missing_db_counts_as_missing_session(data_dir: Path, reports) -> None:
    cli.run(["watch"], now=et(MON, "09:45"))
    assert reports[-1]["key"] == "trade-jev/down"


@pytest.mark.parametrize(
    "when", [et(MON, "09:15"), et(MON, "16:15"), et("2026-11-07", "11:00"), et(THANKSGIVING, "11:00")]
)
def test_watch_outside_hours_weekends_and_holidays(data_dir: Path, reports, when) -> None:
    assert cli.run(["watch"], now=when)["checked"] is False and reports == []


@pytest.mark.parametrize("status", ["holiday", "done", "aborted"])
def test_watch_not_down_for_holiday_done_or_stopped(db: PaperDB, reports, status) -> None:
    db.session("s1", MON, status)
    assert cli.run(["watch"], now=et(MON, "12:00"))["down"] is False and reports == []


def test_watch_config_holiday(db: PaperDB, reports, data_dir: Path) -> None:
    cfg = json.loads((data_dir / "config.json").read_text())
    (data_dir / "config.json").write_text(json.dumps({**cfg, "holidays": [MON]}))
    assert cli.run(["watch"], now=et(MON, "12:00"))["checked"] is False


# --- skipped days (feed off / credentials missing) and the source column ------------------------


def test_watch_skipped_is_healthy_and_counts_as_the_runner_having_run(db: PaperDB, reports) -> None:
    db.session("s1", MON, "skipped", error="TRADE_JEV_SOURCE=off")
    out = cli.run(["watch"], now=et(MON, "12:00"))
    assert out["down"] is False and reports == []
    # skipped before 09:35 also means "no session started" cannot fire: the runner wrote a row
    assert cli.run(["watch"], now=et(MON, "09:35"))["down"] is False and reports == []


def test_status_shows_skipped_reason_and_the_source(db: PaperDB) -> None:
    db.session("s1", MON, "skipped", error="tradovate credentials missing: TRADOVATE_SEC")
    db.set_source("s1", "tradovate")
    out = cli.run(["status"], now=et(MON, "11:00"))
    assert out["running"] is False and out["session"]["source"] == "tradovate"
    assert out["summary"].startswith("skipped: tradovate credentials missing: TRADOVATE_SEC")
    assert "tradovate" in out["summary"]


def test_status_shows_the_source_of_a_real_session_and_tolerates_old_dbs(db: PaperDB) -> None:
    db.session("s1", MON, "done")
    assert cli.run(["status"], now=et(MON, "17:00"))["session"]["source"] is None  # no column yet
    db.set_source("s1", "databento")
    out = cli.run(["status"], now=et(MON, "17:00"))
    assert out["session"]["source"] == "databento" and "[databento]" in out["summary"]


def test_pnl_for_a_skipped_day_says_so_and_excludes_it(db: PaperDB) -> None:
    seed_day(db, "s1", MON)
    db.session("s2", TUE, "skipped", error="TRADE_JEV_SOURCE=off")
    out = cli.run(["pnl", "--date", TUE], now=et(TUE, "17:00"))
    assert out["sessions"] == 0 and out["summary"] == "skipped: TRADE_JEV_SOURCE=off"
    week = cli.run(["pnl", "--date", f"{MON}..{TUE}"], now=et(TUE, "17:00"))
    assert week["sessions"] == 1 and week["trading_days"] == 1
    assert cli.run(["status"], now=et(WED, "09:00"))["last_session_day"] == MON  # skipped is not a trading day
    assert cli.run(["pnl"], now=et(WED, "09:00"))["to"] == MON


def test_close_summary_and_trades_ignore_skipped(db: PaperDB, reports) -> None:
    db.session("s1", TUE, "skipped", error="TRADE_JEV_SOURCE=off")
    assert cli.run(["close-summary"], now=et(TUE, "16:10"))["reported"] is False and reports == []
    assert cli.run(["trades", "--date", TUE], now=et(TUE, "17:00"))["trades"] == []


# --- the CLI contract -----------------------------------------------------------------------


def run_isolated(data_dir: Path, *argv: str) -> subprocess.CompletedProcess[str]:
    """As Resonant runs it: ``python -I -S``, a scratch env, PATH=/usr/bin:/bin."""
    env = {"PATH": "/usr/bin:/bin", "HOME": str(data_dir), "RESONANT_EXT_DATA": str(data_dir),
           "LANG": "en_US.UTF-8"}  # fmt: skip
    return subprocess.run(
        [sys.executable, "-I", "-S", str(EXT_DIR / "cli.py"), *argv],
        capture_output=True, text=True, env=env, cwd=EXT_DIR, timeout=30, check=False,
    )  # fmt: skip


def test_cli_prints_one_json_object_under_isolation(db: PaperDB, data_dir: Path) -> None:
    seed_day(db, "s1", MON)
    # health runs on the real clock here: make today a holiday so its verdict doesn't depend on the hour
    cfg = json.loads((data_dir / "config.json").read_text())
    today = datetime.now(cli.ET).date().isoformat()
    (data_dir / "config.json").write_text(json.dumps({**cfg, "holidays": [today]}))
    for argv in (["pnl", "--date", MON], ["trades", "--date", MON, "--policy", "", "--limit", "0"], ["health"]):
        proc = run_isolated(data_dir, *argv)
        assert proc.returncode == 0, proc.stderr
        assert isinstance(json.loads(proc.stdout), dict)
        assert proc.stdout.count("\n") == 1


def test_cli_failure_is_exit_1_with_one_stderr_line(data_dir: Path) -> None:
    (data_dir / "config.json").write_text("nope")
    proc = run_isolated(data_dir, "health")
    assert proc.returncode == 1 and proc.stdout == ""
    assert proc.stderr.strip().count("\n") == 0


def test_job_report_goes_to_fd3_under_resonant(db: PaperDB, data_dir: Path) -> None:
    """End to end: close-summary writes one `item` event on RESONANT_EVENTS_FD."""
    seed_day(db, "s1", MON)
    r, w = os.pipe()
    env = {"PATH": "/usr/bin:/bin", "HOME": str(data_dir), "RESONANT_EXT_DATA": str(data_dir),
           "RESONANT_EVENTS_FD": str(w), "RESONANT_JOB_DUE_AT": et(MON, "16:10").isoformat()}  # fmt: skip
    proc = subprocess.run(
        [sys.executable, "-I", "-S", str(EXT_DIR / "cli.py"), "close-summary"],
        capture_output=True, text=True, env=env, pass_fds=(w,), timeout=30, check=False,
    )  # fmt: skip
    os.close(w)
    events = os.read(r, 65536).decode().splitlines()
    os.close(r)
    assert proc.returncode == 0, proc.stderr
    (ev,) = [json.loads(e) for e in events]
    assert ev["type"] == "item" and ev["data"]["key"] == f"trade-jev/pnl/{MON}"
    assert ev["data"]["urgency"] == "normal"


def test_manifest_tool_names_match_the_evals() -> None:
    text = (EXT_DIR / "resonant.yaml").read_text()
    for name in ("trade_jev_status", "trade_jev_start", "trade_jev_stop", "trade_jev_pnl", "trade_jev_trades"):
        assert f"  {name}:\n" in text
    assert "max_urgency: urgent" in text


def test_no_dependency_files_in_the_extension() -> None:
    banned = {"pyproject.toml", "uv.lock", "package.json", "setup.py", "setup.cfg", "poetry.lock"}
    found = {p.name for p in EXT_DIR.rglob("*")} & banned
    assert not found and not list(EXT_DIR.rglob("requirements*.txt"))


# --- review round 1: timeouts, the stop marker, real launchctl output ----------------------


def test_start_timeout_is_unknown_not_a_failure(data_dir: Path, launchd) -> None:
    launchd.state, launchd.timeout = "not running", {"kickstart"}
    out = cli.run(["start"], now=et(MON, "09:20"))
    assert out["started"] == "unknown"
    assert cli.main(["start"]) == 0  # exit 1 would promise nothing happened


def test_stop_timeout_is_unknown_and_marks_stopped(data_dir: Path, launchd) -> None:
    launchd.timeout = {"kill"}
    out = cli.run(["stop"], now=et(MON, "12:00"))
    assert out["stopped"] == "unknown"
    assert (data_dir / "stopped_day").read_text() == MON


def test_stop_then_watch_stays_quiet_until_start(db: PaperDB, reports, launchd) -> None:
    db.session("s1", MON, "running", heartbeat=et(MON, "12:00"))
    cli.run(["stop"], now=et(MON, "12:00"))
    # the runner never wrote `aborted`: the row is stale, but the owner stopped it
    assert cli.run(["watch"], now=et(MON, "12:35"))["down"] is False and reports == []
    # the next trading day the marker no longer applies
    assert cli.run(["watch"], now=et(TUE, "09:50"))["down"] is True
    launchd.state = "not running"
    cli.run(["start"], now=et(MON, "12:40"))
    assert not (db.path.parent / "ext-data" / "stopped_day").exists()


def test_failed_stop_leaves_no_marker(data_dir: Path, launchd) -> None:
    launchd.fail = {"kill": 3}
    assert cli.main(["stop"]) == 1
    assert not (data_dir / "stopped_day").exists()


def test_launchd_state_parses_real_print_output(monkeypatch: pytest.MonkeyPatch) -> None:
    """The real wrapper and parser, with subprocess.run faked: nested blocks repeat `state`
    and `pid`-like keys, and only the service's own (first) values count."""
    real = (
        "gui/501/com.tradejev.paper = {\n"
        "\tactive count = 1\n"
        "\tpath = /Users/justinhe/Library/LaunchAgents/com.tradejev.paper.plist\n"
        "\ttype = LaunchAgent\n"
        "\tstate = running\n\n"
        "\tprogram = /usr/bin/python3\n"
        "\tpid = 812\n"
        "\tendpoints = {\n\t}\n"
        "\tjob state = running\n"
        "\tlast exit code = (never exited)\n"
        "\tspawn type = daemon (3)\n"
        "\tsockets = {\n\t\tstate = active\n\t}\n"
        "}\n"
    )
    seen: list[list[str]] = []

    def fake_run(argv, **kw):
        seen.append(argv)
        assert kw["timeout"] == cli.LAUNCHCTL_TIMEOUT_S and kw["stdin"] is subprocess.DEVNULL
        return subprocess.CompletedProcess(argv, 0, real, "")

    monkeypatch.undo()  # drop the autouse fake launchctl: exercise the real wrapper
    monkeypatch.setattr(cli.subprocess, "run", fake_run)
    st = cli.launchd_state(cli.Config())
    assert st == {"loaded": True, "label": "com.tradejev.paper", "state": "running", "pid": 812}
    assert seen == [["/bin/launchctl", "print", f"gui/{os.getuid()}/com.tradejev.paper"]]


def test_unknown_policy_echo_is_clipped(db: PaperDB) -> None:
    seed_day(db, "s1", MON)
    out = cli.run(["trades", "--date", MON, "--policy", "x" * 10000, "--limit", "0"], now=et(MON, "17:00"))
    assert len(json.dumps(out)) <= 4096 and out["trades"] == []


def test_watch_schedule_catches_missing_at_0935() -> None:
    text = (EXT_DIR / "resonant.yaml").read_text()
    # every 15 min all day, every day: the token check has no hours; the session check is script-guarded
    assert '"5,20,35,50 * * * *"' in text and "9-15" not in text.split("id: watch")[1].split("report:")[0]


def test_stop_marker_write_failure_still_reports_success(data_dir: Path, launchd, monkeypatch) -> None:
    def boom(self, *a, **k):
        raise OSError("read-only")

    monkeypatch.setattr(Path, "write_text", boom)
    assert cli.run(["stop"], now=et(MON, "12:00"))["stopped"] is True


# --- Tradovate token: status block and watch alerts ------------------------------------------

SAT = "2026-11-07"  # a Saturday


def tok(path: Path, now, left_min: float | None, fails: int = 0, **extra) -> None:
    """A keep-alive status file whose token expires ``left_min`` minutes after ``now``."""
    f = {"last_ok_at": utc_iso(now), "consecutive_failures": fails, "last_error": None,
         "checked_at": utc_iso(now), **extra}
    if left_min is not None:
        f["expires_at"] = utc_iso(now + timedelta(minutes=left_min))
    write_token_status(path, **f)


def test_token_alert_fires_at_any_hour_and_clears(data_dir: Path, token_status: Path, reports) -> None:
    now = et(SAT, "03:05")  # Saturday 03:05 ET: outside market hours, the token check still runs
    tok(token_status, now, 5)
    out = cli.run(["watch"], now=now)
    assert out["checked"] is False and out["token"] == {"problem": "expiring"}
    [r] = reports
    assert r["key"] == "trade-jev/token" and r["urgency"] == "urgent" and not r.get("clear")
    assert r["title"] == (
        "Tradovate token expired — log into trader.tradovate.com and paste a fresh access token "
        "+ md token into ~/trade-jev/.env")
    later = et(SAT, "03:20")
    tok(token_status, later, 70)  # the owner pasted fresh tokens; the keep-alive renewed
    out = cli.run(["watch"], now=later)
    assert out["token"] == {"problem": None, "cleared": True}
    assert reports[-1]["key"] == "trade-jev/token" and reports[-1]["clear"] is True
    n = len(reports)
    cli.run(["watch"], now=et(SAT, "03:35"))
    assert len(reports) == n  # healthy and already cleared: silent


@pytest.mark.parametrize("hm", ["00:05", "06:50", "12:20", "23:50"])
def test_token_expired_alerts_at_every_hour(data_dir: Path, token_status: Path, reports, hm: str) -> None:
    now = et(MON, hm)
    tok(token_status, now, -30)
    assert cli.run(["watch"], now=now)["token"]["problem"] == "expired"
    assert [r["key"] for r in reports if r["key"] == cli.TOKEN_KEY] == [cli.TOKEN_KEY]


def test_token_failing_needs_two_failures(data_dir: Path, token_status: Path, reports) -> None:
    now = et(TUE, "02:05")
    tok(token_status, now, 40, fails=1, last_error="renew failed: HTTP 500")
    assert cli.run(["watch"], now=now)["token"] == {"problem": None} and reports == []
    tok(token_status, now, 40, fails=2, last_error="renew failed: HTTP 500")
    assert cli.run(["watch"], now=now)["token"] == {"problem": "renewal failing"}
    assert reports[0]["key"] == cli.TOKEN_KEY and reports[0]["urgency"] == "urgent"


def test_token_absent_or_healthy_is_silent(data_dir: Path, token_status: Path, reports) -> None:
    now = et(TUE, "02:05")
    assert not token_status.exists()
    assert cli.run(["watch"], now=now)["token"] == {"problem": None} and reports == []
    tok(token_status, now, 60)
    assert cli.run(["watch"], now=now)["token"] == {"problem": None} and reports == []


def test_token_status_unreadable_is_a_problem(data_dir: Path, token_status: Path, reports) -> None:
    token_status.write_text("{not json")
    assert cli.run(["watch"], now=et(TUE, "02:05"))["token"] == {"problem": "unreadable"}


def test_down_and_token_state_are_independent(db: PaperDB, data_dir: Path, token_status: Path, reports) -> None:
    db.session("s1", MON, "crashed", error="boom")
    now = et(MON, "11:00")
    tok(token_status, now, 3)
    out = cli.run(["watch"], now=now)
    assert out["down"] is True and out["token"]["problem"] == "expiring"
    state = json.loads((data_dir / "watch_state.json").read_text())
    assert set(state) >= {"down", "token"} and state["down"] and state["token"]
    # the token recovers while the session is still down: only the token clears
    later = et(MON, "11:15")
    tok(token_status, later, 70)
    out = cli.run(["watch"], now=later)
    assert out["down"] is True and out["token"]["cleared"] is True
    state = json.loads((data_dir / "watch_state.json").read_text())
    assert state["token"] is None and state["down"]
    clears = [r["key"] for r in reports if r.get("clear")]
    assert clears == [cli.TOKEN_KEY]
    # and the session recovering leaves the (already clear) token state alone
    db.conn.execute("UPDATE paper_sessions SET status='running', heartbeat_at=?",
                    (utc_iso(et(MON, "11:29")),))
    db.conn.commit()
    out = cli.run(["watch"], now=et(MON, "11:30"))
    assert out["down"] is False and out["cleared"] is True
    assert [r["key"] for r in reports if r.get("clear")] == [cli.TOKEN_KEY, cli.DOWN_KEY]


def test_status_has_a_token_block_in_every_branch(db: PaperDB, token_status: Path, data_dir: Path) -> None:
    now = et(MON, "10:00")
    tok(token_status, now, 62)
    out = cli.run(["status"], now=now)  # a DB with no session today
    assert out["token"]["ok"] is True and out["token"]["expires_in_min"] == 62 and "Tradovate token" not in out["summary"]
    db.session("s1", MON, "running", heartbeat=now)
    out = cli.run(["status"], now=now)
    assert out["token"]["expires_in_min"] == 62 and out["token"]["problem"] is None
    tok(token_status, now, 4, last_error="renew failed")
    out = cli.run(["status"], now=now)
    assert out["token"]["problem"] == "expiring" and out["summary"].endswith("| Tradovate token: expiring")
    assert "accessToken" not in json.dumps(out)


def test_status_token_block_without_a_db_or_a_status_file(data_dir: Path, token_status: Path) -> None:
    now = et(MON, "10:00")
    out = cli.run(["status"], now=now)
    assert out["token"]["known"] is False and out["summary"] == "no paper sessions yet"
    tok(token_status, now, -5)
    out = cli.run(["status"], now=now)
    assert out["token"]["problem"] == "expired" and "Tradovate token: expired" in out["summary"]


def test_token_status_path_config_key(data_dir: Path, tmp_path: Path) -> None:
    (data_dir / "config.json").write_text(json.dumps({"token_status_path": str(tmp_path / "x.json")}))
    assert cli.load_config().token_status_path == tmp_path / "x.json"
    (data_dir / "config.json").write_text(json.dumps({"token_status_path": ""}))
    with pytest.raises(cli.Fail):
        cli.load_config()
    (data_dir / "config.json").unlink()
    assert cli.load_config().token_status_path == Path(cli.DEFAULT_TOKEN_STATUS)


def test_real_default_token_status_path_is_the_deploy_checkout() -> None:
    # conftest patches cli.DEFAULT_TOKEN_STATUS; the source line is the real default
    text = (EXT_DIR / "cli.py").read_text()
    assert 'DEFAULT_TOKEN_STATUS = "/Users/justinhe/trade-jev/data/tradovate_token_status.json"' in text


def test_a_crashing_down_check_does_not_suppress_the_token_alert_and_vice_versa(
    data_dir: Path, token_status: Path, reports, monkeypatch: pytest.MonkeyPatch
) -> None:
    now = et(MON, "11:00")
    tok(token_status, now, 3)

    def boom(*a, **k):
        raise RuntimeError("db exploded")
    monkeypatch.setattr(cli, "_watch_down", boom)
    out = cli.run(["watch"], now=now)
    assert out["token"] == {"problem": "expiring"} and out["error"] == "RuntimeError"
    assert [r["key"] for r in reports] == [cli.TOKEN_KEY]
    monkeypatch.undo()
    monkeypatch.setattr(cli, "_watch_token", boom)
    out = cli.run(["watch"], now=now)
    assert out["token"] == {"error": "RuntimeError"} and out["down"] is not None


def test_a_vanished_status_file_sends_no_clear(data_dir: Path, token_status: Path, reports) -> None:
    now = et(SAT, "03:05")
    tok(token_status, now, 3)
    cli.run(["watch"], now=now)
    token_status.unlink()
    n = len(reports)
    cli.run(["watch"], now=et(SAT, "03:20"))
    assert len(reports) == n
    assert json.loads((data_dir / "watch_state.json").read_text())["token"]    # still remembered


def test_a_stopped_renewer_alerts_at_an_off_hour_then_clears(data_dir: Path, token_status: Path, reports) -> None:
    last = et(SAT, "02:00")
    tok(token_status, last, 60)                                                 # healthy token, but checked_at stops
    assert cli.run(["watch"], now=et(SAT, "02:15"))["token"] == {"problem": None}   # 15 min old: fine
    out = cli.run(["watch"], now=et(SAT, "02:35"))                              # 35 min old
    assert out["token"] == {"problem": "renewer stopped"}
    assert reports[-1]["key"] == cli.TOKEN_KEY and reports[-1]["title"] == cli.TOKEN_TITLE
    assert reports[-1]["state"] == {"reason": "renewer stopped"} and reports[-1]["urgency"] == "urgent"
    tok(token_status, et(SAT, "02:40"), 60)                                     # the renewer is back
    assert cli.run(["watch"], now=et(SAT, "02:50"))["token"]["cleared"] is True
    assert reports[-1]["clear"] is True


# --- live Jev visibility: a running session has no paper_summary yet ----------------------

JEV = "jev[raw_l10]"


def seed_running(
    db: PaperDB, day: str = TUE, *, model: str | None = "jev-latest", n: int = 12
) -> None:
    """A session mid-day as the runner leaves it: decisions logged live, no paper_summary rows.
    Decisions every 15 s from 10:00; latency 100 + 10·i ms; one error at #5; raw vs gated mix."""
    now = et(day, "10:05")
    db.session("live-" + day, day, "running", heartbeat=now, symbol="NQZ6", jev_model=model)
    for i in range(n):
        raw = ("BUY", "SELL", "HOLD")[i % 3]
        act = "BUY" if i == 9 else "HOLD"
        err = "TimeoutError: jev" if i == 5 else None
        db.decision("live-" + day, JEV, day, f"10:{i * 15 // 60:02d}", 0, action=act, raw=raw,
                    ms=100.0 + 10 * i, error=err, sec=i * 15 % 60,
                    probs='{"BUY": 0.613, "SELL": 0.2, "HOLD": 0.187}')  # fmt: skip
    db.trade("live-" + day, JEV, day, "10:01", "10:02", 50.0)
    db.trade("live-" + day, "hold", day, "09:30", None, 0.0)


def test_status_running_has_live_jev_stats_without_a_summary(db: PaperDB) -> None:
    seed_running(db)
    out = cli.run(["status"], now=et(TUE, "10:03"))
    j = out["jev_live"]
    assert j["calls_mode"] == "live model calls" and j["model"] == "jev-latest"
    assert j["decisions"] == 12 and j["model_calls"] == 11 and j["errors"] == 1
    assert j["last_error"] == "10:01:15 TimeoutError: jev"
    assert j["avg_ms"] == round(sum(100 + 10 * i for i in range(12) if i != 5) / 11)
    assert j["p95_ms"] == 210  # nearest rank of 11: the 11th
    assert j["last_decision_et"] == "10:02:45" and j["last_decision_age_s"] == 15
    assert j["raw"] == {"BUY": 4, "SELL": 4, "HOLD": 4} and j["gated"] == {"BUY": 1, "HOLD": 11}
    assert "Jev: live model calls (11 calls, 1 errors, avg" in out["summary"]
    assert "last 10:02:45 ET" in out["summary"]


@pytest.mark.parametrize(
    ("model", "ms", "error", "raw", "mode"),
    [
        ("jev-latest", 123.0, None, "BUY", "live model calls"),
        ("stored:runs/r1", 123.0, None, "BUY", "replay/no calls"),  # stored answers, any timing
        ("jev-latest", 0.2, None, "BUY", "replay/no calls"),  # answered from memory (cache hits)
        ("jev-latest", 5000.0, "TimeoutError", "HOLD", "calls failing"),
        ("jev-latest", 0.0, None, "WARMUP", "warming up"),
    ],
)
def test_calls_mode_verdicts(db: PaperDB, model, ms, error, raw, mode) -> None:
    db.session("s1", TUE, "running", heartbeat=et(TUE, "10:01"), jev_model=model)
    for i in range(3):
        db.decision("s1", JEV, TUE, "10:00", 0, action="HOLD", raw=raw, ms=ms, error=error,
                    sec=i * 15)  # fmt: skip
    assert cli.run(["status"], now=et(TUE, "10:01"))["jev_live"]["calls_mode"] == mode


def test_calls_mode_judges_the_recent_window(db: PaperDB) -> None:
    """Calls that worked this morning but fail now: the verdict is about now."""
    db.session("s1", TUE, "running", heartbeat=et(TUE, "11:00"), jev_model="jev-latest")
    for i in range(30):
        bad = i >= 10
        db.decision("s1", JEV, TUE, f"10:{i:02d}", 0, action="HOLD", raw="HOLD",
                    ms=5000.0 if bad else 120.0, error="TimeoutError" if bad else None)  # fmt: skip
    j = cli.run(["status"], now=et(TUE, "11:00"))["jev_live"]
    assert j["calls_mode"] == "calls failing" and j["model_calls"] == 10 and j["errors"] == 20


def test_status_no_decisions_yet(db: PaperDB) -> None:
    db.session("s1", TUE, "running", heartbeat=et(TUE, "09:31"))
    db.trade("s1", JEV, TUE, "09:30", None, 0.0)
    out = cli.run(["status"], now=et(TUE, "09:31"))
    assert out["jev_live"]["calls_mode"] == "no decisions yet" and out["jev_live"]["decisions"] == 0
    assert out["summary"].endswith("; Jev: no decisions yet")


def test_pnl_fills_jev_calls_from_decisions_while_running(db: PaperDB) -> None:
    seed_running(db)
    out = cli.run(["pnl", "--date", "today"], now=et(TUE, "10:03"))
    jev = next(p for p in out["policies"] if p["jev"])
    assert jev["jev_calls"] == 11 and jev["jev_calls_from"] == "decisions"
    assert jev["jev_cost_usd"] is None  # the decision log has no tokens
    db.trade(f"live-{TUE}", "hold", TUE, "09:30", "10:00", 5.0)
    out = cli.run(["pnl", "--date", "today"], now=et(TUE, "10:03"))
    hold = next(p for p in out["policies"] if p["policy"] == "hold")
    assert hold["jev_calls"] is None and "jev_calls_from" not in hold  # baselines make no calls


def test_pnl_adds_live_calls_to_a_crashed_sessions_summary(db: PaperDB) -> None:
    """A restart after a crash: the crashed session's summary counts, plus the running one's log."""
    db.session("crash", TUE, "crashed", started="09:25")
    db.trade("crash", JEV, TUE, "09:40", "09:41", 10.0)
    db.summary("crash", JEV, 1, 1, 10.0, 0.0, calls=7, cost=0.01)
    db.decision("crash", JEV, TUE, "09:40", 0, ms=150.0)  # summarized: not counted again
    db.session("again", TUE, "running", started="10:00", heartbeat=et(TUE, "10:30"))
    for i in range(4):
        db.decision("again", JEV, TUE, "10:10", 0, ms=110.0, sec=i * 15)
    out = cli.run(["pnl", "--date", TUE], now=et(TUE, "10:30"))
    jev = next(p for p in out["policies"] if p["jev"])
    assert jev["jev_calls"] == 11 and jev["jev_calls_from"] == "summary+decisions"


def test_pnl_finished_day_keeps_the_summary_numbers(db: PaperDB) -> None:
    seed_day(db, "s1", MON)
    db.decision("s1", "Jev+filters", MON, "10:00", 0, ms=120.0)
    jev = cli.run(["pnl", "--date", MON], now=et(TUE, "10:00"))["policies"][0]
    assert jev["jev_calls"] == 300 and "jev_calls_from" not in jev


def test_decisions_tool_latest_first_trimmed_oldest_first_shown(db: PaperDB) -> None:
    seed_running(db)
    out = cli.run(["decisions", "--date", "", "--limit", "3"], now=et(TUE, "10:03"))
    assert out["policy"] == JEV and out["total"] == 12 and out["shown"] == 3
    assert out["times"] == "ET"
    assert [d["t"] for d in out["decisions"]] == ["10:02:15", "10:02:30", "10:02:45"]
    d = out["decisions"][-1]
    p = {"BUY": 0.61, "SELL": 0.2, "HOLD": 0.19}
    assert d == {"t": "10:02:45", "raw": "HOLD", "act": "HOLD", "pos": 0, "p": p, "ms": 210.0,
                 "err": None}  # fmt: skip
    assert out["live"]["calls_mode"] == "live model calls"
    head = f"{JEV}: 12 decisions in latest, showing the last 3; Jev: live"
    assert out["summary"].startswith(head)


def test_decisions_tool_default_limit_cap_and_budget(db: PaperDB) -> None:
    seed_running(db, n=60)
    out = cli.run(["decisions", "--date", "today", "--limit", "0"], now=et(TUE, "10:20"))
    assert out["shown"] == 20
    out = cli.run(["decisions", "--date", "today", "--limit", "500"], now=et(TUE, "10:20"))
    assert len(json.dumps(out)) <= cli.RESULT_BUDGET and out["omitted"] > 0
    assert out["shown"] + out.get("omitted", 0) == cli.MAX_DECISIONS


def test_decisions_tool_without_sessions_or_jev(db: PaperDB, data_dir: Path) -> None:
    out = cli.run(["decisions", "--date", "today", "--limit", "0"], now=et(TUE, "10:00"))
    assert out["decisions"] == []
    db.session("s1", TUE, "running", heartbeat=et(TUE, "10:00"))
    db.decision("s1", "hold", TUE, "10:00", 0)
    out = cli.run(["decisions", "--date", "today", "--limit", "0"], now=et(TUE, "10:00"))
    assert out["decisions"] == [] and out["summary"] == "no Jev decisions in today"


def test_decisions_cli_under_isolation(db: PaperDB, data_dir: Path) -> None:
    seed_running(db)
    proc = run_isolated(data_dir, "decisions", "--date", TUE, "--limit", "5")
    assert proc.returncode == 0, proc.stderr
    assert json.loads(proc.stdout)["shown"] == 5


# --- the 08:00 briefing: yesterday's crash must not read as today --------------------------

BENTO = (
    "BentoError: A live data license is required to access GLBX.MDP3.\n"
    "Traceback (most recent call last):\n  File \"/Users/justinhe/trade-jev/src/trade_jev/live.py\""
)


def test_crashed_yesterday_running_today_names_the_day(db: PaperDB, reports) -> None:
    """Mon's session crashed on Databento before trading; its close summary names Mon and the
    error (not "Jev +$0 (0 trades) ... (crashed)") and expires at Tue's 09:30 open. On Tue the
    tools describe Tue's running session."""
    db.session("live-" + MON, MON, "crashed", started="13:50", error=BENTO, symbol="NQZ6")
    for p in (JEV, "hold", "random", "imbalance"):  # the crash's best-effort zero summary
        db.summary("live-" + MON, p, 0, 0, 0.0, 0.0, calls=0, cost=0.0)
    out = cli.run(["close-summary"], now=et(MON, "16:10"))
    (r,) = reports
    assert r["title"] == (
        "Paper NQZ6 Mon 11/2: crashed — "
        "BentoError: A live data license is required to access GLBX.MDP3."
    )
    assert r["key"] == f"trade-jev/pnl/{MON}" and r["ttl"] == "18h"  # 16:10 Mon → 09:30 Tue
    assert out["ttl"] == "18h"

    seed_running(db, TUE)
    status = cli.run(["status"], now=et(TUE, "10:03"))
    assert status["day"] == TUE and status["session"]["id"] == f"live-{TUE}"
    assert status["running"] is True and "crashed" not in status["summary"]
    assert status["jev_live"]["calls_mode"] == "live model calls"
    mon = cli.run(["pnl", "--date", "yesterday"], now=et(TUE, "10:03"))["summary"]
    assert mon.startswith("Paper NQZ6 Mon 11/2: Jev +$0 (0 trades)")
    assert mon.endswith(
        "(crashed: BentoError: A live data license is required to access GLBX.MDP3.)"
    )
    pnl = cli.run(["pnl", "--date", ""], now=et(TUE, "10:03"))
    assert pnl["from"] == TUE
    assert pnl["summary"].startswith("Paper NQZ6 Tue 11/3: Jev +$50 (1 trades)")


@pytest.mark.parametrize(
    ("day", "ttl"),
    # Mon → Tue; Fri → Mon; Wed → Fri (over Thanksgiving)
    [(MON, "18h"), ("2026-11-06", "66h"), ("2026-11-25", "42h")],
)
def test_close_summary_ttl_runs_to_the_next_open(db: PaperDB, reports, day: str, ttl: str) -> None:
    seed_day(db, "s1", day)
    assert cli.run(["close-summary"], now=et(day, "16:10"))["ttl"] == ttl


def test_close_summary_stale_at_the_close_says_so(db: PaperDB, reports) -> None:
    db.session("s1", MON, "running", heartbeat=et(MON, "14:00"))
    db.trade("s1", JEV, MON, "10:00", "10:05", -25.0)
    cli.run(["close-summary"], now=et(MON, "16:10"))
    assert reports[0]["title"] == "Paper NQ Mon 11/2: stale (likely dead); Jev −$25 (1 trades)"


# --- manifest ------------------------------------------------------------------------------

AGENTS = ("com.tradejev.paper", "com.tradejev.view", "com.tradejev.tvtoken", "com.tradejev.copy")


def test_manifest_display_metadata_stdlib() -> None:
    """Without the SDK: every job has a one-line description, and launch_agents names exactly the
    plists the repo installs (deploy/launchd)."""
    text = (EXT_DIR / "resonant.yaml").read_text()
    assert "  trade_jev_decisions:\n" in text
    jobs = text.split("\njobs:\n", 1)[1].split("\npanels:", 1)[0]
    assert jobs.count("  - id: ") == jobs.count("    description: ") == 3
    plists = {p.stem for p in (EXT_DIR.parent / "deploy" / "launchd").glob("*.plist")}
    assert plists == set(AGENTS)
    for label in AGENTS:
        assert f"  - label: {label}\n" in text


from conftest import RESONANT_REPO


def test_manifest_validates_against_the_resonant_sdk(tmp_path: Path) -> None:
    """The real parser (resonant_sdk.manifest.load_manifest) from a Resonant checkout's existing
    venv (--no-sync: nothing is installed; ~/.resonant is never read). Skipped off the Mini."""
    uv = shutil.which("uv")
    if uv is None or not (RESONANT_REPO / "packages" / "resonant-sdk").is_dir():
        pytest.skip("needs uv and a Resonant checkout (RESONANT_REPO)")
    code = (
        "import json, sys; from pathlib import Path\n"
        "from resonant_sdk.manifest import load_manifest\n"
        "m = load_manifest(Path(sys.argv[1]))\n"
        "print(json.dumps({'tools': sorted(m.tools), 'jobs': {j.id: j.description for j in m.jobs},"
        " 'agents': {a.label: a.description for a in m.launch_agents},"
        " 'tool_desc': {k: len(t.description) for k, t in m.tools.items()}}))"
    )
    proc = subprocess.run(  # noqa: S603
        [uv, "run", "--frozen", "--no-sync", "--project", str(RESONANT_REPO), "python", "-c", code,
         str(sdk_manifest(tmp_path))],  # without `recover:` until the checkout has PER-284
        capture_output=True, text=True, timeout=120, check=False,
    )  # fmt: skip
    if proc.returncode != 0 and "No module named" in proc.stderr:
        pytest.skip("the Resonant checkout has no synced venv")
    assert proc.returncode == 0, proc.stderr[-2000:]
    m = json.loads(proc.stdout.strip().splitlines()[-1])
    assert "trade_jev_decisions" in m["tools"]
    assert set(m["jobs"]) == {"close-summary", "watch", "copy-watch"} and all(m["jobs"].values())
    assert set(m["agents"]) == set(AGENTS) and all(m["agents"].values())
    assert all(n <= 300 for n in m["tool_desc"].values())


def test_pnl_has_jev_calls_before_jevs_first_closed_trade(db: PaperDB) -> None:
    """Review: early in a session Jev has decisions but no closed trade; pnl still shows its calls."""
    db.session("s1", TUE, "running", heartbeat=et(TUE, "09:35"))
    db.trade("s1", "hold", TUE, "09:30", "09:31", 5.0)
    for i in range(5):
        db.decision("s1", JEV, TUE, "09:31", 0, action="HOLD", raw="HOLD", ms=120.0, sec=i * 10)
    out = cli.run(["pnl", "--date", "today"], now=et(TUE, "09:35"))
    jev = next(p for p in out["policies"] if p["jev"])
    assert out["jev_policy"] == JEV and jev["trades"] == 0 and jev["pnl"] == 0.0
    assert jev["jev_calls"] == 5 and jev["jev_calls_from"] == "decisions"
    assert out["summary"].startswith("Paper NQ Tue 11/3: Jev +$0 (0 trades), hold +$5")


def test_p95_is_nearest_rank(db: PaperDB) -> None:
    db.session("s1", TUE, "running", heartbeat=et(TUE, "10:05"))
    for i in range(20):  # 101..120 ms: nearest-rank p95 is the 19th value, not the max
        db.decision("s1", JEV, TUE, "10:00", 0, ms=101.0 + i, sec=i)
    assert cli.run(["status"], now=et(TUE, "10:05"))["jev_live"]["p95_ms"] == 119


def test_close_summary_ttl_across_the_dst_change(db: PaperDB, reports) -> None:
    """Fri 2026-03-06 16:10 EST → Mon 03-09 09:30 EDT is 64h20m elapsed (clocks spring forward)."""
    seed_day(db, "s1", "2026-03-06")
    assert cli.run(["close-summary"], now=et("2026-03-06", "16:10"))["ttl"] == "65h"
