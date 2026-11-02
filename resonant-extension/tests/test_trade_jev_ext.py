"""Tests for the trade-jev Resonant extension (resonant-extension/cli.py): every tool, both
jobs' report logic, and the CLI contract (one JSON object, exit codes, python -I -S)."""

from __future__ import annotations

import json
import os
import subprocess
import sys
from datetime import timedelta
from pathlib import Path

import cli
import pytest
from conftest import EXT_DIR, PaperDB, et

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
    assert out["summary"] == "Paper NQ: Jev −$200 (6 trades), hold −$110, random +$30"


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
    assert cli.run(["health"])["db"] == "ok"


def test_health_without_db(data_dir: Path) -> None:
    assert cli.run(["health"]) == {"ok": True, "db": "absent", "summary": "no paper sessions yet"}


# --- close-summary job --------------------------------------------------------------------


def test_close_summary_reports_the_day(db: PaperDB, reports) -> None:
    seed_day(db, "s1", MON)
    out = cli.run(["close-summary"], now=et(MON, "16:10"))
    assert out["reported"] is True
    (r,) = reports
    assert r["key"] == f"trade-jev/pnl/{MON}"
    assert r["title"] == "Paper NQ: Jev +$420 (6 trades), hold −$110, random +$30"
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
    assert reports[0]["title"].endswith("(crashed)") and reports[0]["state"]["status"] == "crashed"


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
    assert '"5,20,35,50 9-15 * * 1-5"' in (EXT_DIR / "resonant.yaml").read_text()


def test_stop_marker_write_failure_still_reports_success(data_dir: Path, launchd, monkeypatch) -> None:
    def boom(self, *a, **k):
        raise OSError("read-only")

    monkeypatch.setattr(Path, "write_text", boom)
    assert cli.run(["stop"], now=et(MON, "12:00"))["stopped"] is True
