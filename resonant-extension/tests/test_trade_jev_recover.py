"""PER-285: stall-aware `health` and `recover`. launchctl is the autouse fake (conftest): no test
ever starts, stops or kickstarts a real com.tradejev.* job."""

from __future__ import annotations

import json
import os
import sqlite3
import subprocess
from pathlib import Path

import cli
import pytest
from conftest import PaperDB, et

THU = "2026-11-05"  # a trading day (the day of the Wi-Fi stall)
SAT = "2026-11-07"
THANKSGIVING = "2026-11-26"
JEV = "jev[raw_l10]"
SVC = f"gui/{os.getuid()}/com.tradejev.paper"


@pytest.fixture(autouse=True)
def no_sleep(monkeypatch: pytest.MonkeyPatch) -> list[float]:
    """recover's wait loop never really sleeps."""
    slept: list[float] = []
    monkeypatch.setattr(cli.clock, "sleep", slept.append)
    return slept


def running(db: PaperDB, day: str, hb: str, last: str | None, *, started: str = "09:15") -> None:
    """A running session with heartbeat at ``hb`` and (if given) Jev decisions every 15 s up to ``last``."""
    db.session("s1", day, "running", started=started, heartbeat=et(day, hb))
    if last is not None:
        for sec in (0, 15, 30, 45):
            db.decision("s1", JEV, day, last, 0, action="HOLD", raw="HOLD", sec=sec)


def health(now) -> tuple[int, dict]:
    out = cli.run(["health"], now=now)
    return (cli.UNHEALTHY if out["ok"] is False else 0), out


# --- health ---------------------------------------------------------------------------------


def test_health_stalled_with_fresh_heartbeat_is_bad(db: PaperDB) -> None:
    """The 2026-11-05 case: heartbeat fresh, no decision since 13:55."""
    running(db, THU, hb="14:00", last="13:55")
    code, out = health(et(THU, "14:00"))
    assert code == cli.UNHEALTHY
    assert out["problem"] == "no decisions for 4m15s"
    assert out["summary"] == "paper session down: no decisions for 4m15s"


def test_health_recent_decision_is_ok(db: PaperDB) -> None:
    running(db, THU, hb="14:00", last="13:59")
    code, out = health(et(THU, "14:00"))
    assert code == 0 and out["summary"] == "ok: session running, last decision 15s ago"
    assert out["db"] == "ok" and out["sessions"] == 1


def test_health_stall_threshold_is_120s(db: PaperDB) -> None:
    db.session("s1", THU, "running", started="09:15", heartbeat=et(THU, "14:00"))
    db.decision("s1", JEV, THU, "13:58", 0)  # 13:58:00, exactly 120 s before 14:00
    assert health(et(THU, "14:00"))[0] == 0
    assert health(et(THU, "14:00").replace(second=1))[0] == cli.UNHEALTHY


def test_health_startup_grace(db: PaperDB) -> None:
    """A session restarted at 14:30 with no decision yet: ok for 3 min, then bad."""
    running(db, THU, hb="14:32", last=None, started="14:30")
    code, out = health(et(THU, "14:32"))
    assert code == 0 and out["summary"] == "ok: session starting, no decision yet"
    code, out = health(et(THU, "14:34"))
    assert code == cli.UNHEALTHY and out["problem"] == "no decisions for 4m00s"


def test_health_grace_counts_from_the_open(db: PaperDB) -> None:
    """Started 09:15 (before the open): decisions begin at 09:30, so 09:32 is still in grace."""
    running(db, THU, hb="09:32", last=None, started="09:15")
    assert health(et(THU, "09:32"))[0] == 0
    assert health(et(THU, "09:34"))[0] == cli.UNHEALTHY


def test_health_catch_up_replay_is_not_a_stall(db: PaperDB) -> None:
    """A late restart replays from the open: decisions timed before the session started."""
    running(db, THU, hb="14:31", last="10:02", started="14:30")
    code, out = health(et(THU, "14:31"))
    assert code == 0 and out["summary"] == "ok: session catching up (replay at 10:02 ET)"


@pytest.mark.parametrize(
    ("status", "hb", "problem"),
    [
        ("crashed", "11:00", "session crashed: boom"),
        ("running", "13:00", "session stale (no heartbeat for 60 min), likely dead"),
    ],
)
def test_health_crashed_or_stale_is_bad(db: PaperDB, status: str, hb: str, problem: str) -> None:
    db.session("s1", THU, status, heartbeat=et(THU, hb), error="boom" if status == "crashed" else None)
    code, out = health(et(THU, "14:00"))
    assert code == cli.UNHEALTHY and out["problem"] == problem


def test_health_missing_session_is_bad(db: PaperDB) -> None:
    db.session("old", "2026-11-04", "done")
    code, out = health(et(THU, "10:00"))
    assert code == cli.UNHEALTHY and out["problem"] == "no session started"


def test_health_missing_db_in_the_window_is_bad(data_dir: Path) -> None:
    code, out = health(et(THU, "10:00"))
    assert code == cli.UNHEALTHY and out["db"] == "absent"


def test_health_no_session_before_0935_is_ok(db: PaperDB) -> None:
    assert health(et(THU, "09:32")) == (0, {"ok": True, "db": "ok", "sessions": 0, "summary": "ok: no session yet"})


@pytest.mark.parametrize("hm", ["09:00", "09:30", "15:51", "16:30", "03:00"])
def test_health_ok_outside_the_window(db: PaperDB, hm: str) -> None:
    running(db, THU, hb="09:20", last="09:20")  # would be stale and stalled inside it
    code, out = health(et(THU, hm))
    assert code == 0 and out["summary"] == "ok: outside the session window (09:31-15:50 ET)"


@pytest.mark.parametrize("day", [SAT, THANKSGIVING])
def test_health_ok_on_weekend_and_holiday(data_dir: Path, day: str) -> None:
    code, out = health(et(day, "11:00"))
    assert code == 0 and out["db"] == "absent"


def test_health_config_holiday_is_ok(data_dir: Path, tmp_path: Path) -> None:
    (data_dir / "config.json").write_text(json.dumps({"db_path": str(tmp_path / "paper.db"), "holidays": [THU]}))
    assert health(et(THU, "11:00"))[0] == 0


@pytest.mark.parametrize("status", ["done", "aborted", "holiday", "skipped"])
def test_health_finished_sessions_are_ok(db: PaperDB, status: str) -> None:
    db.session("s1", THU, status)
    code, out = health(et(THU, "12:00"))
    assert code == 0 and out["summary"] == f"ok: session {status}"


def test_health_stopped_on_purpose_is_ok(db: PaperDB, data_dir: Path) -> None:
    running(db, THU, hb="11:00", last="11:00")
    (data_dir / cli.STOPPED).write_text(THU)
    code, out = health(et(THU, "14:00"))
    assert code == 0 and out["summary"] == "ok: stopped on purpose today (trade_jev_stop)"


def test_health_unopenable_wal_db_is_cant_tell(db: PaperDB, monkeypatch: pytest.MonkeyPatch) -> None:
    """A WAL DB whose -wal/-shm side files are missing fails a read-only open: can't tell, stay ok."""
    running(db, THU, hb="11:00", last="11:00")

    def boom(cfg):
        raise sqlite3.OperationalError("unable to open database file")

    monkeypatch.setattr(cli, "connect", boom)
    code, out = health(et(THU, "14:00"))
    assert code == 0 and out["db"] == "unopenable"
    assert out["summary"] == "ok: can't tell: paper.db won't open read-only"
    assert cli.run(["recover"], now=et(THU, "14:00"))["restarted"] is False


def test_health_real_wal_db_without_side_files(tmp_path: Path, data_dir: Path) -> None:
    """The real thing: a WAL DB in a read-only directory with its side files gone (sqlite's own error)."""
    d = tmp_path / "ro"
    d.mkdir()
    path = d / "paper.db"
    PaperDB(path).conn.execute("PRAGMA journal_mode=WAL").fetchone()
    (data_dir / "config.json").write_text(json.dumps({"db_path": str(path)}))
    for side in ("paper.db-wal", "paper.db-shm"):
        (d / side).unlink(missing_ok=True)
    d.chmod(0o555)
    try:
        code, out = health(et(THU, "14:00"))
    finally:
        d.chmod(0o755)
    if out["db"] == "ok":  # this sqlite build manages without them: nothing to tolerate
        pytest.skip("sqlite opened the WAL DB read-only without side files")
    assert code == 0 and out["db"] == "unopenable"


def test_health_other_db_errors_still_fail(db: PaperDB, monkeypatch: pytest.MonkeyPatch) -> None:
    def boom(cfg):
        raise sqlite3.DatabaseError("file is not a database")

    monkeypatch.setattr(cli, "connect", boom)
    assert cli.main(["health"]) == 1


def test_health_exit_codes_through_main(db: PaperDB, monkeypatch: pytest.MonkeyPatch, capsys) -> None:
    running(db, THU, hb="14:00", last="13:55")
    monkeypatch.setattr(cli, "now_utc", lambda: et(THU, "14:00"))
    assert cli.main(["health"]) == cli.UNHEALTHY == 2
    assert json.loads(capsys.readouterr().out)["ok"] is False
    monkeypatch.setattr(cli, "now_utc", lambda: et(THU, "16:30"))
    assert cli.main(["health"]) == 0


# --- recover --------------------------------------------------------------------------------


def test_recover_restarts_a_stalled_session(db: PaperDB, launchd, monkeypatch: pytest.MonkeyPatch) -> None:
    running(db, THU, hb="14:00", last="13:55")
    states = iter(["running", "running", "not running"])  # before kill, then once while waiting

    def state_after(*args: str) -> subprocess.CompletedProcess[str]:
        if args[0] == "print":
            launchd.state = next(states, "not running")
        return type(launchd).__call__(launchd, *args)

    monkeypatch.setattr(cli, "launchctl", state_after)
    out = cli.run(["recover"], now=et(THU, "14:00"))
    assert out == {"restarted": True, "problem": "no decisions for 4m15s", "service": SVC,
                   "summary": "restarted paper session (no decisions for 4m15s)"}  # fmt: skip
    verbs = [c for c in launchd.calls if c[0] != "print"]
    assert verbs == [("kill", "SIGTERM", SVC), ("kickstart", SVC)]  # kill strictly before kickstart
    assert [c[0] for c in launchd.calls].count("print") == 3  # waited for the old process to exit


def test_recover_crashed_job_not_running_just_kickstarts(db: PaperDB, launchd) -> None:
    db.session("s1", THU, "crashed", heartbeat=et(THU, "11:00"), error="feed died")
    launchd.state = "not running"
    out = cli.run(["recover"], now=et(THU, "14:00"))
    assert out["summary"] == "restarted paper session (session crashed: feed died)"
    assert [c for c in launchd.calls if c[0] != "print"] == [("kickstart", SVC)]


def test_recover_missing_session_kickstarts(db: PaperDB, launchd) -> None:
    launchd.state = "not running"
    out = cli.run(["recover"], now=et(THU, "10:00"))
    assert out["restarted"] is True and out["problem"] == "no session started"


@pytest.mark.parametrize(
    ("day", "hm", "why"),
    [
        (THU, "16:00", "outside the session window (09:31-15:50 ET)"),
        (THU, "15:56", "outside the session window (09:31-15:50 ET)"),
        (THU, "08:00", "outside the session window (09:31-15:50 ET)"),
        (SAT, "11:00", "not a trading day"),
        (THANKSGIVING, "11:00", "not a trading day"),
    ],
)
def test_recover_never_acts_outside_the_window(db: PaperDB, launchd, day: str, hm: str, why: str) -> None:
    running(db, day, hb="09:40", last="09:40")  # stale and stalled, were it judged
    out = cli.run(["recover"], now=et(day, hm))
    assert out == {"restarted": False, "summary": f"nothing to do: {why}"}
    assert launchd.calls == []


def test_recover_healthy_session_is_a_no_op(db: PaperDB, launchd) -> None:
    running(db, THU, hb="14:00", last="13:59")
    out = cli.run(["recover"], now=et(THU, "14:00"))
    assert out == {"restarted": False, "summary": "nothing to do: session running, last decision 15s ago"}
    assert launchd.calls == []


def test_recover_in_startup_grace_is_a_no_op(db: PaperDB, launchd) -> None:
    running(db, THU, hb="14:31", last=None, started="14:30")
    assert cli.run(["recover"], now=et(THU, "14:31"))["restarted"] is False
    assert launchd.calls == []


def test_recover_stopped_on_purpose_is_a_no_op(db: PaperDB, data_dir: Path, launchd) -> None:
    (data_dir / cli.STOPPED).write_text(THU)
    out = cli.run(["recover"], now=et(THU, "11:00"))  # no session, but the owner stopped it
    assert out["summary"] == "nothing to do: stopped on purpose today (trade_jev_stop)"
    assert launchd.calls == []


def test_recover_after_eod_guard(db: PaperDB, launchd, monkeypatch: pytest.MonkeyPatch) -> None:
    """Even if the window were widened past 15:55, recover never acts after EOD."""
    monkeypatch.setattr(cli, "CHECK_UNTIL", cli.CLOSE)
    running(db, THU, hb="15:56", last="15:50")
    assert cli.run(["recover"], now=et(THU, "15:56"))["summary"] == "nothing to do: after EOD"
    assert launchd.calls == []


@pytest.mark.parametrize("verb", ["kill", "kickstart"])
def test_recover_launchctl_failure_exits_1(db: PaperDB, launchd, verb: str, capsys) -> None:
    running(db, THU, hb="14:00", last="13:55")
    launchd.state = "running" if verb == "kill" else "not running"
    launchd.fail[verb] = 5
    assert cli.main(["recover"]) in (0, 1)  # real clock: may be outside the window; pin it below
    capsys.readouterr()
    launchd.calls.clear()
    with pytest.raises(cli.Fail, match=f"launchctl {verb} exited 5"):
        cli.run(["recover"], now=et(THU, "14:00"))
    if verb == "kill":
        assert ("kickstart", SVC) not in launchd.calls


def test_recover_launchctl_timeout_exits_1(db: PaperDB, launchd) -> None:
    running(db, THU, hb="14:00", last="13:55")
    launchd.state = "not running"
    launchd.timeout.add("kickstart")
    with pytest.raises(cli.Fail, match="launchctl kickstart timed out"):
        cli.run(["recover"], now=et(THU, "14:00"))


def test_recover_job_not_loaded_exits_1(db: PaperDB, launchd) -> None:
    running(db, THU, hb="14:00", last="13:55")
    launchd.loaded = False
    with pytest.raises(cli.Fail, match="isn't loaded"):
        cli.run(["recover"], now=et(THU, "14:00"))
    assert [c[0] for c in launchd.calls] == ["print"]


def test_recover_old_process_never_exits(db: PaperDB, launchd, monkeypatch: pytest.MonkeyPatch, no_sleep) -> None:
    """SIGTERM sent but the job stays running: no kickstart on top of it, exit 1."""
    running(db, THU, hb="14:00", last="13:55")
    ticks = iter(range(0, 1000))
    monkeypatch.setattr(cli.clock, "monotonic", lambda: float(next(ticks)))
    with pytest.raises(cli.Fail, match="still running 20s after SIGTERM"):
        cli.run(["recover"], now=et(THU, "14:00"))
    assert ("kickstart", SVC) not in launchd.calls and len(no_sleep) >= 19


def test_recover_does_not_mark_stopped(db: PaperDB, data_dir: Path, launchd) -> None:
    """Unlike trade_jev_stop, a restart must not silence watch/health for the rest of the day."""
    running(db, THU, hb="14:00", last="13:55")
    launchd.state = "not running"
    cli.run(["recover"], now=et(THU, "14:00"))
    assert not (data_dir / cli.STOPPED).exists()


def test_manifest_declares_recover() -> None:
    text = (cli.Path(cli.__file__).parent / "resonant.yaml").read_text()
    assert "\nhealth: {check: ./cli.py health, every: 5m}\n" in text
    assert "\nrecover: {run: ./cli.py recover, timeout: 120s}\n" in text
