"""Tests for daily history (PER-287): ``./cli.py panel paper|chart --date YYYY-MM-DD`` renders a past
ET day for Resonant's per-day archive (manifest ``history: daily``, PER-286). A full past day, a
crashed day, a restart day (two sessions), a skipped day, a weekend and a day before any session
(both ``{"schema": 1, "none": true}``), today and the future, the history tab ending at the day, the
chart's trades-only fallback, a WAL DB without side files, the size and runtime budget on the heavy
DBs, and validation of every output by Resonant's schema and core normalize() (skipped without a
Resonant checkout)."""

from __future__ import annotations

import json
import shutil
import sqlite3
import time
from pathlib import Path
from typing import Any

import cli
import pytest
from conftest import EXT_DIR, RESONANT_REPO, PaperDB, et, sdk_manifest
from test_trade_jev_chart import seed_full_day, seed_large
from test_trade_jev_ext import JEV, run_isolated, seed_day
from test_trade_jev_panel import resonant_validate, seed_heavy

MON, TUE, WED, THU, FRI = "2026-11-02", "2026-11-03", "2026-11-04", "2026-11-05", "2026-11-06"
SAT = "2026-11-07"
NEXT_MON = "2026-11-09"
NONE = {"schema": 1, "none": True}


def paper(day: str, now) -> dict[str, Any]:
    return cli.run(["panel", "paper", "--date", day], now=now)


def chart(day: str, now) -> dict[str, Any]:
    return cli.run(["panel", "chart", "--date", day], now=now)


def widgets(out: dict[str, Any], view: str) -> dict[str, dict[str, Any]]:
    """A view's widgets keyed by id (stats by label)."""
    (v,) = [v for v in out["views"] if v["id"] == view]
    return {w.get("id") or w["label"]: w for w in v["widgets"]}


def seed_restart_day(db: PaperDB, day: str = THU) -> None:
    """As 11-05: the first session SIGTERMed at 14:24 (aborted), a second one to the close."""
    db.session("a", day, "aborted", started="09:15", error="stopped before the session end (SIGTERM)",
               symbol="NQZ6")  # fmt: skip
    db.session("b", day, "done", started="14:24", symbol="NQZ6")
    db.bars("a", day, "09:15", 310, seed=1)
    db.bars("b", day, "14:24", 100, base=21100.0, seed=2)
    db.trade("a", JEV, day, "10:00", "10:30", 990.0, reason="target")
    db.trade("b", JEV, day, "15:00", "15:30", 495.0, reason="target")
    db.trade("b", "imbalance", day, "15:00", "15:10", -60.0)
    for sid, hm, err in (("a", "10:00", None), ("a", "14:00", "TimeoutError: jev"), ("b", "15:00", None)):
        db.decision(sid, JEV, day, hm, 1, ms=120.0, error=err)
    db.summary("a", JEV, 1, 1, 990.0, 0.0, calls=1062)
    db.summary("b", JEV, 1, 1, 495.0, 0.0, calls=342)


# --- the paper panel --------------------------------------------------------------------------


def test_full_past_day(db: PaperDB) -> None:
    seed_day(db, "s1", MON)
    seed_day(db, "s2", TUE, jev=-80.0)
    seed_day(db, "s3", WED)  # after the day: not in its history
    out = paper(TUE, et(FRI, "12:00"))
    assert out["schema"] == 1 and [(v["id"], v["title"]) for v in out["views"]] == [
        ("today", "Tue 11/3"), ("history", "History")]  # fmt: skip
    assert out["health"] == "ok" and out["local_url"] == "http://127.0.0.1:8765/paper"
    assert out["updated_at"] == "2026-11-03T14:25:00+00:00"  # the session's last heartbeat (09:25 ET)
    w = widgets(out, "today")
    assert w["Jev P&L"]["value"] == -80.0 and w["Jev P&L"]["health"] == "bad"
    assert w["Jev P&L"]["hint"] == "Tue 11/3" and w["Jev trades / wins"]["value"] == "6 / 2"
    assert w["Model calls"]["value"] == 300  # paper_summary's count: no decision log that day
    assert w["Session"]["value"] == "done" and w["Session"]["hint"] == "Tue 11/3"
    rows = {r["policy"]: r for r in w["policies"]["rows"]}
    assert list(rows) == ["Jev+filters", "hold", "random"]
    assert rows["hold"]["pnl"] == -110.0 and rows["Jev+filters"]["dd"] == -180.0
    assert w["cum-pnl"]["title"] == "Cumulative P&L, Tue 11/3"
    assert w["trades"]["title"] == "Recent trades, Tue 11/3 (8 closed)"
    assert w["day-sessions"]["title"] == "Sessions (1)" and "crash" not in w
    # live-only parts are left out: no token, launchd agents or data feed
    assert not {"Token expires in", "Tradovate token", "agents"} & set(w)
    h = widgets(out, "history")
    assert h["daily-pnl"]["x"]["values"] == ["Mon 11/2", "Tue 11/3"]
    assert [r["day"] for r in h["sessions"]["rows"]] == ["Tue 11/3", "Mon 11/2"]


def test_no_launchctl_or_token_read_for_a_past_day(db: PaperDB, launchd, token_status: Path) -> None:
    seed_day(db, "s1", MON)
    token_status.write_text("{not json")
    out = paper(MON, et(TUE, "12:00"))
    assert launchd.calls == [] and out["health"] == "ok"


def test_crashed_day(db: PaperDB) -> None:
    seed_day(db, "s0", MON)
    db.session("s1", TUE, "crashed", error="BentoError: no license\nTraceback (most recent ...)")
    db.trade("s1", JEV, TUE, "09:40", "09:45", -60.0)
    out = paper(TUE, et(WED, "12:00"))
    assert out["health"] == "bad"
    w = widgets(out, "today")
    assert w["Session"]["value"] == "crashed" and w["Session"]["hint"] == "BentoError: no license"
    assert w["crash"]["text"] == "Crashed: BentoError: no license"
    assert w["day-sessions"]["rows"][0]["error"] == "BentoError: no license"
    assert w["Jev P&L"]["value"] == -60.0


def test_a_session_left_running_reads_stale(db: PaperDB) -> None:
    """A row still ``running`` on a past day never finished (the process died): stale, red."""
    db.session("s1", TUE, "running", heartbeat=et(TUE, "11:00"))
    out = paper(TUE, et(WED, "12:00"))
    w = widgets(out, "today")
    assert out["health"] == "bad" and w["Session"]["value"] == "stale"
    assert w["crash"]["text"] == "Stale: no error recorded"


def test_restart_day_merges_both_sessions(db: PaperDB) -> None:
    seed_restart_day(db)
    out = paper(THU, et(FRI, "08:00"))
    w = widgets(out, "today")
    assert out["health"] == "ok"  # the restart finished the day
    assert w["Jev P&L"]["value"] == 1485.0 and w["Jev trades / wins"]["value"] == "2 / 2"
    assert w["Model calls"]["value"] == 2 and w["Call errors"]["value"] == 1  # both decision logs
    assert w["Call errors"]["hint"] == "14:00:00 TimeoutError: jev"
    assert w["Session"]["value"] == "done" and w["Session"]["hint"] == "Thu 11/5, 2 sessions"
    s = w["day-sessions"]
    assert s["title"] == "Sessions (2, 1 restart)"
    assert [(r["start"], r["status"]["label"], r["jev"]) for r in s["rows"]] == [
        ("09:15", "aborted", 990.0), ("14:24", "done", 495.0)]  # fmt: skip
    assert s["rows"][0]["error"] == "stopped before the session end (SIGTERM)"
    cum = {x["name"]: x["values"] for x in w["cum-pnl"]["series"]}
    assert cum[JEV][-1] == 1485.0


def test_skipped_day_is_shown_not_none(db: PaperDB) -> None:
    db.session("s1", TUE, "skipped", error="Tradovate token expired")
    out = paper(TUE, et(WED, "12:00"))
    w = widgets(out, "today")
    assert w["Session"]["value"] == "skipped" and w["Session"]["hint"] == "Tradovate token expired"
    assert w["cum-pnl"]["text"] == "No closed trades on Tue 11/3."
    c = chart(TUE, et(WED, "12:00"))
    assert widgets(c, "chart")["none"]["text"] == "Session skipped on Tue 11/3: Tradovate token expired"


@pytest.mark.parametrize("panel", ["paper", "chart"])
def test_no_session_days_print_none(db: PaperDB, panel: str) -> None:
    seed_day(db, "s1", TUE)
    db.session("h", "2026-11-26", "holiday")  # the runner's holiday row: still no session
    now = et("2026-11-27", "12:00")
    for day in (SAT, MON, "2026-10-30", "2026-11-26"):  # weekend, before the first session, holiday
        assert cli.run(["panel", panel, "--date", day], now=now) == NONE, day


def test_a_holiday_row_next_to_a_real_session_is_ignored(db: PaperDB) -> None:
    seed_day(db, "s1", TUE)
    db.session("h", TUE, "holiday", started="23:00")  # started after the real one: would be "last"
    out = paper(TUE, et(WED, "12:00"))
    w = widgets(out, "today")
    assert w["Session"]["value"] == "done" and w["day-sessions"]["title"] == "Sessions (1)"
    assert out["updated_at"] == "2026-11-03T14:25:00+00:00"


@pytest.mark.parametrize("panel", ["paper", "chart"])
def test_no_db_prints_none(db: PaperDB, data_dir: Path, panel: str) -> None:
    (data_dir / "config.json").write_text(json.dumps({"db_path": str(data_dir / "nope.db")}))
    assert cli.run(["panel", panel, "--date", MON], now=et(TUE, "12:00")) == NONE


def test_today_renders_as_now_and_the_future_is_none(db: PaperDB) -> None:
    seed_day(db, "s1", TUE)
    seed_day(db, "s2", WED)  # a row "from the future" relative to now: never shown
    now = et(TUE, "17:00")
    for name in ("paper", "chart"):
        live = cli.run(["panel", name], now=now)
        assert cli.run(["panel", name, "--date", TUE], now=now) == live
        assert cli.run(["panel", name, "--date", WED], now=now) == NONE


def test_bad_date_fails_cleanly(db: PaperDB, data_dir: Path) -> None:
    with pytest.raises(cli.Fail, match="bad --date"):
        paper("yesterday", et(TUE, "12:00"))
    proc = run_isolated(data_dir, "panel", "chart", "--date", "2026-13-01")
    assert proc.returncode == 1 and proc.stdout == "" and "bad --date" in proc.stderr


# --- the chart panel ------------------------------------------------------------------------------


def test_chart_full_past_day(db: PaperDB) -> None:
    seed_full_day(db, TUE)
    seed_full_day(db, WED)
    out = chart(TUE, et(FRI, "12:00"))
    assert [v["title"] for v in out["views"]] == ["Jev trades, Tue 11/3"] and "health" not in out
    p = widgets(out, "chart")["price"]
    assert p["title"] == "NQZ6 1-min mid, Tue 11/3"
    assert len(p["x"]) == 390 and p["x"][0] == int(et(TUE, "09:30").timestamp())
    assert [t["why"] for t in p["trades"]] == ["target", "stop", "reversal", "eod"]


def test_chart_restart_day_merges_bars(db: PaperDB) -> None:
    seed_restart_day(db)
    p = widgets(chart(THU, et(FRI, "08:00")), "chart")["price"]
    assert p["x"] == sorted(set(p["x"])) and len(p["x"]) == 390  # 09:30..14:24 + 14:24..15:59
    assert [t["pnl"] for t in p["trades"]] == [990.0, 495.0]


def test_chart_crashed_day(db: PaperDB) -> None:
    db.session("c", MON, "crashed", error="BentoError: A live data license is required")
    out = chart(MON, et(TUE, "12:00"))
    w = widgets(out, "chart")
    assert out["health"] == "bad"
    assert w["none"]["text"] == "No market data recorded for Mon 11/2."
    assert w["crash"]["text"] == "Session crashed: BentoError: A live data license is required"


def test_chart_from_before_paper_bars_draws_the_decision_mids(db: PaperDB) -> None:
    db.session("old", TUE, "done", symbol="NQZ6")
    db.decision("old", JEV, TUE, "10:00", 0)
    db.trade("old", JEV, TUE, "10:00", "10:10", 25.0)
    p = widgets(chart(TUE, et(WED, "12:00")), "chart")["price"]
    assert p["line"] == [21000.0] and len(p["trades"]) == 1


def test_chart_trades_without_bars_are_a_table(db: PaperDB) -> None:
    db.session("s", TUE, "done")
    db.chart_tables()  # paper_bars exists but holds nothing for the day, and no decision mids
    db.trade("s", JEV, TUE, "10:00", "10:10", 25.0, reason="target")
    db.trade("s", JEV, TUE, "11:00", "11:20", -50.0, side="short", reason="jev")
    db.trade("s", "random", TUE, "11:00", "11:20", 5.0)  # not Jev: not listed
    w = widgets(chart(TUE, et(WED, "12:00")), "chart")
    assert w["none"]["text"] == "No market data recorded for Tue 11/3. Jev's trades:"
    t = w["trades"]
    assert t["title"] == "Jev trades (2)"
    assert t["rows"][0] == {"in": "11:00", "out": "11:20", "side": "short", "entry": 21000.0,
                            "exit": 20997.5, "pnl": -50.0, "why": "reversal"}  # fmt: skip


# --- WAL, budget, CLI and Resonant's validation ----------------------------------------------------


def test_wal_db_without_side_files_fails_so_the_day_is_retried(tmp_path: Path, data_dir: Path) -> None:
    """A WAL DB with its -wal/-shm gone (as health tolerates, PER-285) won't open read-only: exit 1
    with one line, never NONE or a partial day, so Resonant retries the archive later."""
    d = tmp_path / "ro"
    d.mkdir()
    path = d / "paper.db"
    seed_day(PaperDB(path), "s1", MON)
    sqlite3.connect(path).execute("PRAGMA journal_mode=WAL").fetchone()
    (data_dir / "config.json").write_text(json.dumps({"db_path": str(path)}))
    for side in ("paper.db-wal", "paper.db-shm"):
        (d / side).unlink(missing_ok=True)
    d.chmod(0o555)
    try:
        procs = [run_isolated(data_dir, "panel", p, "--date", MON) for p in ("paper", "chart")]
    finally:
        d.chmod(0o755)
    if procs[0].returncode == 0:
        pytest.skip("sqlite opened the WAL DB read-only without side files")
    for proc in procs:
        assert proc.returncode == 1 and proc.stdout == ""
        assert proc.stderr.strip() == f"trade-jev: {cli.CANT_READ}"


HEAVY = (("paper", "2026-10-23", seed_heavy), ("chart", TUE, seed_large))


def heavy_outputs(tmp_path: Path, data_dir: Path) -> dict[str, tuple[dict[str, Any], float]]:
    """Each panel's worst-case day (the live tests' heavy DBs) run as Resonant runs it: the parsed
    output and its wall time. The day after is "now", so it is an archived run."""
    out = {}
    for name, day, seed in HEAVY:
        d = tmp_path / f"heavy-{name}"
        d.mkdir()
        seed(PaperDB(d / "paper.db"))
        (data_dir / "config.json").write_text(json.dumps({"db_path": str(d / "paper.db")}))
        t = time.perf_counter()
        proc = run_isolated(data_dir, "panel", name, "--date", day)
        out[name] = (json.loads(proc.stdout), time.perf_counter() - t)
        assert proc.returncode == 0, proc.stderr
    return out


def test_heavy_days_fit_the_budget_fast(tmp_path: Path, data_dir: Path) -> None:
    """Under 5 s each; the views (raw) within the live tests' bounds. The exact budget, core's
    normalised bytes against max_bytes, is checked in the Resonant validation below."""
    outs = heavy_outputs(tmp_path, data_dir)
    for name, (out, elapsed) in outs.items():
        assert elapsed < 5.0, (name, elapsed)
        assert "none" not in out and out["views"][0]["title"] in ("Fri 10/23", "Jev trades, Tue 11/3")
    paper_views = len(json.dumps(outs["paper"][0]["views"], separators=(",", ":")).encode())
    assert paper_views <= 24576 - 4096, paper_views
    chart_views = len(json.dumps(outs["chart"][0]["views"], separators=(",", ":")).encode())
    assert chart_views <= 64 * 1024, chart_views


def test_cli_prints_none_exactly(db: PaperDB, data_dir: Path) -> None:
    seed_day(db, "s1", TUE)
    for name in ("paper", "chart"):
        proc = run_isolated(data_dir, "panel", name, "--date", "2026-10-31")
        assert proc.returncode == 0, proc.stderr
        assert proc.stdout == '{"schema":1,"none":true}\n'
        assert json.loads(proc.stdout) == NONE


def test_manifest_declares_daily_history_on_both_panels() -> None:
    text = (EXT_DIR / "resonant.yaml").read_text()
    block = text.split("\npanels:\n", 1)[1].split("\n\n", 1)[0]
    for pid in ("paper", "chart"):
        body = block.split(f"  - id: {pid}\n", 1)[1].split("  - id:", 1)[0]
        assert "    history: {every: daily, keep: 365d}" in body, pid


def test_sdk_manifest_keeps_history_once_the_sdk_knows_it(tmp_path: Path) -> None:
    sdk = RESONANT_REPO / "packages" / "resonant-sdk" / "src" / "resonant_sdk" / "manifest.py"
    if not sdk.is_file():
        pytest.skip("no Resonant checkout")
    text = sdk_manifest(tmp_path).read_text()
    assert ("history: {every: daily" in text) == ("history" in sdk.read_text())


def test_resonant_accepts_every_archived_output(tmp_path: Path, data_dir: Path) -> None:
    """Each --date output passes docs/panel.schema.json and core's normalize() under the manifest's
    max_bytes, untruncated; NONE too once the checkout's schema knows it (PER-286)."""
    db = PaperDB(tmp_path / "paper.db")
    seed_day(db, "s0", MON)
    db.session("c", TUE, "crashed", error="BentoError: no license")
    db.trade("c", JEV, TUE, "09:40", "09:45", -60.0)
    seed_restart_day(db)
    db.session("s", WED, "done")
    db.trade("s", JEV, WED, "10:00", "10:10", 25.0)
    now = et(FRI, "12:00")
    heavy = heavy_outputs(tmp_path, data_dir)
    for name in ("paper", "chart"):
        (data_dir / "config.json").write_text(json.dumps({"db_path": str(tmp_path / "paper.db")}))
        outs = {f"{d}": cli.run(["panel", name, "--date", d], now=now) for d in (MON, TUE, WED, THU)}
        outs["heavy"] = heavy[name][0]
        res = resonant_validate(tmp_path, outs, name)
        for day, r in res["runs"].items():
            assert "refused" not in r, (name, day, r)
            assert r["truncated"] is False and r["bytes"] <= res["panel"]["max_bytes"], (name, day, r)
        assert res["runs"]["heavy"]["bytes"] <= res["panel"]["max_bytes"] - 4096, res["runs"]["heavy"]
    schema = RESONANT_REPO / "docs" / "panel.schema.json"
    if '"none"' not in schema.read_text():
        pytest.skip("the Resonant checkout's panel schema doesn't know `none` yet (PER-286)")
    assert shutil.which("uv")
    (data_dir / "config.json").write_text(json.dumps({"db_path": str(tmp_path / "paper.db")}))
    res = resonant_validate(tmp_path, {"none": cli.run(["panel", "paper", "--date", SAT], now=now)})
    assert "refused" not in res["runs"]["none"]
