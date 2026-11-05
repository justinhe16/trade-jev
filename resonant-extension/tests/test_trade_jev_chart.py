"""Tests for the `chart` dashboard panel (``./cli.py panel chart``, PER-283): NQ 1-minute mid candles
with Jev's trades. A running day with an open trade, a full day, a crashed day, a weekend, before
the open, a session from before ``paper_bars`` (the 15 s mid line), the 32 KB budget and the 5 s
runtime on a large DB, and validation by Resonant's schema and core normalize() (skipped without a
Resonant checkout)."""

from __future__ import annotations

import json
import time
from datetime import timedelta
from pathlib import Path
from typing import Any

import cli
from conftest import EXT_DIR, PaperDB, et
from test_trade_jev_ext import JEV, run_isolated, seed_day
from test_trade_jev_panel import resonant_validate

MON, TUE, WED = "2026-11-02", "2026-11-03", "2026-11-04"
SAT = "2026-11-07"
MAX_BYTES = 32768


def chart(now) -> dict[str, Any]:
    return cli.run(["panel", "chart"], now=now)


def widgets(out: dict[str, Any]) -> dict[str, dict[str, Any]]:
    (v,) = out["views"]
    assert v["id"] == "chart"
    return {w["id"]: w for w in v["widgets"]}


def unix(day: str, hm: str) -> int:
    return int(et(day, hm).timestamp())


def assert_ohlc(price: dict[str, Any]) -> None:
    c = price["candles"]
    assert len(c["o"]) == len(c["h"]) == len(c["l"]) == len(c["c"]) == len(price["x"])
    for o, h, lo, cl in zip(c["o"], c["h"], c["l"], c["c"], strict=True):
        assert h >= max(o, cl) and lo <= min(o, cl)
    assert price["x"] == sorted(set(price["x"]))


def seed_full_day(db: PaperDB, day: str = TUE, sid: str | None = None) -> str:
    """A finished session: bars 09:15..16:04 (only 09:30..15:59 are charted), Jev 3 trades
    (target, stop, a reversal) and an eod exit, baselines alongside."""
    sid = sid or "live-" + day
    db.session(sid, day, "done", started="09:15", symbol="NQZ6")
    db.bars(sid, day, "09:15", 410)
    db.trade(sid, JEV, day, "09:40", "09:42", 495.0, reason="target")
    db.trade(sid, JEV, day, "10:00", "10:05", -305.0, side="short", reason="stop")
    db.trade(sid, JEV, day, "11:00", "11:30", 120.0, reason="jev")
    db.trade(sid, JEV, day, "15:00", "15:55", 40.0, side="short", reason="eod")
    db.trade(sid, "imbalance", day, "09:35", "09:50", 60.0)
    db.trade(sid, "random", day, "09:45", "10:05", -40.0)
    return sid


# --- the days -----------------------------------------------------------------------------------


def test_full_day(db: PaperDB) -> None:
    seed_full_day(db)
    out = chart(et(TUE, "16:30"))
    assert out["schema"] == 1 and "health" not in out
    assert out["local_url"] == "http://127.0.0.1:8765/paper"
    w = widgets(out)
    assert set(w) == {"price"}
    p = w["price"]
    assert p["type"] == "price" and p["title"] == "NQZ6 1-min mid, today"
    assert p["x"][0] == unix(TUE, "09:30") and p["x"][-1] == unix(TUE, "15:59") and len(p["x"]) == 390
    assert_ohlc(p)
    assert [t["why"] for t in p["trades"]] == ["target", "stop", "reversal", "eod"]  # Jev only
    assert p["trades"][1] == {
        "side": "short", "entry_t": unix(TUE, "10:00"), "entry_px": 21000.0,
        "exit_t": unix(TUE, "10:05"), "exit_px": 21000.0 - 305 / 20, "pnl": -305.0,
        "currency": "USD", "label": "jev", "why": "stop",
    }  # fmt: skip


def test_running_day_with_an_open_trade(db: PaperDB) -> None:
    sid = "live-" + TUE
    db.session(sid, TUE, "running", started="09:15", symbol="NQZ6",
               heartbeat=et(TUE, "11:00") - timedelta(seconds=20))  # fmt: skip
    db.bars(sid, TUE, "09:15", 105)  # to 10:59
    db.trade(sid, JEV, TUE, "09:40", "09:50", 125.0, reason="target")
    db.trade(sid, JEV, TUE, "10:30", None, 0.0)  # a fixture-style unclosed row: never drawn as closed
    db.open_position(sid, JEV, "long", "10:41", 21012.25, TUE)
    db.open_position(sid, "random", "short", "10:20", 21000.0, TUE)  # a baseline: not shown
    out = chart(et(TUE, "11:00"))
    assert out["local_url"] == "http://127.0.0.1:8767/live" and "health" not in out
    p = widgets(out)["price"]
    assert len(p["x"]) == 90 and p["x"][-1] == unix(TUE, "10:59")
    assert_ohlc(p)
    assert len(p["trades"]) == 2 and p["trades"][0]["why"] == "target"
    assert p["trades"][1] == {"side": "long", "entry_t": unix(TUE, "10:41"), "entry_px": 21012.25,
                              "currency": "USD", "label": "jev"}  # fmt: skip


def test_open_row_is_ignored_once_the_session_is_not_running(db: PaperDB) -> None:
    sid = "live-" + TUE
    db.session(sid, TUE, "running", started="09:15", symbol="NQZ6", heartbeat=et(TUE, "10:00"))
    db.bars(sid, TUE, "09:30", 30)
    db.open_position(sid, JEV, "long", "09:41", 21012.25, TUE)
    out = chart(et(TUE, "11:00"))  # heartbeat an hour old: stale
    assert out["health"] == "bad" and "trades" not in widgets(out)["price"]


def test_crashed_day(db: PaperDB) -> None:
    sid = "live-" + TUE
    db.session(sid, TUE, "crashed", started="09:15", symbol="NQZ6",
               error="BentoError: feed dropped\nTraceback (most recent call last): ...")  # fmt: skip
    db.bars(sid, TUE, "09:15", 60)  # to 10:14
    db.trade(sid, JEV, TUE, "09:40", "09:42", 495.0, reason="target")
    db.open_position(sid, JEV, "short", "10:10", 21000.0, TUE)  # left behind by the crash
    out = chart(et(TUE, "11:00"))
    assert out["health"] == "bad"
    w = widgets(out)
    assert w["price"]["x"][-1] == unix(TUE, "10:14") and len(w["price"]["x"]) == 45
    assert len(w["price"]["trades"]) == 1  # no open trade from a dead session
    assert w["crash"] == {"type": "text", "text": "Session crashed: BentoError: feed dropped",
                          "id": "crash", "title": "Crashed"}  # fmt: skip


def test_crash_before_any_data_still_shows_today(db: PaperDB) -> None:
    seed_full_day(db, MON)
    db.session("live-" + TUE, TUE, "crashed", started="09:15", error="BentoError: no license")
    w = widgets(chart(et(TUE, "11:00")))
    assert w["none"]["text"] == "No market data recorded for today yet."
    assert w["crash"]["text"] == "Session crashed: BentoError: no license"


def test_weekend_shows_the_last_session(db: PaperDB) -> None:
    seed_full_day(db, "2026-11-06")
    db.session("hol", SAT, "holiday", error="weekend")
    out = chart(et(SAT, "12:00"))
    p = widgets(out)["price"]
    assert p["title"] == "NQZ6 1-min mid, Fri 11/6 (last session)" and len(p["trades"]) == 4
    assert p["x"][0] == unix("2026-11-06", "09:30")


def test_before_the_open_shows_the_last_session(db: PaperDB) -> None:
    seed_full_day(db, MON)
    db.session("live-" + TUE, TUE, "running", started="09:15", heartbeat=et(TUE, "09:20"))
    db.bars("live-" + TUE, TUE, "09:15", 5)  # pre-open bars only: nothing in 09:30..16:00
    p = widgets(chart(et(TUE, "09:20")))["price"]
    assert p["title"] == "NQZ6 1-min mid, Mon 11/2 (last session)"
    # and from the open on, today
    db.bars("live-" + TUE, TUE, "09:15", 16, seed=1)
    p = widgets(chart(et(TUE, "09:31")))["price"]
    assert p["title"] == "NQ 1-min mid, today" and p["x"] == [unix(TUE, "09:30")]


def test_a_restart_merges_both_sessions_bars(db: PaperDB) -> None:
    db.session("a", TUE, "aborted", started="09:15", symbol="NQZ6")
    db.session("b", TUE, "done", started="10:00", symbol="NQZ6")
    db.chart_tables()
    db.conn.executemany("INSERT INTO paper_bars VALUES (?,?,?,?,?,?)", [
        ("a", unix(TUE, "09:59"), 10.0, 12.0, 9.0, 11.0),
        ("a", unix(TUE, "10:00"), 11.0, 13.0, 10.0, 12.0),  # a's half of 10:00
        ("b", unix(TUE, "10:00"), 12.5, 14.0, 10.5, 13.0),  # b's half
    ])  # fmt: skip
    db.conn.commit()
    p = widgets(chart(et(TUE, "16:30")))["price"]
    assert p["candles"] == {"o": [10.0, 11.0], "h": [12.0, 14.0], "l": [9.0, 10.0], "c": [11.0, 13.0]}


def test_session_from_before_paper_bars_draws_the_decision_mids(db: PaperDB) -> None:
    sid = "live-" + TUE
    db.session(sid, TUE, "done", symbol="NQZ6")
    for i in range(8):  # 2 minutes of 15 s decisions
        db.decision(sid, JEV, TUE, f"10:0{i // 4}", 0, sec=15 * (i % 4))
    db.trade(sid, JEV, TUE, "10:00", "10:01", 495.0, reason="target")
    p = widgets(chart(et(TUE, "16:30")))["price"]
    assert p["title"] == "NQZ6 mid (15 s samples; no 1-min bars), today"
    assert "candles" not in p and p["line"] == [21000.0, 21000.0]
    assert p["x"] == [unix(TUE, "10:00"), unix(TUE, "10:01")] and len(p["trades"]) == 1


def test_no_db_and_no_sessions(db: PaperDB, data_dir: Path) -> None:
    assert widgets(chart(et(TUE, "10:00")))["none"]["text"] == "no paper sessions yet."
    (data_dir / "config.json").write_text(json.dumps({"db_path": str(data_dir / "nope.db")}))
    assert widgets(chart(et(TUE, "10:00")))["none"]["text"] == "no paper sessions yet."


def test_future_sessions_are_never_charted(db: PaperDB) -> None:
    seed_full_day(db, WED)
    assert widgets(chart(et(TUE, "12:00")))["none"]["text"] == "no paper sessions yet."


# --- size budget and runtime ---------------------------------------------------------------------


def seed_large(db: PaperDB) -> str:
    """The heaviest day: 60 earlier sessions, a restart, a full day of bars from 08:00 to 17:00,
    400 Jev trades (only the newest 100 are drawn), thousands of baseline trades and a decision log."""
    from datetime import date

    d, n = date(2026, 8, 3), 0
    while n < 60:
        if d.weekday() < 5:
            seed_full_day(db, d.isoformat(), sid=f"old{n}")
            n += 1
        d += timedelta(days=1)
    db.session("crash", TUE, "crashed", started="08:00", error="X" * 2000)
    db.bars("crash", TUE, "08:00", 100, base=28000.0, seed=2)
    sid = seed_full_day(db, TUE)
    db.bars(sid, TUE, "08:00", 540, base=28000.123456, seed=3)  # odd decimals: the worst encoding
    t0 = et(TUE, "09:30")
    rows = []
    for i in range(400 * 8):
        t = t0 + timedelta(seconds=7 * i)
        policy = JEV if i % 8 == 0 else ("random", "imbalance", "hold")[i % 3]
        rows.append((sid, policy, ("long", "short")[i % 2], int(t.timestamp() * 1e9), 28000.123456 + i,
                     int((t + timedelta(seconds=5)).timestamp() * 1e9), 28010.654321 + i,
                     ("target", "stop", "jev", "eod")[i % 4], 10.0, 123456.78))  # fmt: skip
    db.conn.executemany(
        "INSERT INTO paper_trades(session_id, policy, side, entry_ns, entry_px, exit_ns, exit_px,"
        " reason, points, pnl) VALUES (?,?,?,?,?,?,?,?,?,?)", rows)  # fmt: skip
    db.conn.executemany(
        "INSERT INTO paper_decisions VALUES (?,?,?,?,?,?,?,?,?,?,?)",
        [(sid, JEV, int((t0 + timedelta(seconds=15 * i)).timestamp() * 1e9), "", 28000.0, 0,
          "HOLD", "HOLD", "{}", 900.0, None) for i in range(1560)],
    )  # fmt: skip
    db.conn.commit()
    return sid


def test_large_day_fits_the_budget_fast(db: PaperDB, data_dir: Path) -> None:
    seed_large(db)
    t = time.perf_counter()
    out = chart(et(TUE, "16:30"))
    elapsed = time.perf_counter() - t
    assert elapsed < 1.0, elapsed  # the budget is 5 s; this is milliseconds
    p = widgets(out)["price"]
    assert len(p["x"]) == 390 and len(p["trades"]) == cli.CHART_TRADES
    # the newest kept, in exit order: the last bulk Jev trade, then the 15:00 eod one
    assert p["trades"][-2]["entry_t"] == unix(TUE, "09:30") + 7 * 8 * 399
    assert p["trades"][-1]["why"] == "eod"
    assert_ohlc(p)
    size = len(json.dumps(out["views"], separators=(",", ":")).encode())
    assert size <= 64 * 1024, size  # raw; core's compact normalised form is checked below
    t = time.perf_counter()
    proc = run_isolated(data_dir, "panel", "chart")
    assert proc.returncode == 0 and time.perf_counter() - t < 5.0
    assert json.loads(proc.stdout)["views"] == json.loads(json.dumps(cli.run(["panel", "chart"])["views"]))


# --- the CLI contract and Resonant's own validation -------------------------------------------


def test_chart_cli_under_isolation(db: PaperDB, data_dir: Path) -> None:
    seed_day(db, "s1", MON)
    proc = run_isolated(data_dir, "panel", "chart")
    assert proc.returncode == 0, proc.stderr
    assert proc.stdout.count("\n") == 1 and json.loads(proc.stdout)["views"][0]["id"] == "chart"


def test_chart_is_in_the_manifest_after_paper() -> None:
    text = (EXT_DIR / "resonant.yaml").read_text()
    block = text.split("\npanels:\n", 1)[1].split("\n\n", 1)[0]
    assert block.index("  - id: paper\n") < block.index("  - id: chart\n")
    chart_block = block.split("  - id: chart\n", 1)[1]
    assert "title: Jev trades" in chart_block and "command: [./cli.py, panel, chart]" in chart_block
    assert "max_bytes: 32768" in chart_block and "timeout_s: 30" in chart_block
    assert 'active: "*/1 9-16 * * 1-5"' in chart_block and 'idle: "*/15 * * * *"' in chart_block


def test_resonant_accepts_every_chart_output(tmp_path: Path, data_dir: Path) -> None:
    """Each scenario passes docs/panel.schema.json and core's normalize() under the manifest's
    max_bytes, untruncated, with the large day well inside 32 KB."""
    outputs: dict[str, dict[str, Any]] = {"empty": chart(et(TUE, "10:00"))}
    db = PaperDB(tmp_path / "paper.db")
    seed_full_day(db, "2026-11-06")
    outputs["weekend"] = chart(et(SAT, "12:00"))
    db.session("c", MON, "crashed", started="09:15", error="BentoError: no license")
    db.bars("c", MON, "09:15", 40)
    outputs["crashed"] = chart(et(MON, "11:00"))
    sid = "live-" + TUE
    db.session(sid, TUE, "running", started="09:15", heartbeat=et(TUE, "11:00"), symbol="NQZ6")
    db.bars(sid, TUE, "09:15", 105)
    db.trade(sid, JEV, TUE, "09:40", "09:50", 125.0, reason="target")
    db.open_position(sid, JEV, "short", "10:41", 21012.25, TUE)
    outputs["running"] = chart(et(TUE, "11:00"))
    db.session("old", WED, "done", symbol="NQZ6")
    db.decision("old", JEV, WED, "10:00", 0)
    outputs["mids"] = chart(et(WED, "16:30"))
    large = tmp_path / "large"
    large.mkdir()
    (data_dir / "config.json").write_text(json.dumps({"db_path": str(large / "paper.db")}))
    seed_large(PaperDB(large / "paper.db"))
    outputs["large"] = chart(et(TUE, "16:30"))
    res = resonant_validate(tmp_path, outputs, "chart")
    assert res["panel"] == {"id": "chart", "command": ["./cli.py", "panel", "chart"],
                            "max_bytes": MAX_BYTES, "idle": "*/15 * * * *",
                            "active": "*/1 9-16 * * 1-5", "tz": "America/New_York"}  # fmt: skip
    for name, r in res["runs"].items():
        assert "refused" not in r, (name, r)
        assert r["truncated"] is False and r["bytes"] <= MAX_BYTES, (name, r)
    assert res["runs"]["large"]["bytes"] <= MAX_BYTES - 4096, res["runs"]["large"]
