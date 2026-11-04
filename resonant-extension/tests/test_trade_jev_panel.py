"""Tests for the `paper` dashboard panel (``./cli.py panel paper``): a running day, a crashed day,
a weekend with no session, no DB at all, the 24 KB budget on a heavy history, and validation of
every output by Resonant's own normaliser and JSON schema (skipped without a Resonant checkout)."""

from __future__ import annotations

import json
import shutil
import subprocess
from datetime import timedelta
from pathlib import Path
from typing import Any

import cli
import pytest
from conftest import EXT_DIR, PaperDB, et, write_token_status
from test_trade_jev_ext import JEV, RESONANT_REPO, run_isolated, seed_day

MON, TUE = "2026-11-02", "2026-11-03"
SAT = "2026-11-07"
MAX_BYTES = 24576


def panel(now) -> dict[str, Any]:
    return cli.run(["panel", "paper"], now=now)


def widgets(out: dict[str, Any], view: str) -> dict[str, dict[str, Any]]:
    """A view's widgets keyed by id (stats by label)."""
    (v,) = [v for v in out["views"] if v["id"] == view]
    return {w.get("id") or w["label"]: w for w in v["widgets"]}


def seed_live_day(db: PaperDB, day: str = TUE) -> None:
    """A session at 11:00 ET: Jev 2 closed trades (+125, −25) and live decisions; imbalance,
    random and hold trading alongside."""
    sid = "live-" + day
    db.session(sid, day, "running", heartbeat=et(day, "11:00") - timedelta(seconds=20),
               symbol="NQZ6")  # fmt: skip
    db.set_source(sid, "tradovate")
    db.trade(sid, JEV, day, "09:40", "09:50", 125.0)
    db.trade(sid, JEV, day, "10:00", "10:20", -25.0, side="short", reason="stop")
    db.trade(sid, JEV, day, "10:30", None, 0.0)  # open: not counted
    db.trade(sid, "imbalance", day, "09:35", "09:50", 60.0)  # same exit second as Jev's first
    db.trade(sid, "random", day, "09:45", "10:05", -40.0)
    db.trade(sid, "hold", day, "09:30", None, 0.0)
    for i in range(8):
        db.decision(sid, JEV, day, "10:59", 1, ms=110.0 + i, sec=i * 5,
                    error="TimeoutError: jev" if i == 3 else None)  # fmt: skip


# --- the three days ---------------------------------------------------------------------------


def test_running_day(db: PaperDB, token_status: Path, launchd) -> None:
    seed_live_day(db)
    now = et(TUE, "11:00")
    write_token_status(token_status, expires_at=(now + timedelta(minutes=50)).isoformat(),
                       checked_at=now.isoformat(), consecutive_failures=0)  # fmt: skip
    out = panel(now)
    assert out["schema"] == 1 and [v["id"] for v in out["views"]] == ["today", "history"]
    assert out["local_url"] == "http://127.0.0.1:8767/live"  # the running session's own view
    assert out["health"] == "warn"  # one call error, everything else fine
    w = widgets(out, "today")
    assert w["Jev P&L"]["value"] == 100.0 and w["Jev P&L"]["health"] == "ok"
    assert w["Jev P&L"]["hint"] == "today"
    assert w["Jev trades / wins"]["value"] == "2 / 1"
    assert w["Model calls"]["value"] == 7 and w["Call errors"]["value"] == 1
    assert w["Call errors"]["health"] == "warn"
    assert w["Calls mode"]["value"] == "live model calls"
    assert w["Session"]["value"] == "running" and "heartbeat 20s ago" in w["Session"]["hint"]
    assert w["Token expires in"]["value"] == 3000 and w["Token expires in"]["format"] == "duration"
    cum = w["cum-pnl"]
    # hold has no closed trade yet, so no line
    assert cum["kind"] == "line" and [s["name"] for s in cum["series"]] == [JEV, "imbalance", "random"]
    x = cum["x"]["values"]
    assert x == sorted(set(x)) and x[0] == int(et(TUE, "09:30").timestamp())
    lines = {s["name"]: s["values"] for s in cum["series"]}
    assert lines[JEV] == [0.0, 125.0, 125.0, 100.0]  # 09:30, 09:50, 10:05, 10:20
    assert lines["imbalance"] == [0.0, 60.0, 60.0, 60.0] and lines["random"][-1] == -40.0
    rows = w["trades"]["rows"]
    assert [r["t"] for r in rows] == ["10:20", "10:05", "09:50", "09:50"]  # newest first
    assert rows[0] == {"t": "10:20", "policy": JEV, "side": "short", "pts": -1.25, "pnl": -25.0,
                       "why": "stop"}  # fmt: skip
    items = {i["name"]: i for i in w["agents"]["items"]}
    assert set(items) == {"com.tradejev.paper", "com.tradejev.view", "com.tradejev.tvtoken",
                          "data feed (tradovate)"}  # fmt: skip
    assert items["com.tradejev.paper"]["state"] == "up"
    assert items["data feed (tradovate)"] == {"name": "data feed (tradovate)", "state": "up",
                                              "detail": "heartbeat 20s ago"}  # fmt: skip
    labels = [c[1].rsplit("/", 1)[1] for c in launchd.calls]
    assert sorted(labels) == ["com.tradejev.paper", "com.tradejev.tvtoken", "com.tradejev.view"]
    h = widgets(out, "history")
    assert h["sessions"]["rows"][0]["status"] == {"label": "running", "tone": "ok"}


def test_crashed_day(db: PaperDB) -> None:
    seed_day(db, "s0", MON)
    db.session("s1", TUE, "crashed", error="BentoError: no license\nTraceback (most recent ...)")
    db.trade("s1", JEV, TUE, "09:40", "09:45", -60.0)
    out = panel(et(TUE, "11:00"))
    assert out["health"] == "bad" and out["local_url"] == "http://127.0.0.1:8765/paper"
    w = widgets(out, "today")
    assert w["Session"]["value"] == "crashed" and w["Session"]["health"] == "bad"
    assert w["Session"]["hint"] == "BentoError: no license"
    assert w["Jev P&L"]["value"] == -60.0 and w["Jev P&L"]["health"] == "bad"
    assert {i["name"]: i["state"] for i in w["agents"]["items"]}["data feed"] == "unknown"
    rows = widgets(out, "history")["sessions"]["rows"]
    assert [r["day"] for r in rows] == ["Tue 11/3", "Mon 11/2"]
    assert rows[0]["status"] == {"label": "crashed", "tone": "bad"}
    assert rows[0]["error"] == "BentoError: no license" and rows[0]["jev"] == -60.0
    assert rows[1]["jev"] == 420.0 and rows[1]["trades"] == 6 and rows[1]["calls"] == 300
    daily = widgets(out, "history")["daily-pnl"]
    assert daily["kind"] == "bar" and daily["x"] == {"type": "category", "values": ["Mon 11/2", "Tue 11/3"]}
    jev = next(s for s in daily["series"] if s["name"] == "Jev+filters")
    assert jev["values"] == [420.0, None]  # Tue's Jev is jev[raw_l10]: a different policy name


def test_no_session_weekend(db: PaperDB, launchd) -> None:
    seed_day(db, "s1", "2026-11-06")  # Friday
    launchd.state = "not running"
    out = panel(et(SAT, "12:00"))
    w = widgets(out, "today")
    assert w["Session"]["value"] == "no session"
    assert w["Session"]["hint"] == "market closed; last Fri 11/6"
    assert w["Jev P&L"]["value"] == 420.0 and w["Jev P&L"]["hint"] == "last session Fri 11/6"
    assert w["Calls mode"]["value"] == "no session today" and w["Model calls"]["value"] == 300
    assert w["cum-pnl"]["title"] == "Cumulative P&L, Fri 11/6"
    assert w["trades"]["title"] == "Recent trades, Fri 11/6 (8 closed)"
    items = {i["name"]: i for i in w["agents"]["items"]}
    assert items["com.tradejev.paper"]["state"] == "up"  # idle on a weekend is fine
    assert items["com.tradejev.view"]["state"] == "warn"  # the always-on viewer isn't running
    assert items["data feed"]["state"] == "unknown"
    assert out["health"] == "warn"


def test_missing_session_in_market_hours_is_bad(db: PaperDB) -> None:
    seed_day(db, "s1", MON)
    out = panel(et(TUE, "10:00"))
    assert out["health"] == "bad"
    assert widgets(out, "today")["Session"]["hint"] == "not started; last Mon 11/2"


def test_no_db_at_all(data_dir: Path) -> None:
    out = panel(et(TUE, "10:00"))
    w = widgets(out, "today")
    assert w["Jev P&L"]["value"] is None and w["cum-pnl"]["type"] == "text"
    assert "trades" not in w
    assert widgets(out, "history")["none"]["text"] == "no paper sessions yet."
    assert w["Tradovate token"]["value"] == "n/a"


def test_token_problems_drive_health(db: PaperDB, token_status: Path) -> None:
    seed_day(db, "s1", MON)
    now = et(MON, "17:00")
    write_token_status(token_status, expires_at=(now - timedelta(minutes=1)).isoformat())
    out = panel(now)
    assert out["health"] == "bad"
    t = widgets(out, "today")["Token expires in"]
    assert t["value"] == 0 and t["health"] == "bad" and t["hint"] == "expired"
    token_status.write_text("{nope")
    assert widgets(panel(now), "today")["Tradovate token"]["value"] == "unreadable"


# --- helpers ----------------------------------------------------------------------------------


def test_cumulative_thins_to_exact_points() -> None:
    day = TUE
    base = int(et(day, "09:30").timestamp())
    trades = [{"policy": "jev", "exit_ns": (base + 10 * (i + 1)) * 10**9, "pnl": 1.0}
              for i in range(1000)]  # fmt: skip
    xs, ys = cli.cumulative(trades, ["jev"], day)  # type: ignore[arg-type]
    assert len(xs) == cli.PANEL_POINTS and xs[0] == base and xs[-1] == base + 10_000
    assert ys["jev"][-1] == 1000.0
    assert all(ys["jev"][i] == (xs[i] - base) / 10 for i in range(len(xs)))  # exact, not averaged


def test_chart_policies_order_and_cap() -> None:
    assert cli.chart_policies(["hold", "random", "Jev+filters", "imbalance", "zeta"]) == [
        "Jev+filters", "imbalance", "random", "hold"]  # fmt: skip
    assert cli.chart_policies(["hold"]) == ["hold"]


# --- size budget --------------------------------------------------------------------------------


def seed_heavy(db: PaperDB) -> None:
    """60 trading days (two sessions each: a crash with a long traceback, then a full day) and a
    final day with 3000 closed trades: the worst case for the 24 KB budget."""
    from datetime import date

    d, n = date(2026, 8, 3), 0
    while n < 60:
        if d.weekday() < 5:
            day = d.isoformat()
            db.session(f"c{n}", day, "crashed", started="09:20", error="X" * 400 + "\nTrace " * 50)
            seed_day(db, f"s{n}", day)
            n += 1
        d += timedelta(days=1)
    db.conn.executemany(
        "INSERT INTO paper_trades(session_id, policy, side, entry_ns, entry_px, exit_ns, exit_px,"
        " reason, points, pnl) VALUES (?,?,?,?,?,?,?,?,?,?)",
        [(f"s{n - 1}", ("random", "imbalance", JEV)[i % 3], "long", 0, 1.0,
          int(et(day, "09:31").timestamp() * 1e9) + i * 7 * 10**9, 1.0, "some reason here", 1.0,
          float(i % 7 - 3)) for i in range(3000)],
    )  # fmt: skip
    db.conn.commit()


def test_heavy_history_stays_under_the_budget(db: PaperDB) -> None:
    seed_heavy(db)
    out = panel(et("2026-10-23", "16:30"))
    size = len(json.dumps(out["views"], separators=(",", ":")).encode())
    assert size <= MAX_BYTES - 4096, size  # headroom for core's normalised form
    cum = widgets(out, "today")["cum-pnl"]
    assert len(cum["x"]["values"]) <= cli.PANEL_POINTS
    assert len(widgets(out, "today")["trades"]["rows"]) == cli.PANEL_TRADES
    h = widgets(out, "history")
    assert len(h["daily-pnl"]["x"]["values"]) == cli.PANEL_DAYS
    assert len(h["sessions"]["rows"]) == cli.PANEL_SESSIONS


# --- the CLI contract and Resonant's own validation -------------------------------------------


def test_panel_cli_under_isolation(db: PaperDB, data_dir: Path) -> None:
    seed_day(db, "s1", MON)
    proc = run_isolated(data_dir, "panel", "paper")
    assert proc.returncode == 0, proc.stderr
    assert proc.stdout.count("\n") == 1 and json.loads(proc.stdout)["schema"] == 1
    bad = run_isolated(data_dir, "panel", "nope")
    assert bad.returncode != 0 and bad.stdout == ""


def test_panel_is_in_the_manifest() -> None:
    text = (EXT_DIR / "resonant.yaml").read_text()
    block = text.split("\npanels:\n", 1)[1].split("\n\n", 1)[0]
    assert "  - id: paper\n" in block and "command: [./cli.py, panel, paper]" in block
    assert "max_bytes: 24576" in block and "tz: America/New_York" in block


VALIDATE = """
import json, sys
from datetime import UTC, datetime
from pathlib import Path
import jsonschema
from resonant.extensions.panels import PanelRefused, normalize
from resonant_sdk.manifest import load_manifest
m = load_manifest(Path(sys.argv[1]))
(p,) = m.panels
schema = json.loads(Path(sys.argv[2]).read_text())
tools = frozenset(m.tools)
res = {"panel": {"id": p.id, "command": list(p.command), "max_bytes": p.max_bytes,
                 "idle": p.refresh.idle, "active": p.refresh.active, "tz": p.tz}, "runs": {}}
for f in sys.argv[3:]:
    raw = json.loads(Path(f).read_text())
    jsonschema.validate(raw, schema)
    n = normalize(raw, max_bytes=p.max_bytes, tools=tools, ran_at=datetime.now(UTC))
    res["runs"][Path(f).stem] = (
        {"refused": n.error if hasattr(n, "error") else str(n)} if isinstance(n, PanelRefused)
        else {"truncated": n.truncated, "bytes": len(json.dumps(n.data["views"], separators=(",", ":")))})
print(json.dumps(res))
"""


def resonant_validate(tmp_path: Path, outputs: dict[str, dict[str, Any]]) -> dict[str, Any]:
    uv = shutil.which("uv")
    schema = RESONANT_REPO / "docs" / "panel.schema.json"
    if uv is None or not schema.is_file():
        pytest.skip("needs uv and a Resonant checkout with docs/panel.schema.json (RESONANT_REPO)")
    files = []
    for name, out in outputs.items():
        f = tmp_path / f"{name}.json"
        f.write_text(json.dumps(out))
        files.append(str(f))
    proc = subprocess.run(  # noqa: S603
        [uv, "run", "--frozen", "--no-sync", "--project", str(RESONANT_REPO), "python", "-c",
         VALIDATE, str(EXT_DIR / "resonant.yaml"), str(schema), *files],
        capture_output=True, text=True, timeout=120, check=False,
    )  # fmt: skip
    if proc.returncode != 0 and "No module named" in proc.stderr:
        pytest.skip("the Resonant checkout has no synced venv")
    assert proc.returncode == 0, proc.stderr[-2000:]
    return json.loads(proc.stdout.strip().splitlines()[-1])


def test_resonant_accepts_every_output(tmp_path: Path, data_dir: Path, token_status: Path) -> None:
    """Each scenario's output passes docs/panel.schema.json and core's normalize() (the code the
    daemon runs) under the manifest's own max_bytes, untruncated; the manifest parses with the panel."""
    outputs: dict[str, dict[str, Any]] = {"empty": panel(et(TUE, "10:00"))}
    db = PaperDB(tmp_path / "paper.db")
    seed_day(db, "f", "2026-11-06")
    outputs["weekend"] = panel(et(SAT, "12:00"))
    db.session("c", MON, "crashed", error="BentoError: no license")
    outputs["crashed"] = panel(et(MON, "11:00"))
    seed_live_day(db)
    write_token_status(token_status, expires_at=et(TUE, "12:00").isoformat())
    outputs["running"] = panel(et(TUE, "11:00"))
    heavy = tmp_path / "heavy"
    heavy.mkdir()
    (data_dir / "config.json").write_text(json.dumps({"db_path": str(heavy / "paper.db")}))
    seed_heavy(PaperDB(heavy / "paper.db"))
    outputs["heavy"] = panel(et("2026-10-23", "16:30"))
    res = resonant_validate(tmp_path, outputs)
    assert res["panel"] == {"id": "paper", "command": ["./cli.py", "panel", "paper"],
                            "max_bytes": MAX_BYTES, "idle": "*/15 * * * *",
                            "active": "*/1 9-16 * * 1-5", "tz": "America/New_York"}  # fmt: skip
    for name, r in res["runs"].items():
        assert "refused" not in r, (name, r)
        assert r["truncated"] is False and r["bytes"] <= MAX_BYTES, (name, r)
