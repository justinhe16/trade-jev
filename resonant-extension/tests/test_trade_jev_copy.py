"""The copy-trading parts of the extension: the `copy` panel (live and --date), the copy-watch job, the
trade_jev_copy_flatten_all tool and the manifest. The copy DB is built with the daemon's own Ledger (so the
schemas can't drift apart); the daemon is a stub HTTP server. Nothing here touches Tradovate or a real path."""

from __future__ import annotations

import http.server
import json
import socket
import sqlite3
import threading
import time
from datetime import datetime
from pathlib import Path
from typing import Any

import cli
import pytest
from conftest import EXT_DIR, RESONANT_REPO, ns
from conftest import et as et_dt
from test_trade_jev_ext import run_isolated
from test_trade_jev_panel import resonant_validate

from trade_jev.copytrade.ledger import Ledger

MON, TUE, WED, THU, FRI, SAT = ("2026-11-02", "2026-11-03", "2026-11-04", "2026-11-05", "2026-11-06", "2026-11-07")


class CopyDB:
    def __init__(self, path: Path, now: datetime) -> None:
        self.now = now
        self.ledger = Ledger(path, now_ns=lambda: int(self.now.timestamp() * 1e9))

    def status(self, name="mff-1", at: datetime | None = None, **kw: Any) -> None:
        at = at or self.now
        row = {"name": name, "day": at.astimezone(cli.ET).date().isoformat(), "updated_at": at.isoformat(),
               "mode": "demo", "env": "demo", "root": "MNQ", "qty": 5, "contract": "MNQZ6", "armed": 1,
               "gate_reason": None, "pos_side": 0, "pos_qty": 0, "avg_px": None, "realized": 0.0, "open_pnl": 0.0,
               "halted": 0, "halt_kind": None, "token_ok": 1, "token_expires_at": None, "entries": 0, "errors": 0,
               "last_error": None, **kw}
        self.ledger.status(row)

    def trade(self, day, name="mff-1", pnl=245.0, hm_in="10:00", hm_out="10:10", reason="target", side="long"):
        tid = self.ledger.trade_open(name, day, "b", 1, "MNQZ6", side, 5, ns(day, hm_in), 20000.0, 20025.0, 19950.0)
        self.ledger.trade_close(tid, ns(day, hm_out), 20025.0, reason, 25.0, 250.0, 5.0, pnl)

    def event(self, day, hm, kind, detail, name="mff-1", notify=False):
        self.now = et_dt(day, hm)
        self.ledger.event(name, kind, detail, notify=notify)

    def order(self, day, state, cl, name="mff-1"):
        self.ledger.order_new(name=name, day=day, boot="b", seq=1, role="entry", cl_ord_id=cl, state=state)

    def halt(self, day, name="mff-1", kind="day", reason="daily loss limit"):
        self.ledger.halt(name, day, kind, reason)


@pytest.fixture
def copy_db(tmp_path: Path, data_dir: Path) -> CopyDB:
    cfg = json.loads((data_dir / "config.json").read_text())
    (data_dir / "config.json").write_text(json.dumps({**cfg, "copy_db_path": str(tmp_path / "copy.db"),
                                                      "copy_accounts_path": str(tmp_path / "accounts.yaml")}))
    return CopyDB(tmp_path / "copy.db", et_dt(TUE, "11:00"))


def panel(now: datetime, *extra: str) -> dict[str, Any]:
    return cli.run(["panel", "copy", *extra], now=now)


def widgets(out: dict[str, Any], view: str = "today") -> dict[str, dict[str, Any]]:
    (v,) = [v for v in out["views"] if v["id"] == view]
    return {w.get("id") or w["label"]: w for w in v["widgets"]}


NOW = et_dt(TUE, "11:00")


def seed_live(db: CopyDB) -> None:
    db.status("mff-1", pos_side=1, pos_qty=5, avg_px=20001.25, realized=245.0, open_pnl=-30.0, entries=2,
              token_expires_at=et_dt(TUE, "12:00").isoformat())
    db.status("mff-2", mode="dry_run", gate_reason=None, token_ok=1)
    db.trade(TUE, pnl=245.0)
    db.event(TUE, "10:20", "missed", "no fill at the limit", notify=False)
    db.event(TUE, "10:40", "rejected", "RiskCheckFailed: no buying power")
    db.halt(TUE, "mff-2", reason="3 rejects in a row")


# --- the live panel ---------------------------------------------------------------------------


def test_live_panel(copy_db: CopyDB) -> None:
    seed_live(copy_db)
    out = panel(NOW)
    assert out["schema"] == 1 and [(v["id"], v["title"]) for v in out["views"]] == [("today", "Today, Tue 11/3")]
    w = widgets(out)
    assert w["pnl"]["value"] == 215.0 and w["pnl"]["format"] == "money"
    rows = {r["name"]: r for r in w["accounts"]["rows"]}
    assert rows["mff-1"]["pos"] == "+5 MNQZ6 @ 20001.25" and rows["mff-1"]["pnl"] == 215.0
    assert rows["mff-1"]["entries"] == 2 and rows["mff-1"]["state"] == {"label": "armed", "tone": "ok"}
    assert rows["mff-2"]["pos"] == "flat" and rows["mff-2"]["mode"] == "dry_run/demo"
    assert [r["kind"]["label"] for r in w["missed"]["rows"]] == ["rejected", "missed"]     # newest first
    assert w["halts"]["rows"][0]["reason"] == "3 rejects in a row" and w["halts"]["rows"][0]["state"]["label"] == "active"
    assert w["trades"]["rows"][0]["pnl"] == 245.0 and w["trades"]["rows"][0]["why"] == "target"
    states = {i["name"]: i for i in w["tokens"]["items"]}
    assert states["mff-1"]["state"] == "up" and "12:00" in states["mff-1"]["detail"]
    assert states["mff-2"]["state"] == "unknown"
    assert out["health"] == "ok"


def test_halted_and_gated_states(copy_db: CopyDB) -> None:
    copy_db.status("a", halted=1, halt_kind="day")
    copy_db.status("b", gate_reason="kill: paused")
    copy_db.status("c", token_ok=0, last_error="token expired", gate_reason="token")
    out = panel(NOW)
    rows = {r["name"]: r["state"] for r in widgets(out)["accounts"]["rows"]}
    assert rows["a"] == {"label": "halted: day", "tone": "bad"} and rows["b"]["tone"] == "warn"
    assert out["health"] == "bad"
    tok = {i["name"]: i for i in widgets(out)["tokens"]["items"]}
    assert tok["c"]["state"] == "down" and "token expired" in tok["c"]["detail"]


def test_stale_status_means_the_daemon_is_down(copy_db: CopyDB) -> None:
    copy_db.status("a", at=et_dt(TUE, "10:50"))
    out = panel(NOW)
    assert out["health"] == "bad" and "daemon down" in widgets(out)["down"]["text"]
    assert widgets(out)["accounts"]["rows"][0]["state"] == {"label": "daemon down", "tone": "bad"}
    copy_db.status("a", at=et_dt(TUE, "10:59"))
    assert panel(NOW)["health"] == "ok"


def test_no_db_or_no_accounts_is_a_text_widget(data_dir: Path, tmp_path: Path) -> None:
    out = panel(NOW)
    assert out["views"][0]["widgets"] == [{"type": "text", "text": "no copy accounts", "id": "none"}]
    assert "health" not in out
    (tmp_path / "x.db").write_bytes(b"")
    cfg = json.loads((data_dir / "config.json").read_text())
    (data_dir / "config.json").write_text(json.dumps({**cfg, "copy_db_path": str(tmp_path / "x.db")}))
    assert panel(NOW)["views"][0]["widgets"][0]["text"] == "no copy accounts"      # an empty / foreign file
    Ledger(tmp_path / "y.db")
    (data_dir / "config.json").write_text(json.dumps({**cfg, "copy_db_path": str(tmp_path / "y.db")}))
    assert panel(NOW)["views"][0]["widgets"][0]["text"] == "no copy accounts"      # the ledger with no status rows


# --- --date ----------------------------------------------------------------------------------


def test_past_day_view_from_trades_orders_events_and_halts(copy_db: CopyDB) -> None:
    copy_db.trade(MON, "a", pnl=245.0)
    copy_db.trade(MON, "a", pnl=-505.0, hm_in="11:00", hm_out="11:30", reason="stop")
    copy_db.trade(MON, "b", pnl=100.0)
    copy_db.order(MON, "missed", "x-1", "a")
    copy_db.order(MON, "rejected", "x-2", "b")
    copy_db.order(MON, "filled", "x-3", "b")
    copy_db.event(MON, "10:20", "missed", "no fill", "a")
    copy_db.halt(MON, "a", reason="daily loss limit")
    out = panel(et_dt(FRI, "12:00"), "--date", MON)
    assert [(v["id"], v["title"]) for v in out["views"]] == [("today", "Mon 11/2")]
    w = widgets(out)
    assert w["pnl"]["value"] == -160.0
    rows = {r["name"]: r for r in w["accounts"]["rows"]}
    assert (rows["a"]["trades"], rows["a"]["pnl"], rows["a"]["missed"]) == (2, -260.0, 1)
    assert (rows["b"]["trades"], rows["b"]["pnl"], rows["b"]["rejected"]) == (1, 100.0, 0 + 1)
    assert len(w["trades"]["rows"]) == 3 and w["halts"]["rows"][0]["name"] == "a"
    assert out["health"] == "bad" and out["updated_at"].startswith("2026-11-02")


def test_a_day_with_no_rows_a_future_day_and_no_db_are_none(copy_db: CopyDB, tmp_path: Path) -> None:
    copy_db.trade(MON)
    assert panel(et_dt(FRI, "12:00"), "--date", TUE) == {"schema": 1, "none": True}
    assert panel(et_dt(FRI, "12:00"), "--date", "2026-11-09") == {"schema": 1, "none": True}
    assert panel(et_dt(FRI, "12:00"), "--date", SAT) == {"schema": 1, "none": True}
    cfg = {"db_path": str(tmp_path / "paper.db"), "copy_db_path": str(tmp_path / "nope.db")}
    (Path(cli.data_dir()) / "config.json").write_text(json.dumps(cfg))
    assert panel(et_dt(FRI, "12:00"), "--date", MON) == {"schema": 1, "none": True}


def test_todays_date_renders_the_live_panel(copy_db: CopyDB) -> None:
    seed_live(copy_db)
    out = panel(NOW, "--date", TUE)
    assert out["views"][0]["title"] == "Today, Tue 11/3" and "pnl" in widgets(out)


def test_bad_date_is_a_clean_failure(copy_db: CopyDB) -> None:
    with pytest.raises(cli.Fail):
        panel(NOW, "--date", "yesterday")


def test_wal_db_without_side_files_fails_so_the_day_is_retried(tmp_path: Path, data_dir: Path) -> None:
    d = tmp_path / "ro"
    d.mkdir()
    path = d / "copy.db"
    db = CopyDB(path, NOW)
    db.trade(MON)
    db.ledger.close()
    sqlite3.connect(path).execute("PRAGMA journal_mode=WAL").fetchone()
    (data_dir / "config.json").write_text(json.dumps({"copy_db_path": str(path)}))
    for side in ("copy.db-wal", "copy.db-shm"):
        (d / side).unlink(missing_ok=True)
    d.chmod(0o555)
    try:
        proc = run_isolated(data_dir, "panel", "copy", "--date", MON)
    finally:
        d.chmod(0o755)
    if proc.returncode == 0:
        pytest.skip("sqlite opened the WAL DB read-only without side files")
    assert proc.returncode == 1 and proc.stdout == "" and proc.stderr.strip() == f"trade-jev: {cli.COPY_CANT_READ}"


def test_the_panel_fits_its_byte_budget_with_busy_days(copy_db: CopyDB, tmp_path: Path) -> None:
    for i in range(3):
        copy_db.status(f"acct-{i}", pos_side=1, pos_qty=5, avg_px=20000.0, token_expires_at=NOW.isoformat())
        for j in range(40):
            copy_db.trade(TUE, f"acct-{i}", pnl=float(j), hm_in="10:00", hm_out="10:05")
            copy_db.event(TUE, "10:20", "missed", "x" * 200, f"acct-{i}")
        copy_db.halt(TUE, f"acct-{i}", reason="y" * 400)
    for out in (panel(NOW), panel(et_dt(FRI, "12:00"), "--date", TUE)):
        assert len(json.dumps(out, separators=(",", ":"))) < 24576 - 4096


def test_resonant_accepts_every_copy_output(tmp_path: Path, copy_db: CopyDB) -> None:
    seed_live(copy_db)
    copy_db.trade(MON)
    outs = {"live": panel(NOW), "past": panel(et_dt(FRI, "12:00"), "--date", MON)}
    schema = RESONANT_REPO / "docs" / "panel.schema.json"
    if schema.is_file() and "none" in json.loads(schema.read_text()).get("properties", {}):   # PER-286
        outs["none"] = panel(et_dt(FRI, "12:00"), "--date", WED)
    res = resonant_validate(tmp_path, outs, "copy")
    assert res["panel"]["command"] == ["./cli.py", "panel", "copy"] and res["panel"]["max_bytes"] == 24576
    assert res["panel"]["tz"] == "America/New_York" and res["panel"]["active"] == "*/1 9-16 * * 1-5"
    for name, r in res["runs"].items():
        assert "refused" not in r, (name, r)
        assert r.get("truncated") in (False, None), (name, r)


def test_panel_cli_under_isolation(copy_db: CopyDB, data_dir: Path) -> None:
    seed_live(copy_db)
    proc = run_isolated(data_dir, "panel", "copy")
    assert proc.returncode == 0, proc.stderr
    assert proc.stdout.count("\n") == 1 and json.loads(proc.stdout)["views"][0]["id"] == "today"


# --- copy-watch -------------------------------------------------------------------------------


def watch(now: datetime) -> dict[str, Any]:
    return cli.run(["copy-watch"], now=now)


def test_events_are_reported_once_and_deduped_by_id(copy_db: CopyDB, reports: list[dict[str, Any]]) -> None:
    copy_db.event(MON, "10:00", "halt", "yesterday's old news", notify=True)          # first run: today's only
    copy_db.event(TUE, "10:30", "halt", "day: daily loss limit hit", notify=True)
    copy_db.event(TUE, "10:31", "missed", "not a text", notify=False)
    out = watch(NOW)
    assert out["events"] == {"checked": True, "reported": 1}
    (r,) = reports
    assert r["key"] == f"trade-jev/copy/mff-1/halt/{TUE}" and r["urgency"] == "urgent"
    assert r["title"] == "Copy mff-1: day: daily loss limit hit" and r["state"] == {"id": 2}
    assert watch(NOW)["events"]["reported"] == 0 and len(reports) == 1                # nothing new
    copy_db.event(TUE, "10:50", "flattened", "heartbeat lost", name="Mff 2", notify=True)
    assert watch(NOW)["events"]["reported"] == 1
    assert reports[-1]["key"] == f"trade-jev/copy/mff-2/flattened/{TUE}"
    st = json.loads((cli.data_dir() / cli.WATCH_STATE).read_text())
    assert st["copy"]["last_id"] == 4


def test_a_report_key_is_always_legal(copy_db: CopyDB, reports: list[dict[str, Any]]) -> None:
    from emit import report as real_report  # the SDK's own validation of key and state

    copy_db.event(TUE, "10:30", "weird/kind!", "x" * 400, name="Ünï Name!!", notify=True)
    cli.report = lambda key, title, **kw: (real_report(key, title, **kw), True)[1]
    try:
        assert watch(NOW)["events"]["reported"] == 1
    finally:
        cli.report = real_report


def test_copy_watch_leaves_the_other_watch_state_alone(copy_db: CopyDB, reports: list[dict[str, Any]]) -> None:
    cli._write_state(cli.data_dir() / cli.WATCH_STATE, {"down": {"day": TUE, "reason": "x"}, "token": None})
    copy_db.event(TUE, "10:30", "halt", "h", notify=True)
    watch(NOW)
    st = json.loads((cli.data_dir() / cli.WATCH_STATE).read_text())
    assert st["down"] == {"day": TUE, "reason": "x"} and st["copy"]["last_id"] == 1


def test_no_db_is_quiet(data_dir: Path, reports: list[dict[str, Any]]) -> None:
    assert watch(NOW)["events"] == {"checked": False, "reason": "no copy.db"} and reports == []


def enable(tmp_path: Path, text: str = "accounts:\n  - name: a\n    enabled: true\n") -> None:
    (tmp_path / "accounts.yaml").write_text(text)


def test_daemon_down_alert_during_trading_hours_and_clear(copy_db: CopyDB, tmp_path: Path,
                                                         reports: list[dict[str, Any]]) -> None:
    enable(tmp_path)
    copy_db.status("a", at=et_dt(TUE, "10:00"))                      # last published 1 h ago
    out = watch(NOW)
    assert out["daemon"] == {"checked": True, "down": True}
    (r,) = reports
    assert r["key"] == f"trade-jev/copy/daemon/down/{TUE}" and r["urgency"] == "urgent"
    copy_db.status("a", at=NOW)
    assert watch(NOW)["daemon"] == {"checked": True, "down": False, "cleared": True}
    assert reports[-1]["clear"] is True and reports[-1]["key"] == r["key"]
    assert watch(NOW)["daemon"] == {"checked": True, "down": False} and len(reports) == 2


def test_daemon_down_when_it_never_published(data_dir: Path, tmp_path: Path, reports: list[dict[str, Any]]) -> None:
    cfg = json.loads((data_dir / "config.json").read_text())
    (data_dir / "config.json").write_text(json.dumps({**cfg, "copy_accounts_path": str(tmp_path / "accounts.yaml")}))
    enable(tmp_path)
    assert watch(NOW)["daemon"] == {"checked": True, "down": True}


def test_daemon_check_is_skipped_off_hours_and_without_enabled_accounts(copy_db: CopyDB, tmp_path: Path,
                                                                       reports: list[dict[str, Any]]) -> None:
    enable(tmp_path)
    assert watch(et_dt(TUE, "08:00"))["daemon"]["checked"] is False
    assert watch(et_dt(TUE, "16:30"))["daemon"]["checked"] is False
    assert watch(et_dt(SAT, "11:00"))["daemon"]["checked"] is False
    enable(tmp_path, "accounts:\n  - name: a\n    enabled: false\n# enabled: true\n")
    assert watch(NOW)["daemon"] == {"checked": False, "summary": "no enabled copy accounts"}
    (tmp_path / "accounts.yaml").unlink()
    assert watch(NOW)["daemon"]["checked"] is False and reports == []


def test_the_two_checks_are_isolated(copy_db: CopyDB, tmp_path: Path, reports: list[dict[str, Any]],
                                    monkeypatch: pytest.MonkeyPatch) -> None:
    enable(tmp_path)
    copy_db.event(TUE, "10:30", "halt", "h", notify=True)

    def boom(*a: Any, **k: Any) -> None:
        raise RuntimeError("x")
    monkeypatch.setattr(cli, "_copy_events_check", boom)
    out = watch(NOW)
    assert out["events"] == {"error": "RuntimeError"} and out["daemon"]["down"] is True and len(reports) == 1


# --- trade_jev_copy_flatten_all ----------------------------------------------------------------


class Daemon:
    """A stub of the copy daemon's POST /flatten-all."""

    def __init__(self, status=200, body=None, delay=0.0):
        self.seen: list[dict[str, Any]] = []
        outer = self

        class H(http.server.BaseHTTPRequestHandler):
            def log_message(self, *a: Any) -> None:
                pass

            def do_POST(self) -> None:
                outer.seen.append({"path": self.path, "ctype": self.headers.get("Content-Type"),
                                   "hdr": self.headers.get(cli.COPY_HEADER), "host": self.headers.get("Host")})
                self.rfile.read(int(self.headers.get("Content-Length", 0)))
                time.sleep(delay)
                data = json.dumps(body if body is not None else {"ok": True, "accounts": {"a": "flat", "b": "flat"}})
                try:
                    self.send_response(status)
                    self.send_header("Content-Length", str(len(data)))
                    self.end_headers()
                    self.wfile.write(data.encode())
                except OSError:
                    pass

        self.srv = http.server.ThreadingHTTPServer(("127.0.0.1", 0), H)
        self.port = self.srv.server_address[1]
        threading.Thread(target=self.srv.serve_forever, daemon=True).start()

    def close(self) -> None:
        self.srv.shutdown()
        self.srv.server_close()


def point_at(data_dir: Path, port: int) -> None:
    cfg = json.loads((data_dir / "config.json").read_text())
    (data_dir / "config.json").write_text(json.dumps({**cfg, "copy_port": port}))


def free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def test_flatten_all_success_posts_with_its_header(data_dir: Path) -> None:
    d = Daemon()
    try:
        point_at(data_dir, d.port)
        out = cli.run(["copy-flatten-all"], now=NOW)
    finally:
        d.close()
    assert out["flattened"] is True and out["accounts"] == {"a": "flat", "b": "flat"}
    assert "flattened and halted for today: a, b" == out["summary"]
    assert d.seen == [{"path": "/flatten-all", "ctype": "application/json", "hdr": "flatten-all",
                       "host": f"127.0.0.1:{d.port}"}]


def test_flatten_all_reports_an_account_that_did_not_confirm_flat(data_dir: Path) -> None:
    d = Daemon(body={"ok": True, "accounts": {"a": "flat", "b": "halted: still in a position"}})
    try:
        point_at(data_dir, d.port)
        out = cli.run(["copy-flatten-all"], now=NOW)
    finally:
        d.close()
    assert out["flattened"] is False and "b halted: still in a position" in out["summary"]


def test_flatten_all_daemon_down_is_exit_1_with_the_fallback(data_dir: Path) -> None:
    point_at(data_dir, free_port())
    with pytest.raises(cli.Fail, match="not reachable.*flatten-all --direct"):
        cli.run(["copy-flatten-all"], now=NOW)
    proc = run_isolated(data_dir, "copy-flatten-all")
    assert proc.returncode == 1 and proc.stdout == "" and proc.stderr.count("\n") == 1


def test_flatten_all_timeout_may_have_gone_through_so_it_is_unknown_exit_0(
        data_dir: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    d = Daemon(delay=1.0)
    monkeypatch.setattr(cli, "COPY_FLATTEN_TIMEOUT_S", 0.3)
    try:
        point_at(data_dir, d.port)
        out = cli.run(["copy-flatten-all"], now=NOW)
    finally:
        d.close()
    assert out["flattened"] == "unknown" and "timed out" in out["summary"]


def test_flatten_all_http_refusal_and_garbage_are_failures(data_dir: Path) -> None:
    for status, body in ((403, {"error": "missing header"}), (200, {"ok": True})):
        d = Daemon(status=status, body=body)
        try:
            point_at(data_dir, d.port)
            with pytest.raises(cli.Fail):
                cli.run(["copy-flatten-all"], now=NOW)
        finally:
            d.close()


def test_flatten_all_ignores_proxy_environment(data_dir: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    d = Daemon()
    monkeypatch.setenv("http_proxy", "http://127.0.0.1:9")
    monkeypatch.setenv("HTTP_PROXY", "http://127.0.0.1:9")
    try:
        point_at(data_dir, d.port)
        assert cli.run(["copy-flatten-all"], now=NOW)["flattened"] is True
    finally:
        d.close()


# --- config and manifest ------------------------------------------------------------------------


def test_config_defaults_and_validation(data_dir: Path) -> None:
    cfg = cli.load_config()
    assert str(cfg.copy_db_path) == "/Users/justinhe/trade-jev/data/copy.db" and cfg.copy_port == 8768
    for bad in ({"copy_port": 0}, {"copy_port": "x"}, {"copy_db_path": ""}, {"copy_accounts_path": 3}):
        (data_dir / "config.json").write_text(json.dumps(bad))
        with pytest.raises(cli.Fail):
            cli.load_config()


def test_manifest_declares_the_copy_tool_job_and_panel() -> None:
    text = (EXT_DIR / "resonant.yaml").read_text()
    tool = text.split("  trade_jev_copy_flatten_all:\n", 1)[1].split("\n\n", 1)[0].split("\njobs:", 1)[0]
    assert "run: ./cli.py copy-flatten-all" in tool and "effect: write" in tool and "level: L2" in tool
    job = text.split("  - id: copy-watch\n", 1)[1].split("\n\n", 1)[0]
    assert 'schedule: "*/5 * * * *"' in job and "effect: read" in job and "level: L0" in job
    assert "run: ./cli.py copy-watch" in job and "tz: America/New_York" in job
    block = text.split("\npanels:\n", 1)[1].split("\n\n", 1)[0]
    body = block.split("  - id: copy\n", 1)[1]
    assert "title: Copy accounts" in body and "command: [./cli.py, panel, copy]" in body
    assert 'idle: "*/15 * * * *"' in body and 'active: "*/1 9-16 * * 1-5"' in body
    assert "history: {every: daily, keep: 365d}" in body and "max_bytes: 24576" in body
    assert "  - label: com.tradejev.copy\n" in text
    assert "health: {check: ./cli.py health, every: 5m}" in text and "recover: {run: ./cli.py recover" in text
    assert "max_urgency: urgent" in text


def test_the_extension_stays_stdlib_only() -> None:
    import ast

    stdlib = set(__import__("sys").stdlib_module_names) | {"panel", "emit"}
    for f in ("cli.py", "panel.py", "emit.py"):
        tree = ast.parse((EXT_DIR / f).read_text())
        mods = {n.names[0].name.split(".")[0] for n in ast.walk(tree) if isinstance(n, ast.Import)}
        mods |= {n.module.split(".")[0] for n in ast.walk(tree) if isinstance(n, ast.ImportFrom) and n.module}
        assert mods <= stdlib, (f, mods - stdlib)
