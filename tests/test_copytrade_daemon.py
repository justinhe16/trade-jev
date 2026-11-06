import asyncio
import http.client
import json
import socket
import sqlite3
import time

import pytest

from trade_jev.copytrade import config as cfg
from trade_jev.copytrade.daemon import (FLATTEN_ALL_HEADER, AlreadyRunning, Daemon, db_kill_status, kill_state,
                                        lock_held, take_lock)
from trade_jev.copytrade.ledger import Ledger
from trade_jev.data import et_to_ns
from trade_jev.executor.broker import FakeBroker, Position

BOOT = "daemonboot" * 2
T0 = et_to_ns("2026-10-05", "10:00:00")
M0 = time.monotonic()


def now_ns():
    return T0 + int((time.monotonic() - M0) * 1e9)


def acct(name, aid, **kw):
    base = dict(name=name, enabled=True, mode="demo", env="demo", account_id=aid, root="MNQ", qty=5,
                daily_loss_limit=1000.0, daily_profit_stop=490.0, commission=0.5, secrets_prefix=f"COPY_{name.upper()}")
    base.update(kw)
    return cfg.Account(**base)


class Kill:
    """Scriptable kill sources: the API and the DB."""

    def __init__(self):
        self.api = {"paused": False, "autonomy": "on"}
        self.db = {"paused": False, "autonomy": "on"}

    def read_api(self):
        if self.api is None:
            raise OSError("api down")
        return self.api

    def read_db(self):
        if self.db is None:
            raise OSError("db unreadable")
        return self.db


class StubKeepalive:
    def __init__(self, ok=True):
        self.ok = ok

    def health(self):
        return {"ok": self.ok, "expires_at": None, "error": None if self.ok else "token expired"}

    async def run(self):
        await asyncio.sleep(3600)


class Rig:
    def __init__(self, tmp_path, names=("a1", "a2"), brokers=None, keepalives=None):
        self.ledger = Ledger(tmp_path / "copy.db", now_ns=now_ns)
        self.brokers = brokers or {n: FakeBroker() for n in names}
        self.kill = Kill()
        self.cfg = cfg.CopyConfig(tuple(acct(n, 11 + i, secrets_prefix=f"COPY_{n.upper()}")
                                        for i, n in enumerate(names)))
        self.keepalives = keepalives or {}
        self.d = Daemon(self.cfg, self.ledger, lambda a, ka: self.brokers[a.name],
                        (lambda a: self.keepalives[a.name]) if self.keepalives else None,
                        kill_api=self.kill.read_api, kill_db=self.kill.read_db, kill_poll_s=0.02, publish_s=0.05,
                        tick_s=0.02, now_ns=now_ns)

    def ex(self, n):
        return self.d.workers[n].ex

    def hb(self, seq_last=0, target=0, status="LIVE"):
        self.d.receive("heartbeat", {"type": "heartbeat", "boot": BOOT, "seq_last": seq_last, "target": target,
                                     "status": status, "mode": "live", "bid": 20000.0, "ask": 20000.25})

    def intent(self, seq, target):
        t = now_ns()
        self.d.receive("intent", {"type": "intent", "seq": seq, "boot": BOOT, "target": target, "reason": "jev",
                                  "mode": "live", "symbol": "NQZ6", "t_ns": t, "sent_wall_ns": t,
                                  "ref": {"bid": 20000.0, "ask": 20000.25}, "stop_ticks": 200, "target_ticks": 100,
                                  "armed": False, "catch_up": False})


async def until(cond, timeout=3.0):
    end = time.monotonic() + timeout
    while time.monotonic() < end:
        if cond():
            return True
        await asyncio.sleep(0.01)
    return cond()


def go(rig, scenario):
    async def main():
        await rig.d.start()
        try:
            await scenario()
        finally:
            await rig.d.stop()
    asyncio.run(main())


async def ready(rig):
    assert await until(lambda: all(w.ex.clean for w in rig.d.workers.values()))
    assert await until(lambda: rig.d.kill != "unknown" and all(w.ex.kill == rig.d.kill for w in rig.d.workers.values()))
    rig.hb()
    await asyncio.sleep(0.1)


# ---------------------------------------------------------------- isolation between accounts

def test_normal_flow_both_accounts_mirror_the_intent(tmp_path):
    rig = Rig(tmp_path)

    async def scenario():
        await ready(rig)
        rig.intent(1, 1)
        assert await until(lambda: all(b.pos.side == 1 for b in rig.brokers.values()))
        rig.intent(2, 0)
        assert await until(lambda: all(b.pos.side == 0 for b in rig.brokers.values()))
    go(rig, scenario)


def test_one_account_raising_leaves_the_others_trading(tmp_path):
    rig = Rig(tmp_path)

    async def boom(*a):
        raise ValueError("bug in a1")
    rig.brokers["a1"].enter = boom

    async def scenario():
        await ready(rig)
        rig.intent(1, 1)
        assert await until(lambda: rig.brokers["a2"].pos.side == 1)
        assert await until(lambda: rig.ex("a1").halted is not None)
        assert "worker crashed" in rig.ex("a1").halted and rig.d.workers["a1"].crashes == 1
        rig.intent(2, 0)
        assert await until(lambda: rig.brokers["a2"].pos.side == 0)      # a2 carries on
        rig.intent(3, 1)
        assert await until(lambda: rig.brokers["a2"].pos.side == 1)
        assert rig.brokers["a1"].pos.side == 0                           # a1 is halted, still alive
    go(rig, scenario)


def test_one_account_hanging_leaves_the_others_trading(tmp_path):
    rig = Rig(tmp_path)

    async def hang(*a):
        await asyncio.sleep(3600)
    rig.brokers["a1"].enter = hang

    async def scenario():
        await ready(rig)
        rig.intent(1, 1)
        assert await until(lambda: rig.brokers["a2"].pos.side == 1)
        rig.intent(2, 0)
        assert await until(lambda: rig.brokers["a2"].pos.side == 0)
        rig.intent(3, 1)
        assert await until(lambda: rig.brokers["a2"].pos.side == 1)
    go(rig, scenario)


def test_one_account_token_dead_leaves_the_others_trading(tmp_path):
    rig = Rig(tmp_path, keepalives={"a1": StubKeepalive(ok=False), "a2": StubKeepalive()})

    async def scenario():
        await ready(rig)
        rig.d.publish_once()
        rig.intent(1, 1)
        assert await until(lambda: rig.brokers["a2"].pos.side == 1)
        assert rig.brokers["a1"].pos.side == 0
        row = {r["name"]: r for r in rig.ledger.statuses()}
        assert row["a1"]["token_ok"] == 0 and row["a1"]["gate_reason"] == "token" and row["a2"]["token_ok"] == 1
        rig.intent(2, 0)
        assert await until(lambda: rig.brokers["a2"].pos.side == 0)       # exits are never gated
        rig.keepalives["a1"].ok = True                                   # the token recovers by itself
        rig.d.publish_once()
        rig.intent(3, 1)
        assert await until(lambda: rig.brokers["a1"].pos.side == 1)
    go(rig, scenario)


def test_status_rows_are_published(tmp_path):
    rig = Rig(tmp_path)

    async def scenario():
        await ready(rig)
        assert await until(lambda: len(rig.ledger.statuses()) == 2)
        assert {r["name"] for r in rig.ledger.statuses()} == {"a1", "a2"}
        assert rig.d.status()["accounts"][0]["name"] == "a1"
    go(rig, scenario)


# ---------------------------------------------------------------- kill

def hold_positions(rig):
    for b in rig.brokers.values():
        b.pos = Position(1, 5, 20000.0)


def test_kill_api_paused_flattens_all_accounts(tmp_path):
    rig = Rig(tmp_path)

    async def scenario():
        await ready(rig)
        rig.intent(1, 1)
        assert await until(lambda: all(b.pos.side == 1 for b in rig.brokers.values()))
        rig.kill.api = {"paused": True, "autonomy": "off"}
        assert await until(lambda: all(b.pos.side == 0 for b in rig.brokers.values()))
        assert rig.d.kill == "paused"
        rig.intent(2, 1)
        await asyncio.sleep(0.15)
        assert all(b.pos.side == 0 for b in rig.brokers.values())        # entries blocked while paused
        rig.kill.api = {"paused": False, "autonomy": "on"}               # resume
        assert await until(lambda: rig.d.kill == "clear" and all(w.ex.kill == "clear" for w in rig.d.workers.values()))
        rig.intent(3, 0)
        rig.intent(4, 1)
        assert await until(lambda: all(b.pos.side == 1 for b in rig.brokers.values()))
    go(rig, scenario)


def test_api_down_and_db_paused_flattens(tmp_path):
    rig = Rig(tmp_path)

    async def scenario():
        await ready(rig)
        rig.intent(1, 1)
        assert await until(lambda: all(b.pos.side == 1 for b in rig.brokers.values()))
        rig.kill.api = None
        rig.kill.db = {"paused": True, "autonomy": "off"}
        assert await until(lambda: all(b.pos.side == 0 for b in rig.brokers.values()))
    go(rig, scenario)


def test_both_sources_down_holds_and_texts_once(tmp_path):
    rig = Rig(tmp_path)

    async def scenario():
        await ready(rig)
        rig.intent(1, 1)
        assert await until(lambda: all(b.pos.side == 1 for b in rig.brokers.values()))
        rig.kill.api = rig.kill.db = None
        assert await until(lambda: rig.d.kill == "unknown")
        await asyncio.sleep(0.2)                                         # many polls
        assert all(b.pos.side == 1 for b in rig.brokers.values())        # holds the position
        texts = [r for r in rig.ledger._q("SELECT * FROM copy_events WHERE kind='kill_unknown'")]
        assert len(texts) == 2 and all(t["notify"] == 1 for t in texts)  # one per account, not one per poll
        rig.intent(2, -1)
        await asyncio.sleep(0.1)
        assert all(b.pos.side == 0 for b in rig.brokers.values())        # a reversal closes (exit half) ...
        assert all(("enter", -1, 5, 19999.0) not in b.calls for b in rig.brokers.values())   # ... never re-enters
        rig.kill.api = {"paused": False, "autonomy": "on"}
        assert await until(lambda: rig.d.kill == "clear")
        rig.kill.api = rig.kill.db = None                                # down again later: a new text
        assert await until(lambda: rig.d.kill == "unknown")
        assert await until(lambda: len(rig.ledger._q("SELECT * FROM copy_events WHERE kind='kill_unknown'")) == 4)
    go(rig, scenario)


def test_non_boolean_paused_is_unknown_and_autonomy_off_blocks_entries(tmp_path):
    k = Kill()
    k.api, k.db = {"paused": "yes"}, {"paused": 1}
    assert kill_state(k.read_api, k.read_db) == "unknown"
    k.api = {"paused": False, "autonomy": "off"}
    assert kill_state(k.read_api, k.read_db) == "autonomy_off"
    k.api = {"paused": True}
    assert kill_state(k.read_api, k.read_db) == "paused"
    k.api = None
    k.db = {"paused": False, "autonomy": "on"}
    assert kill_state(k.read_api, k.read_db) == "clear"
    rig = Rig(tmp_path)
    rig.kill.api = {"paused": False, "autonomy": "off"}

    async def scenario():
        await ready(rig)
        assert await until(lambda: rig.d.kill == "autonomy_off")
        rig.intent(1, 1)
        await asyncio.sleep(0.15)
        assert all(b.pos.side == 0 for b in rig.brokers.values())
    go(rig, scenario)


def test_startup_while_paused_flattens_and_blocks(tmp_path):
    rig = Rig(tmp_path)
    hold_positions(rig)
    rig.kill.api = {"paused": True, "autonomy": "off"}

    async def scenario():
        assert await until(lambda: all(w.ex.clean for w in rig.d.workers.values()))
        assert all(b.pos.side == 0 for b in rig.brokers.values())
        assert await until(lambda: all(w.ex.kill == "paused" for w in rig.d.workers.values()))
        rig.hb()
        rig.intent(1, 1)
        await asyncio.sleep(0.15)
        assert all(b.pos.side == 0 for b in rig.brokers.values())
    go(rig, scenario)


def test_db_kill_status_reads_read_only_and_rejects_garbage(tmp_path):
    p = tmp_path / "resonant.db"
    conn = sqlite3.connect(p)
    conn.execute("CREATE TABLE kv(key TEXT PRIMARY KEY, value TEXT, updated_at TEXT)")
    conn.execute("INSERT INTO kv VALUES('paused', 'true', 'x'), ('autonomy', '\"off\"', 'x')")
    conn.commit()
    conn.close()
    assert db_kill_status(p) == {"paused": True, "autonomy": "off"}
    with pytest.raises(Exception):
        db_kill_status(tmp_path / "missing.db")
    conn = sqlite3.connect(p)
    conn.execute("DELETE FROM kv WHERE key='paused'")
    conn.commit()
    conn.close()
    with pytest.raises(ValueError):
        db_kill_status(p)


# ---------------------------------------------------------------- flatten-all, lock

def test_flatten_all_halts_every_account_for_the_day(tmp_path):
    rig = Rig(tmp_path)

    async def scenario():
        await ready(rig)
        rig.intent(1, 1)
        assert await until(lambda: all(b.pos.side == 1 for b in rig.brokers.values()))
        res = await rig.d.flatten_all()
        assert res == {"a1": "flat", "a2": "flat"}
        assert all(b.pos.side == 0 for b in rig.brokers.values())
        assert all(w.ex.halted == "flatten-all" for w in rig.d.workers.values())
        rig.intent(2, 0)
        rig.intent(3, 1)
        await asyncio.sleep(0.15)
        assert all(b.pos.side == 0 for b in rig.brokers.values())
    go(rig, scenario)


def test_lock_prevents_a_second_daemon(tmp_path):
    d = tmp_path / "copy"
    assert not lock_held(d)
    fh = take_lock(d)
    try:
        with pytest.raises(AlreadyRunning):
            take_lock(d)
        assert lock_held(d)
    finally:
        fh.close()
    assert not lock_held(d)
    take_lock(d).close()


# ---------------------------------------------------------------- the HTTP face

def free_port():
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def request(port, method, path, body=None, headers=None, host=None):
    c = http.client.HTTPConnection("127.0.0.1", port, timeout=5)
    h = dict(headers or {})
    c.putrequest(method, path, skip_host=True)
    c.putheader("Host", host or f"127.0.0.1:{port}")
    for k, v in h.items():
        c.putheader(k, v)
    data = json.dumps(body).encode() if body is not None else b""
    c.putheader("Content-Length", str(len(data)))
    c.endheaders(data)
    r = c.getresponse()
    out = (r.status, r.read())
    c.close()
    return out


def test_http_requires_loopback_host_and_json_content_type(tmp_path):
    rig = Rig(tmp_path)
    port = free_port()

    async def scenario():
        await ready(rig)
        msg = {"type": "heartbeat", "boot": BOOT, "seq_last": 9, "target": 1, "status": "LIVE", "mode": "live"}
        call = lambda *a, **k: asyncio.to_thread(request, port, *a, **k)  # noqa: E731
        js = {"Content-Type": "application/json"}
        assert (await call("POST", "/heartbeat", msg, js, host="evil.example.com"))[0] == 403
        assert (await call("POST", "/heartbeat", msg, js, host="127.0.0.1.evil.com"))[0] == 403
        assert (await call("POST", "/heartbeat", msg, {"Content-Type": "text/plain"}))[0] == 415
        assert (await call("POST", "/heartbeat", msg, {"Content-Type": "application/x-www-form-urlencoded"}))[0] == 415
        assert (await call("POST", "/heartbeat", msg, {}))[0] == 415
        await asyncio.sleep(0.1)
        assert rig.ex("a1").last_seq == 0                                 # none of the refused ones got in
        status, body = await call("POST", "/heartbeat", msg, {**js, "Content-Type": "application/json; charset=utf-8"})
        assert status == 200 and json.loads(body) == {"queued": True}
        assert await until(lambda: rig.ex("a1").last_seq == 9)
        assert (await call("POST", "/nope", msg, js))[0] == 404
        assert (await call("POST", "/flatten-all", {}, js))[0] == 403     # needs its header
        assert (await call("GET", "/status", None, {}, host="evil.com"))[0] == 403
        status, body = await call("GET", "/status", None, {})
        assert status == 200 and {a["name"] for a in json.loads(body)["accounts"]} == {"a1", "a2"}
        assert (await call("POST", "/intent", [1, 2], js))[0] == 400

    async def main():
        await rig.d.start(port)
        try:
            await scenario()
        finally:
            await rig.d.stop()
    asyncio.run(main())


def test_http_flatten_all_with_its_header(tmp_path):
    rig = Rig(tmp_path)
    port = free_port()

    async def main():
        await rig.d.start(port)
        try:
            await ready(rig)
            rig.intent(1, 1)
            assert await until(lambda: all(b.pos.side == 1 for b in rig.brokers.values()))
            status, body = await asyncio.to_thread(
                request, port, "POST", "/flatten-all", {}, {"Content-Type": "application/json",
                                                           FLATTEN_ALL_HEADER: "flatten-all"})
            assert status == 200 and json.loads(body) == {"ok": True, "accounts": {"a1": "flat", "a2": "flat"}}
            assert all(b.pos.side == 0 for b in rig.brokers.values())
        finally:
            await rig.d.stop()
    asyncio.run(main())
