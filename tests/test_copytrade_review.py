"""Regression tests for the PR #11 code review (each fails without its fix)."""

import asyncio
import json
import os
import time

import pytest

from tests.copytrade_fakes import ACCT_ID, FakeTradovate
from tests.test_copytrade_broker import Rig as BrokerRig
from tests.test_copytrade_daemon import Rig as DaemonRig
from tests.test_copytrade_executor import BOOT1, Rig, acct
from trade_jev.copytrade import __main__ as cm
from trade_jev.copytrade import broker as cb
from trade_jev.copytrade import config as cfg
from trade_jev.copytrade.executor import CopyExecutor, validate_message
from trade_jev.copytrade.ledger import Ledger
from trade_jev.data import et_to_ns
from trade_jev.executor.broker import Ack, DryRun, FakeBroker, Position


# ---- 1. never cancel protective orders while a position may exist

@pytest.fixture
def rig(tmp_path):
    return BrokerRig(tmp_path)


def live_orders(r):
    return len(r.run(r.broker.orders(True)))


@pytest.mark.parametrize("mode", ["refused", "captcha", "first_read_fails"])
def test_a_flatten_that_cannot_close_leaves_the_bracket_alone(rig, mode):
    assert rig.enter().ok and live_orders(rig) == 2
    if mode == "refused":
        rig.fake.liquidate_fail_times = 99
    elif mode == "captcha":
        rig.fake.liquidate_pticket = True
        orig = rig.fake._liquidate

        def captcha(b):
            return 200, {"p-ticket": "t", "p-time": 1, "p-captcha": True}
        rig.fake._liquidate = captcha
    else:
        rig.fake.read_fail.add("/position/list")
    ack = rig.run(rig.broker.flatten())
    rig.fake.read_fail.clear()
    assert not ack.ok
    assert rig.run(rig.broker.position(True)).qty == 5      # still long
    assert live_orders(rig) == 2                             # ... and still protected
    assert rig.fake.n("/order/cancelorder") == 0


def test_liquidate_failure_reason_is_honoured(rig):
    assert rig.enter().ok
    rig.fake.liquidate_fail_times = 99
    ack = rig.run(rig.broker.flatten())
    assert "refused" in ack.error or "still in a position" in ack.error
    ok, oid, err = rig.run(rig.broker._liquidate({"contractId": 99}))
    assert ok is False and oid is None and "UnknownReason" in err


def test_flatten_reports_the_liquidations_real_fill_price(rig):
    assert rig.enter().ok
    rig.fake.last_px = 24480.5
    ack = rig.run(rig.broker.flatten())
    assert ack.ok and ack.fill_px == 24480.5


# ---- 2. a cancelled kill / flatten-all flatten is retried until flat

def make_ex(tmp_path, broker=None, **kw):
    r = Rig(tmp_path, broker=broker, **kw)
    r.start()
    return r


def test_kill_flatten_cancelled_half_way_is_retried_by_the_watchdog(tmp_path):
    r = make_ex(tmp_path)
    r.intent(1, 1)
    real = r.br.flatten

    async def slow():
        await asyncio.sleep(5)
        return await real()
    r.br.flatten = slow
    with pytest.raises(asyncio.TimeoutError):
        r.run(asyncio.wait_for(r.ex.kill_flatten("paused"), 0.05))     # the daemon's _timed
    assert r.ex.need_flat and r.br.pos.side == 1
    r.br.flatten = real
    r.clk.advance(3)
    r.run(r.ex.watchdog_tick())
    assert r.br.pos.side == 0 and not r.ex.need_flat


def test_watchdog_flattens_any_position_seen_while_paused(tmp_path):
    r = make_ex(tmp_path)
    r.ex.kill = "paused"
    r.br.pos = Position(1, 5, 20000.0)          # something opened (or a flatten never happened)
    r.clk.advance(3)
    r.run(r.ex.watchdog_tick())
    assert r.br.pos.side == 0


def test_flatten_all_cancelled_half_way_is_retried(tmp_path):
    r = make_ex(tmp_path)
    r.intent(1, 1)
    real = r.br.flatten

    async def slow():
        await asyncio.sleep(5)
        return await real()
    r.br.flatten = slow
    with pytest.raises(asyncio.TimeoutError):
        r.run(asyncio.wait_for(r.ex.flatten_all(), 0.05))
    r.br.flatten = real
    r.clk.advance(3)
    r.run(r.ex.watchdog_tick())
    assert r.br.pos.side == 0


def test_a_failed_flatten_does_not_sweep_in_the_executor_paths(tmp_path):
    r = make_ex(tmp_path)
    r.intent(1, 1)
    r.br.fail_next.add("flatten")
    r.run(r.ex.kill_flatten("paused"))
    assert ("sweep",) not in r.br.calls and r.br.bracket is not None


# ---- 3. orphan bracket children

def test_no_fill_with_children_that_cannot_be_cancelled_is_not_a_plain_miss(rig):
    rig.fake.entry = "none"
    orig = rig.fake._place

    def place(b):
        out = orig(b)
        for o in rig.fake.orders.values():
            if o.get("role") == "child":
                o["ordStatus"] = "Working"      # Tradovate did not cancel them with the parent
        return out
    rig.fake._place = place
    rig.fake.cancel_fail = True
    ack = rig.enter()
    assert not ack.ok and not ack.missed and ack.info == {"orphans": True}
    assert any(k == "orphan_orders" and n for k, _, n in rig.events)


def test_flat_account_with_live_orders_is_swept_and_alerted(tmp_path):
    class Orphans(FakeBroker):
        live = True

        async def orders(self, fresh=False):
            return [{"id": 1}, {"id": 2}] if self.live else []

        async def sweep(self):
            self.calls.append(("sweep",))
            self.live = False
            return True
    r = make_ex(tmp_path, broker=Orphans())
    r.clk.advance(11)
    r.hb()
    assert ("sweep",) in r.br.calls and r.events("orphan_swept") and r.texts()


def test_flat_account_whose_orphans_cannot_be_swept_halts_once(tmp_path):
    class Stuck(FakeBroker):
        async def orders(self, fresh=False):
            return [{"id": 1}]

        async def sweep(self):
            return False
    r = make_ex(tmp_path, broker=Stuck())
    for _ in range(3):
        r.clk.advance(11)
        r.hb()
    assert r.ex.halted and "could not be cancelled" in r.ex.halted
    assert len([e for e in r.events("halt")]) == 1


# ---- 4. short flat cache

def test_flat_position_cache_is_at_most_five_seconds(rig):
    assert cb.READ_TTL_FLAT_S <= 5.0
    rig.run(rig.broker.position())
    rig.clock.t += 5.5
    rig.run(rig.broker.position())
    assert rig.fake.n("/position/list") == 2


# ---- 5. parent Filled but flat

def test_parent_filled_but_position_flat_is_ambiguous_never_swept(rig):
    def vanish(fake):
        fake.pos.clear()          # the position is not visible (yet)
    rig.fake.after_place = vanish
    ack = rig.enter()
    assert not ack.ok and not ack.missed and ack.info == {"ambiguous": True}
    assert rig.fake.n("/order/cancelorder") == 0
    assert live_orders(rig) == 2        # the bracket is kept
    assert any(k == "entry_ambiguous" and n for k, _, n in rig.events)


def test_executor_halts_on_an_ambiguous_entry(tmp_path):
    class Amb(FakeBroker):
        async def enter(self, *a):
            return Ack(False, error="ambiguous", info={"ambiguous": True})
    r = make_ex(tmp_path, broker=Amb())
    assert r.intent(1, 1)["result"] == "halted: entry failed" and r.ex.halted and r.texts()


# ---- 6. EOD guard

def test_eod_guard_read_error_retries_instead_of_halting(tmp_path):
    r = Rig(tmp_path, hm="15:30:00")
    r.start()
    r.intent(1, 1, t_ns=et_to_ns("2026-10-05", "15:30:00"))
    r.clk.advance(26 * 60 + 5)
    real = r.br.position

    async def broken(fresh=False):
        raise RuntimeError("503")
    r.br.position = broken
    r.ex.note_rx()
    r.run(r.ex.watchdog_tick())
    assert not r.ex.halted and r.br.pos.side == 1
    r.br.position = real
    r.ex.note_rx()
    r.run(r.ex.watchdog_tick())
    assert r.br.pos.side == 0 and r.trades()[0]["exit_reason"] == "eod"


# ---- 7. frozen quotes are not marks

def test_a_frozen_quote_or_non_live_status_gives_no_mark(tmp_path):
    r = Rig(tmp_path, commission=0.0)
    r.start()
    r.intent(1, 1)
    r.hb(bid=19990.0)
    assert r.ex.pnl_parts()[1] != 0.0              # a fresh, moving quote counts
    for _ in range(7):                              # the heartbeat thread keeps sending the same quote
        r.clk.advance(10)
        r.hb(bid=19990.0)
    assert r.ex.pnl_parts()[1] == 0.0 and not r.ex.halted      # frozen > 60 s: open is ignored
    r.hb(bid=19991.0)
    assert r.ex.pnl_parts()[1] != 0.0
    r.hb(bid=19992.0, status="CATCH-UP")
    assert r.ex.pnl_parts()[1] == 0.0


# ---- 8. real close prices

def test_closes_use_the_brokers_fill_and_flag_estimates(tmp_path):
    r = Rig(tmp_path, commission=0.0)
    r.start()
    r.intent(1, 1)
    real = r.br.flatten

    async def with_fill():
        a = await real()
        a.fill_px = 19990.0
        return a
    r.br.flatten = with_fill
    r.intent(2, 0)
    t = r.trades()[0]
    assert t["exit_px"] == 19990.0 and not t["estimated"]
    async def no_fill():
        a = await real()
        a.fill_px = None
        return a
    r.br.flatten = no_fill
    r.intent(3, 1)
    r.hb(bid=20003.0)
    r.intent(4, 0)
    t = r.trades()[1]
    assert t["estimated"] == 1 and t["exit_px"] == 20003.0 and r.events("estimated_close")


def test_partial_fill_close_is_booked_at_the_liquidation_price_and_feeds_the_stops(tmp_path):
    f = FakeTradovate()
    f.entry, f.partial_qty, f.last_px = "partial", 2, 24490.0     # bought 2 at the limit, closed 10.75 lower
    br = BrokerRig(tmp_path, fake=f)
    br.broker.set_context(boot=BOOT1, seq=1)
    ex_rig = Rig(tmp_path / "ex", broker=br.broker, commission=0.0, daily_loss_limit=40.0)
    ex_rig.ex.ledger = br.ledger
    ex_rig.start()
    ex_rig.intent(1, 1, ref={"bid": 24500.0, "ask": 24500.25})
    (t,) = ex_rig.trades() if ex_rig.trades() else [None]
    rows = [dict(x) for x in br.ledger._q("SELECT * FROM copy_trades")]
    assert rows and rows[0]["exit_reason"] == "partial" and rows[0]["exit_px"] == 24490.0 and rows[0]["points"] < -10
    assert br.ledger.day_pnl("a1", "2026-10-05") < 0


# ---- 9. kill defaults to unknown

def test_kill_state_defaults_to_unknown_until_the_first_poll(tmp_path):
    led = Ledger(tmp_path / "c.db")
    ex = CopyExecutor(acct(), FakeBroker(), led)
    assert ex.kill == "unknown" and ex.gate_reason() in ("startup not clean", "kill: unknown")
    ex.clean = True
    assert ex.gate_reason() == "kill: unknown"
    r = DaemonRig(tmp_path / "d")
    assert r.d.kill == "unknown"


# ---- 12/13. input validation

@pytest.mark.parametrize("kind,msg,bad", [
    ("intent", {"seq": 1, "target": 5, "t_ns": 1}, "target"), ("intent", {"seq": "x", "target": 1, "t_ns": 1}, "seq"),
    ("intent", {"seq": 1, "t_ns": 1}, "target"), ("intent", {"seq": True, "target": 1, "t_ns": 1}, "seq"),
    ("intent", [], "object"), ("heartbeat", {"target": 7}, "target"), ("heartbeat", {"seq_last": -1}, "seq_last"),
    ("heartbeat", {"bid": "x"}, "bid"),
])
def test_malformed_messages_are_refused(kind, msg, bad):
    assert bad in validate_message(kind, msg)


def test_a_malformed_intent_never_day_halts_an_account(tmp_path):
    r = Rig(tmp_path)
    r.start()
    for bad in ({"seq": "x", "target": 1}, {"seq": 3, "target": 9, "t_ns": 1, "boot": BOOT1}, {"boot": BOOT1}):
        out = r.run(r.ex.on_intent(bad))
        assert out["skipped"].startswith("malformed intent")
    assert not r.ex.halted and r.br.pos.side == 0 and r.ex.errors == 0


def test_http_refuses_malformed_messages_and_a_negative_content_length(tmp_path):
    from tests.test_copytrade_daemon import free_port, request, ready, until
    rig = DaemonRig(tmp_path)
    port = free_port()

    async def main():
        await rig.d.start(port)
        try:
            await ready(rig)
            js = {"Content-Type": "application/json"}
            call = lambda *a, **k: asyncio.to_thread(request, port, *a, **k)  # noqa: E731
            assert (await call("POST", "/intent", {"seq": 1, "target": 5, "t_ns": 1}, js))[0] == 400
            assert (await call("POST", "/intent", {"seq": "x", "target": 1, "t_ns": 1}, js))[0] == 400
            assert (await call("POST", "/heartbeat", {"target": 9}, js))[0] == 400
            ok = {"seq": 1, "target": 0, "t_ns": 1, "boot": "b"}
            assert (await call("POST", "/intent", ok, js))[0] == 200
            import http.client
            c = http.client.HTTPConnection("127.0.0.1", port, timeout=5)
            c.putrequest("POST", "/intent", skip_host=True)
            c.putheader("Host", f"127.0.0.1:{port}")
            c.putheader("Content-Type", "application/json")
            c.putheader("Content-Length", "-1")
            c.endheaders()
            assert c.getresponse().status == 400
            await asyncio.sleep(0.1)
            assert not any(w.ex.halted for w in rig.d.workers.values())
        finally:
            await rig.d.stop()
    asyncio.run(main())


# ---- 12. flatten-all --direct with a dead token store

class RenewingHttp:
    """FakeTradovate plus an auth server that accepts the .env seed token."""

    def __init__(self, inner):
        self.inner, self.renews = inner, 0

    async def __call__(self, method, url, headers, body):
        if url.endswith("/auth/renewaccesstoken"):
            self.renews += 1
            assert headers["Authorization"] == "Bearer seed"
            from trade_jev import tradovate_token as tt
            return 200, {"accessToken": "fresh", "mdAccessToken": "m", "expirationTime": tt.iso_utc(time.time() + 4800)}
        return await self.inner(method, url, headers, body)


def test_direct_flatten_renews_from_the_env_seed_when_the_store_is_dead(tmp_path, capsys):
    from trade_jev import tradovate_token as tt
    dotenv = tmp_path / ".env"
    dotenv.write_text("COPY_A1_ACCESS_TOKEN=seed\n")
    store = tmp_path / "copy" / "a1" / "tradovate_token.json"
    tt.write_store(store, "dead", "dead", time.time() - 100)
    fake = FakeTradovate()
    fake._make_position(1, 5, 24500.0)
    http = RenewingHttp(fake)
    conf = cfg.CopyConfig((acct(),))
    rc = cm.main(["flatten-all", "--direct"], cfg=conf, db=tmp_path / "c.db", directory=tmp_path / "copy",
                 dotenv=dotenv, http_factory=lambda pool: http, isatty=True, ask=lambda p: "FLATTEN")
    assert rc == 0 and http.renews == 1 and "a1: flat" in capsys.readouterr().out
    assert fake.pos[99]["netPos"] == 0


def test_direct_flatten_failure_points_at_the_tradovate_ui(tmp_path, capsys):
    conf = cfg.CopyConfig((acct(),))
    rc = cm.main(["flatten-all", "--direct"], cfg=conf, db=tmp_path / "c.db", directory=tmp_path / "copy",
                 dotenv=tmp_path / ".env", http_factory=lambda pool: FakeTradovate(), isatty=True,
                 ask=lambda p: "FLATTEN")
    out = capsys.readouterr().out
    assert rc == 1 and "FAILED" in out and "Tradovate UI" in out


# ---- nits

def test_dry_run_position_does_not_swallow_a_real_type_error():
    class Bad(FakeBroker):
        async def position(self, fresh=False):
            raise TypeError("real bug")
    with pytest.raises(TypeError, match="real bug"):
        asyncio.run(DryRun(Bad()).position())


def test_negative_limit_message_says_to_write_a_positive_number(tmp_path):
    import yaml
    from tests.test_copytrade_config import GOOD
    p = tmp_path / "a.yaml"
    p.write_text(yaml.safe_dump({"accounts": [{**GOOD, "daily_loss_limit": -1000}]}))
    with pytest.raises(cfg.ConfigError, match="positive number"):
        cfg.load(p)


def test_executor_log_lines_go_to_stdout_for_copy_log(tmp_path, capsys):
    r = Rig(tmp_path)
    r.start()
    r.intent(1, 1)
    out = capsys.readouterr().out
    assert "[copy]" in out and "a1 enter" in out and "a1 done" in out


def test_order_list_calls_time_out_at_the_http_timeout_not_later(rig):
    async def hang(*a):
        await asyncio.sleep(60)
    rig.broker.http = hang
    rig.broker.timeout = 0.05
    t = time.monotonic()
    with pytest.raises(cb.BrokerError):
        rig.run(rig.broker.orders(True))
    assert time.monotonic() - t < 1.0
