import asyncio
import json

import pytest

from tests.copytrade_fakes import ACCT_ID, Clock, FakeTradovate
from trade_jev.copytrade import config as cfg
from trade_jev.copytrade.broker import NullBroker, TradovateBroker
from trade_jev.copytrade.ledger import Ledger
from trade_jev.executor.broker import DryRun

TOKEN = "tok-SECRET-123"
BOOT = "abcdef0123456789"


def acct(**kw):
    base = dict(name="a1", enabled=True, mode="demo", env="demo", account_id=ACCT_ID, root="MNQ", qty=5,
                daily_loss_limit=1000.0, daily_profit_stop=490.0, commission=0.5, secrets_prefix="COPY_A1")
    base.update(kw)
    return cfg.Account(**base)


class Rig:
    def __init__(self, tmp_path, fake=None, **kw):
        self.fake = fake or FakeTradovate()
        self.clock = Clock()
        self.ledger = Ledger(tmp_path / "copy.db")
        self.events = []
        self.broker = TradovateBroker(acct(**kw), lambda: TOKEN, lambda: [TOKEN], http=self.fake, ledger=self.ledger,
                                      emit=lambda k, d, n=False: self.events.append((k, d, n)),
                                      sleep=self.clock.sleep, clock=self.clock)
        self.broker.set_symbol("MNQZ6")
        self.broker.set_context(boot=BOOT, seq=7)

    def enter(self, side=1, qty=5, limit=24500.25, sl=200, tp=100):
        return asyncio.run(self.broker.enter(side, qty, limit, sl, tp))

    def run(self, coro):
        return asyncio.run(coro)


@pytest.fixture
def rig(tmp_path):
    return Rig(tmp_path)


def kinds(rig):
    return [k for k, _, _ in rig.events]


# ---------------------------------------------------------------- the entry request

def test_exact_oso_body(rig):
    ack = rig.enter(1, 5, 24500.25, 200, 100)
    assert ack.ok
    (body,) = rig.fake.posts("/order/placeoso")
    assert body == {
        "accountSpec": "DEMO11", "accountId": ACCT_ID, "action": "Buy", "symbol": "MNQZ6", "orderQty": 5,
        "orderType": "Limit", "price": 24500.25, "timeInForce": "IOC", "isAutomated": True,
        "clOrdId": "cj-a1-abcdef01-7-e",
        "bracket1": {"action": "Sell", "orderType": "Limit", "price": 24525.25, "timeInForce": "GTC",
                     "clOrdId": "cj-a1-abcdef01-7-tp"},
        "bracket2": {"action": "Sell", "orderType": "Stop", "stopPrice": 24450.25, "timeInForce": "GTC",
                     "clOrdId": "cj-a1-abcdef01-7-sl"}}


@pytest.mark.parametrize("root,symbol", [("MNQ", "MNQZ6"), ("NQ", "NQZ6")])
@pytest.mark.parametrize("side", [1, -1])
def test_prices_for_root_and_side(tmp_path, root, symbol, side):
    r = Rig(tmp_path, root=root)
    r.broker.set_symbol(symbol)
    ack = r.enter(side, 5, 20000.0, 200, 100)     # 50 pt stop, 25 pt target
    assert ack.ok
    b = r.fake.posts("/order/placeoso")[0]
    assert b["symbol"] == symbol and b["action"] == ("Buy" if side > 0 else "Sell")
    assert b["bracket1"]["price"] == 20000.0 + side * 25 and b["bracket2"]["stopPrice"] == 20000.0 - side * 50
    assert b["bracket1"]["action"] == b["bracket2"]["action"] == ("Sell" if side > 0 else "Buy")


def test_modify_both_children_from_own_fill(rig):
    rig.fake.fill_px = 24500.0          # price improvement: filled better than the limit
    ack = rig.enter(1, 5, 24500.25)
    assert ack.ok and ack.fill_px == 24500.0
    mods = rig.fake.posts("/order/modifyorder")
    assert len(mods) == 2 and not rig.fake.posts("/order/placeorder")
    assert {m["orderId"] for m in mods} == {ack.info["tp_id"], ack.info["sl_id"]}
    tp = next(m for m in mods if m["orderType"] == "Limit")
    sl = next(m for m in mods if m["orderType"] == "Stop")
    assert (tp["price"], sl["stopPrice"]) == (24525.0, 24450.0)
    assert all(m["orderQty"] == 5 and m["timeInForce"] == "GTC" for m in mods)
    assert ack.info["tp_px"] == 24525.0 and ack.info["sl_px"] == 24450.0 and ack.info["modified"] is True
    assert not rig.fake.posts("/order/cancelorder")     # never cancel-replace


def test_fill_at_limit_needs_no_modify(rig):
    assert rig.enter().ok
    assert rig.fake.posts("/order/modifyorder") == [] or len(rig.fake.posts("/order/modifyorder")) == 2


def test_modify_failure_keeps_provisional_bracket(rig):
    rig.fake.fill_px = 24500.0
    rig.fake.modify_fail = True
    ack = rig.enter()
    assert ack.ok and ack.info["modified"] is False
    assert "modify_failed" in kinds(rig)
    live = rig.run(rig.broker.orders(True))
    assert len(live) == 2     # the provisional children are still resting


def test_child_dead_after_fill_flattens(rig):
    rig.fake.child_dead = True
    ack = rig.enter()
    assert not ack.ok and "bracket missing" in ack.error and ack.info["closed"]
    assert rig.run(rig.broker.position(True)).side == 0
    assert "bracket_missing" in kinds(rig) and rig.events[-1][2] is True


def test_modify_that_kills_a_child_is_caught_by_the_verify(rig):
    rig.fake.fill_px = 24500.0
    rig.fake.modify_kills_child = True
    ack = rig.enter()
    assert not ack.ok and rig.run(rig.broker.position(True)).side == 0


def test_miss_no_fill(rig):
    rig.fake.entry = "none"
    ack = rig.enter()
    assert not ack.ok and ack.missed and not ack.rejected
    assert rig.run(rig.broker.orders(True)) == []
    assert rig.fake.n("/order/modifyorder") == 0


def test_partial_fill_liquidates_and_records_as_miss(rig):
    rig.fake.entry, rig.fake.partial_qty = "partial", 2
    ack = rig.enter()
    assert ack.missed and ack.info["partial"] and ack.info["qty"] == 2
    assert ack.ok          # the liquidation itself worked
    assert rig.run(rig.broker.position(True)).side == 0 and rig.run(rig.broker.orders(True)) == []
    assert len(rig.fake.posts("/order/liquidateposition")) == 1
    assert rig.ledger.orders_in_state("a1", "missed")[0]["failure_reason"] == "partial"


def test_reject(rig):
    rig.fake.entry = "reject"
    ack = rig.enter()
    assert ack.rejected and not ack.ok and "RiskCheckFailed" in ack.error
    assert [r["state"] for r in rig.ledger._q("SELECT state FROM copy_orders")] == ["rejected"] * 3


def test_http_4xx_is_a_rejection_not_unknown(rig):
    rig.fake.entry = "http400"
    assert rig.enter().rejected and len(rig.fake.posts("/order/placeoso")) == 1


def test_p_ticket_on_entry_is_a_miss_and_never_resent(rig):
    rig.fake.entry = "pticket"
    ack = rig.enter()
    assert ack.missed and len(rig.fake.posts("/order/placeoso")) == 1
    assert rig.broker.blocked_reason() is None       # a plain penalty ticket does not block for an hour


@pytest.mark.parametrize("mode", ["captcha", "429"])
def test_captcha_and_429_block_entries_for_an_hour_and_text(rig, mode):
    rig.fake.entry = mode
    ack = rig.enter()
    assert ack.missed and rig.broker.blocked_reason()
    assert any(k == "entries_blocked" and n for k, _, n in rig.events)
    rig.fake.entry = "full"
    n = len(rig.fake.posts("/order/placeoso"))
    assert rig.enter().missed and len(rig.fake.posts("/order/placeoso")) == n     # still blocked
    rig.clock.t += 3601
    assert rig.enter().ok


def test_refuses_entry_without_bracket_ticks(rig):
    assert rig.enter(sl=0).rejected and rig.fake.n("/order/placeoso") == 0


def test_refuses_when_account_not_flat(rig):
    rig.fake._make_position(1, 1, 24000.0)
    ack = rig.enter()
    assert not ack.ok and not ack.missed and rig.fake.n("/order/placeoso") == 0


def test_sweeps_stale_orders_before_entering(rig):
    rig.fake.entry = "none"
    rig.enter()                      # leaves canceled orders only
    rig.fake.orders[1] = {"id": 1, "accountId": ACCT_ID, "ordStatus": "Working", "clOrdId": "old"}
    rig.fake.entry = "full"
    assert rig.enter().ok
    assert any(c[2] == {"orderId": 1, "isAutomated": True} for c in rig.fake.calls if c[1] == "/order/cancelorder")


def test_pre_entry_read_failure_is_a_miss_nothing_sent(rig):
    rig.fake.read_fail.add("/position/list")
    ack = rig.enter()
    assert ack.missed and rig.fake.n("/order/placeoso") == 0


def test_missing_token_is_a_miss(tmp_path):
    r = Rig(tmp_path)
    r.broker.token_fn = lambda: None
    assert r.enter().missed and r.fake.n("/order/placeoso") == 0


def test_rows_written_before_the_post(rig):
    seen = {}

    def peek(fake):
        seen["rows"] = [dict(r) for r in rig.ledger._q("SELECT role, state, cl_ord_id FROM copy_orders")]
    rig.fake.after_place = peek
    rig.enter()
    assert [(r["role"], r["state"]) for r in seen["rows"]] == [("entry", "sending"), ("tp", "sending"), ("sl", "sending")]
    assert {r["cl_ord_id"] for r in seen["rows"]} == {"cj-a1-abcdef01-7-e", "cj-a1-abcdef01-7-tp", "cj-a1-abcdef01-7-sl"}
    final = {r["role"]: r["state"] for r in rig.ledger._q("SELECT role, state FROM copy_orders")}
    assert final == {"entry": "filled", "tp": "working", "sl": "working"}


# ---------------------------------------------------------------- unknown outcomes (timeouts)

def test_timeout_with_order_found_resumes_without_reposting(rig):
    rig.fake.entry = "timeout_placed"
    ack = rig.enter()
    assert ack.ok and ack.info["recovered"] is True
    assert len(rig.fake.posts("/order/placeoso")) == 1          # an entry is never re-POSTed
    assert rig.run(rig.broker.position(True)).qty == 5
    assert rig.clock.t >= 1.5


def test_timeout_order_not_found_and_flat_is_a_miss(rig):
    rig.fake.entry = "timeout_lost"
    ack = rig.enter()
    assert ack.missed and len(rig.fake.posts("/order/placeoso")) == 1
    assert rig.fake.n("/order/list") >= 3     # the pre-entry read plus two searches


def test_timeout_with_unexplained_position_flattens(rig):
    rig.fake.entry = "timeout_unexplained"
    ack = rig.enter()
    assert not ack.ok and not ack.missed and "outcome unknown" in ack.error
    assert rig.run(rig.broker.position(True)).side == 0
    assert any(k == "entry_unexplained" and n for k, _, n in rig.events)
    assert len(rig.fake.posts("/order/placeoso")) == 1


def test_5xx_on_the_order_post_is_unknown_too(rig):
    rig.fake.entry = "down"
    ack = rig.enter()
    assert ack.missed and len(rig.fake.posts("/order/placeoso")) == 1


def test_startup_treats_sending_rows_as_unknown_via_ledger(rig):
    rig.ledger.order_new(name="a1", day="2026-11-05", boot=BOOT, seq=1, role="entry", cl_ord_id="x-e")
    assert [r["cl_ord_id"] for r in rig.ledger.orders_in_state("a1", "sending")] == ["x-e"]


# ---------------------------------------------------------------- flatten

def test_flatten_liquidates_the_positions_own_contract_and_sweeps(rig):
    rig.fake.liquidate_cancels = False         # liquidate does not cancel the brackets: the sweep must
    assert rig.enter().ok
    ack = rig.run(rig.broker.flatten())
    assert ack.ok
    (liq,) = rig.fake.posts("/order/liquidateposition")
    assert liq == {"accountId": ACCT_ID, "contractId": 99, "admin": False, "isAutomated": True}
    assert rig.run(rig.broker.orders(True)) == [] and rig.run(rig.broker.position(True)).side == 0
    assert len(rig.fake.posts("/order/cancelorder")) == 2


def test_flatten_with_too_late_cancels_is_fine(rig):
    assert rig.enter().ok
    rig.fake.liquidate_cancels = False
    rig.fake.cancel_too_late = True
    assert rig.run(rig.broker.flatten()).ok is False   # the orders really are still live: not sweepable
    rig.fake.cancel_too_late = False
    assert rig.run(rig.broker.flatten()).ok


def test_flatten_too_late_when_children_already_gone(rig):
    assert rig.enter().ok
    for o in rig.fake.orders.values():
        if o.get("role") == "child":
            o["ordStatus"] = "Filled"
    ack = rig.run(rig.broker.flatten())
    assert ack.ok


def test_flatten_opposite_position_flattens_again_and_reports(rig):
    assert rig.enter().ok
    rig.fake.liquidate_flip = True
    ack = rig.run(rig.broker.flatten())
    assert not ack.ok and "opposite" in ack.error
    assert rig.run(rig.broker.position(True)).side == 0
    assert len(rig.fake.posts("/order/liquidateposition")) == 2


def test_flatten_retries_only_when_the_readback_still_shows_a_position(rig):
    assert rig.enter().ok
    rig.fake.liquidate_fail_times = 2
    ack = rig.run(rig.broker.flatten())
    assert ack.ok and len(rig.fake.posts("/order/liquidateposition")) == 3
    assert rig.run(rig.broker.flatten()).ok
    assert len(rig.fake.posts("/order/liquidateposition")) == 3          # flat: nothing more sent


def test_flatten_gives_up_after_three_tries(rig):
    assert rig.enter().ok
    rig.fake.liquidate_fail_times = 99
    ack = rig.run(rig.broker.flatten())
    assert not ack.ok and "still in a position" in ack.error
    assert len(rig.fake.posts("/order/liquidateposition")) == 3


def test_p_ticket_on_flatten_waits_and_resends_with_the_ticket(rig):
    assert rig.enter().ok
    rig.fake.liquidate_pticket = True
    t0 = rig.clock.t
    ack = rig.run(rig.broker.flatten())
    sent = rig.fake.posts("/order/liquidateposition")
    assert ack.ok and len(sent) == 2 and sent[1]["p-ticket"] == "liq" and "p-ticket" not in sent[0]
    assert rig.clock.t - t0 >= 1.0


def test_flatten_of_a_flat_account_sends_no_liquidation_but_sweeps(rig):
    rig.fake.orders[5] = {"id": 5, "accountId": ACCT_ID, "ordStatus": "Working"}
    assert rig.run(rig.broker.flatten()).ok
    assert rig.fake.n("/order/liquidateposition") == 0 and rig.fake.orders[5]["ordStatus"] == "Canceled"


def test_sweep_failure_is_reported(rig):
    rig.fake.orders[5] = {"id": 5, "accountId": ACCT_ID, "ordStatus": "Working"}
    rig.fake.cancel_fail = True
    assert rig.run(rig.broker.sweep()) is False
    assert rig.run(rig.broker.flatten()).ok is False


def test_only_our_account_is_read(rig):
    assert rig.run(rig.broker.position(True)).side == 0           # the other account's netPos 3 is ignored
    assert rig.run(rig.broker.orders(True)) == []


# ---------------------------------------------------------------- reads and caching

def test_position_cache_ttls_and_invalidation(rig):
    rig.run(rig.broker.position())
    rig.run(rig.broker.position())
    assert rig.fake.n("/position/list") == 1                  # flat: cached 30 s
    rig.clock.t += 31
    rig.run(rig.broker.position())
    assert rig.fake.n("/position/list") == 2
    assert rig.enter().ok
    n = rig.fake.n("/position/list")
    rig.run(rig.broker.position())
    rig.run(rig.broker.position())
    assert rig.fake.n("/position/list") <= n + 1               # in a position: cached 2 s
    rig.clock.t += 2.5
    rig.run(rig.broker.position())
    assert rig.fake.n("/position/list") >= n + 1


def test_exit_info_reads_the_filled_child(rig):
    ack = rig.enter()
    rig.fake.bracket_hit("tp", 24525.25)
    info = rig.run(rig.broker.exit_info({"tp_id": ack.info["tp_id"], "sl_id": ack.info["sl_id"], "side": 1,
                                         "tp_px": 24525.25, "sl_px": 24450.25}))
    assert info == (24525.25, "target")


def test_cash_realized_matches_the_day(rig):
    assert rig.run(rig.broker.cash_realized("2026-11-05")) == -12.5
    assert rig.run(rig.broker.cash_realized("2026-11-04")) is None


# ---------------------------------------------------------------- hosts, secrets, null

@pytest.mark.parametrize("env,host", [("demo", "https://demo.tradovateapi.com"), ("live", "https://live.tradovateapi.com")])
def test_host_follows_env(tmp_path, env, host):
    r = Rig(tmp_path, env=env, mode="demo" if env == "demo" else "live")
    r.run(r.broker.position(True))
    assert set(r.fake.hosts) == {host}


def test_no_token_in_errors_events_or_the_database(tmp_path):
    fake = FakeTradovate(token_echo=TOKEN)
    r = Rig(tmp_path, fake=fake)
    fake.entry = "reject"
    ack = r.enter()
    fake.entry = "http400"
    ack2 = r.enter()
    fake.read_fail.add("/position/list")
    ack3 = r.enter()
    blob = json.dumps([ack.error, ack2.error, ack3.error, r.events])
    r.ledger.conn.commit()
    dump = "\n".join(str(tuple(row)) for t in ("copy_orders", "copy_events", "copy_signals")
                     for row in r.ledger._q(f"SELECT * FROM {t}"))
    assert TOKEN not in blob and TOKEN not in dump
    assert "***" in blob


def test_authorization_header_carries_the_token_and_401_flags_auth(tmp_path):
    seen = []

    async def http(method, url, headers, body):
        seen.append(headers)
        return 401, {"errorText": "bad token"}
    r = Rig(tmp_path)
    r.broker.http = http
    with pytest.raises(Exception):
        r.run(r.broker.position(True))
    assert seen[0] == {"Authorization": f"Bearer {TOKEN}"} and r.broker.auth_failed


def test_null_broker_and_dry_run_never_touch_the_network():
    b = DryRun(NullBroker())
    assert asyncio.run(b.position()).side == 0
    ack = asyncio.run(b.enter(1, 5, 1.0, 1, 1))
    assert ack.dry and not ack.ok
    assert asyncio.run(b.flatten()).dry is True
    assert asyncio.run(b.sweep()) is True and asyncio.run(b.orders()) == []
