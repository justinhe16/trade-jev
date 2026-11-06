import asyncio

import pytest

from trade_jev.copytrade import config as cfg
from trade_jev.copytrade.broker import NullBroker
from trade_jev.copytrade.executor import CopyExecutor
from trade_jev.copytrade.ledger import Ledger
from trade_jev.data import et_to_ns
from trade_jev.executor.broker import DryRun, FakeBroker

DAY = "2026-10-05"
BOOT1, BOOT2 = "boot1" * 4, "boot2" * 4


class Clock:
    def __init__(self, hm="10:00:00", day=DAY):
        self.ns, self.mono = et_to_ns(day, hm), 1000.0

    def advance(self, s):
        self.ns += int(s * 1e9)
        self.mono += s


def acct(**kw):
    base = dict(name="a1", enabled=True, mode="demo", env="demo", account_id=11, root="MNQ", qty=5,
                daily_loss_limit=1000.0, daily_profit_stop=490.0, commission=0.5, secrets_prefix="COPY_A1")
    base.update(kw)
    return cfg.Account(**base)


class Rig:
    def __init__(self, tmp_path, broker=None, hm="10:00:00", ledger=None, clock=None, **kw):
        self.clk = clock or Clock(hm)
        self.br = broker or FakeBroker()
        self.ledger = ledger or Ledger(tmp_path / "copy.db", now_ns=lambda: self.clk.ns)
        self.ex = CopyExecutor(acct(**kw), self.br, self.ledger, now_ns=lambda: self.clk.ns, mono=lambda: self.clk.mono)
        self.ex.set_kill("clear")       # the daemon's first kill poll says so; the default is unknown
        self.seq = {}

    def run(self, coro):
        return asyncio.run(coro)

    def start(self, boot=BOOT1, **hbkw):
        assert self.run(self.ex.startup())
        return self.hb(boot=boot, **hbkw)

    def hb(self, boot=BOOT1, seq_last=None, target=None, status="LIVE", mode="live", bid=20000.0, ask=20000.25):
        self.ex.note_rx()
        msg = {"type": "heartbeat", "boot": boot, "seq_last": self.ex.last_seq if seq_last is None else seq_last,
               "target": self.ex.target if target is None else target, "status": status, "mode": mode,
               "armed": False, "bid": bid, "ask": ask}
        return self.run(self.ex.on_heartbeat(msg))

    def intent(self, seq, target, boot=BOOT1, **kw):
        msg = {"type": "intent", "seq": seq, "boot": boot, "target": target, "reason": "jev", "mode": "live",
               "symbol": "NQZ6", "t_ns": self.clk.ns, "sent_wall_ns": self.clk.ns,
               "ref": {"bid": 20000.0, "ask": 20000.25}, "stop_ticks": 200, "target_ticks": 100,
               "armed": False, "catch_up": False}
        msg.update(kw)
        return self.run(self.ex.on_intent(msg))

    def trades(self):
        return [dict(r) for r in self.ledger._q("SELECT * FROM copy_trades ORDER BY id")]

    def events(self, kind=None):
        rows = [dict(r) for r in self.ledger._q("SELECT * FROM copy_events ORDER BY id")]
        return [r for r in rows if kind is None or r["kind"] == kind]

    def texts(self):
        return [r for r in self.events() if r["notify"]]

    def outcome(self, seq, boot=BOOT1):
        return self.ledger._q("SELECT outcome FROM copy_signals WHERE boot=? AND seq=?", (boot, seq))[0]["outcome"]


@pytest.fixture
def rig(tmp_path):
    r = Rig(tmp_path)
    r.start()
    return r


def entered(rig, seq=1, target=1):
    r = rig.intent(seq, target)
    assert r["result"].startswith("entered"), r
    return r


# ---------------------------------------------------------------- entries, sizing, P&L

def test_enters_at_own_size_with_slip_and_records_the_trade(rig):
    entered(rig)
    assert rig.br.pos.side == 1 and rig.br.pos.qty == 5
    assert rig.br.calls and ("enter", 1, 5, 20001.25) in rig.br.calls      # ask 20000.25 + 4 ticks
    (t,) = rig.trades()
    assert (t["qty"], t["side"], t["contract"], t["entry_px"]) == (5, "long", "MNQZ6", 20001.25)
    assert rig.br.symbol == "MNQZ6" and rig.ex.entries == 1


def test_short_entry_limit_is_bid_minus_slip(rig):
    rig.intent(1, -1)
    assert ("enter", -1, 5, 19999.0) in rig.br.calls


def test_pnl_math_5_mnq_target_and_stop(rig):
    entered(rig)
    rig.br.hit_target()                               # +25 points: 25 x $2 x 5 = $250 less 2 x 5 x $0.50
    rig.hb()
    assert rig.trades()[0]["exit_reason"] == "target"
    assert rig.trades()[0]["points"] == 25 and rig.trades()[0]["gross"] == 250 and rig.trades()[0]["pnl"] == 245
    assert rig.ex.target == 1 and rig.br.pos.side == 0
    rig.intent(2, 0)
    rig.intent(3, 1)
    rig.br.hit_stop()                                 # -50 points: -$500 less $5
    rig.hb()
    t = rig.trades()[1]
    assert (t["exit_reason"], t["points"], t["pnl"]) == ("stop", -50, -505)
    assert rig.ledger.day_pnl("a1", DAY) == 245 - 505


def test_nq_root_uses_twenty_dollar_points(tmp_path):
    r = Rig(tmp_path, root="NQ", qty=1, commission=0.0)
    r.start()
    assert r.intent(1, 1)["result"].startswith("entered")
    assert ("enter", 1, 1, 20001.25) in r.br.calls and r.br.symbol == "NQZ6"
    r.br.hit_target()
    r.hb()
    assert r.trades()[0]["pnl"] == 25 * 20


def test_open_pnl_marks_bid_for_long_ask_for_short(rig):
    entered(rig)                                      # entry 20001.25, 5 x $2/pt
    rig.hb(bid=20003.25)
    realized, open_ = rig.ex.pnl_parts()
    assert open_ == 2 * 2 * 5 and realized == -2.5    # entry commission accrued
    rig.intent(2, 0)
    rig.intent(3, -1)                                 # short at bid - 1pt = 19999.0
    rig.hb(ask=19997.0)
    assert rig.ex.pnl_parts()[1] == 2 * 2 * 5


def test_no_fresh_mark_means_open_is_ignored(rig):
    entered(rig)
    rig.hb(bid=19000.0)
    rig.clk.advance(30)
    assert rig.ex.pnl_parts()[1] == 0.0


def test_tp_sl_override_in_points(tmp_path):
    r = Rig(tmp_path, tp_points=10, sl_points=20)
    r.start()
    seen = {}
    orig = r.br.enter

    async def spy(side, qty, limit, stop_ticks, target_ticks):
        seen.update(stop=stop_ticks, target=target_ticks)
        return await orig(side, qty, limit, stop_ticks, target_ticks)
    r.br.enter = spy
    r.intent(1, 1)
    assert seen == {"stop": 80, "target": 40}


def test_slip_ticks_per_account(tmp_path):
    r = Rig(tmp_path, slip_ticks=8)
    r.start()
    r.intent(1, 1)
    assert ("enter", 1, 5, 20002.25) in r.br.calls


def test_missed_entry_is_not_an_error_and_not_a_halt(rig):
    rig.br.miss_next = True
    r = rig.intent(1, 1)
    assert r["result"].startswith("missed") and not rig.ex.halted and rig.trades() == []
    assert rig.ex.errors == 0 and rig.texts() == []


def test_three_rejects_in_a_row_halt_the_day(rig):
    from trade_jev.executor.broker import Ack

    async def reject(*a):
        return Ack(False, rejected=True, error="RiskCheckFailed")
    rig.br.enter = reject
    rig.intent(1, 1)
    rig.intent(2, 0)
    rig.intent(3, 1)
    assert not rig.ex.halted
    rig.intent(4, 0)
    rig.intent(5, 1)
    assert rig.ex.halted and "rejected" in rig.ex.halted and rig.texts()


def test_entry_cap_halts_the_day(tmp_path):
    r = Rig(tmp_path)
    r.ex.lim.max_orders = 2
    r.start()
    for seq, tgt in ((1, 1), (2, 0), (3, 1), (4, 0)):
        r.intent(seq, tgt)
    assert r.intent(5, 1)["result"] == "halted: entry cap" and r.ex.halted


def test_non_live_intents_and_bad_symbols_are_skipped(rig):
    assert rig.intent(1, 1, mode="replay")["result"].startswith("skipped: replay")
    assert rig.intent(2, 1, symbol="ESZ6")["result"].startswith("skipped: refusing symbol")
    assert rig.br.pos.side == 0


def test_no_quote_is_a_miss(rig):
    assert rig.intent(1, 1, ref={"bid": None, "ask": None})["result"] == "missed: no quote"


def test_outside_session_is_skipped(tmp_path):
    r = Rig(tmp_path, hm="09:00:00")
    r.start()
    assert r.intent(1, 1)["result"].startswith("skipped: outside session")


# ---------------------------------------------------------------- boots, seqs, replays

def test_restart_with_reset_seq_still_trades(rig):
    for s, t in ((1, 1), (2, 0), (3, 1), (4, 0), (5, 1)):
        rig.intent(s, t)
    assert rig.br.pos.side == 1
    rig.hb(boot=BOOT2, seq_last=0, target=0)           # the paper runner restarted: new boot, seq starts again
    assert rig.br.pos.side == 0                        # its position belonged to the previous run
    assert rig.texts() and rig.trades()[-1]["exit_reason"] == "session_restart"
    r = rig.intent(1, 1, boot=BOOT2)
    assert r["result"].startswith("entered") and rig.br.pos.side == 1


def test_new_boot_via_intent_with_a_position_flattens_and_texts(rig):
    entered(rig)
    r = rig.intent(1, 1, boot=BOOT2)
    assert rig.trades()[0]["exit_reason"] == "session_restart"
    assert r["result"].startswith("entered") and len(rig.trades()) == 2
    assert any("flatten" in t["kind"] for t in rig.texts())


def test_replayed_seq_is_refused_even_after_a_restart(rig, tmp_path):
    entered(rig)
    rig.intent(2, 0)
    ex2 = CopyExecutor(acct(), rig.br, rig.ledger, now_ns=lambda: rig.clk.ns, mono=lambda: rig.clk.mono)
    rig.run(ex2.startup())
    r = rig.run(ex2.on_intent({"seq": 1, "boot": BOOT1, "target": 1, "mode": "live", "symbol": "NQZ6",
                               "t_ns": rig.clk.ns, "sent_wall_ns": rig.clk.ns, "ref": {"bid": 1, "ask": 1}}))
    assert r["skipped"] == "old or duplicate seq" and rig.br.pos.side == 0


def test_duplicate_and_old_seq_ignored(rig):
    entered(rig, seq=5)
    assert rig.intent(5, -1)["skipped"] == "old or duplicate seq"
    assert rig.intent(4, -1)["skipped"] == "old or duplicate seq"
    assert rig.br.pos.side == 1


def test_heartbeat_behind_is_ignored_ahead_adopts_and_never_enters(rig):
    entered(rig, seq=3)
    assert rig.hb(seq_last=2, target=0)["ignored"] == "old heartbeat"
    assert rig.br.pos.side == 1                         # not flattened by a stale heartbeat
    rig.intent(4, 0)
    assert rig.br.pos.side == 0
    rig.hb(seq_last=6, target=1)                        # intents 5 and 6 were lost: paper is long
    assert rig.ex.last_seq == 6 and rig.ex.target == 1 and rig.br.pos.side == 0   # adopted, never entered
    assert rig.events("lost_intent")


def test_daemon_restart_mid_paper_session_adopts_without_entering(tmp_path):
    r = Rig(tmp_path)
    r.start(seq_last=7, target=1)
    assert r.ex.last_seq == 7 and r.ex.target == 1 and r.br.pos.side == 0 and not r.texts()
    assert r.intent(8, 0)["result"] == "already flat"


# ---------------------------------------------------------------- drift: paper exits first, account stops first

def test_paper_exits_first_account_is_flattened_at_market(rig):
    entered(rig)
    rig.hb(seq_last=2, target=0)                        # paper exited (intent 2 lost): the account follows at market
    assert rig.br.pos.side == 0 and rig.trades()[0]["exit_reason"] == "drift"
    assert rig.ex.target == 0 and rig.texts() == []


def test_exit_intent_closes_and_records(rig):
    entered(rig)
    r = rig.intent(2, 0, reason="target")
    assert r["result"] == "flat" and rig.br.pos.side == 0 and rig.trades()[0]["exit_reason"] == "paper_exit"


def test_account_stops_first_stays_flat_with_no_reentry_and_no_text(rig):
    entered(rig)
    rig.br.hit_stop()
    rig.hb()
    rig.hb()
    assert rig.br.pos.side == 0 and rig.ex.target == 1
    assert [c for c in rig.br.calls if c[0] == "enter"] == [("enter", 1, 5, 20001.25)]
    assert rig.texts() == [] and rig.events("stayed_flat")
    assert rig.intent(2, 0)["result"] == "already flat"
    assert rig.intent(3, 1)["result"].startswith("entered")        # the next signal trades normally


def test_stale_reversal_closes_without_entering(rig):
    entered(rig)
    old = dict(sent_wall_ns=rig.clk.ns)
    rig.clk.advance(6)
    r = rig.intent(2, -1, **old)
    assert r["result"].startswith("skipped: stale")
    assert rig.br.pos.side == 0 and rig.trades()[0]["exit_reason"] == "reversal"


def test_catch_up_reversal_closes_without_entering(rig):
    entered(rig)
    r = rig.intent(2, -1, catch_up=True)
    assert r["result"] == "skipped: catch-up signal" and rig.br.pos.side == 0


def test_reversal_enters_the_other_way(rig):
    entered(rig)
    r = rig.intent(2, -1)
    assert r["result"].startswith("entered") and rig.br.pos.side == -1
    assert [t["exit_reason"] for t in rig.trades()][:1] == ["reversal"] and len(rig.trades()) == 2


def test_account_holds_a_position_paper_never_had_flattens_and_halts(rig):
    rig.br.pos = rig.br.pos.__class__(1, 5, 20000.0)
    rig.hb()
    assert rig.br.pos.side == 0 and rig.ex.halted and rig.texts()


def test_flatten_that_leaves_an_opposite_position_halts_and_retries(rig):
    entered(rig)
    rig.br.flip_on_flatten = True
    r = rig.intent(2, 0)
    assert r == {"ok": True, "result": "halted: still in a position"} and rig.ex.halted
    assert rig.ex.need_flat
    rig.clk.advance(3)
    rig.run(rig.ex.watchdog_tick())                    # retried every 2 s
    assert rig.br.pos.side == 0 and not rig.ex.need_flat


def test_flatten_failure_halts_and_alerts(rig):
    entered(rig)
    rig.br.fail_next.add("flatten")
    assert rig.intent(2, 0)["result"] == "halted: flatten failed"
    assert rig.ex.halted and "FLATTEN FAILED" in rig.ex.alert and rig.texts()


# ---------------------------------------------------------------- gating: entries only

def test_exits_are_never_gated(rig):
    entered(rig)
    rig.ex.set_kill("unknown")
    rig.ex.token_ok = False
    rig.ex.hb_ok = False
    rig.run(rig.ex.halt_day("day", "test halt"))
    r = rig.intent(2, 0)
    assert r["result"] == "flat" and rig.br.pos.side == 0


@pytest.mark.parametrize("mutate,why", [
    (lambda e: e.set_kill("paused"), "kill"), (lambda e: e.set_kill("unknown"), "kill"),
    (lambda e: e.set_kill("autonomy_off"), "kill"), (lambda e: setattr(e, "token_ok", False), "token"),
])
def test_entries_are_gated(rig, mutate, why):
    mutate(rig.ex)
    r = rig.intent(1, 1)
    assert r["result"].startswith("skipped") and why in r["result"] and rig.br.pos.side == 0


def test_disabled_account_and_unclean_startup_block_entries(tmp_path):
    r = Rig(tmp_path, enabled=False)
    r.start()
    assert "disabled" in r.intent(1, 1)["result"]
    r2 = Rig(tmp_path / "b")
    r2.hb()
    assert r2.intent(1, 1)["result"] == "skipped: startup not clean"


@pytest.mark.parametrize("status", ["CATCH-UP", "STALE", "WAITING", "CLOSED"])
def test_non_live_status_blocks_entries_and_holds(rig, status):
    entered(rig)
    rig.hb(status=status)
    assert rig.br.pos.side == 1                          # holds the position
    r = rig.intent(2, -1)
    assert r["result"] == "skipped: paper link not live" or r["result"].startswith("skipped")
    assert rig.br.pos.side == 0                          # the reversal's exit half still ran
    assert rig.intent(3, 1)["result"].startswith("skipped")


def test_halted_entries_blocked_exits_allowed_then_clear_halts(rig):
    rig.run(rig.ex.halt_day("day", "manual"))
    assert rig.intent(1, 1)["result"].startswith("skipped: halted")
    assert rig.ledger.clear_halts("a1", DAY) == 1
    assert rig.intent(2, 0)["result"] == "already flat"
    rig.ex._refresh_day()
    assert rig.intent(3, 1)["result"].startswith("entered")


# ---------------------------------------------------------------- heartbeat loss, slow broker, EOD

def test_heartbeat_loss_flattens_and_texts(rig):
    entered(rig)
    rig.clk.advance(16)
    rig.run(rig.ex.watchdog_tick())
    assert rig.br.pos.side == 0 and rig.trades()[0]["exit_reason"] == "link_lost" and rig.texts()
    assert "link" in rig.ex.gate_reason()
    assert rig.intent(2, 1)["result"].startswith("skipped")
    rig.hb()                                            # the next LIVE heartbeat clears the block
    assert rig.ex.gate_reason() is None


def test_heartbeat_loss_with_no_position_is_silent(rig):
    rig.clk.advance(16)
    rig.run(rig.ex.watchdog_tick())
    assert rig.texts() == []


def test_heartbeat_loss_after_1555_flattens_without_a_text(tmp_path):
    r = Rig(tmp_path, hm="15:50:00")
    r.start()
    r.intent(1, 1, t_ns=et_to_ns(DAY, "15:00:00"))
    r.clk.advance(7 * 60)
    r.run(r.ex.watchdog_tick())
    assert r.br.pos.side == 0 and r.texts() == []


def test_no_false_timeout_during_a_slow_broker_call(rig):
    entered(rig)
    ex, clk, orig = rig.ex, rig.clk, rig.br.position

    async def slow(fresh=False):
        clk.advance(20)          # the broker call takes 20 s; meanwhile paper's heartbeats keep arriving
        ex.note_rx()
        return await orig(fresh)
    rig.br.position = slow
    rig.hb()
    rig.br.position = orig
    rig.run(ex.watchdog_tick())
    assert rig.br.pos.side == 1 and not ex.link_lost


def test_eod_guard_flattens_and_sweeps_at_1556(tmp_path):
    r = Rig(tmp_path, hm="15:30:00")
    r.start()
    r.intent(1, 1, t_ns=et_to_ns(DAY, "15:30:00"))
    r.clk.advance(26 * 60 + 5)                                # 15:56:05
    r.hb(seq_last=1, target=1)
    assert r.br.pos.side == 0 and r.trades()[0]["exit_reason"] == "eod"
    assert ("sweep",) in r.br.calls


# ---------------------------------------------------------------- daily stops and halts

def test_loss_stop_at_the_exact_threshold(tmp_path):
    r = Rig(tmp_path, commission=0.0)
    r.start()
    entered(r)
    r.hb(bid=20001.25 - 49.75)                         # -49.75 pt x $10 = -$497.50
    assert not r.ex.halted and r.br.pos.side == 1
    r.hb(bid=20001.25 - 100)                           # -100 pt x 2 x 5 = -$1,000 = the limit
    assert r.ex.halted and "loss limit" in r.ex.halted and r.br.pos.side == 0
    assert r.trades()[0]["exit_reason"] == "daily_stop" and r.texts()
    assert r.intent(2, 0)["result"] == "already flat"
    assert r.intent(3, 1)["result"].startswith("skipped: halted")


def test_profit_stop_at_the_exact_threshold(tmp_path):
    r = Rig(tmp_path, commission=0.0)
    r.start()
    entered(r)
    r.hb(bid=20001.25 + 48.75)                         # +$487.50
    assert not r.ex.halted
    r.hb(bid=20001.25 + 49.0)                          # +$490.00 = the stop
    assert r.ex.halted and "profit stop" in r.ex.halted and r.br.pos.side == 0


def test_realized_losses_count_toward_the_stop(tmp_path):
    r = Rig(tmp_path, commission=0.0, daily_loss_limit=500.0)
    r.start()
    entered(r)
    r.br.hit_stop()                                    # -$500
    r.hb()
    assert r.ex.halted and r.intent(2, 0)["result"] == "already flat"


def test_cash_balance_can_only_make_the_loss_check_stricter(tmp_path):
    r = Rig(tmp_path, commission=0.0)
    r.start()

    async def cash(day=None):
        return -1200.0
    r.br.cash_realized = cash
    r.clk.advance(61)
    r.hb()
    assert r.ex.halted and "loss limit" in r.ex.halted
    r2 = Rig(tmp_path / "b", commission=0.0)
    r2.start()

    async def cash2(day=None):
        return +900.0
    r2.br.cash_realized = cash2
    r2.clk.advance(61)
    r2.hb()
    assert not r2.ex.halted                            # profit check is ledger only: a cash +900 means nothing


def test_halts_survive_a_restart_and_roll_at_et_midnight(tmp_path):
    r = Rig(tmp_path)
    r.start()
    r.run(r.ex.halt_day("day", "stop"))
    ex2 = CopyExecutor(acct(), r.br, r.ledger, now_ns=lambda: r.clk.ns, mono=lambda: r.clk.mono)
    assert ex2.halted == "stop" and ex2.halt_kind == "day" and ex2.gate_reason() == "startup not clean"
    ex2.set_kill("clear")
    r.run(ex2.startup())
    assert ex2.gate_reason().startswith("halted")
    r.clk.advance(24 * 3600)                           # next ET day
    assert ex2.halted is None or ex2.gate_reason() != "halted (day)"
    ex2._refresh_day()
    assert ex2.day == "2026-10-06" and ex2.halted is None and ex2.entries == 0


def test_gate_halts_clear_by_themselves(rig):
    rig.ex.set_kill("unknown")
    assert rig.ex.gate_reason() == "kill: unknown"
    rig.ex.set_kill("clear")
    assert rig.ex.gate_reason() is None


def test_bracket_invariant_flattens_halts_and_texts(rig):
    entered(rig)
    rig.br.bracket = None                              # a child died
    rig.clk.advance(11)
    rig.hb()
    assert rig.br.pos.side == 0 and rig.ex.halted and "bracket" in rig.ex.halted and rig.texts()


def test_reduced_quantity_on_two_polls_flattens_the_rest(rig):
    entered(rig)
    rig.br.pos.qty = 3
    rig.clk.advance(11)
    rig.hb()
    assert rig.br.pos.side == 1
    rig.clk.advance(11)
    rig.hb()
    assert rig.br.pos.side == 0


def test_oversized_position_flattens_and_halts(rig):
    entered(rig)
    rig.br.pos.qty = 9
    rig.hb()
    assert rig.br.pos.side == 0 and rig.ex.halted


# ---------------------------------------------------------------- kill and flatten-all

def test_kill_flatten_closes_and_blocks(rig):
    entered(rig)
    rig.run(rig.ex.kill_flatten("paused"))
    assert rig.br.pos.side == 0 and rig.trades()[0]["exit_reason"] == "kill" and rig.ex.kill == "paused"
    assert rig.intent(2, 1)["result"].startswith("skipped: kill")


def test_flatten_all_halts_the_day_and_flattens(rig):
    entered(rig)
    assert rig.run(rig.ex.flatten_all()) == "flat"
    assert rig.br.pos.side == 0 and rig.ex.halted and rig.trades()[0]["exit_reason"] == "flatten_all"
    assert rig.intent(2, 1)["result"].startswith("skipped: halted")


# ---------------------------------------------------------------- startup and dry run

def test_startup_flattens_a_position_and_closes_the_open_trade_row(tmp_path):
    r = Rig(tmp_path)
    r.br.pos = r.br.pos.__class__(1, 5, 20000.0)
    r.ledger.trade_open("a1", DAY, BOOT1, 1, "MNQZ6", "long", 5, r.clk.ns, 20000.0, None, None)
    assert r.run(r.ex.startup())
    assert r.br.pos.side == 0 and r.trades()[0]["exit_reason"] == "startup" and r.texts()


def test_startup_when_flat_is_silent(tmp_path):
    r = Rig(tmp_path)
    assert r.run(r.ex.startup()) and r.ex.clean and r.texts() == []


def test_startup_failure_leaves_the_account_unclean(tmp_path):
    r = Rig(tmp_path)
    r.br.pos = r.br.pos.__class__(1, 5, 20000.0)
    r.br.fail_next.add("flatten")
    assert r.run(r.ex.startup()) is False and not r.ex.clean and r.texts()


def test_startup_treats_sending_rows_as_unknown(tmp_path):
    r = Rig(tmp_path)
    r.ledger.order_new(name="a1", day=DAY, boot=BOOT1, seq=1, role="entry", cl_ord_id="cj-x-e")
    assert r.run(r.ex.startup())
    assert r.ledger.orders_in_state("a1", "unknown")


def test_dry_run_sends_nothing(tmp_path):
    inner = FakeBroker()
    r = Rig(tmp_path, broker=DryRun(inner), mode="dry_run")
    r.start()
    res = r.intent(1, 1)
    assert res["result"].startswith("would enter +1 x 5 MNQZ6 @ 20001.25")
    r.intent(2, 0)
    assert [c for c in inner.calls if c[0] in ("enter", "flatten")] == []
    assert r.trades() == [] and r.ledger.orders_in_state("a1", "dry") and r.ex.entries == 0
    assert r.ex.gate_reason() is None


def test_dry_run_with_null_broker_ignores_token_state(tmp_path):
    r = Rig(tmp_path, broker=DryRun(NullBroker()), mode="dry_run")
    r.start()
    r.ex.token_ok = False
    assert r.intent(1, 1)["result"].startswith("would enter")


def test_status_row(rig):
    entered(rig)
    rig.hb(bid=20002.25)
    row = rig.ex.status_row()
    assert (row["pos_side"], row["pos_qty"], row["entries"], row["armed"], row["halted"]) == (1, 5, 1, 1, 0)
    assert row["contract"] == "MNQZ6" and row["open_pnl"] == 10.0
    rig.ledger.status(row)
    assert rig.ledger.statuses()[0]["name"] == "a1"
