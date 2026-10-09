import asyncio

from trade_jev.data import et_to_ns
from trade_jev.executor.broker import DryRun, FakeBroker
from trade_jev.executor.core import Executor, Limits

DAY = "2026-10-05"
T = et_to_ns(DAY, "10:00:00")


class Clock:
    def __init__(self):
        self.ns, self.mono = T, 0.0

    def advance(self, s):
        self.ns += int(s * 1e9)
        self.mono += s


class Reports:
    def __init__(self):
        self.fills, self.statuses = [], []

    async def fill(self, b):
        self.fills.append(b)

    async def status(self, b):
        self.statuses.append(b)


def make(**lim):
    clk, rep, br = Clock(), Reports(), FakeBroker()
    ex = Executor(br, Limits(**lim), rep, now_ns=lambda: clk.ns, mono=lambda: clk.mono)
    return ex, br, clk, rep


def intent(seq, target, clk, **kw):
    msg = {"seq": seq, "target": target, "reason": "jev", "mode": "live", "t_ns": clk.ns,
           "sent_wall_ns": clk.ns, "ref": {"bid": 20000.00, "ask": 20000.25},
           "stop_ticks": 200, "target_ticks": 100, "armed": True, "catch_up": False}
    msg.update(kw)
    return msg


def run(coro):
    return asyncio.run(coro)


def arm(ex, clk, target=0):
    run(ex.on_heartbeat({"armed": True, "target": target}))


def test_enters_with_marketable_limit_and_reports_fill():
    ex, br, clk, rep = make()
    arm(ex, clk)
    r = run(ex.on_intent(intent(1, +1, clk)))
    assert r["result"] == "entered"
    enters = lambda: [c for c in br.calls if c[0] == "enter"]  # noqa: E731
    assert br.pos.side == 1 and enters()[-1][3] == 20000.75   # ask + 2 ticks
    assert rep.fills[-1]["action"] == "BUY" and rep.fills[-1]["seq"] == 1
    r = run(ex.on_intent(intent(2, -1, clk)))    # sell limit = bid − 2 ticks
    assert enters()[-1] == ("enter", -1, 1, 19999.50)


def test_ignores_duplicate_old_stale_and_catch_up():
    ex, br, clk, _ = make()
    arm(ex, clk)
    run(ex.on_intent(intent(5, +1, clk)))
    n = len(br.calls)
    assert run(ex.on_intent(intent(5, -1, clk)))["skipped"] == "old or duplicate seq"
    assert run(ex.on_intent(intent(4, -1, clk)))["skipped"] == "old or duplicate seq"
    old = intent(6, -1, clk)
    clk.advance(6)
    assert run(ex.on_intent(old))["skipped"].startswith("stale")
    assert run(ex.on_intent(intent(7, -1, clk, catch_up=True)))["skipped"] == "catch-up signal"
    assert run(ex.on_intent(intent(8, -1, clk, mode="replay")))["skipped"].startswith("replay")
    assert len(br.calls) == n and br.pos.side == 1


def test_reversal_flattens_reads_back_then_enters():
    ex, br, clk, _ = make()
    arm(ex, clk)
    run(ex.on_intent(intent(1, +1, clk)))
    br.calls.clear()
    run(ex.on_intent(intent(2, -1, clk)))
    ops = [c[0] for c in br.calls]
    assert ops[:5] == ["position", "flatten", "position", "enter", "position"]   # then a status read
    assert br.pos.side == -1 and br.pos.qty == 1


def test_target_zero_flattens_and_same_side_does_nothing():
    ex, br, clk, _ = make()
    arm(ex, clk)
    run(ex.on_intent(intent(1, +1, clk)))
    assert run(ex.on_intent(intent(2, 0, clk, reason="stop")))["result"] == "flat"
    assert br.pos.side == 0
    assert run(ex.on_intent(intent(3, 0, clk)))["result"] == "already flat"


def test_broker_exit_first_stays_flat_and_alerts():
    ex, br, clk, _ = make()
    arm(ex, clk)
    run(ex.on_intent(intent(1, +1, clk)))
    br.hit_stop()                                   # broker bracket fired; paper still long
    run(ex.on_heartbeat({"armed": True, "target": 1}))
    assert br.pos.side == 0 and "broker exited first" in ex.alert
    assert [c[0] for c in br.calls].count("enter") == 1   # never re-enters on its own


def test_paper_flat_but_real_position_is_flattened():
    ex, br, clk, _ = make()
    arm(ex, clk)
    br.pos.side, br.pos.qty, br.pos.avg_px, br.last_px = 1, 1, 20000.0, 20000.0   # e.g. a stray position
    run(ex.on_heartbeat({"armed": True, "target": 0}))
    assert br.pos.side == 0


def test_daily_loss_limit_flattens_and_halts_for_the_day():
    ex, br, clk, _ = make(max_daily_loss=2000)
    arm(ex, clk)
    run(ex.on_intent(intent(1, +1, clk)))
    br.last_px = br.pos.avg_px - 101               # 101 points × $20 = −$2,020 open
    run(ex.watchdog_tick())
    assert br.pos.side == 0 and ex.halted and ex.day_halt
    run(ex.on_heartbeat({"armed": False}))          # re-arm does not clear a daily-loss halt
    arm(ex, clk)
    assert ex.halted
    assert run(ex.on_intent(intent(2, +1, clk)))["skipped"].startswith("halted")


def test_max_entries_halts_new_entries_but_flattens_dont_count():
    ex, br, clk, _ = make(max_orders=2)
    arm(ex, clk)
    run(ex.on_intent(intent(1, +1, clk)))          # entry 1
    run(ex.on_intent(intent(2, 0, clk)))           # flatten: not counted
    run(ex.on_intent(intent(3, -1, clk)))          # entry 2
    assert run(ex.on_intent(intent(4, +1, clk)))["result"] == "halted"   # flatten ok, entry 3 refused
    assert br.pos.side == 0 and "max 2 entries" in ex.halted


def test_disarmed_makes_no_broker_calls():
    ex, br, clk, _ = make()
    r = run(ex.on_intent(intent(1, +1, clk, armed=False)))
    assert r["skipped"] == "disarmed" and br.calls == []


def test_heartbeat_loss_flattens_and_halts_until_rearmed():
    ex, br, clk, _ = make(heartbeat_timeout_s=15)
    arm(ex, clk)
    run(ex.on_intent(intent(1, +1, clk)))
    clk.advance(16)
    run(ex.watchdog_tick())
    assert br.pos.side == 0 and ex.halted and not ex.armed
    arm(ex, clk)                                    # still armed on the page: halt stays
    assert ex.halted
    run(ex.on_heartbeat({"armed": False}))
    arm(ex, clk)                                    # off → on clears it
    assert ex.halted is None


def test_entries_only_in_session_but_exits_always():
    ex, br, clk, _ = make()
    arm(ex, clk)
    late = et_to_ns(DAY, "15:56:00")
    clk.ns = late
    assert run(ex.on_intent(intent(1, +1, clk)))["skipped"].startswith("outside session")
    br.pos.side, br.pos.qty, br.pos.avg_px, br.last_px = 1, 1, 20000.0, 20000.0
    assert run(ex.on_intent(intent(2, 0, clk, reason="eod")))["result"] == "flat"


def test_failed_entry_halts():
    ex, br, clk, _ = make()
    arm(ex, clk)
    br.fail_next.add("enter")
    assert run(ex.on_intent(intent(1, +1, clk)))["result"] == "halted"
    assert "entry failed" in ex.halted


def test_dry_run_never_changes_the_position():
    clk, rep, br = Clock(), Reports(), FakeBroker()
    ex = Executor(DryRun(br), Limits(), rep, now_ns=lambda: clk.ns, mono=lambda: clk.mono)
    arm(ex, clk)
    assert run(ex.on_intent(intent(1, +1, clk)))["result"] == "dry-run: would enter"
    assert br.pos.side == 0 and rep.fills == [] and ex.orders == 0
