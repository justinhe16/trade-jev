import asyncio
from datetime import datetime
from types import SimpleNamespace

import numpy as np

from trade_jev.data import ET, LEVELS, TICK, Day, et_to_ns, load_day, secs
from trade_jev.encode import Context, Position
from trade_jev.harness import Config, run_day
from trade_jev.live import (UNDEF, Batch, Engine, Guarded, LiveDay, ParquetSource, Recorder, front_month,
                            mbp10_row)
from trade_jev.policies import Decision, Gated
from trade_jev.settings import Settings

DAY = "2026-06-23"


def synthetic(n_sec=420, per_sec=4, seed=0) -> Batch:
    """Random-walk book from 09:29:00 ET, `per_sec` rows a second, with trades."""
    rng = np.random.default_rng(seed)
    n = n_sec * per_sec
    ts = et_to_ns(DAY, "09:29:00") + np.sort(rng.integers(0, n_sec * 10**9, n)).astype(np.int64)
    bid = 80_000 + np.cumsum(rng.integers(-2, 3, n)).astype(np.int32)
    bid_px = bid[:, None] - np.arange(LEVELS, dtype=np.int32)
    ask_px = bid[:, None] + 1 + np.arange(LEVELS, dtype=np.int32)
    sz = rng.integers(1, 30, (n, LEVELS)).astype(np.int64)
    delta = np.where(rng.random(n) < 0.3, rng.integers(-5, 6, n), 0).astype(np.int64)
    return Batch(ts, bid_px, sz, ask_px, sz[:, ::-1].copy(), delta)


def as_day(b: Batch) -> Day:
    n = len(b)
    books = np.stack([b.bid_px, b.bid_sz, b.ask_px, b.ask_sz], axis=1).astype(np.int64)
    return Day(DAY, "NQU6", b.ts, b.bid_px[:, 0].copy(), b.ask_px[:, 0].copy(), np.cumsum(b.delta),
               np.arange(n), books)


class Script:
    """Deterministic BUY/SELL/HOLD per decision time."""
    name = "jev[test]"

    def __init__(self, seed=1):
        self.seed = seed

    async def __call__(self, ctx):
        r = np.random.default_rng([self.seed, ctx.t_ns % 10**12]).random()
        a = "BUY" if r < 0.4 else "SELL" if r < 0.8 else "HOLD"
        return Decision(a, {a: 0.55 + r / 2.5})


def test_liveday_matches_day_when_fed_in_chunks():
    b = synthetic()
    ref = as_day(b)
    live = LiveDay(DAY, "NQU6", cap=16)  # forces several regrows
    for i in range(0, len(b), 37):
        live.append(b[i:i + 37])
    assert np.array_equal(live.ts, ref.ts) and np.array_equal(live.bid, ref.bid)
    assert np.array_equal(live.ask, ref.ask) and np.array_equal(live.cum_delta, ref.cum_delta)
    t = ref.ts[0] + np.arange(0, 400) * secs(1)
    assert np.array_equal(live.row_at(t), ref.row_at(t))
    row = live.store_book()
    assert np.array_equal(live.book(row), ref.book(row))


def run_engine(b: Batch, cfg: Config, gate: Settings, chunk=50):
    eng = Engine(DAY, "NQU6", cfg, gate, Script(), baselines=["random", "imbalance"])

    async def go():
        for i in range(0, len(b), chunk):
            await eng.feed(b[i:i + chunk])
        await eng.finish()
    asyncio.run(go())
    return eng


def test_engine_reproduces_run_day_trades():
    b = synthetic()
    day = as_day(b)
    for cfg, gate in [
        (Config(cadence_s=15, start_et="09:30:00", end_et="09:35:00", stop_ticks=6, target_ticks=8), Settings(0.6, 2, 30, 6, 8)),
        (Config(cadence_s=5, start_et="09:30:00", end_et="09:35:00", latency_ms=0, stop_ticks=0, target_ticks=0), Settings(0.0, 1, 0, 0, 0)),
    ]:
        eng = run_engine(b, cfg, gate)
        grid = eng.grid[(eng.grid >= day.ts[0]) & (eng.grid <= day.ts[-1])]
        for name, pol in [("jev[test]", Gated(Script(), gate.min_conf, gate.agree, gate.min_hold_s)),
                          ("random", None), ("imbalance", None)]:
            from trade_jev.policies import BASELINES
            ref = asyncio.run(run_day(day, grid, pol or BASELINES[name](), cfg))
            got = eng.results[name]
            assert [(t.entry_ns, t.exit_ns, t.side, t.pnl, t.reason) for t in got.trades] == \
                   [(t.entry_ns, t.exit_ns, t.side, t.pnl, t.reason) for t in ref.trades], (name, cfg)
            assert len(got.trades) > 0 or name == "imbalance"
            assert [d["action"] for d in got.decisions] == [d["action"] for d in ref.decisions]


def test_engine_flattens_at_session_end():
    b = synthetic()
    eng = run_engine(b, Config(cadence_s=15, start_et="09:30:00", end_et="09:34:00", stop_ticks=0, target_ticks=0),
                     Settings(0.0, 1, 0, 0, 0))
    assert eng.closed
    assert all(bk.pos.side == 0 for bk in eng.books.values())


def lvl(bp, bs, ap, as_):
    return SimpleNamespace(bid_px=bp, bid_sz=bs, ask_px=ap, ask_sz=as_)


def test_mbp10_row_pads_empty_levels_and_signs_trades():
    px = lambda p: int(p * 1e9)  # noqa: E731
    levels = [lvl(px(100.00), 3, px(100.25), 4), lvl(UNDEF, 0, px(100.50), 2)] + [lvl(UNDEF, 0, UNDEF, 0)] * 8
    rec = SimpleNamespace(levels=levels, action="T", side="B", size=7, ts_event=123)
    ts, bp, bs, ap, as_, delta = mbp10_row(rec)
    assert ts == 123 and delta == 7
    assert bp[:3] == [400, 399, 398] and bs[:3] == [3, 0, 0]
    assert ap[:3] == [401, 402, 403] and as_[:3] == [4, 2, 0]
    assert len(bp) == LEVELS
    assert mbp10_row(SimpleNamespace(levels=levels, action="T", side="A", size=2, ts_event=1))[5] == -2
    assert mbp10_row(SimpleNamespace(levels=levels, action="A", side="B", size=2, ts_event=1))[5] == 0
    empty = [lvl(UNDEF, 0, UNDEF, 0)] * 10
    assert mbp10_row(SimpleNamespace(levels=empty, action="A", side="N", size=0, ts_event=1)) is None


class Slow:
    name = "jev[slow]"

    def __init__(self, delays):
        self.delays = iter(delays)

    async def __call__(self, ctx):
        await asyncio.sleep(next(self.delays))
        return Decision("BUY", {"BUY": 0.9})


def test_jev_timeout_counts_as_hold_and_breaks_streak():
    day = as_day(synthetic(n_sec=100))
    g = Gated(Guarded(Slow([0, 0.2, 0, 0]), timeout_s=0.05), 0.7, 2)
    out = [asyncio.run(g(Context(day, int(day.ts[i]), i, {15: 0, 60: 0}, Position()))).action for i in range(4)]
    assert out == ["HOLD", "HOLD", "HOLD", "BUY"]
    assert g.inner.errors == 1


def test_recorded_feed_loads_with_load_day(tmp_path):
    b = synthetic(n_sec=120)
    rec = Recorder(DAY, "NQU6", out_dir=tmp_path, rows_per_group=100)
    for i in range(0, len(b), 33):
        rec.add(b[i:i + 33])
    rec.close()
    times = b.ts[[10, 200, 400]]
    day = load_day(DAY, times, data_dir=tmp_path)
    ref = as_day(b)
    assert np.array_equal(day.ts, ref.ts) and np.array_equal(day.bid, ref.bid)
    assert np.array_equal(day.cum_delta, ref.cum_delta)
    for r in day.book_rows:
        assert np.array_equal(day.book(r), ref.book(r))
    # and streams back through ParquetSource unchanged
    async def read():
        return [p async for p in ParquetSource(rec.path, speed=0).batches()]
    parts = asyncio.run(read())
    assert np.array_equal(np.concatenate([p.ask_px for p in parts]), b.ask_px)
    assert abs(float(day.mid(0)) - (b.bid_px[0, 0] + b.ask_px[0, 0]) * TICK / 2) < 1e-9


def test_front_month_rolls_before_expiry():
    f = lambda d: front_month(datetime.fromisoformat(d).replace(tzinfo=ET))  # noqa: E731
    assert f("2026-10-02") == "NQZ6"
    assert f("2026-12-10") == "NQH7"
    assert f("2026-06-15") == "NQU6"


class MemSource:
    live, speed = False, 0

    def __init__(self, b, chunk=50):
        self.b, self.chunk = b, chunk

    async def batches(self):
        for i in range(0, len(self.b), self.chunk):
            yield self.b[i:i + self.chunk]


def test_intents_follow_the_jev_paper_trades():
    from trade_jev.live import Session
    b = synthetic()
    cfg = Config(cadence_s=5, start_et="09:30:00", end_et="09:35:00", stop_ticks=6, target_ticks=8)
    s = Session(None, cfg, Settings(0.6, 2, 0, 6, 8), DAY, "NQU6", MemSource(b), Script(), [], None, None)
    asyncio.run(s.run())
    trades = s.engine.results[s.engine.jev_name].trades
    intents = s.intents
    assert len(trades) > 3
    assert [i["seq"] for i in intents] == list(range(1, len(intents) + 1))
    assert all(a["target"] != b_["target"] for a, b_ in zip(intents, intents[1:]))   # only changes
    assert intents[-1]["target"] == 0                                                # flat at the end
    entries = [i for i in intents if i["target"]]
    assert [i["target"] for i in entries] == [t.side for t in trades]                # one per trade
    assert [i["paper_fill_px"] for i in entries] == [t.entry_px for t in trades]
    assert all(i["ref"]["ask"] is not None and i["mode"] == "replay" for i in intents)
