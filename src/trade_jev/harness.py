"""Single-contract position/PnL harness driven by a policy every `cadence` seconds.

Semantics
- position ∈ {-1, 0, +1}. BUY → be long (open or reverse), SELL → be short, HOLD → no change.
- Market fills cross the spread using the book `latency` after the decision time
  (buy @ best ask, sell @ best bid). Commission charged per side.
- Safety net: stop / target in ticks, checked on every book update between decisions.
  Stop fills at the touch that triggered it (gaps hurt); target fills at the target price.
- Flat at session end.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass, field

import numpy as np

from trade_jev.data import POINT_VALUE, TICK, Day, ns_to_et, secs
from trade_jev.encode import LOOKBACKS, Context, Position
from trade_jev.settings import DEFAULT


@dataclass
class Config:
    cadence_s: float = 15.0
    start_et: str = "09:30:00"
    end_et: str = "15:55:00"
    latency_ms: float = 250.0
    commission: float = 2.50  # $ per side
    stop_ticks: int = DEFAULT.stop_ticks      # 0 = off
    target_ticks: int = DEFAULT.target_ticks  # 0 = off


@dataclass
class Trade:
    day: str
    side: int
    entry_ns: int
    entry_px: float
    exit_ns: int
    exit_px: float
    reason: str  # jev | stop | target | eod
    points: float
    pnl: float   # $ net of commission


@dataclass
class DayResult:
    day: str
    policy: str
    trades: list[Trade] = field(default_factory=list)
    decisions: list[dict] = field(default_factory=list)

    @property
    def pnl(self) -> float:
        return sum(t.pnl for t in self.trades)


class Book:
    """Tracks the open position for one policy on one day."""

    def __init__(self, day: Day, cfg: Config, result: DayResult):
        self.day, self.cfg, self.res = day, cfg, result
        self.pos = Position()
        self.watch_from = 0  # first row not yet checked for stop/target

    def _close(self, row: int, px: float, reason: str) -> None:
        p = self.pos
        pts = (px - p.entry_px) * p.side
        self.res.trades.append(Trade(
            self.day.day, p.side, p.entry_ns, p.entry_px, int(self.day.ts[row]), px, reason,
            round(pts, 2), round(pts * POINT_VALUE - 2 * self.cfg.commission, 2),
        ))
        self.pos = Position()

    def check_exits(self, upto_row: int) -> None:
        """Fire stop/target on rows [watch_from, upto_row]."""
        p, lo = self.pos, self.watch_from
        self.watch_from = max(self.watch_from, upto_row + 1)
        s, t = self.cfg.stop_ticks, self.cfg.target_ticks
        if p.side == 0 or upto_row < lo or not (s or t):
            return
        entry = round(p.entry_px / TICK)
        if p.side > 0:
            touch = self.day.bid[lo:upto_row + 1]
            stop = (touch <= entry - s) if s else np.zeros(len(touch), bool)
            tgt = (touch >= entry + t) if t else np.zeros(len(touch), bool)
            tgt_px = (entry + t) * TICK
        else:
            touch = self.day.ask[lo:upto_row + 1]
            stop = (touch >= entry + s) if s else np.zeros(len(touch), bool)
            tgt = (touch <= entry - t) if t else np.zeros(len(touch), bool)
            tgt_px = (entry - t) * TICK
        hit = stop | tgt
        if not hit.any():
            return
        k = int(np.argmax(hit))
        if stop[k]:
            self._close(lo + k, float(touch[k]) * TICK, "stop")
        else:
            self._close(lo + k, tgt_px, "target")

    def go(self, side: int, t_ns: int) -> None:
        """Move to `side` with a market order sent at t_ns."""
        if side == self.pos.side:
            return
        f = int(self.day.row_at(t_ns + secs(self.cfg.latency_ms / 1000)))
        self.check_exits(f)  # a stop may fire while our order is in flight
        if side == self.pos.side:
            return
        px = float(self.day.ask[f] if side > 0 else self.day.bid[f]) * TICK
        if self.pos.side != 0:
            self._close(f, px, "jev")
        self.pos = Position(side, px, int(self.day.ts[f]))
        self.watch_from = f + 1


async def run_day(day: Day, grid: np.ndarray, policy, cfg: Config) -> DayResult:
    res = DayResult(day.day, policy.name)
    bk = Book(day, cfg, res)
    rows = day.row_at(grid)
    prev = {lb: day.row_at(grid - secs(lb)) for lb in LOOKBACKS}

    for i, t in enumerate(grid):
        row = int(rows[i])
        if row < 0:
            continue
        bk.check_exits(row)
        before = bk.pos.side
        ctx = Context(day, int(t), row, {lb: int(prev[lb][i]) for lb in LOOKBACKS}, bk.pos)
        d = await policy(ctx)
        res.decisions.append({
            "day": day.day, "t_ns": int(t), "time_et": ns_to_et(int(t)), "mid": ctx.mid,
            "position": before, "action": d.action, "raw_action": d.raw_action, "probs": d.probs,
            "tokens": d.tokens, "cached": d.cached, "state": d.state,
        })
        if d.action == "BUY":
            bk.go(+1, int(t))
        elif d.action == "SELL":
            bk.go(-1, int(t))

    # session end: flatten at the touch
    end = int(grid[-1]) + secs(cfg.cadence_s) if len(grid) else 0
    last = int(day.row_at(end))
    bk.check_exits(last)
    if bk.pos.side != 0:
        px = float(day.bid[last] if bk.pos.side > 0 else day.ask[last]) * TICK
        bk._close(last, px, "eod")
    return res


def equity_curve(day: Day, grid: np.ndarray, trades: list[Trade], step_s: int = 1) -> dict:
    """Per-second mid and equity (realized net + unrealized at mid) for plotting."""
    if not len(grid):
        return {"t_ns": [], "mid": [], "equity": [], "position": []}
    t = np.arange(grid[0], grid[-1] + secs(15), secs(step_s), dtype=np.int64)
    rows = np.maximum(day.row_at(t), 0)
    mid = day.mid(rows)
    eq = np.zeros(len(t))
    pos = np.zeros(len(t), dtype=np.int8)
    for tr in trades:
        a, b = np.searchsorted(t, [tr.entry_ns, tr.exit_ns])
        eq[a:b] += (mid[a:b] - tr.entry_px) * tr.side * POINT_VALUE
        pos[a:b] = tr.side
        eq[b:] += tr.pnl
    return {"t_ns": t.tolist(), "mid": mid.tolist(), "equity": np.round(eq, 2).tolist(),
            "position": pos.tolist()}


def trade_dict(t: Trade) -> dict:
    return asdict(t)
