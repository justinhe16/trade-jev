"""Broker interface the executor drives, plus a fake broker for tests and rehearsals.

A broker only does mechanical things: read the position, enter with an attached bracket, flatten.
All decisions (what to do, when to refuse, when to halt) live in `core.Executor`.
"""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from typing import Protocol

NQ_POINT_VALUE = 20.0


@dataclass
class Position:
    side: int = 0        # +1 long, -1 short, 0 flat
    qty: int = 0
    avg_px: float | None = None
    contract: str | None = None   # copy trading: the contract the position is in (None when unknown)


@dataclass
class Ack:
    ok: bool
    fill_px: float | None = None
    ack_ms: float = 0.0
    error: str | None = None
    dry: bool = False    # dry-run: nothing was clicked
    missed: bool = False     # copy trading: a normal miss (no fill at the limit): not an error, never a halt
    rejected: bool = False   # copy trading: the broker refused the order
    info: dict | None = None  # copy trading: entry details (qty, tp_px, sl_px, order ids)


@dataclass
class Account:
    realized: float = 0.0   # $ today, after costs
    open: float = 0.0       # $ unrealized


class Broker(Protocol):
    name: str
    dry: bool

    async def position(self) -> Position: ...
    async def working_orders(self) -> list[dict]: ...
    async def account(self) -> Account: ...
    async def enter(self, side: int, qty: int, limit_px: float, stop_ticks: int, target_ticks: int) -> Ack: ...
    async def flatten(self) -> Ack: ...
    async def healthy(self) -> tuple[bool, str | None]: ...


@dataclass
class FakeBroker:
    """In-memory broker. Fills entries at the limit (or `fill_px` if set) and flattens at `last_px`.
    `hit_stop()` / `hit_target()` simulate the broker-side bracket firing. `fail_next` injects errors."""

    name: str = "fake"
    dry: bool = False
    tick: float = 0.25
    commission: float = 2.5
    point_value: float = NQ_POINT_VALUE
    pos: Position = field(default_factory=Position)
    bracket: tuple[float, float] | None = None   # (stop_px, target_px) resting at the broker
    realized: float = 0.0
    last_px: float | None = None
    fill_px: float | None = None
    fail_next: set[str] = field(default_factory=set)
    miss_next: bool = False          # the next entry gets no fill (Ack.missed)
    flip_on_flatten: bool = False    # the next flatten leaves the opposite position (a bracket fill racing it)
    last_exit: tuple[float, str] | None = None   # (px, "target" | "stop" | "flatten") of the last close
    symbol: str | None = None
    calls: list[tuple] = field(default_factory=list)

    def _fail(self, op: str) -> Ack | None:
        if op in self.fail_next:
            self.fail_next.discard(op)
            return Ack(False, error=f"injected {op} failure")
        return None

    async def position(self, fresh: bool = False) -> Position:
        self.calls.append(("position",))
        if (a := self._fail("position")) is not None:
            raise RuntimeError(a.error)
        return Position(self.pos.side, self.pos.qty, self.pos.avg_px)

    async def working_orders(self) -> list[dict]:
        if not self.bracket:
            return []
        return [{"type": "stop", "px": self.bracket[0]}, {"type": "limit", "px": self.bracket[1]}]

    async def account(self) -> Account:
        open_ = 0.0
        if self.pos.side and self.last_px is not None:
            open_ = (self.last_px - self.pos.avg_px) * self.pos.side * self.point_value * self.pos.qty
        return Account(self.realized, open_)

    async def enter(self, side, qty, limit_px, stop_ticks, target_ticks) -> Ack:
        self.calls.append(("enter", side, qty, limit_px))
        t0 = time.monotonic()
        if (a := self._fail("enter")) is not None:
            return a
        if self.miss_next:
            self.miss_next = False
            return Ack(False, error="no fill at the limit", missed=True)
        px = self.fill_px if self.fill_px is not None else limit_px
        self.pos = Position(side, qty, px)
        self.realized -= self.commission * qty
        self.bracket = (px - side * stop_ticks * self.tick, px + side * target_ticks * self.tick)
        self.last_px = px
        return Ack(True, px, (time.monotonic() - t0) * 1000)

    def _close(self, px: float, why: str = "flatten") -> None:
        p = self.pos
        self.last_exit = (px, why)
        self.realized += (px - p.avg_px) * p.side * self.point_value * p.qty - self.commission * p.qty
        self.pos, self.bracket, self.last_px = Position(), None, px

    async def flatten(self) -> Ack:
        self.calls.append(("flatten",))
        if (a := self._fail("flatten")) is not None:
            return a
        if not self.pos.side:
            return Ack(True)
        px = self.last_px if self.last_px is not None else self.pos.avg_px
        side, qty = self.pos.side, self.pos.qty
        self._close(px)
        if self.flip_on_flatten:
            self.flip_on_flatten = False
            self.pos = Position(-side, qty, px)
        return Ack(True, px)

    async def healthy(self) -> tuple[bool, str | None]:
        return True, None

    def hit_stop(self) -> None:
        self._close(self.bracket[0], "stop")

    def hit_target(self) -> None:
        self._close(self.bracket[1], "target")

    # -- copy trading extras (the CopyExecutor drives these on any broker)

    def set_symbol(self, symbol: str) -> None:
        self.symbol = symbol

    async def orders(self, fresh: bool = False) -> list[dict]:
        return await self.working_orders() if self.pos.side else []

    async def sweep(self) -> bool:
        self.calls.append(("sweep",))
        if not self.pos.side:
            self.bracket = None
        return True

    async def exit_info(self, trade: dict) -> tuple[float, str] | None:
        return self.last_exit

    async def cash_realized(self, day: str | None = None) -> float | None:
        return None


class DryRun:
    """Wraps a broker: reads are real, `enter` / `flatten` only locate and highlight (if the broker can)
    and never click. The executor treats these acks as not executed."""

    def __init__(self, inner):
        self.inner, self.name, self.dry = inner, f"dry-run({inner.name})", True

    async def position(self, fresh=False):
        try:
            return await self.inner.position(fresh)
        except TypeError:
            return await self.inner.position()

    async def working_orders(self):
        return await self.inner.working_orders()

    async def account(self):
        return await self.inner.account()

    def set_symbol(self, symbol):
        if hasattr(self.inner, "set_symbol"):
            self.inner.set_symbol(symbol)

    async def orders(self, fresh=False):
        return await self.inner.orders(fresh) if hasattr(self.inner, "orders") else []

    async def sweep(self):
        return True   # dry: nothing was ever placed

    async def exit_info(self, trade):
        return None

    async def cash_realized(self, day=None):
        return None

    async def healthy(self):
        return await self.inner.healthy()

    async def enter(self, side, qty, limit_px, stop_ticks, target_ticks) -> Ack:
        if hasattr(self.inner, "rehearse_enter"):
            await self.inner.rehearse_enter(side, qty, limit_px)
        return Ack(False, error="dry-run: not clicked", dry=True)

    async def flatten(self) -> Ack:
        if hasattr(self.inner, "rehearse_flatten"):
            await self.inner.rehearse_flatten()
        return Ack(False, error="dry-run: not clicked", dry=True)
