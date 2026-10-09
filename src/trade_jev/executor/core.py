"""Executor core: turns the live runner's intents into broker actions, inside hard limits.

Rules (agreed for Phase 2–4):
- Edge-triggered: act only on a new intent (higher `seq`). Never re-enter on our own.
- Target ±1: flat → enter; opposite → flatten, read back flat, then enter; same side → nothing.
- Target 0: flatten if in a position.
- Heartbeat safety: target 0 but in a position → flatten. Target ±1 but flat (the broker's bracket exited
  first) → alert, stay flat.
- Refuse an intent when: disarmed, halted, old/duplicate seq, older than `stale_s`, catch-up, replay
  (unless allowed), or an entry outside session hours. Exits are always allowed while armed.
- Halt (flatten where it says so, then refuse entries until re-armed off→on):
    position above max qty → flatten + halt · day P&L ≤ −max_daily_loss → flatten + halt for the day ·
    max entries reached → halt · broker error or read-back mismatch → halt ·
    no heartbeat for `heartbeat_timeout_s` while armed → flatten + halt.
"""

from __future__ import annotations

import asyncio
import json
import time
from dataclasses import asdict, dataclass
from datetime import datetime
from pathlib import Path

from trade_jev.data import ET, TICK


@dataclass
class Limits:
    qty: int = 1
    max_daily_loss: float = 2000.0
    max_orders: int = 40      # entries (new positions) per day; flattens never count
    slip_ticks: int = 2
    stale_s: float = 5.0
    heartbeat_timeout_s: float = 15.0
    session_start_et: str = "09:30:00"
    session_end_et: str = "15:55:00"
    allow_replay: bool = False


def _et(t_ns: int) -> str:
    return datetime.fromtimestamp(t_ns / 1e9, ET).strftime("%H:%M:%S")


class Executor:
    def __init__(self, broker, limits: Limits | None = None, report=None, log_path: Path | None = None,
                 now_ns=time.time_ns, mono=time.monotonic):
        self.broker, self.lim = broker, limits or Limits()
        self.report = report            # object with fill(dict) / status(dict); None in tests
        self.now_ns, self.mono = now_ns, mono
        self.log_fh = log_path.open("a") if log_path else None
        self.lock = asyncio.Lock()
        self.armed = False              # as told by the live runner
        self.halted: str | None = None
        self.day_halt = False           # daily loss: no re-arm today
        self.need_rearm = False         # after a halt, wait for armed off → on
        self.last_seq = 0
        self.target = 0
        self.last_hb = mono()
        self.orders = 0
        self.errors = 0
        self.alert: str | None = None
        self.last_action: dict | None = None
        self.last_latency_ms: float | None = None
        self.preflight: dict | None = None
        self._sent: dict[int, int] = {}   # seq → when the live runner sent it (wall ns)
        self._mode: str | None = None

    # ---------------------------------------------------------------- log / report

    def log(self, event: str, **kw) -> None:
        rec = {"ts": self.now_ns(), "event": event, **kw}
        if self.log_fh:
            self.log_fh.write(json.dumps(rec, default=str) + "\n")
            self.log_fh.flush()

    async def status(self) -> dict:
        try:
            pos = await self.broker.position()
            acct = await self.broker.account()
            pos_d, pnl = asdict(pos), acct.realized + acct.open
        except Exception as e:  # noqa: BLE001
            pos_d, pnl = None, None
            self.alert = f"can't read position: {e}"
        return {"broker": self.broker.name, "armed_ack": self.armed and not self.halted, "halted": self.halted,
                "position": pos_d, "last_action": self.last_action, "last_latency_ms": self.last_latency_ms,
                "day_pnl": pnl, "orders_today": self.orders, "errors": self.errors, "alert": self.alert,
                "preflight": self.preflight}

    async def _push_status(self) -> None:
        if self.report:
            await self.report.status(await self.status())

    # ---------------------------------------------------------------- arming

    def _set_armed(self, armed: bool) -> None:
        if not armed:
            if self.armed:
                self.log("disarmed")
            self.armed = False
            if self.halted and not self.day_halt:
                self.need_rearm = False      # off seen: the next arm clears the halt
            return
        if not self.armed:
            if self.halted and not self.day_halt and not self.need_rearm:
                self.log("halt cleared by re-arm", reason=self.halted)
                self.halted = None
                self.alert = None
            self.log("armed")
        self.armed = True

    async def halt(self, reason: str, flatten: bool = False, day: bool = False) -> None:
        self.halted = reason
        self.need_rearm = True
        self.day_halt = self.day_halt or day
        self.alert = reason
        self.log("halt", reason=reason, flatten=flatten, day=day)
        if flatten:
            await self._flatten("halt")

    # ---------------------------------------------------------------- messages

    async def on_heartbeat(self, msg: dict) -> dict:
        async with self.lock:
            self.last_hb = self.mono()
            self._set_armed(bool(msg.get("armed")))
            if not self.armed or self.halted:
                return {"ok": True, "armed": self.armed, "halted": self.halted}
            pos = await self.broker.position()
            await self._check_limits(pos)
            target = int(msg.get("target", self.target))
            if self.halted:
                pass
            elif target == 0 and pos.side != 0 and self.target == 0:
                self.log("safety flatten", reason="paper flat, real position open", pos=asdict(pos))
                await self._flatten("safety")
            elif target != 0 and pos.side == 0 and self.target == target:
                msg_ = "broker exited first; staying flat until the next signal"
                if self.alert != msg_:
                    self.alert = msg_
                    self.log("mismatch", target=target, real=0)
            return {"ok": True}

    async def on_intent(self, msg: dict) -> dict:
        async with self.lock:
            seq = int(msg.get("seq", 0))
            self.log("intent", **{k: msg.get(k) for k in ("seq", "target", "reason", "mode", "t_ns", "armed",
                                                           "catch_up", "ref")})
            if seq <= self.last_seq:
                return self._skip(seq, "old or duplicate seq")
            self.last_seq = seq
            self.target = int(msg["target"])
            self._sent[seq], self._mode = msg.get("sent_wall_ns"), msg.get("mode")
            if "armed" in msg:
                self._set_armed(bool(msg["armed"]))
            entry = self.target != 0
            why = self._refuse(msg, entry)
            if why:
                return self._skip(seq, why)
            t0 = self.mono()
            result = await self._execute(msg)
            ms = (self.mono() - t0) * 1000
            # Jev answer → order acknowledged (wall clock, live only)
            total = (self.now_ns() - msg["sent_wall_ns"]) / 1e6 if msg.get("sent_wall_ns") and msg.get("mode") == "live" else None
            self.last_latency_ms = round(total if total is not None else ms, 1)
            self.last_action = {"seq": seq, "target": self.target, "result": result, "ms": round(ms, 1),
                                "et": _et(self.now_ns())}
            self.log("done", seq=seq, result=result, ms=round(ms, 1), total_ms=total)
        await self._push_status()
        return {"ok": True, "result": result}

    def _skip(self, seq: int, why: str) -> dict:
        self.log("skip", seq=seq, why=why)
        return {"ok": True, "skipped": why}

    def _refuse(self, msg: dict, entry: bool) -> str | None:
        if not self.armed:
            return "disarmed"
        if msg.get("catch_up"):
            return "catch-up signal"
        if msg.get("mode") != "live" and not self.lim.allow_replay:
            return "replay intent (start with --allow-replay)"
        if msg.get("mode") == "live" and self.now_ns() - int(msg.get("sent_wall_ns") or 0) > self.lim.stale_s * 1e9:
            return f"stale (older than {self.lim.stale_s:g}s)"
        if self.halted and entry:
            return f"halted: {self.halted}"
        if entry:
            et = _et(int(msg["t_ns"]))
            if not (self.lim.session_start_et <= et < self.lim.session_end_et):
                return f"outside session ({et} ET)"
        return None

    # ---------------------------------------------------------------- actions

    async def _execute(self, msg: dict) -> str:
        try:
            pos = await self.broker.position()
        except Exception as e:  # noqa: BLE001
            self.errors += 1
            await self.halt(f"can't read position: {e}")
            return "halted"
        target = self.target
        if target == 0:
            return await self._flatten(msg.get("reason", "exit"), seq=msg["seq"]) if pos.side else "already flat"
        if pos.side == target:
            return "already on that side"
        if pos.side == -target:
            r = await self._flatten("reverse", seq=msg["seq"])
            if r != "flat":
                return r
        if self.orders >= self.lim.max_orders:
            await self.halt(f"max {self.lim.max_orders} entries reached")
            return "halted"
        ref = msg.get("ref") or {}
        side_px = ref.get("ask") if target > 0 else ref.get("bid")
        if side_px is None:
            return "no quote"
        limit_px = round((side_px + target * self.lim.slip_ticks * TICK) / TICK) * TICK
        ack = await self.broker.enter(target, self.lim.qty, limit_px,
                                      int(msg.get("stop_ticks", 0)), int(msg.get("target_ticks", 0)))
        self.log("enter", seq=msg["seq"], side=target, limit_px=limit_px, ack=asdict(ack))
        if ack.dry:
            return "dry-run: would enter"
        self.orders += 1
        if not ack.ok:
            self.errors += 1
            await self.halt(f"entry failed: {ack.error}")
            return "halted"
        pos = await self.broker.position()
        if pos.side != target or pos.qty > self.lim.qty:
            await self.halt(f"read-back mismatch after entry: {asdict(pos)}", flatten=pos.qty > self.lim.qty)
            return "halted"
        await self._report_fill(msg, "BUY" if target > 0 else "SELL", ack)
        return "entered"

    async def _flatten(self, why: str, seq: int | None = None) -> str:
        ack = await self.broker.flatten()
        self.log("flatten", why=why, seq=seq, ack=asdict(ack))
        if ack.dry:
            return "dry-run: would flatten"
        if not ack.ok:
            self.errors += 1
            self.halted = self.halted or f"flatten failed: {ack.error}"
            self.alert = f"FLATTEN FAILED: {ack.error}. Check the broker now."
            return "halted"
        pos = await self.broker.position()
        if pos.side != 0:
            self.halted = f"still in a position after flatten: {asdict(pos)}"
            self.alert = self.halted
            return "halted"
        if seq is not None and ack.fill_px is not None:
            await self._report_fill({"seq": seq, "sent_wall_ns": self._sent.get(seq), "mode": self._mode}, "FLAT", ack)
        return "flat"

    async def _check_limits(self, pos) -> None:
        if pos.qty > self.lim.qty:
            await self.halt(f"position size {pos.qty} above max {self.lim.qty}", flatten=True)
            return
        acct = await self.broker.account()
        if acct.realized + acct.open <= -self.lim.max_daily_loss:
            await self.halt(f"daily loss limit (−${self.lim.max_daily_loss:,.0f}) hit", flatten=True, day=True)

    async def _report_fill(self, msg: dict, action: str, ack) -> None:
        if not self.report or ack.fill_px is None:
            return
        sent = msg.get("sent_wall_ns")
        latency = round((self.now_ns() - sent) / 1e6, 1) if sent and msg.get("mode") == "live" else None
        await self.report.fill({"source": "executor", "seq": msg.get("seq"), "action": action,
                                "price": ack.fill_px, "contracts": self.lim.qty, "ack_ms": round(ack.ack_ms, 1),
                                "latency_ms": latency,
                                "note": f"auto · intent {msg.get('seq')}"})

    # ---------------------------------------------------------------- watchdog

    async def watchdog_tick(self) -> None:
        """Called every second: heartbeat loss and the daily loss limit."""
        async with self.lock:
            if not self.armed:
                return
            if self.mono() - self.last_hb > self.lim.heartbeat_timeout_s:
                self.armed = False
                pos = await self.broker.position()
                await self.halt(f"no heartbeat from the live runner for {self.lim.heartbeat_timeout_s:g}s",
                                flatten=bool(pos.side))
                return
            if not self.halted:
                await self._check_limits(await self.broker.position())
