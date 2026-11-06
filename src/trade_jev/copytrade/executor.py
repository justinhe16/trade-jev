"""CopyExecutor: one copy account mirroring Jev's paper position changes.

Subclasses the paper-side Executor (core.py, unchanged) for its shape (Limits, log, halt vocabulary) but
overrides every decision, because a copy account differs in ways that the base class's rules cannot express:

  entries (and only entries) pass `gate()`: enabled, startup clean, kill clear, link healthy (a fresh LIVE
  heartbeat from the paper runner), token OK, no halt. Exits are never gated, never stale, never blocked by
  catch-up: closing is always allowed.
  intents are keyed by (boot, seq): the paper runner restarts its seq at 1, `boot` tells runs apart; a replay of
  a (boot, seq) already in the ledger is refused, even across a daemon restart.
  the account is not told it is armed: it arms itself from the heartbeat (mode live, status LIVE, fresh).
  drift: when the paper position differs from the account's and the account holds a position, it is flattened
  at market (paper exits first, stale/halted reversals); an account that is flat while paper is in stays flat.
  stops: realized + open <= -daily_loss_limit or >= daily_profit_stop flattens and halts the ET day (persisted).
  Halt kinds: `day` (stops, flatten failure, read-back mismatch, 3 rejects in a row, entry cap, flatten-all, a
  worker crash) lasts until the next ET day or `clear-halts`; `gate` reasons (link, token, kill) clear alone.

P&L is the account's own ledger: points x point value x qty - commission per contract per side (entry
commission accrued at entry); the open trade is marked to the paper quote (bid for long, ask for short).
"""

from __future__ import annotations

import time
from datetime import datetime

from trade_jev.copytrade.config import Account, ConfigError, symbol_for
from trade_jev.copytrade.ledger import Ledger, et_day
from trade_jev.data import ET, TICK
from trade_jev.executor.core import Executor, Limits

HEARTBEAT_LOST_S = 15.0
ORDER_POLL_S = 10.0
CASH_POLL_S = 60.0
RETRY_FLATTEN_S = 2.0
MARK_FRESH_S = 10.0
QUOTE_FROZEN_S = 60.0     # an unchanged bid/ask this long is a frozen paper feed, not a mark
EOD_FLATTEN_ET = "15:56:00"
TEXT_CUTOFF_ET = "15:55:00"
MAX_REJECTS = 3
KILL_STATES = ("clear", "paused", "autonomy_off", "unknown")


def validate_message(kind: str, msg) -> str | None:
    """None when an intent / heartbeat from the paper runner is well-formed, else why not. A malformed message is
    refused at the door (HTTP 400) and never reaches an account, so it can never day-halt one."""
    if not isinstance(msg, dict):
        return "not an object"
    isint = lambda v: isinstance(v, int) and not isinstance(v, bool)  # noqa: E731
    if kind == "intent":
        if not isint(msg.get("seq")) or msg["seq"] < 1:
            return "seq must be a positive integer"
        if not isint(msg.get("target")) or msg["target"] not in (-1, 0, 1):
            return "target must be -1, 0 or 1"
        if msg.get("boot") is not None and not isinstance(msg["boot"], str):
            return "boot must be a string"
        if not isint(msg.get("t_ns")):
            return "t_ns must be an integer"
    else:
        if msg.get("seq_last") is not None and (not isint(msg["seq_last"]) or msg["seq_last"] < 0):
            return "seq_last must be a non-negative integer"
        if msg.get("target") is not None and (not isint(msg["target"]) or msg["target"] not in (-1, 0, 1)):
            return "target must be -1, 0 or 1"
        if msg.get("boot") is not None and not isinstance(msg["boot"], str):
            return "boot must be a string"
        for k in ("bid", "ask"):
            if msg.get(k) is not None and (isinstance(msg[k], bool) or not isinstance(msg[k], (int, float))):
                return f"{k} must be a number"
    return None


def _et_time(t_ns: int) -> str:
    return datetime.fromtimestamp(t_ns / 1e9, ET).strftime("%H:%M:%S")


class CopyExecutor(Executor):
    def __init__(self, acct: Account, broker, ledger: Ledger, limits: Limits | None = None,
                 now_ns=time.time_ns, mono=time.monotonic):
        super().__init__(broker, limits or Limits(qty=acct.qty, slip_ticks=acct.slip_ticks), None, None, now_ns, mono)
        self.acct, self.ledger = acct, ledger
        self.boot: str | None = None
        self.symbol: str | None = None
        self.clean = False             # startup flatten/sweep done
        self.kill = "unknown"          # unknown until the first kill poll says otherwise; clear | paused | autonomy_off | unknown (set by the daemon's kill watcher)
        self.token_ok, self.token_expires = True, None
        self.hb: dict = {}
        self.last_rx = mono()          # stamped at the HTTP layer (note_rx), never inside the lock
        self._quote: tuple | None = None
        self._quote_t = mono()
        self._orphan_alerted = False
        self.hb_ok = False             # a LIVE, fresh heartbeat was seen since the link last dropped
        self.link_lost = False
        self.trade: dict | None = None   # the open copy trade: id, side, qty, entry_px, tp_px, sl_px, ...
        self.rejects = 0
        self.entries = 0
        self.day = et_day(now_ns())
        self.day_halt_row = None
        self.need_flat = False
        self._last_flat_try = -1e9
        self._last_order_poll = -1e9
        self._last_cash = -1e9
        self.cash_loss: float | None = None
        self._reduced = 0
        self.last_error: str | None = None
        self.pos_cache = None
        self._eod_done: str | None = None
        self.kill_text_sent = False
        self._refresh_day(force=True)

    # ------------------------------------------------------------------ plumbing

    def note_rx(self) -> None:
        """Called by the HTTP layer on receipt of any message from the paper runner."""
        self.last_rx = self.mono()

    def log(self, event: str, **kw) -> None:
        """The base class's JSON log has no file here; the lines go to stdout, which launchd sends to logs/copy.log."""
        extra = " ".join(f"{k}={v}" for k, v in kw.items() if v is not None)[:400]
        print(f"[copy] {time.strftime('%F %T')} {self.acct.name} {event} {extra}".rstrip(), flush=True)

    def event(self, kind: str, detail: str = "", notify: bool = False, seq: int | None = None) -> None:
        self.log(kind, detail=detail)
        self.ledger.event(self.acct.name, kind, detail, seq=seq, notify=notify, day=self.day)

    def _refresh_day(self, force: bool = False) -> None:
        d = et_day(self.now_ns())
        if d != self.day or force:
            self.day = d
            self.entries = self.ledger.day_entries(self.acct.name, d)
            self.rejects = 0
            self.cash_loss = None
        row = self.ledger.active_halt(self.acct.name, self.day)
        self.day_halt_row = row
        self.halted = row["reason"] if row else None
        self.day_halt = row is not None

    @property
    def halt_kind(self) -> str | None:
        return self.day_halt_row["kind"] if self.day_halt_row else None

    async def halt_day(self, kind: str, reason: str, flatten: bool = False, notify: bool = True,
                       exit_reason: str = "flatten") -> None:
        self._refresh_day()
        if not self.day_halt_row:
            self.ledger.halt(self.acct.name, self.day, kind, reason)
            self._refresh_day()
        self.alert = reason
        self.event("halt", f"{kind}: {reason}", notify)
        if flatten:
            await self._flatten("halt", exit_reason)

    async def halt(self, reason: str, flatten: bool = False, day: bool = False) -> None:   # base-class vocabulary
        await self.halt_day("day", reason, flatten)

    def gate_reason(self) -> str | None:
        """Why a NEW ENTRY is refused right now (None = allowed). Exits never ask."""
        self._refresh_day()
        a = self.acct
        if not a.enabled:
            return "disabled"
        if not self.clean:
            return "startup not clean"
        if self.kill != "clear":
            return f"kill: {self.kill}"
        if self.day_halt_row:
            return f"halted ({self.halt_kind})"
        if self.link_lost or not self.hb_ok:
            return "paper link not live"
        if not getattr(self.broker, "dry", False) and (not self.token_ok or getattr(self.broker, "auth_failed", False)):
            return "token"
        if (b := getattr(self.broker, "blocked_reason", lambda: None)()):
            return f"entries blocked: {b}"
        return None

    def _mark_for(self, side: int) -> float | None:
        """The paper quote for a position's side (bid long, ask short), only while it can be trusted: a LIVE
        heartbeat arrived recently AND the quote has moved within QUOTE_FROZEN_S (the heartbeat thread keeps
        sending the last quote when the paper loop hangs). Otherwise None: open P&L is ignored."""
        if not self.hb_ok or self.mono() - self.last_rx > MARK_FRESH_S or self.mono() - self._quote_t > QUOTE_FROZEN_S:
            return None
        return self.hb.get("bid" if side > 0 else "ask")

    def _mark(self) -> float | None:
        return self._mark_for(self.trade["side"]) if self.trade else None

    def pnl_parts(self) -> tuple[float, float]:
        """(realized, open): realized from the ledger (closed trades) less the open trade's accrued entry
        commission; open from the mark. Open is 0 with no fresh mark."""
        realized = self.ledger.day_pnl(self.acct.name, self.day)
        open_ = 0.0
        if self.trade:
            t = self.trade
            realized -= t["qty"] * self.acct.commission
            m = self._mark()
            if m is not None:
                open_ = (m - t["entry_px"]) * t["side"] * self.acct.point_value * t["qty"]
        return realized, open_

    # ------------------------------------------------------------------ startup

    async def startup(self) -> bool:
        """Close out whatever a previous process left: open ledger trades, any position, any live order. True
        when the account is flat and clean; False (try again) when the broker can't be reached or flattened."""
        self._refresh_day(force=True)
        try:
            pos = await self.broker.position(True)
            live = await self.broker.orders(True)
        except Exception as e:  # noqa: BLE001
            self.last_error = f"startup: {e}"
            return False
        stuck = [r for r in self.ledger.orders_in_state(self.acct.name, "sending")]
        dirty = bool(pos.side or live or stuck)
        if dirty:
            ack = await self.broker.flatten()
            if not ack.ok and not ack.dry:
                self.last_error = f"startup flatten: {ack.error}"
                self.event("startup_flatten_failed", str(ack.error), True)
                return False
            for t in self.ledger.open_trades(self.acct.name):
                self._close_row(dict(t), ack.fill_px, "startup")
            for r in stuck:
                self.ledger.order_update(r["cl_ord_id"], state="unknown")
            self.event("startup_flatten", f"found {pos.side * pos.qty:+d} contracts, {len(live)} orders, "
                       f"{len(stuck)} unsent rows: flattened", True)
        self.trade = None
        self.clean = True
        return True

    # ------------------------------------------------------------------ account position helpers

    async def _position(self, fresh: bool = False):
        pos = await self.broker.position(fresh)
        self.pos_cache = pos
        return pos

    def _close_row(self, t: dict, exit_px, reason: str) -> None:
        """Close the ledger row of trade `t` (a dict with id/side/qty/entry_px). The exit is the real fill when the
        broker gave one; else the trusted paper mark, else the last known quote, else the entry price, and the
        row is then flagged `estimated`."""
        a = self.acct
        side = t["side"] if isinstance(t["side"], int) else (1 if t["side"] == "long" else -1)
        px, estimated = exit_px, False
        if px is None:
            estimated = True
            px = self._mark_for(side)
            if px is None and self.hb:
                px = self.hb.get("bid" if side > 0 else "ask")
            if px is None:
                px = t["entry_px"]
            self.event("estimated_close", f"{reason}: no fill price from the broker, booked at {px}")
        points = (px - t["entry_px"]) * side
        gross = points * a.point_value * t["qty"]
        comm = 2 * t["qty"] * a.commission
        self.ledger.trade_close(t["id"], self.now_ns(), px, reason, points, gross, comm, gross - comm, estimated)

    # ------------------------------------------------------------------ messages

    def _new_boot(self, boot: str) -> None:
        self.boot, self.last_seq, self.target = boot, 0, 0
        self.event("new_boot", f"paper run {boot[:8]}")

    async def on_heartbeat(self, msg: dict) -> dict:
        async with self.lock:
            if (bad := validate_message("heartbeat", msg)):
                return {"ok": True, "ignored": f"malformed heartbeat: {bad}"}
            self._refresh_day()
            boot = msg.get("boot")
            if boot and boot != self.boot:
                self._new_boot(boot)
                if self.clean and self.trade:   # a position that belongs to a previous paper run
                    await self._flatten("new paper run", "session_restart", notify=True)
            self.hb = msg
            q = (msg.get("bid"), msg.get("ask"))
            if q != self._quote:
                self._quote, self._quote_t = q, self.mono()
            live = msg.get("mode") == "live" and msg.get("status") == "LIVE"
            if self.link_lost and live:
                self.link_lost = False
            self.hb_ok = live
            seq_last = int(msg.get("seq_last") or 0)
            if self.boot and seq_last < self.last_seq:
                return {"ok": True, "ignored": "old heartbeat"}
            if self.boot and seq_last > self.last_seq:   # an intent was lost: adopt, never enter
                self.last_seq, self.target = seq_last, int(msg.get("target") or 0)
                self.event("lost_intent", f"adopted seq {seq_last}, target {self.target}; no entry")
            elif self.boot and msg.get("target") is not None:
                self.target = int(msg["target"])       # same seq: paper's own word on its position
            if not self.clean:
                return {"ok": True}
            await self._reconcile()
            return {"ok": True}

    async def _reconcile(self) -> None:
        """Position vs paper target vs trade row, then stops, bracket invariant, EOD. Holds the lock."""
        try:
            pos = await self._position()
        except Exception as e:  # noqa: BLE001
            self.last_error = f"can't read position: {e}"
            self.errors += 1
            return
        self.last_error = None if self.last_error and self.last_error.startswith("can't read") else self.last_error
        if self.trade and not pos.side:
            await self._native_exit()
        elif pos.side:
            if not self.trade or pos.side != self.trade["side"]:
                # a position we did not open (or the wrong side): flatten, halt, tell the owner
                await self.halt_day("day", f"unexpected position {pos.side * pos.qty:+d}", False)
                await self._flatten("unexpected position", "drift", notify=True)
                return
            if self.target != pos.side:   # paper is flat or the other way: the account follows
                await self._flatten("drift", "drift")
                return
            await self._check_position(pos)
        elif not pos.side and not self.trade and self.mono() - self._last_order_poll >= ORDER_POLL_S:
            await self._poll_orphans()
            if self.target != 0 and self.alert != "stayed flat" and not getattr(self.broker, "dry", False):
                self.alert = "stayed flat"
                self.event("stayed_flat", f"paper target {self.target}, account flat: staying flat (no text)")
        elif self.target != 0 and not self.trade:
            if self.alert != "stayed flat" and not getattr(self.broker, "dry", False):
                self.alert = "stayed flat"
                self.event("stayed_flat", f"paper target {self.target}, account flat: staying flat (no text)")
        await self._check_stops()
        await self._eod_guard()

    async def _poll_orphans(self) -> None:
        """Flat, no trade, no entry in flight (the lock is ours): any live order is an orphan that could open a
        position against paper. Sweep it; alert if it can't be swept (once, until a sweep works)."""
        self._last_order_poll = self.mono()
        try:
            live = await self.broker.orders(True)
        except Exception as e:  # noqa: BLE001
            self.last_error = f"can't read orders: {e}"
            return
        if not live:
            self._orphan_alerted = False
            return
        if await self.broker.sweep():
            self.event("orphan_swept", f"{len(live)} live order(s) on a flat account cancelled", True)
            self._orphan_alerted = False
            return
        if not self._orphan_alerted:
            self._orphan_alerted = True
            await self.halt_day("day", f"{len(live)} live order(s) on a flat account could not be cancelled")

    async def _check_position(self, pos) -> None:
        a = self.acct
        if pos.qty > a.qty:
            await self.halt_day("day", f"position size {pos.qty} above {a.qty}", True)
            return
        if self.mono() - self._last_order_poll >= ORDER_POLL_S:
            self._last_order_poll = self.mono()
            try:
                live = await self.broker.orders(True)
            except Exception as e:  # noqa: BLE001
                self.last_error = f"can't read orders: {e}"
                return
            if len(live) < 2:
                await self.halt_day("day", f"bracket invariant: {len(live)} live sibling order(s)", True)
                return
        if pos.qty < a.qty:
            self._reduced += 1
            if self._reduced >= 2:
                self.event("reduced", f"{pos.qty} of {a.qty} on two reads: flattening the rest", True)
                await self._flatten("reduced", "partial", notify=False)
        else:
            self._reduced = 0

    async def _native_exit(self) -> None:
        t = self.trade
        info = await self.broker.exit_info(t)
        px, why = info if info else (None, "flatten")
        reason = {"target": "target", "stop": "stop"}.get(why, "drift")
        self._close_row(t, px, reason)
        self.trade = None
        self._reduced = 0
        ok = await self.broker.sweep()
        self.event("native_exit", f"{reason} at {px}")
        if not ok:
            await self.halt_day("day", "orphan orders could not be cancelled after an exit")

    async def _check_stops(self) -> None:
        if self.day_halt_row:
            return
        a = self.acct
        realized, open_ = self.pnl_parts()
        total = realized + open_
        if self.mono() - self._last_cash >= CASH_POLL_S and hasattr(self.broker, "cash_realized"):
            self._last_cash = self.mono()
            c = await self.broker.cash_realized(self.day)
            self.cash_loss = c
        loss_view = total
        if self.cash_loss is not None:
            loss_view = min(total, self.cash_loss + open_)
        if loss_view <= -a.daily_loss_limit:
            await self.halt_day("day", f"daily loss limit (-${a.daily_loss_limit:,.0f}) hit at ${loss_view:,.2f}", True,
                                 exit_reason="daily_stop")
        elif total >= a.daily_profit_stop:
            await self.halt_day("day", f"daily profit stop (+${a.daily_profit_stop:,.0f}) hit at ${total:,.2f}", True,
                                 exit_reason="daily_stop")

    async def _eod_guard(self) -> None:
        """From 15:56 ET until flat: flatten and sweep. A transient read error retries on the next tick."""
        if _et_time(self.now_ns()) < EOD_FLATTEN_ET or self._eod_done == self.day:
            return
        try:
            pos = await self._position(True)
            if pos.side or await self.broker.orders(True):
                if await self._flatten("end of day", "eod", notify=False) == "flat":
                    await self.broker.sweep()
                pos = await self._position(True)
            if not pos.side:
                self._eod_done = self.day
        except Exception as e:  # noqa: BLE001 — retried every tick, never a day halt
            self.last_error = f"eod guard: {type(e).__name__}"

    async def on_intent(self, msg: dict) -> dict:
        async with self.lock:
            self._refresh_day()
            if (bad := validate_message("intent", msg)):
                return self._skip(0, f"malformed intent: {bad}")
            boot, seq = msg.get("boot"), int(msg.get("seq") or 0)
            if not boot:
                return self._skip(seq, "no boot id")
            if boot != self.boot:
                self._new_boot(boot)
                if self.trade and self.clean:
                    await self._flatten("new paper run", "session_restart", notify=True)
            if seq <= self.last_seq or self.ledger.signal_seen(self.acct.name, boot, seq):
                return self._skip(seq, "old or duplicate seq")
            self.last_seq = seq
            self.target = target = int(msg["target"])
            self.ledger.signal(self.acct.name, boot, seq, self.day, target, msg.get("reason"), msg.get("t_ns"),
                               "received")
            if msg.get("mode") != "live":
                return self._done(boot, seq, "skipped: replay intent")
            if not self.clean:
                return self._done(boot, seq, "skipped: startup not clean")
            if target == 0:
                return self._done(boot, seq, await self._exit(msg))
            return self._done(boot, seq, await self._entry(msg))

    def _skip(self, seq: int, why: str) -> dict:
        self.log("skip", seq=seq, why=why)
        return {"ok": True, "skipped": why}

    def _done(self, boot: str, seq: int, outcome: str) -> dict:
        self.ledger.signal_outcome(self.acct.name, boot, seq, outcome)
        self.log("done", seq=seq, outcome=outcome)
        return {"ok": True, "result": outcome}

    async def _exit(self, msg: dict) -> str:
        """Paper went flat: close. Never gated, never stale."""
        try:
            pos = await self._position(True)
        except Exception as e:  # noqa: BLE001
            self.last_error = f"can't read position: {e}"
            self.need_flat = True
            return f"error: can't read position: {e}"
        if not pos.side:
            if self.trade:
                await self._native_exit()
            return "already flat"
        r = await self._flatten(msg.get("reason") or "exit", "paper_exit")
        return r

    async def _entry(self, msg: dict) -> str:
        target, a = self.target, self.acct
        try:
            pos = await self._position(True)
        except Exception as e:  # noqa: BLE001
            self.last_error = f"can't read position: {e}"
            return f"skipped: can't read position: {e}"
        if pos.side == target:
            return "already on that side"
        if pos.side == -target:   # reversal: close first (exit half is never gated), then recheck age
            r = await self._flatten("reverse", "reversal")
            if r != "flat":
                return r
        if msg.get("catch_up"):
            return "skipped: catch-up signal"
        age = self.now_ns() - int(msg.get("sent_wall_ns") or 0)
        if age > self.lim.stale_s * 1e9:
            return f"skipped: stale (older than {self.lim.stale_s:g}s)"
        why = self.gate_reason()
        if why:
            return f"skipped: {why}"
        et = _et_time(int(msg["t_ns"]))
        if not (self.lim.session_start_et <= et < self.lim.session_end_et):
            return f"skipped: outside session ({et} ET)"
        if self.entries >= self.lim.max_orders:
            await self.halt_day("day", f"max {self.lim.max_orders} entries reached")
            return "halted: entry cap"
        try:
            sym = symbol_for(a.root, msg.get("symbol"))
        except ConfigError as e:
            self.event("bad_symbol", str(e), True)
            return f"skipped: {e}"
        ref = msg.get("ref") or {}
        touch = ref.get("ask") if target > 0 else ref.get("bid")
        if touch is None:
            return "missed: no quote"
        limit_px = round((touch + target * a.slip_ticks * TICK) / TICK) * TICK
        tp_t = round(a.tp_points / TICK) if a.tp_points else int(msg.get("target_ticks") or 0)
        sl_t = round(a.sl_points / TICK) if a.sl_points else int(msg.get("stop_ticks") or 0)
        self.symbol = sym
        self.broker.set_symbol(sym)
        if hasattr(self.broker, "set_context"):
            self.broker.set_context(boot=self.boot, seq=int(msg["seq"]))
        ack = await self.broker.enter(target, a.qty, limit_px, sl_t, tp_t)
        self.log("enter", seq=msg["seq"], side=target, limit_px=limit_px, ack=ack.__dict__)
        if ack.dry:
            self.ledger.order_new(name=a.name, day=self.day, boot=self.boot, seq=int(msg["seq"]), role="entry",
                                  cl_ord_id=f"cj-{a.name}-{str(self.boot)[:8]}-{msg['seq']}-e", contract=sym,
                                  action="Buy" if target > 0 else "Sell", qty=a.qty, order_type="Limit",
                                  limit_px=limit_px, tif="IOC", state="dry")
            return f"would enter {target:+d} x {a.qty} {sym} @ {limit_px:.2f}"
        self.entries += 1
        if ack.info and (ack.info.get("partial") or ack.info.get("closed")):
            self._record_closed_at_entry(msg, ack, target, sym)
        if ack.info and ack.info.get("partial") and not ack.ok:   # the partial fill could not be closed
            self.need_flat = True
            await self.halt_day("day", f"partial fill could not be closed: {ack.error}")
            return "halted: partial fill not closed"
        if ack.missed:
            self.rejects = 0
            self.event("missed", str(ack.error), False, seq=int(msg["seq"]))
            return f"missed: {ack.error}"
        if ack.rejected:
            self.rejects += 1
            self.event("rejected", str(ack.error), False, seq=int(msg["seq"]))
            if self.rejects >= MAX_REJECTS:
                await self.halt_day("day", f"{MAX_REJECTS} rejected entries in a row: {ack.error}")
            return f"rejected: {ack.error}"
        if not ack.ok:
            self.errors += 1
            self.last_error = str(ack.error)
            await self.halt_day("day", f"entry failed: {ack.error}")
            return "halted: entry failed"
        self.rejects = 0
        info = ack.info or {}
        qty = int(info.get("qty") or a.qty)
        try:
            pos = await self._position(True)
        except Exception as e:  # noqa: BLE001
            pos = None
            self.last_error = f"can't read position: {e}"
        if pos is None or pos.side != target or pos.qty != a.qty:
            await self.halt_day("day", f"read-back mismatch after entry: {pos}", True)
            return "halted: read-back mismatch"
        entry_px = ack.fill_px if ack.fill_px is not None else limit_px
        tid = self.ledger.trade_open(a.name, self.day, self.boot, int(msg["seq"]), sym, "long" if target > 0 else "short",
                                     qty, self.now_ns(), entry_px, info.get("tp_px"), info.get("sl_px"))
        self.trade = {"id": tid, "side": target, "qty": qty, "entry_px": entry_px, "tp_px": info.get("tp_px"),
                      "sl_px": info.get("sl_px"), "tp_id": info.get("tp_id"), "sl_id": info.get("sl_id")}
        self._last_order_poll = self.mono()
        self.alert = None
        return f"entered {target:+d} x {qty} {sym} @ {entry_px:.2f}"

    def _record_closed_at_entry(self, msg, ack, target, sym) -> None:
        """A partial fill or a bracket failure that the broker closed itself: keep the P&L in the ledger."""
        i = ack.info or {}
        px = i.get("entry_px")
        if px is None:
            return
        side = i.get("side") or target
        tid = self.ledger.trade_open(self.acct.name, self.day, self.boot, int(msg["seq"]), sym,
                                     "long" if side > 0 else "short", int(i.get("qty") or self.acct.qty),
                                     self.now_ns(), px, None, None)
        self._close_row({"id": tid, "side": side, "qty": int(i.get("qty") or self.acct.qty), "entry_px": px},
                        ack.fill_px, i.get("exit_reason") or "partial")

    # ------------------------------------------------------------------ flatten

    async def _flatten(self, why: str, exit_reason: str, notify: bool = False) -> str:
        self._last_flat_try = self.mono()
        ack = await self.broker.flatten()
        self.log("flatten", why=why, ack=ack.__dict__)
        if ack.dry:
            self.need_flat = False
            return "dry-run: would flatten"
        if not ack.ok:
            self.errors += 1
            self.last_error = str(ack.error)
            self.need_flat = True
            await self.halt_day("day", f"flatten failed: {ack.error}")
            self.alert = f"FLATTEN FAILED: {ack.error}. Check the broker now."
            return "halted: flatten failed"
        try:
            pos = await self._position(True)
        except Exception as e:  # noqa: BLE001
            self.need_flat = True
            self.last_error = f"can't read position: {e}"
            return "halted: can't confirm flat"
        if pos.side:
            self.need_flat = True
            await self.halt_day("day", f"still in a position after flatten: {pos.side * pos.qty:+d}")
            return "halted: still in a position"
        self.need_flat = False
        if self.trade:
            t, self.trade = self.trade, None
            self._close_row(t, ack.fill_px, exit_reason)
            self._reduced = 0
        if notify:
            self.event("flattened", f"{why}", True)
        return "flat"

    # ------------------------------------------------------------------ watchdog / kill / flatten-all

    async def watchdog_tick(self) -> None:
        """Called about every second by the account worker."""
        async with self.lock:
            self._refresh_day()
            if not self.clean:
                return
            if self.need_flat and self.mono() - self._last_flat_try >= RETRY_FLATTEN_S:
                await self._flatten("retry", "flatten")
            elif ((self.kill == "paused" or self.halted == "flatten-all")
                  and self.mono() - self._last_flat_try >= RETRY_FLATTEN_S):
                # a kill / flatten-all flatten may have been cancelled half way: any position seen is flattened
                try:
                    pos = await self._position()
                except Exception:  # noqa: BLE001
                    pos = None
                if pos is not None and pos.side:
                    await self._flatten("kill" if self.kill == "paused" else "flatten-all", "kill"
                                        if self.kill == "paused" else "flatten_all")
            if self.mono() - self.last_rx > HEARTBEAT_LOST_S:
                if not self.link_lost:
                    self.link_lost, self.hb_ok = True, False
                    self.event("link_lost", f"no paper heartbeat for {HEARTBEAT_LOST_S:g}s")
                try:
                    pos = await self._position()
                except Exception:  # noqa: BLE001
                    return
                if pos.side:
                    late = _et_time(self.now_ns()) >= TEXT_CUTOFF_ET
                    await self._flatten("heartbeat lost", "link_lost", notify=not late)
                return
            await self._eod_guard()

    async def kill_flatten(self, state: str) -> None:
        """The daemon's kill watcher saw `paused`. `need_flat` is set BEFORE waiting for the lock or the broker, so a
        flatten that is cancelled half way (the daemon's timeout) is retried by the watchdog until flat."""
        self.kill = state
        if state != "paused":
            return
        self.need_flat = True
        async with self.lock:
            await self._flatten("kill", "kill", notify=bool(self.trade))

    async def flatten_all(self) -> str:
        """The owner's flatten-all: halt the account for the ET day, then flatten (retried like a kill)."""
        self.need_flat = True
        async with self.lock:
            await self.halt_day("day", "flatten-all", False)
            return await self._flatten("flatten-all", "flatten_all", notify=True)

    def set_kill(self, state: str) -> None:
        if state not in KILL_STATES:
            state = "unknown"
        self.kill = state

    # ------------------------------------------------------------------ status

    def status_row(self) -> dict:
        self._refresh_day()
        a = self.acct
        realized, open_ = self.pnl_parts()
        pos = self.pos_cache
        why = self.gate_reason()
        return {"name": a.name, "day": self.day, "updated_at": datetime.fromtimestamp(self.now_ns() / 1e9, ET).isoformat(),
                "mode": a.mode, "env": a.env, "root": a.root, "qty": a.qty, "contract": self.symbol,
                "armed": 1 if why is None else 0, "gate_reason": why,
                "pos_side": (self.trade["side"] if self.trade else (pos.side if pos else 0)) or 0,
                "pos_qty": (self.trade["qty"] if self.trade else (pos.qty if pos else 0)) or 0,
                "avg_px": self.trade["entry_px"] if self.trade else None,
                "realized": round(realized, 2), "open_pnl": round(open_, 2),
                "halted": 1 if self.day_halt_row else 0, "halt_kind": self.halt_kind,
                "token_ok": 1 if self.token_ok else 0, "token_expires_at": self.token_expires,
                "entries": self.entries, "errors": self.errors, "last_error": (self.last_error or None)}

