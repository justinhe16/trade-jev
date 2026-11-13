"""Tradovate order access for one copy account, behind the executor's broker interface.

Only this module (and the rehearse/preflight commands) calls order, position and account endpoints; the paper
feed (tradovate.py) never does. Every call goes through `_call`: a bearer token from `token_fn`, a 4 s timeout,
redacted errors, and the response classes of the plan:

  failureReason other than Success   -> rejected (a normal refusal)
  p-ticket                            -> an entry counts a miss and is never resent; a flatten waits p-time and
                                         resends with the ticket
  p-captcha or HTTP 429               -> entries blocked for an hour, text
  timeout / 5xx / transport error on an order POST -> the outcome is unknown: an entry is never POSTed again
                                         (the clOrdId is searched in order/list, see `_recover`)

The bracket is `placeoso` (IOC limit parent, a limit TP and a stop SL as OCO children), then `modifyorder`
on both children to the real fill price. TP/SL are always attached with the entry, never placed after the fill.
`http` has the signature of tradovate.default_http: (method, url, headers, body) -> (status, dict); a JSON list
comes back as {"d": [...]}.
"""

from __future__ import annotations

import asyncio
import json
import re
import time
import urllib.error
import urllib.request
from concurrent.futures import ThreadPoolExecutor

from trade_jev import tradovate
from trade_jev.copytrade.config import Account, symbol_for  # noqa: F401  (symbol_for re-exported for callers)
from trade_jev.copytrade.ledger import Ledger, et_day
from trade_jev.data import TICK
from trade_jev.executor.broker import Ack, Account as PnlAccount, Position
from trade_jev.tradovate import TradovateError, redact

REST = {"demo": "https://demo.tradovateapi.com/v1", "live": "https://live.tradovateapi.com/v1"}
HTTP_TIMEOUT_S = 4.0
TERMINAL = {"Filled", "Canceled", "Rejected", "Expired", "Completed"}
NOT_FILLED = {"Canceled", "Rejected", "Expired"}
LIVE = {"Working", "Suspended", "PendingNew", "PendingReplace"}
SETTLE_S, SETTLE_STEP_S = 2.0, 0.2
CANCEL_RESOLVE_S = 3.0   # after a cancel, how long to wait for the parent to read terminal (IOC parents linger "Working")
CONN_ERR = re.compile(r"URLError|TimeoutError|timed out|nodename nor servname|Errno|ConnectionError|"
                      r"Connection (?:refused|reset)|SSLError|HTTP (?:5\d\d|429)", re.I)
VERIFY_S = 1.0
FLAT_WAIT_S = 3.0
BLOCK_S = 3600.0
MAX_PENALTY_WAIT_S = 30.0
READ_TTL_POSITION_S, READ_TTL_FLAT_S, READ_TTL_ORDERS_S = 2.0, 5.0, 10.0


def is_conn_error(err) -> bool:
    """True for a network-class failure (DNS, timeout, reset, HTTP 5xx / 429): the read said nothing about the account."""
    return bool(CONN_ERR.search(f"{type(err).__name__}: {err}"))


class BrokerError(Exception):
    """A read or request failed (message redacted). Nothing was changed by a failed read."""


class UnknownOutcome(BrokerError):
    """An order POST timed out or hit a 5xx: the order may or may not exist."""


class RateLimited(BrokerError):
    pass


def make_http(pool: ThreadPoolExecutor | None = None, timeout: float = HTTP_TIMEOUT_S):
    """The real transport (urllib in the account's own thread pool). Tests never call this: an autouse fixture
    makes it raise, and they inject a fake instead."""
    async def http(method: str, url: str, headers: dict, body: dict | None) -> tuple[int, dict]:
        def call() -> tuple[int, bytes]:
            data = json.dumps(body).encode() if body is not None else None
            req = urllib.request.Request(url, data=data, method=method,
                                         headers={"Content-Type": "application/json", "Accept": "application/json",
                                                  **headers})
            try:
                with urllib.request.urlopen(req, timeout=timeout) as r:
                    return r.status, r.read()
            except urllib.error.HTTPError as e:
                return e.code, e.read()
            except (urllib.error.URLError, TimeoutError, OSError) as e:
                raise TradovateError(f"{type(e).__name__}: {e}") from None
        status, raw = await asyncio.get_running_loop().run_in_executor(pool, call)
        try:
            d = json.loads(raw)
        except ValueError:
            d = {"errorText": raw.decode("utf-8", "replace")[:200]}
        return status, d if isinstance(d, dict) else {"d": d}
    return http


def _rows(d: dict) -> list[dict]:
    rows = d.get("d")
    if not isinstance(rows, list):
        raise BrokerError("unexpected response (expected a list)")
    return [r for r in rows if isinstance(r, dict)]


def _round(px: float) -> float:
    return round(px / TICK) * TICK


class NullBroker:
    """Always flat, no network. dry_run accounts use DryRun(NullBroker())."""
    name, dry = "null", False
    auth_failed = False

    def __init__(self):
        self.symbol = None

    def set_symbol(self, symbol): self.symbol = symbol
    def set_context(self, **kw): pass
    def blocked_reason(self): return None
    async def position(self, fresh=False): return Position()
    async def orders(self, fresh=False): return []
    async def working_orders(self): return []
    async def account(self): return PnlAccount()
    async def healthy(self): return True, None
    async def enter(self, *a, **k): return Ack(False, error="null broker", dry=True)
    async def flatten(self): return Ack(True)
    async def sweep(self): return True
    async def exit_info(self, trade): return None
    async def cash_realized(self, day=None): return None


class TradovateBroker:
    name, dry = "tradovate", False

    def __init__(self, acct: Account, token_fn, secrets_fn=lambda: (), http=None, ledger: Ledger | None = None,
                 emit=None, now_ns=time.time_ns, sleep=asyncio.sleep, clock=time.monotonic,
                 timeout: float = HTTP_TIMEOUT_S, ids=None):
        self.acct, self.token_fn, self.secrets_fn = acct, token_fn, secrets_fn
        self.http = http or make_http(timeout=timeout)
        self.ledger, self.emit = ledger, emit or (lambda kind, detail, notify=False: None)
        self.now_ns, self.sleep, self.clock, self.timeout = now_ns, sleep, clock, timeout
        self.base = REST[acct.env]
        self.symbol: str | None = None
        self.ctx = {"boot": "00000000", "seq": 0}
        self.auth_failed = False
        self.blocked_until = 0.0
        self.blocked_why = ""
        self._pos: tuple[float, list[dict]] | None = None
        self._ord: tuple[float, list[dict]] | None = None
        self._spec_name: str | None = None
        self._contracts: dict[str, int] = {}

    # -- plumbing

    def set_symbol(self, symbol: str) -> None:
        self.symbol = symbol

    def set_context(self, **kw) -> None:
        self.ctx.update(kw)

    def _red(self, text) -> str:
        return redact(text, [t for t in self.secrets_fn() if t])[:300]

    def _invalidate(self) -> None:
        self._pos = self._ord = None

    def blocked_reason(self) -> str | None:
        return self.blocked_why if self.clock() < self.blocked_until else None

    def _block(self, why: str) -> None:
        self.blocked_until, self.blocked_why = self.clock() + BLOCK_S, why
        self.emit("entries_blocked", f"{why}: no new entries for an hour", True)

    async def _call(self, method: str, path: str, body: dict | None = None, *, order: bool = False) -> dict:
        tok = self.token_fn()
        if not tok:
            raise BrokerError("no valid access token")
        fail = UnknownOutcome if order else BrokerError
        try:
            status, d = await asyncio.wait_for(
                self.http(method, self.base + path, {"Authorization": f"Bearer {tok}"}, body), self.timeout + 0.5)
        except (TradovateError, asyncio.TimeoutError) as e:
            raise fail(self._red(f"{path}: {type(e).__name__}: {e}")) from None
        if status == 429:
            raise RateLimited(f"{path}: HTTP 429")
        if status in (401, 403):
            self.auth_failed = True
            raise BrokerError(f"{path}: HTTP {status} (token refused)")
        self.auth_failed = False
        if status >= 500:
            raise fail(self._red(f"{path}: HTTP {status}"))
        if status >= 400 and "p-ticket" not in d:
            raise BrokerError(self._red(f"{path}: HTTP {status}: {d.get('errorText') or ''}"))
        return d

    @staticmethod
    def _failure(d: dict) -> tuple[str, str] | None:
        r = d.get("failureReason")
        if r and r != "Success":
            return str(r), str(d.get("failureText") or "")
        return None

    # -- reads (cached; any action invalidates)

    async def _positions(self, fresh: bool = False) -> list[dict]:
        if not fresh and self._pos:
            t, rows = self._pos
            if self.clock() - t < (READ_TTL_POSITION_S if rows else READ_TTL_FLAT_S):
                return rows
        d = await self._call("GET", "/position/list")
        rows = [r for r in _rows(d) if r.get("accountId") == self.acct.account_id and r.get("netPos")]
        self._pos = (self.clock(), rows)
        return rows

    async def position(self, fresh: bool = False) -> Position:
        rows = await self._positions(fresh)
        if not rows:
            return Position()
        net = rows[0]["netPos"]
        return Position(1 if net > 0 else -1, sum(abs(int(r["netPos"])) for r in rows), rows[0].get("netPrice"))

    async def _raw_orders(self, fresh: bool = True) -> list[dict]:
        if not fresh and self._ord and self.clock() - self._ord[0] < READ_TTL_ORDERS_S:
            return self._ord[1]
        d = await self._call("GET", "/order/list")
        rows = [r for r in _rows(d) if r.get("accountId") == self.acct.account_id]
        self._ord = (self.clock(), rows)
        return rows

    @staticmethod
    def _norm(o: dict) -> dict:
        return {"id": o.get("id"), "status": o.get("ordStatus"), "cl_ord_id": o.get("clOrdId"),
                "parent_id": o.get("parentId"), "action": o.get("action")}

    async def orders(self, fresh: bool = False) -> list[dict]:
        """The account's live (non-terminal) orders."""
        return [self._norm(o) for o in await self._raw_orders(fresh) if o.get("ordStatus") in LIVE]

    async def working_orders(self) -> list[dict]:
        return await self.orders()

    async def account(self) -> PnlAccount:
        return PnlAccount()

    async def healthy(self) -> tuple[bool, str | None]:
        return (not self.auth_failed, "token refused" if self.auth_failed else None)

    async def cash_realized(self, day: str | None = None) -> float | None:
        """cashBalance/deps realizedPnL: weak (forum only), used to reconcile the ledger's loss, never alone."""
        try:
            d = await self._call("GET", f"/cashBalance/deps?masterid={self.acct.account_id}")
            best = None
            for r in _rows(d):
                td = r.get("tradeDate")
                if isinstance(td, dict) and day:
                    if f"{td.get('year')}-{int(td.get('month', 0)):02d}-{int(td.get('day', 0)):02d}" != day:
                        continue
                if isinstance(r.get("realizedPnL"), (int, float)):
                    best = float(r["realizedPnL"])
            return best
        except (BrokerError, ValueError, TypeError):
            return None

    async def _spec(self) -> str:
        if self._spec_name is None:
            for a in _rows(await self._call("GET", "/account/list")):
                if a.get("id") == self.acct.account_id and a.get("name"):
                    self._spec_name = str(a["name"])
            if self._spec_name is None:
                raise BrokerError("account id not found in account/list")
        return self._spec_name

    async def _contract_ok(self, symbol: str) -> None:
        if symbol in self._contracts:
            return
        d = await self._call("GET", f"/contract/find?name={symbol}")
        if d.get("name") != symbol or not isinstance(d.get("id"), int):
            raise BrokerError(f"contract/find {symbol}: no exact match")
        self._contracts[symbol] = d["id"]

    async def _order_status(self, oid: int) -> str | None:
        d = await self._call("GET", f"/order/item?id={oid}")
        return d.get("ordStatus")

    async def _fills(self, order_id: int) -> list[dict]:
        try:
            return _rows(await self._call("GET", f"/fill/deps?masterid={order_id}"))
        except BrokerError:
            return []

    async def _fill_px(self, order_id: int, role: str, action: str) -> float | None:
        fills = await self._fills(order_id)
        qty = sum(int(f.get("qty") or 0) for f in fills)
        if not qty:
            return None
        day = et_day(self.now_ns())
        for f in fills:
            if self.ledger and f.get("id") is not None:
                self.ledger.fill(self.acct.name, int(f["id"]), day, order_id, role, self.symbol, action,
                                 int(f.get("qty") or 0), f.get("price"), self.now_ns())
        return sum(float(f["price"]) * int(f.get("qty") or 0) for f in fills if f.get("price") is not None) / qty

    # -- order rows (written before the POST)

    def _row(self, role: str, cl: str, **f) -> None:
        if self.ledger:
            c = self.ctx
            self.ledger.order_new(name=self.acct.name, day=et_day(self.now_ns()), boot=c["boot"], seq=c["seq"],
                                  role=role, cl_ord_id=cl, contract=self.symbol, qty=self.acct.qty,
                                  sent_ns=self.now_ns(), **f)

    def _upd(self, cl: str, **f) -> None:
        if self.ledger:
            if "failure_text" in f:
                f["failure_text"] = self._red(f["failure_text"])
            self.ledger.order_update(cl, **f)

    # -- entry

    async def enter(self, side: int, qty: int, limit_px: float, stop_ticks: int, target_ticks: int) -> Ack:
        t0 = self.clock()
        if (why := self.blocked_reason()):
            return Ack(False, missed=True, error=f"entries blocked: {why}")
        if stop_ticks <= 0 or target_ticks <= 0 or not self.symbol:
            return Ack(False, rejected=True, error="no TP/SL or no symbol: refusing an entry without a bracket")
        try:
            if await self._positions(fresh=True):
                return Ack(False, error="account is not flat before entry")
            if await self.orders(fresh=True) and not await self.sweep():
                return Ack(False, error="stale orders on the account could not be cancelled")
            spec = await self._spec()
            await self._contract_ok(self.symbol)
        except BrokerError as e:
            return Ack(False, missed=True, error=f"pre-entry check failed: {e}")
        c = self.ctx
        base = f"cj-{self.acct.name}-{str(c['boot'])[:8]}-{c['seq']}"
        cl, cl_tp, cl_sl = f"{base}-e", f"{base}-tp", f"{base}-sl"
        limit_px = _round(limit_px)
        tp_pts, sl_pts = target_ticks * TICK, stop_ticks * TICK
        tp_px, sl_px = _round(limit_px + side * tp_pts), _round(limit_px - side * sl_pts)
        action, opp = ("Buy", "Sell") if side > 0 else ("Sell", "Buy")
        tif = "IOC"
        self._row("entry", cl, action=action, order_type="Limit", limit_px=limit_px, tif=tif)
        self._row("tp", cl_tp, action=opp, order_type="Limit", limit_px=tp_px, tif="GTC", parent_broker_id=None)
        self._row("sl", cl_sl, action=opp, order_type="Stop", stop_px=sl_px, tif="GTC")
        body = {"accountSpec": spec, "accountId": self.acct.account_id, "action": action, "symbol": self.symbol,
                "orderQty": qty, "orderType": "Limit", "price": limit_px, "timeInForce": tif, "isAutomated": True,
                "clOrdId": cl,
                "bracket1": {"action": opp, "orderType": "Limit", "price": tp_px, "timeInForce": "GTC",
                             "clOrdId": cl_tp},
                "bracket2": {"action": opp, "orderType": "Stop", "stopPrice": sl_px, "timeInForce": "GTC",
                             "clOrdId": cl_sl}}
        self._invalidate()
        try:
            d = await self._call("POST", "/order/placeoso", body, order=True)
        except UnknownOutcome as e:
            self._upd(cl, state="unknown", failure_text=str(e))
            return await self._recover(cl, side, qty, tp_pts, sl_pts, t0)
        except RateLimited as e:
            self._block("HTTP 429")
            for x in (cl, cl_tp, cl_sl):
                self._upd(x, state="rejected", failure_reason="429")
            return Ack(False, missed=True, error=str(e))
        except BrokerError as e:   # a 4xx: refused, nothing placed
            for x in (cl, cl_tp, cl_sl):
                self._upd(x, state="rejected", failure_reason="http", failure_text=str(e))
            return Ack(False, rejected=True, error=str(e))
        ms = (self.clock() - t0) * 1000
        if "p-ticket" in d or "p-time" in d:
            if d.get("p-captcha"):
                self._block("Tradovate wants a captcha")
            for x in (cl, cl_tp, cl_sl):
                self._upd(x, state="missed", failure_reason="p-ticket", ack_ms=ms)
            return Ack(False, missed=True, error="penalty ticket: entry not resent")
        if (f := self._failure(d)):
            for x in (cl, cl_tp, cl_sl):
                self._upd(x, state="rejected", failure_reason=f[0], failure_text=f[1], ack_ms=ms)
            return Ack(False, rejected=True, error=self._red(f"{f[0]}: {f[1]}"))
        if not isinstance(d.get("orderId"), int):
            self._upd(cl, state="unknown", failure_text="no orderId in the response")
            return await self._recover(cl, side, qty, tp_pts, sl_pts, t0)
        oid, o1, o2 = d["orderId"], d.get("oso1Id"), d.get("oso2Id")
        self._upd(cl, broker_order_id=oid, state="acked", ack_ms=ms)
        self._upd(cl_tp, broker_order_id=o1, parent_broker_id=oid, state="acked")
        self._upd(cl_sl, broker_order_id=o2, parent_broker_id=oid, state="acked")
        return await self._settle(oid, o1, o2, (cl, cl_tp, cl_sl), side, qty, limit_px, tp_pts, sl_pts, t0)

    async def _recover(self, cl, side, qty, tp_pts, sl_pts, t0) -> Ack:
        """An entry POST whose outcome is unknown. Never re-POST: wait, then look for the clOrdId."""
        await self.sleep(1.5)
        why = "timeout"
        for i in range(2):
            try:
                self._invalidate()
                rows = await self._raw_orders(True)
            except BrokerError as e:
                why = str(e)
                break
            hit = next((o for o in rows if o.get("clOrdId") == cl), None)
            if hit is not None:
                oid = hit["id"]
                kids = [o["id"] for o in rows if o.get("parentId") == oid]
                self.emit("entry_recovered", f"order found after a {why}: resuming", False)
                limit = next((float(o["price"]) for o in rows if o["id"] == oid and o.get("price")), None)
                self._upd(cl, broker_order_id=oid, state="acked")
                lp = limit if limit is not None else 0.0
                return await self._settle(oid, kids[0] if kids else None, kids[1] if len(kids) > 1 else None,
                                          (cl, cl[:-2] + "-tp", cl[:-2] + "-sl"), side, qty, lp, tp_pts, sl_pts, t0,
                                          recovered=True)
            if i == 0:
                await self.sleep(0.5)
        else:
            try:
                flat = not await self._positions(True) and not await self.orders(True)
            except BrokerError as e:
                why, flat = str(e), False
            if flat:
                for x in (cl, cl[:-2] + "-tp", cl[:-2] + "-sl"):
                    self._upd(x, state="missed", failure_reason="timeout")
                return Ack(False, missed=True, error="entry timed out and no order was found")
        # unexplained: a position or order we cannot account for, or reads that fail
        self.emit("entry_unexplained", "entry outcome unknown: flattening", True)
        fa = await self.flatten()
        return Ack(False, error="entry outcome unknown (timeout); account flattened"
                   + ("" if fa.ok else f" but: {fa.error}"), info={"closed": True, "exit_reason": "flatten"})

    async def _cancel_and_resolve(self, oid) -> str | None:
        """Cancel a resting parent and wait until it reads terminal (an IOC that does not cross can read "Working" for
        a few seconds after the cancel). Cancels a second time if it is still Working after the window. Returns the
        last status read; a failed read raises BrokerError (the caller decides network vs other)."""
        status = None
        for _ in range(2):
            try:
                await self._call("POST", "/order/cancelorder", {"orderId": oid, "isAutomated": True}, order=True)
            except BrokerError as e:
                if is_conn_error(e):
                    raise
                # a refusal (e.g. already terminal): the status read below says which
            end = self.clock() + CANCEL_RESOLVE_S
            while True:
                await self.sleep(SETTLE_STEP_S)
                status = await self._order_status(oid)
                if status in TERMINAL or self.clock() >= end:
                    break
            if status in TERMINAL:
                break
        return status

    async def _settle(self, oid, o1, o2, cls, side, qty, limit_px, tp_pts, sl_pts, t0, recovered=False) -> Ack:
        cl, cl_tp, cl_sl = cls
        action = "Buy" if side > 0 else "Sell"
        end, status = self.clock() + SETTLE_S, None
        while True:
            try:
                status = await self._order_status(oid)
            except BrokerError:
                status = None
            if status in TERMINAL or self.clock() >= end:
                break
            await self.sleep(SETTLE_STEP_S)
        try:
            if status not in TERMINAL:   # still resting (the Day fallback, or an IOC that lingers): cancel, then re-poll
                status = await self._cancel_and_resolve(oid)
            pos = await self.position(fresh=True)
            for _ in range(3):   # a fill can be visible on the order before the position
                if status == "Filled" and pos.qty != qty:
                    await self.sleep(SETTLE_STEP_S)
                    pos = await self.position(fresh=True)
        except BrokerError as e:
            ms = (self.clock() - t0) * 1000
            if is_conn_error(e):
                # the broker reads are failing: the entry's fate is unknown. Leave the bracket (it protects a fill),
                # let the executor halt for `connection` (which probes, flattens if needed, and clears itself).
                self.emit("entry_unresolved", "cannot confirm the entry outcome: broker reads failing", False)
                return Ack(False, error=self._red(f"entry outcome unconfirmed, broker reads failing ({e})"),
                           ack_ms=ms, info={"connection": True})
            self.emit("entry_unexplained", "cannot read back an entry: flattening", True)
            fa = await self.flatten()
            return Ack(False, error=self._red(f"entry read-back failed ({e}); account flattened"
                                              + ("" if fa.ok else f" but: {fa.error}")),
                       info={"closed": True, "exit_reason": "flatten"})
        ms = (self.clock() - t0) * 1000
        if pos.qty == 0:
            if status not in NOT_FILLED:
                # the parent is Filled (or its state is unknown) but the position reads flat: ambiguous. Never
                # sweep (that would cancel the bracket of a position that may be about to show): halt and alert.
                self.emit("entry_ambiguous", f"entry order {status or 'unreadable'} but the position reads flat: "
                          "bracket kept, halting", True)
                return Ack(False, error=f"entry order {status or 'unreadable'} but the position reads flat; "
                           "bracket left in place", ack_ms=ms, info={"ambiguous": True})
            swept = await self.sweep()
            for x in (cl, cl_tp, cl_sl):
                self._upd(x, state="missed" if x == cl else "canceled")
            if not swept:   # live orphan children could open a position against the intent: not a plain miss
                self.emit("orphan_orders", "no fill, but the bracket children could not be cancelled", True)
                return Ack(False, error="no fill, but orphan orders could not be cancelled", ack_ms=ms,
                           info={"orphans": True})
            return Ack(False, missed=True, error="no fill at the limit; cancelled; still copying", ack_ms=ms,
                       info={"unfilled": True})
        entry_px = await self._fill_px(oid, "entry", action) or pos.avg_px or limit_px
        if pos.side != side or pos.qty != qty:   # partial (or wrong side): liquidate, cancel, record as a miss
            fa = await self.flatten()
            self._upd(cl, state="missed", failure_reason="partial")
            return Ack(fa.ok, missed=True, fill_px=fa.fill_px, ack_ms=ms,
                       error=f"partial fill ({pos.qty} of {qty}): closed" + ("" if fa.ok else f"; {fa.error}"),
                       info={"partial": True, "qty": pos.qty, "side": pos.side, "entry_px": entry_px,
                             "exit_reason": "partial"})
        self._upd(cl, state="filled")
        tp_px, sl_px = _round(entry_px + side * tp_pts), _round(entry_px - side * sl_pts)
        ids = [o1, o2]
        if o1 is None or o2 is None:
            try:
                kids = [o["id"] for o in await self._raw_orders(True) if o.get("parentId") == oid]
                ids = (kids + [None, None])[:2]
            except BrokerError:
                pass
        modified = True
        for kid, body in ((ids[0], {"orderType": "Limit", "price": tp_px}),
                          (ids[1], {"orderType": "Stop", "stopPrice": sl_px})):
            if kid is None:
                modified = False
                continue
            try:
                d = await self._call("POST", "/order/modifyorder",
                                     {"orderId": kid, "orderQty": qty, "timeInForce": "GTC", "isAutomated": True,
                                      **body}, order=True)
                if self._failure(d):
                    raise BrokerError(f"{d.get('failureReason')}: {d.get('failureText') or ''}")
            except BrokerError as e:   # keep the provisional bracket (its error is at most the slip cap)
                modified = False
                self.emit("modify_failed", self._red(f"bracket modify failed, provisional bracket kept: {e}"), False)
        self._invalidate()
        live, read_ok = 0, False
        end = self.clock() + VERIFY_S
        while True:
            try:
                rows = await self._raw_orders(True)
                live = len({o["id"] for o in rows if o.get("ordStatus") in LIVE
                            and (o.get("parentId") == oid or o["id"] in (o1, o2))})
                read_ok = True
            except BrokerError:    # a failed read is retried inside the window; it is not "a child is missing"
                pass
            if (read_ok and live >= 2) or self.clock() >= end:
                break
            await self.sleep(SETTLE_STEP_S)
        if not read_ok:
            self.emit("bracket_unverified", "could not read the orders to verify the bracket; the position stays "
                      "and the invariant check will look again", True)
        elif live < 2:
            self.emit("bracket_missing", "bracket missing after a fill: position closed", True)
            fa = await self.flatten()
            return Ack(False, error="bracket missing after the fill; position closed"
                       + ("" if fa.ok else f" but: {fa.error}"), fill_px=fa.fill_px,
                       info={"closed": True, "exit_reason": "bracket", "entry_px": entry_px, "qty": qty,
                             "side": side})
        self._upd(cl_tp, state="working", limit_px=tp_px)
        self._upd(cl_sl, state="working", stop_px=sl_px)
        return Ack(True, entry_px, ms, info={"qty": qty, "tp_px": tp_px, "sl_px": sl_px, "order_id": oid,
                                            "tp_id": ids[0], "sl_id": ids[1], "modified": modified,
                                            "recovered": recovered})

    async def probe_unfilled_ioc(self, limit_px: float, tp_pts: float = 10.0, sl_pts: float = 10.0) -> dict:
        """REHEARSE ONLY (demo, owner at a terminal): place a 1-lot OSO whose IOC buy limit is far below the market, so
        the parent expires unfilled, and report what happens to the bracket children before sweeping. Answers: does
        Tradovate cancel OSO children when an IOC parent expires unfilled? (The daemon never relies on it: it sweeps.)"""
        spec = await self._spec()
        await self._contract_ok(self.symbol)
        limit_px = _round(limit_px)
        d = await self._call("POST", "/order/placeoso", {
            "accountSpec": spec, "accountId": self.acct.account_id, "action": "Buy", "symbol": self.symbol,
            "orderQty": 1, "orderType": "Limit", "price": limit_px, "timeInForce": "IOC", "isAutomated": True,
            "bracket1": {"action": "Sell", "orderType": "Limit", "price": _round(limit_px + tp_pts),
                         "timeInForce": "GTC"},
            "bracket2": {"action": "Sell", "orderType": "Stop", "stopPrice": _round(limit_px - sl_pts),
                         "timeInForce": "GTC"}}, order=True)
        if (f := self._failure(d)) or not isinstance(d.get("orderId"), int):
            return {"error": f"{f[0]}: {f[1]}" if f else "no orderId"}
        await self.sleep(1.0)
        rows = {o["id"]: o for o in await self._raw_orders(True)}
        kids = [rows.get(d.get("oso1Id")), rows.get(d.get("oso2Id"))]
        out = {"parent": (rows.get(d["orderId"]) or {}).get("ordStatus"),
               "children": [(k or {}).get("ordStatus") for k in kids]}
        out["children_live"] = len([k for k in kids if k and k.get("ordStatus") in LIVE])
        out["swept"] = await self.sweep()
        return out

    # -- flatten / sweep

    async def flatten(self) -> Ack:
        """Liquidate every open position (the position's own contract) and poll flat; ONLY once a read-back shows
        flat, cancel every live order (never cancel protective orders while a position may exist). A retry
        follows only a read-back that still shows a position. `fill_px` is the liquidation's real fill when
        fill/deps has it."""
        if not self.acct.account_id:
            return Ack(True)
        self._invalidate()
        orig_side = 0
        flipped = False
        last_err = None
        liq_ids: list[int] = []
        try:
            rows = await self._positions(True)
            orig_side = (1 if rows[0]["netPos"] > 0 else -1) if rows else 0
            for attempt in range(3):
                if not rows:
                    break
                accepted = False
                for r in rows:
                    ok, oid, err = await self._liquidate(r)
                    if oid is not None:
                        liq_ids.append(oid)
                    if not ok:
                        last_err = err
                    else:
                        accepted = True
                end = self.clock() + (FLAT_WAIT_S if accepted else 0.0)   # a refusal needs no 3 s wait
                while True:
                    rows = await self._positions(True)
                    if not rows or self.clock() >= end:
                        break
                    await self.sleep(0.3)
                if rows and (1 if rows[0]["netPos"] > 0 else -1) != orig_side:
                    flipped = True      # a bracket fill raced the liquidation and left the opposite position
                    for r in rows:
                        _, oid, _ = await self._liquidate(r)
                        if oid is not None:
                            liq_ids.append(oid)
                    break
        except RateLimited as e:
            last_err = str(e)
        except BrokerError as e:
            last_err = self._red(e)
        try:
            rows = await self._positions(True)       # the read-back
        except BrokerError as e:
            return Ack(False, error=self._red(f"flatten read-back failed: {e}"))
        if rows:                                      # still in a position: leave the bracket alone
            return Ack(False, error=("opposite position after flatten; STILL IN A POSITION" if flipped else
                                     "still in a position after flatten") + (f": {last_err}" if last_err else ""))
        swept = await self.sweep()                    # flat is confirmed: now the orders
        px = None
        for oid in liq_ids:
            px = await self._fill_px(oid, "flatten", "Sell" if orig_side > 0 else "Buy")
            if px is not None:
                break
        if flipped:
            return Ack(False, fill_px=px, error="opposite position after flatten (flattened again)")
        if not swept:
            return Ack(False, fill_px=px, error="flat, but live orders could not be cancelled")
        return Ack(True, fill_px=px)

    async def _liquidate(self, row: dict) -> tuple[bool, int | None, str | None]:
        """(accepted, liquidation order id, error). A failureReason is a refusal; a timeout is left to the read-back."""
        body = {"accountId": self.acct.account_id, "contractId": row["contractId"], "admin": False,
                "isAutomated": True}
        for _ in range(2):
            try:
                d = await self._call("POST", "/order/liquidateposition", body, order=True)
            except UnknownOutcome:
                return True, None, None    # the read-back decides
            if "p-ticket" in d or "p-time" in d:
                if d.get("p-captcha"):
                    self._block("Tradovate wants a captcha")
                    return False, None, "captcha required"
                await self.sleep(min(float(d.get("p-time") or 1), MAX_PENALTY_WAIT_S))
                body = {**body, "p-ticket": d.get("p-ticket")}
                continue
            if (f := self._failure(d)):
                return False, None, self._red(f"liquidate refused: {f[0]} {f[1]}")
            return True, d.get("orderId") if isinstance(d.get("orderId"), int) else None, None
        return False, None, "penalty ticket not cleared"

    async def sweep(self) -> bool:
        """Cancel every non-terminal order on the account. TooLate (it filled or died meanwhile) is fine."""
        self._invalidate()
        try:
            live = [o for o in await self._raw_orders(True) if o.get("ordStatus") in LIVE]
            for o in live:
                try:
                    d = await self._call("POST", "/order/cancelorder", {"orderId": o["id"], "isAutomated": True},
                                         order=True)
                except UnknownOutcome:
                    continue
                f = self._failure(d)
                if f and f[0] != "TooLate":
                    self.emit("cancel_refused", self._red(f"cancel {o['id']}: {f[0]}"), False)
            for _ in range(3):
                self._invalidate()
                if not [o for o in await self._raw_orders(True) if o.get("ordStatus") in LIVE]:
                    return True
                await self.sleep(0.3)
            return False
        except BrokerError:
            return False

    async def exit_info(self, trade: dict) -> tuple[float, str] | None:
        """After the position vanished on its own: which child filled, and at what price."""
        for key, why, role in (("tp_id", "target", "tp"), ("sl_id", "stop", "sl")):
            oid = trade.get(key)
            if oid is None:
                continue
            try:
                if await self._order_status(oid) == "Filled":
                    px = await self._fill_px(oid, role, "Sell" if trade.get("side", 1) > 0 else "Buy")
                    if px is not None:
                        return px, why
                    return float(trade["tp_px" if role == "tp" else "sl_px"]), why
            except BrokerError:
                continue
        return None
