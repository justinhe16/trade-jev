"""Fake Tradovate REST (the `http` transport of tradovate.default_http) and a fake clock for copy trading tests.
Nothing here opens a socket."""

from __future__ import annotations

import itertools

from trade_jev.tradovate import TradovateError

ACCT_ID = 11


class Clock:
    """A fake monotonic clock whose sleep() advances it, so settle / retry loops run instantly."""

    def __init__(self):
        self.t = 0.0

    def __call__(self):
        return self.t

    async def sleep(self, s):
        self.t += s


class FakeTradovate:
    def __init__(self, account_id: int = ACCT_ID, spec: str = "DEMO11", token_echo: str | None = None):
        self.account_id, self.spec, self.token_echo = account_id, spec, token_echo
        self.ids = itertools.count(1000)
        self.fill_ids = itertools.count(5000)
        self.calls: list[tuple[str, str, dict | None]] = []
        self.hosts: list[str] = []
        self.orders: dict[int, dict] = {}
        self.fills: dict[int, list[dict]] = {}
        self.pos: dict[int, dict] = {}        # contractId -> {"netPos", "netPrice"}
        self.contract_id = 99
        self.last_px = 24500.0
        # behaviours (set by tests)
        self.entry = "full"        # full | none | partial | reject | pticket | captcha | 429 | timeout_placed |
                                   # timeout_lost | timeout_unexplained | http400 | down
        self.partial_qty = 1
        self.fill_px: float | None = None
        self.modify_fail = False
        self.modify_kills_child = False
        self.child_dead = False     # children are not live after the fill
        self.liquidate_cancels = True
        self.liquidate_flip = False
        self.liquidate_fail_times = 0
        self.liquidate_pticket = False
        self.cancel_too_late = False
        self.cancel_fail = False
        self.read_fail: set[str] = set()
        self.status_never_terminal = False
        self.after_place = None
        # an IOC that does not cross lingers "Working": entry "none" parents start Working (not Canceled), and read
        # Canceled only after `linger_cancels` accepted cancel POSTs (0 = the first cancel resolves it)
        self.ioc_lingers = False
        self.linger_cancels = 0     # accepted cancels that change nothing before one takes effect, plus one
        self.stale_polls = 0        # status reads that still say Working after the cancel took effect
        self.status_fail_after_cancel = False
        self._cancels = 0

    # -- helpers

    def _live(self, o):
        return o["ordStatus"] in ("Working", "Suspended", "PendingNew", "PendingReplace")

    def n(self, path: str, method: str | None = None) -> int:
        return len([c for c in self.calls if c[1].startswith(path) and (method is None or c[0] == method)])

    def posts(self, path: str) -> list[dict]:
        return [c[2] for c in self.calls if c[0] == "POST" and c[1] == path]

    def _err(self, text="boom"):
        return 400, {"errorText": text + (f" {self.token_echo}" if self.token_echo else "")}

    def _make_position(self, side: int, qty: int, px: float):
        self.pos[self.contract_id] = {"netPos": side * qty, "netPrice": px}

    # -- transport

    async def __call__(self, method, url, headers, body):
        base, _, path = url.partition("/v1")
        self.hosts.append(base)
        self.calls.append((method, path, body))
        key = path.split("?")[0]
        if key in self.read_fail:
            raise TradovateError("injected read failure")
        q = dict(p.split("=") for p in path.split("?", 1)[1].split("&")) if "?" in path else {}
        if key == "/position/list":
            rows = [{"id": 1, "accountId": self.account_id, "contractId": c, **v} for c, v in self.pos.items()]
            rows.append({"id": 2, "accountId": 777, "contractId": 5, "netPos": 3, "netPrice": 1.0})   # not ours
            return 200, {"d": rows}
        if key == "/order/list":
            rows = [dict(o) for o in self.orders.values()]
            rows.append({"id": 9999, "accountId": 777, "ordStatus": "Working", "clOrdId": "other"})   # not ours
            return 200, {"d": rows}
        if key == "/order/item":
            if self.status_fail_after_cancel and self._cancels:
                raise TradovateError("TimeoutError: timed out")
            o = self.orders.get(int(q["id"]))
            if o is None:
                return 404, {"errorText": "no such order"}
            if o.get("stale", 0) > 0:
                o["stale"] -= 1
                return 200, {**o, "ordStatus": "Working"}
            if self.status_never_terminal and o.get("role") == "entry":
                return 200, {**o, "ordStatus": "Working"}
            return 200, dict(o)
        if key == "/fill/deps":
            return 200, {"d": self.fills.get(int(q["masterid"]), [])}
        if key == "/account/list":
            return 200, {"d": [{"id": self.account_id, "name": self.spec}]}
        if key == "/contract/find":
            return 200, {"id": self.contract_id, "name": q["name"]}
        if key == "/cashBalance/deps":
            return 200, {"d": [{"tradeDate": {"year": 2026, "month": 11, "day": 5}, "realizedPnL": -12.5}]}
        if key == "/order/placeoso":
            return self._place(body)
        if key == "/order/modifyorder":
            return self._modify(body)
        if key == "/order/cancelorder":
            return self._cancel(body)
        if key == "/order/liquidateposition":
            return self._liquidate(body)
        return 404, {"errorText": f"unknown {path}"}

    # -- order endpoints

    def _place(self, b):
        e = self.entry
        if e == "down":
            return 503, {}
        if e == "429":
            return 429, {"errorText": "too many requests"}
        if e == "http400":
            return self._err("bad request")
        if e == "pticket":
            return 200, {"p-ticket": "tkt", "p-time": 5}
        if e == "captcha":
            return 200, {"p-ticket": "tkt", "p-time": 5, "p-captcha": True}
        if e == "reject":
            return 200, {"failureReason": "RiskCheckFailed", "failureText": "no buying power"
                         + (f" {self.token_echo}" if self.token_echo else "")}
        if e == "timeout_lost":
            raise TradovateError("TimeoutError: timed out")
        if e == "timeout_unexplained":
            self._make_position(1 if b["action"] == "Buy" else -1, b["orderQty"], b["price"])
            raise TradovateError("TimeoutError: timed out")
        oid, o1, o2 = next(self.ids), next(self.ids), next(self.ids)
        side = 1 if b["action"] == "Buy" else -1
        px = self.fill_px if self.fill_px is not None else b["price"]
        parent = {"id": oid, "accountId": b["accountId"], "ordStatus": "Canceled", "clOrdId": b["clOrdId"],
                  "action": b["action"], "price": b["price"], "role": "entry"}
        kids = []
        for kid, br in ((o1, b["bracket1"]), (o2, b["bracket2"])):
            kids.append({"id": kid, "accountId": b["accountId"], "ordStatus": "Working", "parentId": oid,
                         "clOrdId": br.get("clOrdId"), "action": br["action"], "price": br.get("price"),
                         "stopPrice": br.get("stopPrice"), "orderType": br["orderType"], "role": "child"})
        filled = 0
        if e in ("full", "timeout_placed"):
            filled = b["orderQty"]
        elif e == "partial":
            filled = self.partial_qty
        if filled:
            parent["ordStatus"] = "Filled" if filled == b["orderQty"] else "Canceled"
            self._make_position(side, filled, px)
            self.fills[oid] = [{"id": next(self.fill_ids), "orderId": oid, "price": px, "qty": filled}]
            if self.child_dead:
                for k in kids:
                    k["ordStatus"] = "Canceled"
        else:
            for k in kids:
                k["ordStatus"] = "Canceled"
            if self.ioc_lingers:
                parent["ordStatus"] = "Working"
        self.orders[oid] = parent
        for k in kids:
            self.orders[k["id"]] = k
        if self.after_place:
            self.after_place(self)
        if e == "timeout_placed":
            raise TradovateError("TimeoutError: timed out")
        return 200, {"failureReason": "Success", "orderId": oid, "oso1Id": o1, "oso2Id": o2}

    def _modify(self, b):
        o = self.orders.get(b["orderId"])
        if self.modify_fail or o is None:
            return 200, {"failureReason": "UnknownReason", "failureText": "modify refused"}
        if "price" in b:
            o["price"] = b["price"]
        if "stopPrice" in b:
            o["stopPrice"] = b["stopPrice"]
        if self.modify_kills_child:
            o["ordStatus"] = "Canceled"
        return 200, {"failureReason": "Success", "orderId": b["orderId"]}

    def _cancel(self, b):
        o = self.orders.get(b["orderId"])
        if o is None or not self._live(o) or self.cancel_too_late:
            return 200, {"failureReason": "TooLate", "failureText": "order is no longer working"}
        if self.cancel_fail:
            return 200, {"failureReason": "UnknownReason", "failureText": "cannot cancel"}
        self._cancels += 1
        if o.get("role") == "entry" and self.ioc_lingers and self._cancels <= self.linger_cancels:
            return 200, {"failureReason": "Success", "orderId": b["orderId"]}    # accepted, still reads Working
        o["ordStatus"] = "Canceled"
        if o.get("role") == "entry":
            o["stale"] = self.stale_polls
        return 200, {"failureReason": "Success", "orderId": b["orderId"]}

    def _liquidate(self, b):
        if self.liquidate_pticket and "p-ticket" not in b:
            return 200, {"p-ticket": "liq", "p-time": 1}
        if self.liquidate_fail_times > 0:
            self.liquidate_fail_times -= 1
            return 200, {"failureReason": "UnknownReason", "failureText": "try again"}
        p = self.pos.get(b["contractId"])
        if p and p["netPos"]:
            oid = next(self.ids)
            side = 1 if p["netPos"] > 0 else -1
            qty = abs(p["netPos"])
            self.fills[oid] = [{"id": next(self.fill_ids), "orderId": oid, "price": self.last_px, "qty": qty}]
            self.orders[oid] = {"id": oid, "accountId": self.account_id, "ordStatus": "Filled", "clOrdId": None,
                                "action": "Sell" if side > 0 else "Buy", "role": "liq"}
            if self.liquidate_flip:
                self.liquidate_flip = False
                self.pos[b["contractId"]] = {"netPos": -side * qty, "netPrice": self.last_px}
            else:
                self.pos[b["contractId"]] = {"netPos": 0, "netPrice": 0}
            if self.liquidate_cancels:
                for o in self.orders.values():
                    if o.get("role") == "child" and self._live(o):
                        o["ordStatus"] = "Canceled"
            return 200, {"orderId": oid}
        return 200, {"failureReason": "UnknownReason", "failureText": "no position"}

    # -- test hooks

    def bracket_hit(self, which: str, px: float):
        """A child fills at the broker: position goes flat, the other child is canceled (OCO)."""
        kids = [o for o in self.orders.values() if o.get("role") == "child"]
        tp = next(k for k in kids if k["orderType"] == "Limit" and self._live(k))
        sl = next(k for k in kids if k["orderType"] == "Stop" and self._live(k))
        hit, other = (tp, sl) if which == "tp" else (sl, tp)
        hit["ordStatus"] = "Filled"
        other["ordStatus"] = "Canceled"
        self.fills[hit["id"]] = [{"id": next(self.fill_ids), "orderId": hit["id"], "price": px, "qty": 1}]
        for c in self.pos:
            self.pos[c] = {"netPos": 0, "netPrice": 0}
