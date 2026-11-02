"""Tradovate source: frame codec, DOM / tick parsing, aggressor rule, auth (fake HTTP), a local fake md
websocket server driving TradovateSource, reconnects, token / host fallbacks, and full paper sessions.
No network (the default HTTP layer is replaced by one that fails), no real credentials."""

import asyncio
import importlib.util
import json
import sqlite3
from datetime import datetime, timezone
from pathlib import Path

import pytest
import websockets

from trade_jev import live, paperdb, tradovate as tv
from trade_jev.data import LEVELS, et_to_ns
from tests.test_live import Script
from tests.test_paper import db, rows, sandbox  # noqa: F401 — fixtures

DAYX = "2027-06-02"  # a future Wednesday: the session deadline (wall clock) is not already past
SECRETS = ["pw-SECRET", "sec-SECRET", "ACC-1", "MD-1", "ACC-2"]


@pytest.fixture(autouse=True)
def no_network(monkeypatch):
    async def boom(*a, **k):
        raise AssertionError("a test reached the real HTTP layer")
    monkeypatch.setattr(tv, "default_http", boom)


def cfg(env="demo", **kw):
    return tv.TradovateConfig("user", "pw-SECRET", "1234", "sec-SECRET", "dev-1", env, **kw)


def ns(hms: str) -> int:
    return et_to_ns(DAYX, hms)


def iso(ns_: int) -> str:
    return datetime.fromtimestamp(ns_ / 1e9, timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")


# ---------------------------------------------------------------- codec

def test_frame_codec():
    assert tv.decode_frame("o") == ("open", None)
    assert tv.decode_frame("h") == ("heartbeat", None)
    assert tv.decode_frame('a[{"s":200,"i":1},{"e":"props"}]') == ("messages", [{"s": 200, "i": 1}, {"e": "props"}])
    assert tv.decode_frame('c[1000,"bye"]') == ("close", [1000, "bye"])
    assert tv.decode_frame("a{not json")[0] == "unknown"
    assert tv.decode_frame("")[0] == "unknown" and tv.decode_frame("zzz")[0] == "unknown"
    assert tv.encode_request("md/subscribeDOM", 3, {"symbol": "NQZ6"}) == 'md/subscribeDOM\n3\n\n{"symbol": "NQZ6"}'
    assert tv.encode_request("x", 1) == "x\n1\n\n"
    assert tv.encode_request("x", 2, {"a": 1}, query="q=1") == 'x\n2\nq=1\n{"a": 1}'


# ---------------------------------------------------------------- DOM

def test_book_row_sorts_pads_and_caps():
    bids = [(100, 5), (103, 7), (101, 2), (102, 0)]   # a zero-size level is empty
    asks = [(106, 1), (104, 3), (105, 4)]
    bp, bs, ap, as_ = tv.book_row(bids, asks)
    assert bp[:4] == [103, 101, 100, 99] and bs[:4] == [7, 2, 5, 0]      # sorted desc, padded one tick out
    assert ap[:4] == [104, 105, 106, 107] and as_[:4] == [3, 4, 1, 0]
    assert len(bp) == len(ap) == LEVELS
    many = tv.book_row([(1000 - i, 1) for i in range(30)], [(1001 + i, 2) for i in range(30)])
    assert many[0] == list(range(1000, 990, -1)) and many[2] == list(range(1001, 1011))
    assert tv.book_row([], asks) is None and tv.book_row(bids, []) is None
    assert tv.book_row([(100, 1)], [(100, 1)]) is None and tv.book_row([(101, 1)], [(100, 1)]) is None  # locked / crossed


def test_parse_dom_documented_form_with_float_sizes():
    d = {"doms": [{"contractId": 7, "timestamp": "2027-06-02T13:30:00.250Z",
                   "bids": [{"price": 25000.0, "size": 4.4}, {"price": 24999.75, "size": 9.6}],
                   "offers": [{"price": 25000.25, "size": 3.0}]}]}
    [(cid, ts, book)] = tv.parse_dom(d)
    assert cid == 7 and ts == int(datetime(2027, 6, 2, 13, 30, 0, tzinfo=timezone.utc).timestamp()) * 10**9 + 250_000_000
    bp, bs, ap, as_ = book
    assert bp[:3] == [100000, 99999, 99998] and bs[:3] == [4, 10, 0]
    assert ap[:2] == [100001, 100002] and as_[:2] == [3, 0]


def test_parse_dom_compact_form_and_missing_side():
    d = {"d": [{"id": 7, "t": 1_780_000_000_123, "b": [[25000.0, 5], [24999.5, 2]], "a": [[25000.25, 1]], "r": True},
               {"id": 7, "t": 1_780_000_000_124, "b": [], "a": [[25000.25, 1]]}]}
    (c1, t1, b1), (c2, t2, b2) = tv.parse_dom(d)
    assert c1 == 7 and t1 == 1_780_000_000_123_000_000
    assert b1[0][:3] == [100000, 99998, 99997] and b1[1][:2] == [5, 2] and b1[2][0] == 100001
    assert b2 is None  # one side empty: no usable row
    assert tv.parse_dom({"q": []}) is None and tv.parse_dom("x") is None and tv.parse_dom({"d": "nope"}) is None
    assert tv.parse_dom({"d": [5, {"id": 1, "t": "bad", "b": [["x", 1]], "a": [[1, 1]]}]})[0][1] is None


# ---------------------------------------------------------------- ticks

def packet(i, tks, bp=80000, bt=1_800_000_000_000, ts=0.25, eoh=None):  # bt: epoch ms or ISO
    out = [{"id": i, "s": 1, "td": "2027-06-02", "bp": bp, "bt": bt, "ts": ts, "tks": tks}]
    if eoh is not None:
        out.append({"id": eoh, "eoh": True})
    return {"charts": out}


def test_tick_decoding_relative_fields_eoh_gate_sort_and_dedupe():
    dec = tv.TickDecoder()
    dec.ids = {1, 2}
    hist = packet(1, [{"t": 1, "p": 5, "s": 9, "id": 1}], eoh=1)       # history, then the marker
    assert dec.feed({"charts": [hist["charts"][0]]}) == []              # before eoh: ignored
    assert dec.feed({"charts": [hist["charts"][1]]}) == [] and dec.eoh
    live_ = packet(2, [{"t": 30, "p": 2, "s": 3.0, "id": 12, "b": 1, "bs": 5, "a": 2, "as": 4},
                       {"t": 10, "p": 1, "s": 2, "id": 11},
                       {"t": 10, "p": 1, "s": 2, "id": 11},            # duplicate id
                       {"t": 20, "p": 0, "s": 1, "id": 9}])
    got = dec.feed(live_)
    assert [t.id for t in got] == [11, 9, 12]                            # (time, id) order; dup dropped
    assert got[0].price == 80001 and got[0].ts_ns == (1_800_000_000_000 + 10) * 1_000_000 and got[0].size == 2
    assert (got[2].price, got[2].size, got[2].bid, got[2].ask) == (80002, 3, 80001, 80002)
    assert got[0].bid is None and got[0].ask is None                     # absent unless bs / as present
    assert dec.feed(live_) == []                                         # replayed packet: all duplicates
    assert dec.feed(packet(99, [{"t": 1, "p": 1, "s": 1, "id": 50}])) == []  # another chart's id


def test_tick_size_scaling_converts_to_quarter_ticks():
    dec = tv.TickDecoder()
    dec.eoh = True
    [t] = dec.feed(packet(1, [{"t": 1, "p": 4, "s": 1, "id": 1}], bp=10000, ts=0.5))   # (10000+4)*0.5 = 5002.0
    assert t.price == 20008


def test_tick_decoder_iso_base_time_and_eoh_packet_ticks_are_history():
    dec = tv.TickDecoder()
    marker = {"id": 1, "eoh": True, "tks": [{"t": 1, "p": 1, "s": 5, "id": 1}]}
    assert dec.feed({"charts": [marker]}) == [] and dec.eoh
    [t] = dec.feed(packet(1, [{"t": 500, "p": 1, "s": 1, "id": 2}], bt="2027-06-02T13:30:00Z"))
    assert t.ts_ns == int(datetime(2027, 6, 2, 13, 30, tzinfo=timezone.utc).timestamp()) * 10**9 + 500_000_000


def test_a_bad_event_does_not_drop_the_socket():
    events, _ = market_script(start="09:30:00", stop="09:30:30", step_s=2)

    async def go():
        async with FakeMd([events]) as md:
            src = make_source(md.url, stop="09:30:20")
            orig = src._on_event
            calls = [0]

            def flaky(m):
                calls[0] += 1
                if calls[0] == 5:
                    raise ValueError("boom")
                orig(m)
            src._on_event = flaky
            await drain(src)
            return src
    assert asyncio.run(go()).reconnects == 0


def test_aggressor_rule():
    a = tv.Aggressor()
    assert a.classify(100) == 0                          # first trade, nothing known
    assert a.classify(101) == 1 and a.classify(100) == -1 and a.classify(100) == -1   # tick rule, unchanged = previous
    assert a.classify(105, bid=103, ask=104) == 1        # at/above the tick's ask
    assert a.classify(102, bid=103, ask=104) == -1       # at/below the tick's bid
    a.book(110, 111)
    assert a.classify(111) == 1 and a.classify(110) == -1  # DOM best bid/ask when the tick has none
    b = tv.Aggressor()
    b.book(100, 102)
    assert b.classify(101) == 0                          # inside the spread, no history: unknown
    assert b.classify(102, bid=99, ask=103) == 1         # below the tick's ask, but at the DOM ask: buy
    assert tv.Aggressor().classify(5, bid=None, ask=4) == 1


# ---------------------------------------------------------------- auth

def login_ok(token="ACC-1", md="MD-1", minutes=90):
    exp = datetime.fromtimestamp(datetime.now(timezone.utc).timestamp() + minutes * 60, timezone.utc)
    return (200, {"accessToken": token, "mdAccessToken": md, "expirationTime": exp.isoformat().replace("+00:00", "Z"),
                  "userId": 5, "userStatus": "Active", "hasMarketData": True})


class FakeHttp:
    def __init__(self, login=None, renew=None, find=None):
        self.login_q, self.renew_q = list(login or [login_ok()]), list(renew or [])
        self.find = find or (200, {"id": 777, "name": "NQZ6"})
        self.calls = []

    async def __call__(self, method, url, headers, body):
        self.calls.append((method, url, dict(headers), body))
        if url.endswith("/auth/accesstokenrequest"):
            return self.login_q.pop(0)
        if url.endswith("/auth/renewaccesstoken"):
            return self.renew_q.pop(0)
        if "/contract/find" in url:
            return self.find
        raise AssertionError(url)


def auth_with(http, env="demo", **kw):
    async def nosleep(s):
        slept.append(s)
    slept = []
    a = tv.TradovateAuth(cfg(env), http=http, sleep=kw.pop("sleep", nosleep), **kw)
    a.slept = slept
    return a


def test_config_from_env_and_missing_credentials():
    env = {"TRADOVATE_USERNAME": "u", "TRADOVATE_PASSWORD": "p", "TRADOVATE_CID": "12", "TRADOVATE_SEC": "s",
           "TRADOVATE_DEVICE_ID": "d"}
    assert tv.missing_credentials(env) == []
    assert tv.missing_credentials({**env, "TRADOVATE_SEC": " ", "TRADOVATE_USERNAME": ""}) == \
        ["TRADOVATE_USERNAME", "TRADOVATE_SEC"]
    c = tv.TradovateConfig.from_env(env)
    assert (c.env, c.app_id, c.app_version, c.md_url) == ("live", "trade-jev", "1.0", None)
    assert c.base == "https://live.tradovateapi.com/v1" and c.md_candidates() == [tv.MD_URL]
    d = tv.TradovateConfig.from_env({**env, "TRADOVATE_ENV": "demo"})
    assert d.base == "https://demo.tradovateapi.com/v1" and d.md_candidates() == [tv.MD_URL, tv.MD_DEMO_URL]
    o = tv.TradovateConfig.from_env({**env, "TRADOVATE_ENV": "demo", "TRADOVATE_MD_URL": "ws://x/y"})
    assert o.md_candidates() == ["ws://x/y"]  # the override replaces everything: no fallback
    with pytest.raises(tv.TradovateError, match="TRADOVATE_ENV"):
        tv.TradovateConfig.from_env({**env, "TRADOVATE_ENV": "prod"})
    with pytest.raises(tv.TradovateError, match="TRADOVATE_SEC"):
        tv.TradovateConfig.from_env({**env, "TRADOVATE_SEC": ""})
    assert "pw" not in repr(cfg()) and "sec-SECRET" not in repr(cfg())


def test_repr_never_shows_secrets():
    http = FakeHttp()
    a = auth_with(http)
    asyncio.run(a.login())
    for obj in (a.cfg, a):
        text = repr(obj)
        assert not any(s in text for s in SECRETS), text


def test_login_ok_sends_the_documented_body_and_keeps_both_tokens(capsys):
    http = FakeHttp()
    a = auth_with(http)
    asyncio.run(a.login())
    method, url, _, body = http.calls[0]
    assert (method, url) == ("POST", "https://demo.tradovateapi.com/v1/auth/accesstokenrequest")
    assert body == {"name": "user", "password": "pw-SECRET", "appId": "trade-jev", "appVersion": "1.0",
                    "cid": 1234, "sec": "sec-SECRET", "deviceId": "dev-1"}   # cid is an int when all digits
    assert (a.access_token, a.md_token, a.user_status, a.has_market_data) == ("ACC-1", "MD-1", "Active", True)
    assert a.expires_at > a.clock() + 80 * 60
    out = capsys.readouterr().out
    assert "login ok" in out and not any(s in out for s in SECRETS)
    c2 = tv.TradovateConfig("u", "p", "abc-1", "s", "d")
    assert tv.TradovateAuth(c2)._login_body()["cid"] == "abc-1"


def assert_clean(exc_or_text, capsys=None):
    text = str(exc_or_text) + (capsys.readouterr().out if capsys else "")
    assert not any(s in text for s in SECRETS), text


def test_login_errors_are_classified_and_redacted(capsys):
    cases = [
        ((200, {"errorText": "Incorrect username or password pw-SECRET"}), tv.BadCredentials, "bad credentials"),
        ((200, {"errorText": "Incorrect app credentials sec-SECRET: cid not registered"}), tv.ApiKeyRejected,
         "this account has no API key: cid/sec rejected"),
        ((200, {"errorText": "Application is not registered"}), tv.ApiKeyRejected, "no API key"),
        ((403, {}), tv.AuthError, "login rejected (HTTP 403"),     # a bare 4xx is not "no API key"
        ((401, {}), tv.AuthError, "login rejected (HTTP 401"),
        ((200, {"errorText": "Account is locked sec-SECRET"}), tv.AuthError, "login failed"),
    ]
    for resp, kind, text in cases:
        a = auth_with(FakeHttp(login=[resp]))
        with pytest.raises(kind) as e:
            asyncio.run(a.login())
        assert type(e.value) is kind and text in str(e.value), (resp, str(e.value))
        assert_clean(e.value, capsys)
    assert not issubclass(tv.BadCredentials, tv.ApiKeyRejected)
    assert "API Access requires" in tv.ApiKeyRejected.hint and "prop-firm" in tv.ApiKeyRejected.hint


def test_login_penalty_retries_once_with_the_ticket():
    http = FakeHttp(login=[(200, {"p-ticket": "T-9", "p-time": 7, "p-captcha": False}), login_ok()])
    a = auth_with(http)
    asyncio.run(a.login())
    assert a.slept == [7.0]
    assert "p-ticket" not in http.calls[0][3] and http.calls[1][3]["p-ticket"] == "T-9"
    assert http.calls[1][3]["name"] == "user" and a.access_token == "ACC-1"


def test_second_penalty_and_captcha_fail_clearly():
    pen = (200, {"p-ticket": "T", "p-time": 1, "p-captcha": False})
    a = auth_with(FakeHttp(login=[pen, pen]))
    with pytest.raises(tv.AuthError, match="penalised the login twice"):
        asyncio.run(a.login())
    assert a.slept == [1.0]
    a = auth_with(FakeHttp(login=[(200, {"p-ticket": "T", "p-time": 30, "p-captcha": True})]))
    with pytest.raises(tv.CaptchaRequired, match="log in once by hand in the Tradovate web app"):
        asyncio.run(a.login())
    assert a.slept == []


def test_long_penalty_is_reported_not_slept():
    a = auth_with(FakeHttp(login=[(200, {"p-ticket": "T", "p-time": 3600, "p-captcha": False})]))
    with pytest.raises(tv.AuthError, match="more than 120s"):
        asyncio.run(a.login())
    assert a.slept == []
    b = auth_with(FakeHttp(login=[(200, {"p-ticket": "T", "p-time": 120, "p-captcha": False}), login_ok()]))
    asyncio.run(b.login())
    assert b.slept == [120.0]


def test_login_and_renew_never_run_concurrently():
    active, peak = [0], [0]

    class Slow(FakeHttp):
        async def __call__(self, method, url, headers, body):
            active[0] += 1
            peak[0] = max(peak[0], active[0])
            await asyncio.sleep(0.01)
            active[0] -= 1
            self.login_q = [login_ok()]
            self.renew_q = [(200, {"accessToken": "ACC-2"})]
            return await super().__call__(method, url, headers, body)

    async def go():
        a = auth_with(Slow())
        a.expires_at = 0
        a.access_token = "ACC-1"
        await asyncio.gather(a.login(), a.ensure_fresh(), a.renew_or_login(), a.login())
    asyncio.run(go())
    assert peak[0] == 1


def test_renew_keeps_the_old_md_token_and_failure_falls_back_to_login(capsys):
    exp = login_ok(minutes=90)[1]["expirationTime"]
    http = FakeHttp(login=[login_ok(), login_ok("ACC-2", "MD-1")],
                    renew=[(200, {"accessToken": "ACC-2", "expirationTime": exp}),
                           (200, {"errorText": "Token expired ACC-2"})])
    now = [1_000_000.0]
    a = auth_with(http, clock=lambda: now[0])
    asyncio.run(a.login())
    a.expires_at = now[0] + 10 * 60             # within the 15 min margin
    asyncio.run(a.ensure_fresh())
    assert http.calls[1][:2] == ("GET", "https://demo.tradovateapi.com/v1/auth/renewaccesstoken")
    assert http.calls[1][2]["Authorization"] == "Bearer ACC-1"
    assert (a.access_token, a.md_token) == ("ACC-2", "MD-1")   # mdAccessToken absent: kept
    assert a.redact("x ACC-1 y ACC-2") == "x *** y ***"        # the replaced token stays redacted
    now[0] = a.expires_at - 60
    asyncio.run(a.ensure_fresh())                              # renew fails -> a full login
    assert [c[1].rsplit("/", 1)[1] for c in http.calls[2:]] == ["renewaccesstoken", "accesstokenrequest"]
    assert a.access_token == "ACC-2"
    now[0] = 1_000_000.0
    asyncio.run(a.ensure_fresh())                              # fresh: nothing to do
    assert len(http.calls) == 4
    assert_clean("", capsys)


def test_renew_loop_renews_before_expiry_and_stops():
    http = FakeHttp(renew=[(200, {"accessToken": "ACC-2"})])
    sleeps = []

    async def sleep(s):
        sleeps.append(s)
        if len(sleeps) > 1:
            raise asyncio.CancelledError

    async def go():
        a = auth_with(http, sleep=sleep)
        await a.login()
        a.start_renew_loop()
        await asyncio.sleep(0.05)
        await a.stop_renew_loop()
        return a
    a = asyncio.run(go())
    assert 30 <= sleeps[0] <= 90 * 60 - tv.RENEW_MARGIN_S + 1 and a.access_token == "ACC-2"


def test_contract_find():
    http = FakeHttp()
    a = auth_with(http)
    asyncio.run(a.login())
    assert asyncio.run(a.contract_id("NQZ6")) == 777
    assert http.calls[-1][1] == "https://demo.tradovateapi.com/v1/contract/find?name=NQZ6"
    assert http.calls[-1][2]["Authorization"] == "Bearer ACC-1"
    b = auth_with(FakeHttp(find=(200, {"errorText": "nope"})))
    asyncio.run(b.login())
    with pytest.raises(tv.TradovateError):
        asyncio.run(b.contract_id("NQZ6"))


def test_no_order_endpoints_are_ever_requested():
    http = FakeHttp()
    a = auth_with(http)
    asyncio.run(a.login())
    asyncio.run(a.contract_id("NQZ6"))
    for _, url, _, _ in http.calls:
        assert not any(w in url for w in ("order", "position", "account", "cashBalance"))


# ---------------------------------------------------------------- fake md server

def dom_event(i, ts_ns, bid, ask, depth=3, cid=777):
    """Alternates the documented and the compact DOM forms. `bid` / `ask` are best prices in ticks."""
    bids = [(bid - k, 5 + k) for k in range(depth)]
    asks = [(ask + k, 4 + k) for k in range(depth)]
    if i % 2 == 0:
        return {"e": "md", "d": {"doms": [{"contractId": cid, "timestamp": iso(ts_ns),
                                          "bids": [{"price": p * 0.25, "size": s + 0.0} for p, s in bids],
                                          "offers": [{"price": p * 0.25, "size": s + 0.0} for p, s in asks]}]}}
    return {"e": "md", "d": {"d": [{"id": cid, "t": ts_ns // 1_000_000, "r": True,
                                   "b": [[p * 0.25, s] for p, s in bids], "a": [[p * 0.25, s] for p, s in asks]}]}}


def tick_event(cid_rt, ts_ns, price, size, tid, bid=None, ask=None):
    bt = 1_800_000_000_000
    t = {"t": ts_ns // 1_000_000 - bt, "p": price - 80000, "s": size, "id": tid}
    if bid is not None:
        t.update(b=bid - 80000, bs=10)
    if ask is not None:
        t.update(a=ask - 80000, **{"as": 10})
    return {"e": "chart", "d": {"charts": [{"id": cid_rt, "s": "t", "bp": 80000, "bt": bt, "ts": 0.25, "tks": [t]}]}}


def market_script(start="09:29:20", stop="09:37:10", step_s=2, history=True):
    """Per-connection script: noise, a history tick + eoh, then DOM snapshots every `step_s` seconds with a
    trade between them (alternating buy at the ask / sell at the bid). Returns (events(idx), expected_delta)."""
    t0, t1 = ns(start), ns(stop)
    expected = [0]

    def events(idx):
        hist, rt = 11 + idx * 10, 12 + idx * 10
        out = [{"e": "props", "d": {}}, {"e": "md", "d": {"q": [{"id": 777, "t": 1, "bid": 1}]}},
               {"e": "md", "d": {"weird": True}}]
        if history:
            out.append({"e": "chart", "d": {"charts": [
                {"id": hist, "s": "t", "bp": 80000, "bt": 1_800_000_000_000, "ts": 0.25,
                 "tks": [{"t": 1, "p": 3, "s": 999, "id": 1}]}, {"id": hist, "eoh": True}]}})
        n = 0
        for ts in range(t0, t1 + 1, step_s * 10**9):
            bid = 80000 + (n // 7) % 5
            out.append(dom_event(n, ts, bid, bid + 1))
            side = 1 if n % 2 == 0 else -1
            px = bid + 1 if side > 0 else bid
            out.append(tick_event(rt, ts + 500_000_000, px, 2, 100 + n, bid=bid, ask=bid + 1))
            expected[0] += side * 2
            n += 1
        return out
    return events, expected


class FakeMd:
    """Local md websocket: o / authorize / [] / subscribeDOM / getChart / cancelChart, then a script."""

    def __init__(self, scripts, accept=("MD-1", "ACC-1"), refuse=()):
        self.scripts, self.accept, self.refuse = scripts, set(accept), set(refuse)
        self.n = 0
        self.requests: list[tuple[int, str, str]] = []
        self.auths: list[tuple[int, str]] = []

    async def __aenter__(self):
        self.server = await websockets.serve(self.handler, "127.0.0.1", 0, ping_interval=None)
        self.url = f"ws://127.0.0.1:{self.server.sockets[0].getsockname()[1]}/v1/websocket"
        return self

    async def __aexit__(self, *a):
        self.server.close()
        await self.server.wait_closed()

    async def handler(self, ws):
        idx, self.n = self.n, self.n + 1
        await ws.send("o")
        subs, streamer = set(), None

        async def hb():
            while True:
                await asyncio.sleep(0.05)
                await ws.send("h")

        async def stream():
            script = self.scripts[min(idx, len(self.scripts) - 1)]
            if script is None:
                return
            events = script(idx) if callable(script) else script
            for k, ev in enumerate(events):
                if ev == "DROP":
                    await ws.close()
                    return
                await ws.send("a" + json.dumps([ev]))
                if k % 20 == 0:
                    await asyncio.sleep(0)
        hbt = asyncio.create_task(hb())
        try:
            async for msg in ws:
                if msg == "[]":
                    continue
                endpoint, id_, _, body = msg.split("\n", 3)
                self.requests.append((idx, endpoint, body))
                reply = {"s": 200, "i": int(id_)}
                if endpoint == "authorize":
                    self.auths.append((idx, body))
                    if body not in self.accept:
                        reply = {"s": 401, "i": int(id_), "d": "bad token"}
                elif endpoint in self.refuse:
                    reply = {"s": 403, "i": int(id_), "d": "not entitled ACC-1"}
                elif endpoint == "md/getChart":
                    reply["d"] = {"historicalId": 11 + idx * 10, "realtimeId": 12 + idx * 10}
                    subs.add("chart")
                elif endpoint == "md/subscribeDOM":
                    subs.add("dom")
                await ws.send("a" + json.dumps([reply]))
                if subs == {"chart", "dom"} and streamer is None:
                    streamer = asyncio.create_task(stream())
        except websockets.ConnectionClosed:
            pass
        finally:
            hbt.cancel()
            if streamer:
                streamer.cancel()


def make_source(url, stop="09:37:00", http=None, **kw):
    a = tv.TradovateAuth(cfg(), http=http or FakeHttp())
    return tv.TradovateSource("NQZ6", ns(stop), flush_s=0.02, auth=a, md_urls=url if isinstance(url, list) else [url],
                              backoff_s=0.05, backoff_max_s=0.2, **kw)


async def drain(src, limit_s=20):
    out = []
    async with asyncio.timeout(limit_s):
        async for b in src.batches():
            out.append(b)
    return out


def test_source_merges_dom_and_ticks_into_monotonic_rows():
    import numpy as np
    events, expected = market_script()

    async def go():
        async with FakeMd([events]) as md:
            src = make_source(md.url)
            return src, md, await drain(src)
    src, md, bs = asyncio.run(go())
    ts = np.concatenate([b.ts for b in bs])
    delta = np.concatenate([b.delta for b in bs])
    assert (np.diff(ts) >= 0).all() and ts[-1] > ns("09:37:00")
    # history tick (size 999, before eoh) never counted; every realtime trade is +-2 at the right side
    assert set(np.unique(delta)) <= {-2, 0, 2} and 999 not in delta
    assert src.ticks == src.buys + src.sells + src.unknown and src.unknown == 0 and src.dropped_trades == 0
    assert int(delta.sum()) in (0, 2)        # buys and sells alternate, starting with a buy
    assert 0 < (delta != 0).sum() <= src.ticks  # (the source may have read a little past the stop row)
    b0 = bs[0]
    assert b0.bid_px.shape[1] == LEVELS and b0.bid_px.dtype == np.int32 and b0.bid_sz.dtype == np.int64
    assert (b0.ask_px[:, 0] > b0.bid_px[:, 0]).all()
    assert b0.bid_px[0, 0] == 80000 and b0.bid_sz[0, 0] == 5 and b0.ask_sz[0, 0] == 4
    assert b0.bid_px[0, 3] == 80000 - 3 and b0.bid_sz[0, 3] == 0            # padded: 3 levels given
    assert src.contract_id == 777 and src.md_token_kind == "mdAccessToken" and src.dom_updates > 100
    assert src.unknown_events >= 1 and src.reconnects == 0 and src.max_bid_levels == 3
    # clean shutdown: cancelChart (realtime id) and unsubscribeDOM were sent
    names = [r[1] for r in md.requests]
    assert names[:3] == ["authorize", "md/subscribeDOM", "md/getChart"]
    assert "md/cancelChart" in names and "md/unsubscribeDOM" in names
    cancel = json.loads(next(b for _, e, b in md.requests if e == "md/cancelChart"))
    assert cancel == {"subscriptionId": 12}
    assert json.loads(next(b for _, e, b in md.requests if e == "md/getChart"))["chartDescription"]["underlyingType"] == "Tick"
    assert not any("order" in e or "position" in e for _, e, _ in md.requests)
    assert src.live is True


def test_trade_deltas_follow_the_aggressor_rule_exactly():
    import numpy as np
    t = ns("09:30:00")

    def ev(idx):
        return [{"e": "chart", "d": {"charts": [{"id": 12, "eoh": True}]}},
                tick_event(12, t, 80001, 1, 1),                         # before any book: dropped
                dom_event(0, t + 10**9, 80000, 80001),
                tick_event(12, t + 2 * 10**9, 80001, 3, 2, bid=80000, ask=80001),   # at tick ask: buy
                tick_event(12, t + 3 * 10**9, 80000, 4, 3, bid=80000, ask=80001),   # at tick bid: sell
                tick_event(12, t + 4 * 10**9, 80001, 5, 4),                          # DOM ask: buy
                tick_event(12, t + 5 * 10**9, 80000, 6, 5),                          # DOM bid: sell
                dom_event(1, ns("09:40:00"), 80002, 80003)]

    async def go():
        async with FakeMd([ev]) as md:
            src = make_source(md.url, stop="09:39:00")
            return src, await drain(src)
    src, bs = asyncio.run(go())
    delta = np.concatenate([b.delta for b in bs])
    assert [int(d) for d in delta if d] == [3, -4, 5, -6]
    assert src.dropped_trades == 1 and (src.buys, src.sells, src.unknown) == (2, 2, 1)


def test_token_fallback_tries_md_token_first_then_access_token(capsys):
    events, _ = market_script(stop="09:30:30", start="09:30:00")

    async def go():
        async with FakeMd([events], accept=["ACC-1"]) as md:
            src = make_source(md.url, stop="09:30:20")
            await drain(src)
            return src, md
    src, md = asyncio.run(go())
    assert [a for _, a in md.auths][:2] == ["MD-1", "ACC-1"]
    assert md.auths[0][0] != md.auths[1][0]               # the second try is a fresh connection
    assert src.md_token_kind == "accessToken"
    out = capsys.readouterr().out
    assert "token accessToken" in out and not any(s in out for s in SECRETS)


def test_md_host_fallback_for_demo_remembers_the_host_that_worked(capsys):
    events, _ = market_script(stop="09:30:30", start="09:30:00")

    async def go():
        async with FakeMd([events]) as md:
            src = make_source(["ws://127.0.0.1:1/v1/websocket", md.url], stop="09:30:20")   # first refuses
            await drain(src)
            return src, md
    src, md = asyncio.run(go())
    assert src.md_host == md.url.split("/")[2] and src._good[0] == md.url
    assert src.md_token_kind == "mdAccessToken" and src.dom_updates > 0
    assert "connect failed" in capsys.readouterr().out


def test_md_connect_failure_everywhere_is_retried_not_fatal():
    async def go():
        src = make_source(["ws://127.0.0.1:1/x"])
        with pytest.raises(TimeoutError):
            await drain(src, limit_s=0.6)
        return src
    assert asyncio.run(go()).reconnects >= 2


def test_reconnect_reauthorizes_resubscribes_and_ignores_replayed_history():
    import numpy as np
    first = market_script(start="09:30:00", stop="09:30:20", step_s=2)[0]
    second = market_script(start="09:30:20", stop="09:37:10", step_s=5)[0]

    def conn0(idx):
        return [*first(idx), "DROP"]

    async def go():
        async with FakeMd([conn0, second]) as md:
            src = make_source(md.url)
            return src, md, await drain(src)
    src, md, bs = asyncio.run(go())
    assert src.reconnects >= 1 and len({i for i, _ in md.auths}) == 2
    conn1 = [e for i, e, _ in md.requests if i == 1]
    assert conn1[:3] == ["authorize", "md/subscribeDOM", "md/getChart"]
    ts = np.concatenate([b.ts for b in bs])
    delta = np.concatenate([b.delta for b in bs])
    assert (np.diff(ts) >= 0).all() and ts[-1] > ns("09:37:00")
    assert 999 not in delta and (ts > ns("09:30:20")).any()   # rows kept coming after the drop
    # the second connection's chart ids differ (new subscription) and its history was gated by its own eoh
    gc = [json.loads(b) for i, e, b in md.requests if e == "md/getChart"]
    assert len(gc) == 2


def test_md_refusal_fails_fast_with_a_clear_redacted_error(capsys, monkeypatch):
    monkeypatch.setattr(tv, "REFUSAL_WINDOW_S", 0.0)
    async def go(**kw):
        async with FakeMd([[]], **kw) as md:
            src = make_source(md.url)
            with pytest.raises(tv.MdRefused) as e:
                await drain(src, limit_s=15)
            return e.value, md
    err, md = asyncio.run(go(refuse={"md/subscribeDOM"}))
    assert str(err).startswith("Tradovate market data refused:") and "no CME market-data entitlement" in str(err)
    assert "not entitled ***" in str(err) and "ACC-1" not in str(err)
    assert md.n == tv.MAX_REFUSALS
    err, md = asyncio.run(go(accept=[]))                    # authorize refused on every token
    assert "refused" in str(err) and md.n == tv.MAX_REFUSALS * 2   # md token, then access token, per attempt
    assert not any(s in capsys.readouterr().out for s in SECRETS)


def test_brief_md_refusal_at_the_open_keeps_retrying(monkeypatch):
    """A few quick refusals (well inside REFUSAL_WINDOW_S) must not crash an unattended day."""
    monkeypatch.setattr(tv, "REFUSAL_WINDOW_S", 1000.0)

    async def go():
        async with FakeMd([[]], refuse={"md/subscribeDOM"}) as md:
            src = make_source(md.url)
            with pytest.raises(TimeoutError):
                await drain(src, limit_s=2)
            return md
    md = asyncio.run(go())
    assert md.n > tv.MAX_REFUSALS


@pytest.mark.parametrize("healthy_s, expect", [(0.0, [0.05, 0.05, 0.05]), (1000.0, [0.05, 0.1, 0.2])])
def test_backoff_resets_only_after_a_healthy_connection(monkeypatch, capsys, healthy_s, expect):
    monkeypatch.setattr(tv, "HEALTHY_S", healthy_s)
    drop = lambda idx: [*market_script(start="09:30:00", stop="09:30:06", step_s=2)[0](idx), "DROP"]  # noqa: E731
    final = market_script(start="09:30:10", stop="09:37:10", step_s=20)[0]

    async def go():
        async with FakeMd([drop, drop, drop, final]) as md:
            await drain(make_source(md.url))
    asyncio.run(go())
    import re
    delays = [float(x) for x in re.findall(r"reconnecting in ([\d.]+)s", capsys.readouterr().out)]
    assert delays[:3] == expect


def test_server_shutdown_event_triggers_a_reconnect():
    sd = [{"e": "shutdown", "d": {"reasonCode": "X", "reason": "restart"}}]
    ok = market_script(start="09:30:00", stop="09:37:10", step_s=20)[0]

    async def go():
        async with FakeMd([sd, ok]) as md:
            src = make_source(md.url)
            await drain(src)
            return src
    assert asyncio.run(go()).reconnects >= 1


def test_fatal_auth_error_surfaces_from_batches_redacted(capsys):
    http = FakeHttp(login=[(200, {"errorText": "Incorrect app credentials sec-SECRET"})])

    async def go():
        src = make_source("ws://127.0.0.1:1/x", http=http)
        await drain(src)
    with pytest.raises(tv.ApiKeyRejected) as e:
        asyncio.run(go())
    assert "no API key" in str(e.value)
    assert_clean(e.value, capsys)


def test_contract_lookup_failure_falls_back_to_the_first_dom_contract_id(capsys):
    events, _ = market_script(stop="09:30:30", start="09:30:00")

    async def go():
        async with FakeMd([events]) as md:
            src = make_source(md.url, stop="09:30:20", http=FakeHttp(find=(404, {"errorText": "no such contract"})))
            await drain(src)
            return src
    src = asyncio.run(go())
    assert src.contract_id == 777 and src.dom_updates > 0
    assert "first DOM" in capsys.readouterr().out


# ---------------------------------------------------------------- tradovate_check.py

def load_check():
    spec = importlib.util.spec_from_file_location("tradovate_check", Path(__file__).parents[1] / "scripts" / "tradovate_check.py")
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


ENV = {"TRADOVATE_USERNAME": "user", "TRADOVATE_PASSWORD": "pw-SECRET", "TRADOVATE_CID": "1234",
       "TRADOVATE_SEC": "sec-SECRET", "TRADOVATE_DEVICE_ID": "dev-1", "TRADOVATE_ENV": "demo"}


def test_check_script_missing_credentials_names_only(capsys):
    code = asyncio.run(load_check().run(["--seconds", "1"], env={"TRADOVATE_USERNAME": "u"}))
    out = capsys.readouterr().out
    assert code == 2 and out.startswith("FAIL: tradovate credentials missing: TRADOVATE_PASSWORD, TRADOVATE_CID")


def test_check_script_says_plainly_when_the_account_has_no_api_key(monkeypatch, capsys):
    http = FakeHttp(login=[(200, {"errorText": "Incorrect app credentials sec-SECRET"})])
    monkeypatch.setattr(tv, "default_http", http)
    code = asyncio.run(load_check().run(["--seconds", "1", "--symbol", "NQZ6"], env=ENV))
    out = capsys.readouterr().out
    assert code == 3
    assert "FAIL: this account has no API key: cid/sec rejected" in out
    assert "Incorrect app credentials ***" in out and "prop-firm evaluation accounts may not allow it" in out
    assert "demo.tradovateapi.com" in out and not any(s in out for s in SECRETS)


def test_check_script_reports_bad_credentials_separately(monkeypatch, capsys):
    monkeypatch.setattr(tv, "default_http", FakeHttp(login=[(200, {"errorText": "Incorrect username or password"})]))
    code = asyncio.run(load_check().run(["--seconds", "1", "--symbol", "NQZ6"], env=ENV))
    out = capsys.readouterr().out
    assert code == 3 and "FAIL: bad credentials" in out and "no API key" not in out


def test_check_script_full_run_prints_the_report(monkeypatch, capsys):
    events, _ = market_script(start="09:30:00", stop="09:30:40", step_s=2)

    async def go():
        async with FakeMd([events]) as md:
            monkeypatch.setattr(tv, "default_http", FakeHttp())
            env = {**ENV, "TRADOVATE_MD_URL": md.url}
            return await load_check().run(["--seconds", "1.5", "--symbol", "NQZ6"], env=env)
    code = asyncio.run(go())
    out = capsys.readouterr().out
    assert code == 0 and "login ok" in out and "hasMarketData True" in out
    assert "token kind mdAccessToken" in out and "contract id 777" in out
    assert "DOM:" in out and "ticks:" in out and out.rstrip().endswith("OK")
    assert not any(s in out for s in SECRETS)


def test_check_script_no_dom_data_is_a_failure(monkeypatch, capsys):
    async def go():
        async with FakeMd([[{"e": "props", "d": {}}]]) as md:
            monkeypatch.setattr(tv, "default_http", FakeHttp())
            return await load_check().run(["--seconds", "0.6", "--symbol", "NQZ6"], env={**ENV, "TRADOVATE_MD_URL": md.url})
    assert asyncio.run(go()) == 4
    assert "no DOM updates: likely no CME market-data entitlement" in capsys.readouterr().out


# ---------------------------------------------------------------- skip gate

def test_skip_reason_is_a_pure_function_of_source_and_env():
    full = dict(ENV)
    assert live.skip_reason("tradovate", full) is None
    assert live.skip_reason("databento", {}) is None
    assert live.skip_reason("tradovate", {**full, "TRADE_JEV_SOURCE": "off"}) == "TRADE_JEV_SOURCE=off"
    assert live.skip_reason("databento", {"TRADE_JEV_SOURCE": "OFF"}) == "TRADE_JEV_SOURCE=off"
    why = live.skip_reason("tradovate", {k: v for k, v in full.items() if k not in ("TRADOVATE_SEC", "TRADOVATE_USERNAME")})
    assert why == "tradovate credentials missing: TRADOVATE_USERNAME, TRADOVATE_SEC"


@pytest.fixture
def no_feed(monkeypatch):
    """Fails the test if any feed or Jev client is constructed."""
    def boom(*a, **k):
        raise AssertionError("a feed was started")
    for name in ("TradovateSource", "DatabentoSource"):
        monkeypatch.setattr(live, name, boom)
    monkeypatch.delenv("DATABENTO_API_KEY", raising=False)
    for k in ENV:
        monkeypatch.delenv(k, raising=False)


@pytest.mark.parametrize("env, argv, reason, source", [
    ({"TRADE_JEV_SOURCE": "off"}, [], "TRADE_JEV_SOURCE=off", "off"),
    ({"TRADE_JEV_SOURCE": "off", "TRADOVATE_USERNAME": "u"}, ["--source", "tradovate"],     # explicit --source beats off
     "tradovate credentials missing: TRADOVATE_PASSWORD, TRADOVATE_CID, TRADOVATE_SEC, TRADOVATE_DEVICE_ID", "tradovate"),
    ({"TRADE_JEV_SOURCE": "tradovate", "TRADOVATE_USERNAME": "u", "TRADOVATE_PASSWORD": "pw-SECRET"}, [],
     "tradovate credentials missing: TRADOVATE_CID, TRADOVATE_SEC, TRADOVATE_DEVICE_ID", "tradovate"),
])
def test_skipped_day_exits_0_with_a_skipped_row_and_no_feed(sandbox, db, no_feed, monkeypatch, capsys, env, argv, reason, source):  # noqa: F811
    for k, v in env.items():
        monkeypatch.setenv(k, v)
    monkeypatch.setattr(live, "now_et", lambda: datetime(2026, 11, 2, 9, 15, tzinfo=live.ET))
    assert asyncio.run(live.amain(["--exit-at-close", "--no-open", *argv])) == 0
    [s] = rows(db, "SELECT * FROM paper_sessions")
    assert (s["session_id"], s["status"], s["error"], s["source"]) == ("live-2026-11-02", "skipped", reason, source)
    out = capsys.readouterr().out
    assert reason in out and "pw-SECRET" not in out and "pw-SECRET" not in json.dumps(s)
    assert not (sandbox / "runs").exists()


def test_explicit_source_on_the_command_line_wins_over_env_off(sandbox, db, no_feed, monkeypatch):  # noqa: F811
    monkeypatch.setenv("TRADE_JEV_SOURCE", "off")
    monkeypatch.setattr(live, "now_et", lambda: datetime(2026, 11, 2, 9, 15, tzinfo=live.ET))
    with pytest.raises(SystemExit):  # not skipped: it goes on to the databento path and wants its key
        asyncio.run(live.amain(["--exit-at-close", "--no-open", "--source", "databento"]))
    assert not db.exists()  # no skipped row
    assert live.skip_reason("databento", {"TRADE_JEV_SOURCE": "off"}, explicit=True) is None
    assert live.skip_reason("databento", {"TRADE_JEV_SOURCE": "off"}) == "TRADE_JEV_SOURCE=off"


def test_skipped_never_overwrites_a_real_session_but_replaces_a_skip(db):  # noqa: F811
    rec = paperdb.Recorder("live-2026-11-02")
    rec.start("2026-11-02", "NQZ6", {}, None, None, source="tradovate")
    rec.close()
    paperdb.record_skip("live-2026-11-02", "2026-11-02", "NQZ6", "skipped", "TRADE_JEV_SOURCE=off")
    assert rows(db, "SELECT status, source FROM paper_sessions")[0] == {"status": "running", "source": "tradovate"}
    paperdb.record_skip("live-2026-11-03", "2026-11-03", "NQZ6", "skipped", "TRADE_JEV_SOURCE=off", source="databento")
    paperdb.record_skip("live-2026-11-03", "2026-11-03", "NQZ6", "skipped", "tradovate credentials missing: X", source="tradovate")
    paperdb.record_holiday("live-2026-11-03", "2026-11-03", "NQZ6", "weekend")  # a holiday may replace a skip
    s = rows(db, "SELECT * FROM paper_sessions WHERE session_id='live-2026-11-03'")
    assert len(s) == 1 and s[0]["status"] == "holiday" and s[0]["error"] == "weekend"


def test_source_column_is_added_to_an_old_db_idempotently(tmp_path):
    path = tmp_path / "old.db"
    con = sqlite3.connect(path)
    con.executescript("""CREATE TABLE paper_sessions (session_id TEXT PRIMARY KEY, day TEXT, symbol TEXT, status TEXT,
        started_at TEXT, ended_at TEXT, heartbeat_at TEXT, settings_json TEXT, jev_model TEXT, run_dir TEXT, error TEXT);
        INSERT INTO paper_sessions (session_id, day, status) VALUES ('old', '2026-10-01', 'done');""")
    con.commit()
    con.close()
    for _ in range(2):
        c = paperdb.connect(path)
        cols = [r[1] for r in c.execute("PRAGMA table_info(paper_sessions)")]
        c.close()
        assert cols.count("source") == 1 and cols[-1] == "source"
    s = paperdb.list_sessions(path)["sessions"][0]
    assert s["source"] is None and s["status"] == "done"


def test_viewer_api_returns_source_and_skipped(db):  # noqa: F811
    import threading
    import urllib.request
    from http.server import ThreadingHTTPServer
    from trade_jev.view import Handler
    rec = paperdb.Recorder("live-2026-11-02")
    rec.start("2026-11-02", "NQZ6", {}, None, None, source="tradovate")
    rec.close()
    paperdb.record_skip("live-2026-11-03", "2026-11-03", "NQZ6", "skipped", "TRADE_JEV_SOURCE=off", source="databento")
    srv = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    srv.daemon_threads = True
    threading.Thread(target=srv.serve_forever, daemon=True).start()
    try:
        base = f"http://127.0.0.1:{srv.server_address[1]}"
        data = json.loads(urllib.request.urlopen(base + "/api/paper/sessions", timeout=5).read())
        got = {s["session_id"]: (s["status"], s["source"], s["error"]) for s in data["sessions"]}
        assert got == {"live-2026-11-02": ("running", "tradovate", None),
                       "live-2026-11-03": ("skipped", "databento", "TRADE_JEV_SOURCE=off")}
        one = json.loads(urllib.request.urlopen(base + "/api/paper/session?id=live-2026-11-03", timeout=5).read())
        assert one["source"] == "databento" and one["status"] == "skipped"
        js = urllib.request.urlopen(base + "/paper.js", timeout=5).read().decode()
        assert '"skipped"' in js and "s.source" in js
    finally:
        srv.shutdown()
        srv.server_close()


# ---------------------------------------------------------------- full paper sessions

class FakeJev(Script):
    api_calls = cache_hits = 0

    def __init__(self, *a, **k):
        super().__init__()


class FakeClient:
    async def aclose(self):
        pass


@pytest.fixture
def tradovate_env(monkeypatch, tmp_path, sandbox):  # noqa: F811
    import typesafe_sdk
    for k, v in ENV.items():
        monkeypatch.setenv(k, v)
    monkeypatch.delenv("TRADE_JEV_SOURCE", raising=False)
    monkeypatch.setattr(tv, "default_http", FakeHttp())
    monkeypatch.setattr(typesafe_sdk, "AsyncTypeSafeClient", FakeClient)
    monkeypatch.setattr(live, "JevPolicy", FakeJev)

    class TmpRecorder(live.Recorder):
        def __init__(self, day, symbol, **kw):
            super().__init__(day, symbol, out_dir=tmp_path / "live", **kw)
    monkeypatch.setattr(live, "Recorder", TmpRecorder)
    return tmp_path


SESSION_ARGS = ["--source", "tradovate", "--exit-at-close", "--no-open", "--port", "0",
                "--start-et", "09:30:00", "--end-et", "09:32:00", "--min-conf", "0.5", "--agree", "1", "--min-hold", "0"]


def run_session(monkeypatch, now_hms, events):
    monkeypatch.setattr(live, "now_et", lambda: datetime.fromtimestamp(ns(now_hms) / 1e9, live.ET))

    async def go():
        async with FakeMd([events]) as md:
            monkeypatch.setenv("TRADOVATE_MD_URL", md.url)
            return await live.amain(SESSION_ARGS), md
    return asyncio.run(asyncio.wait_for(go(), 60))


def test_full_paper_session_on_tradovate_data(tradovate_env, db, monkeypatch, capsys):  # noqa: F811
    events, _ = market_script(start="09:20:00", stop="09:37:10", step_s=2)
    code, md = run_session(monkeypatch, "09:15:00", events)
    assert code == 0
    [s] = rows(db, "SELECT * FROM paper_sessions")
    assert (s["session_id"], s["status"], s["source"], s["error"]) == ("live-2027-06-02", "done", "tradovate", None)
    settings = json.loads(s["settings_json"])
    assert "late_start_warmup_s" not in settings and "warmup_until_et" not in settings
    decs = rows(db, "SELECT * FROM paper_decisions ORDER BY t_ns")
    assert len(decs) == 8 and all(d["error"] is None for d in decs)          # 09:30:00 .. 09:31:45
    summ = {r["policy"]: r for r in rows(db, "SELECT * FROM paper_summary")}
    assert set(summ) == {"jev[test]", "hold", "random", "imbalance"}
    assert sum(r["trades"] for r in summ.values()) > 0
    assert len(rows(db, "SELECT id FROM paper_trades")) == sum(r["trades"] for r in summ.values())
    run = tradovate_env / "runs" / "live-2027-06-02"
    assert (run / "summary.json").exists()
    side = json.loads(next((tradovate_env / "live").glob("*.json")).read_text())
    assert side["feed"] == "tradovate" and side["writer"] == "trade_jev.live" and side["rows"] > 100
    assert any((tradovate_env / "live").glob("*.parquet"))
    out = capsys.readouterr().out
    assert "late start" not in out and not any(x in out for x in SECRETS)
    assert "md socket authorized" in out and "mdAccessToken" in out
    assert "md/cancelChart" in [e for _, e, _ in md.requests]


@pytest.mark.parametrize("now_hms, first, n_decisions", [("09:29:20", "09:29:20", 8), ("09:31:10", "09:31:10", 3)])
def test_late_start_warms_up_with_hold_decisions(tradovate_env, db, monkeypatch, capsys, now_hms, first, n_decisions):  # noqa: F811
    events, _ = market_script(start=first, stop="09:37:10", step_s=2)
    code, _ = run_session(monkeypatch, now_hms, events)
    assert code == 0
    [s] = rows(db, "SELECT * FROM paper_sessions")
    assert s["status"] == "done" and s["source"] == "tradovate"
    settings = json.loads(s["settings_json"])
    until = {"09:29:20": "09:30:20", "09:31:10": "09:32:10"}[first]
    assert settings["late_start_warmup_s"] == 60 and settings["warmup_until_et"] == until
    decs = rows(db, "SELECT * FROM paper_decisions ORDER BY t_ns")
    assert len(decs) == n_decisions
    warm = [d for d in decs if d["raw_action"] == "WARMUP"]
    assert all(d["error"] is None for d in decs)         # warm-up is not an error (the viewer's error count)
    assert [d["time_et"] for d in decs][:2] == (["09:30:00", "09:30:15"] if n_decisions == 8 else ["09:31:15", "09:31:30"])
    assert len(warm) == (2 if n_decisions == 8 else 3)
    assert all(d["action"] == "HOLD" and json.loads(d["probs_json"]) == {} and d["jev_ms"] == 0 for d in warm)
    assert all(d["error"] is None for d in decs if d not in warm)
    out = capsys.readouterr().out
    assert f"late start: warming up 60s on live data (no catch-up replay on Tradovate); decisions until {until} ET are HOLD" in out


def test_session_with_no_book_data_is_recorded_as_crashed(tradovate_env, db, monkeypatch):  # noqa: F811
    monkeypatch.setattr(live, "now_et", lambda: datetime.fromtimestamp(ns("09:15:00") / 1e9, live.ET))
    monkeypatch.setattr(live.Session, "run", lambda self: asyncio.sleep(0))   # a silent feed: no rows at all

    async def go():
        async with FakeMd([None]) as md:
            monkeypatch.setenv("TRADOVATE_MD_URL", md.url)
            return await live.amain(SESSION_ARGS)
    with pytest.raises(RuntimeError, match="no Tradovate book data"):
        asyncio.run(asyncio.wait_for(go(), 30))
    s = rows(db, "SELECT * FROM paper_sessions")[0]
    assert s["status"] == "crashed" and "no CME market-data entitlement" in s["error"]


def test_api_key_rejection_crashes_the_session_with_the_plain_reason(tradovate_env, db, monkeypatch, capsys):  # noqa: F811
    monkeypatch.setattr(tv, "default_http", FakeHttp(login=[(200, {"errorText": "Incorrect app credentials sec-SECRET"})]))
    monkeypatch.setattr(live, "now_et", lambda: datetime.fromtimestamp(ns("09:15:00") / 1e9, live.ET))
    monkeypatch.setenv("TRADOVATE_MD_URL", "ws://127.0.0.1:1/x")
    with pytest.raises(tv.ApiKeyRejected):
        asyncio.run(asyncio.wait_for(live.amain(SESSION_ARGS), 30))
    s = rows(db, "SELECT * FROM paper_sessions")[0]
    assert s["status"] == "crashed" and "this account has no API key: cid/sec rejected" in s["error"]
    assert "sec-SECRET" not in s["error"] and "***" in s["error"]
    assert "sec-SECRET" not in capsys.readouterr().out
