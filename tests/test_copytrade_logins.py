"""Logins: several accounts under one Tradovate sign-in. Config, one shared keepalive, legacy-store seeding,
failure isolation and the broker's per-account filtering. Fakes and tmp dirs only."""

import asyncio
import json
import time

import pytest
import yaml

from tests.copytrade_fakes import Clock
from tests.test_copytrade_tokens import AuthHttp, Clk, NOW, env_file
from trade_jev import tradovate_token as tt
from trade_jev.copytrade import __main__ as cm
from trade_jev.copytrade import config as cfg
from trade_jev.copytrade.broker import TradovateBroker
from trade_jev.copytrade.ledger import Ledger
from trade_jev.copytrade.tokens import LoginKeepalive, RenewBudget

A_ID, B_ID = 73193569, 73609011
BASE = {"enabled": True, "env": "demo", "root": "MNQ", "qty": 5, "daily_loss_limit": 1000, "daily_profit_stop": 490,
        "commission": 0.5}


def acct(name, aid, **kw):
    return {**BASE, "name": name, "account_id": aid, **kw}


def load(tmp_path, accounts, **top):
    p = tmp_path / "accounts.yaml"
    p.write_text(yaml.safe_dump({"accounts": accounts, **top}))
    return cfg.load(p)


def two_on_one(**top):
    return [acct("mff-1", A_ID, login="mff"), acct("mff-2", B_ID, login="mff")], {"logins": {"mff": {
        "secrets_prefix": "COPY_MFF1"}}, **top}


# ---------------------------------------------------------------- config

def test_explicit_login_shared_by_two_accounts(tmp_path):
    accts, top = two_on_one()
    c = load(tmp_path, accts, **top)
    a, b = c.accounts
    assert (a.login, a.secrets_prefix, a.explicit_login) == ("mff", "COPY_MFF1", True)
    assert (b.login, b.secrets_prefix) == ("mff", "COPY_MFF1")
    assert list(c.logins()) == ["mff"] and [x.name for x in c.logins()["mff"]] == ["mff-1", "mff-2"]
    assert cfg.login_dir(tmp_path, a) == tmp_path / "logins" / "mff"


def test_legacy_prefix_is_an_implicit_login(tmp_path):
    (a,) = load(tmp_path, [acct("a1", 11, secrets_prefix="COPY_A1")]).accounts
    assert (a.login, a.secrets_prefix, a.explicit_login) == ("a1", "COPY_A1", False)
    assert cfg.login_dir(tmp_path, a) == tmp_path / "a1"


def test_mixed_explicit_and_legacy_and_unused_login(tmp_path):
    accts = [acct("m1", 1, login="mff"), acct("old", 2, secrets_prefix="COPY_OLD")]
    c = load(tmp_path, accts, logins={"mff": {"secrets_prefix": "COPY_M"}, "spare": {"secrets_prefix": "COPY_S"}})
    assert set(c.logins()) == {"mff", "old"}
    assert [cfg.login_dir(tmp_path, a).name for a in c.accounts] == ["mff", "old"]


def test_account_built_without_login_is_its_own():
    a = cfg.Account(name="x", enabled=True, mode="demo", env="demo", account_id=1, root="MNQ", qty=1,
                    daily_loss_limit=1.0, daily_profit_stop=1.0, commission=0.5, secrets_prefix="COPY_X")
    assert a.login == "x" and not a.explicit_login


LOGINS = {"mff": {"secrets_prefix": "COPY_M"}}


@pytest.mark.parametrize("accounts,logins,msg", [
    ([acct("a", 1, login="mff", secrets_prefix="COPY_M")], LOGINS, "not both"),
    ([acct("a", 1)], LOGINS, "needs login"),
    ([acct("a", 1, login="nope")], LOGINS, "not in logins"),
    ([acct("a", 1, login="mff")], None, "not in logins"),
    ([acct("a", 1, login="mff"), acct("a", 2, login="mff")], LOGINS, "duplicate name"),
    ([acct("a", 1, login="mff"), acct("b", 1, login="mff")], LOGINS, "duplicate account_id"),
    ([acct("a", 1, login="mff")], {"mff": {"secrets_prefix": "COPY_M"}, "o": {"secrets_prefix": "COPY_M"}},
     "duplicate secrets_prefix"),
    ([acct("a", 1, login="mff"), acct("b", 2, secrets_prefix="COPY_M")], LOGINS, "duplicate secrets_prefix"),
    ([acct("a", 1, secrets_prefix="COPY_X"), acct("b", 2, secrets_prefix="COPY_X")], None, "duplicate secrets_prefix"),
    ([acct("mff", 1, secrets_prefix="COPY_X")], LOGINS, "also account names"),
    ([acct("a", 1, login="mff")], {"mff": {"secrets_prefix": "TRADOVATE"}}, "TRADOVATE"),
    ([acct("a", 1, login="mff")], {"mff": {"secrets_prefix": "TRADOVATE_X"}}, "TRADOVATE"),
    ([acct("a", 1, login="mff")], {"mff": {"secrets_prefix": "lower"}}, "UPPER_SNAKE"),
    ([acct("a", 1, login="mff")], {"mff": {}}, "UPPER_SNAKE"),
    ([acct("a", 1, login="mff")], {"mff": {"secrets_prefix": "COPY_M", "extra": 1}}, "unknown key"),
    ([acct("a", 1, login="mff")], {"bad name": {"secrets_prefix": "COPY_M"}}, "letters, digits"),
    ([acct("a", 1, login="mff")], ["mff"], "mapping"),
    ([acct("a", 1, login="mff"), acct("b", 2, login="mff", env="live")], LOGINS, "share one env"),
])
def test_login_validation_errors(tmp_path, accounts, logins, msg):
    top = {"logins": logins} if logins is not None else {}
    with pytest.raises(cfg.ConfigError, match=msg):
        load(tmp_path, accounts, **top)


def test_duplicate_login_name_in_yaml_rejected(tmp_path):
    p = tmp_path / "accounts.yaml"
    p.write_text("logins:\n  mff:\n    secrets_prefix: COPY_A\n  mff:\n    secrets_prefix: COPY_B\naccounts: []\n")
    with pytest.raises(cfg.ConfigError, match="duplicate key"):
        cfg.load(p)


# ---------------------------------------------------------------- one keepalive per login

def build_env(tmp_path, accounts, logins):
    c = load(tmp_path, accounts, logins=logins)
    dotenv = tmp_path / ".env"
    env_file(dotenv, COPY_MFF1_ACCESS_TOKEN="x")
    return c, dotenv, tmp_path / "copy"


def test_one_keepalive_per_login_shared_by_two_accounts(tmp_path):
    accts = [acct("mff-1", A_ID, login="mff", mode="demo"), acct("mff-2", B_ID, login="mff", mode="demo"),
             acct("solo", 5, secrets_prefix="COPY_SOLO", mode="demo")]
    c, dotenv, d = build_env(tmp_path, accts, {"mff": {"secrets_prefix": "COPY_MFF1"}})
    kas, got = {}, []
    for a in c.accounts:
        broker, ka = cm.build_account(a, Ledger(tmp_path / "copy.db"), d, http_factory=lambda pool: None,
                                      dotenv=dotenv, keepalives=kas, cfg=c)
        got.append((a.name, ka, broker))
    assert got[0][1] is got[1][1] and got[0][1] is not got[2][1] and set(kas) == {"mff", "solo"}
    assert got[0][1].store == d / "logins" / "mff" / "tradovate_token.json"
    assert got[2][1].store == d / "solo" / "tradovate_token.json"      # implicit: where it always lived
    assert got[0][1].name == "mff" and got[0][1].prefix == "COPY_MFF1"
    # both brokers read the one keepalive's token
    assert got[0][2].token_fn == got[1][2].token_fn == got[0][1].access_token


def test_daemon_runs_the_renew_loop_once_per_login(tmp_path):
    from trade_jev.copytrade.daemon import Daemon
    accts = [acct("mff-1", A_ID, login="mff", mode="demo"), acct("mff-2", B_ID, login="mff", mode="demo")]
    c, dotenv, d = build_env(tmp_path, accts, {"mff": {"secrets_prefix": "COPY_MFF1"}})
    kas = {}
    built = {a.name: cm.build_account(a, Ledger(tmp_path / "copy.db"), d, http_factory=lambda p: None, dotenv=dotenv,
                                      keepalives=kas, cfg=c) for a in c.accounts}
    runs = []

    async def fake_run(self, max_ticks=None):
        runs.append(self.name)
        await asyncio.sleep(3600)
    LoginKeepalive.run, orig = fake_run, LoginKeepalive.run

    async def go():
        dm = Daemon(c, Ledger(tmp_path / "copy.db"), lambda a, ka: built[a.name][0], lambda a: built[a.name][1],
                    kill_db=lambda: {"paused": False}, kill_api=lambda: {"paused": False, "autonomy": "on"})
        await dm.start(None)
        await asyncio.sleep(0.05)
        await dm.stop()
    try:
        asyncio.run(go())
    finally:
        LoginKeepalive.run = orig
    assert runs == ["mff"]


def test_preflight_uses_the_login_store(tmp_path):
    from tests.copytrade_fakes import FakeTradovate
    accts = [acct("mff-1", 12, login="mff"), acct("mff-2", 11, login="mff")]
    c, dotenv, d = build_env(tmp_path, accts, {"mff": {"secrets_prefix": "COPY_MFF1"}})
    tt.write_store(d / "logins" / "mff" / "tradovate_token.json", "tok", "md", time.time() + 3600)
    fake = FakeTradovate()
    rc = cm.main(["preflight", "mff-2"], cfg=c, db=tmp_path / "copy.db", directory=d, dotenv=dotenv,
                 http_factory=lambda pool: fake)
    assert rc == 0 and not (d / "mff-2").exists()      # no per-account store appears


# ---------------------------------------------------------------- legacy seeding

def make_login(tmp_path, legacy, clock=None, store=None, **dot):
    clock = clock or Clk()
    dotenv = tmp_path / ".env"
    env_file(dotenv, **dot)
    store = store or tmp_path / "copy" / "logins" / "mff" / "tradovate_token.json"
    return LoginKeepalive("mff", "COPY_MFF1", "demo", store, dotenv, RenewBudget(tmp_path / "copy" / "r.json",
                          clock=clock), http=AuthHttp(), clock=clock, legacy_stores=legacy)


def test_new_login_store_seeds_from_the_freshest_valid_legacy_store(tmp_path):
    old, fresh, dead = (tmp_path / "copy" / n / "tradovate_token.json" for n in ("a", "b", "c"))
    tt.write_store(old, "tok-old", "md-old", NOW + 1000)
    tt.write_store(fresh, "tok-fresh", "md-fresh", NOW + 3000)
    tt.write_store(dead, "tok-dead", "md-dead", NOW - 10)
    ka = make_login(tmp_path, [old, fresh, dead, tmp_path / "copy" / "missing" / "tradovate_token.json"],
                    COPY_MFF1_ACCESS_TOKEN="stale-env")
    st = tt.read_store(ka.store)
    assert (st.access, st.md) == ("tok-fresh", "md-fresh") and ka.access_token() == "tok-fresh"
    assert (tmp_path / "copy" / "a" / "tradovate_token.json").exists()      # legacy stores are left alone


def test_seeding_never_overwrites_an_existing_login_store(tmp_path):
    store = tmp_path / "copy" / "logins" / "mff" / "tradovate_token.json"
    tt.write_store(store, "tok-login", "md", NOW + 500)
    legacy = tmp_path / "copy" / "a" / "tradovate_token.json"
    tt.write_store(legacy, "tok-legacy", "md", NOW + 3000)
    assert make_login(tmp_path, [legacy]).access_token() == "tok-login"


def test_no_valid_legacy_store_falls_back_to_env(tmp_path):
    dead = tmp_path / "copy" / "a" / "tradovate_token.json"
    tt.write_store(dead, "tok-dead", "md", NOW - 10)
    ka = make_login(tmp_path, [dead], COPY_MFF1_ACCESS_TOKEN="env-tok")
    assert not ka.store.exists() and ka._seed().access == "env-tok"


def test_build_account_seeds_explicit_login_from_an_accounts_legacy_store(tmp_path):
    accts = [acct("mff-1", A_ID, login="mff"), acct("mff-2", B_ID, login="mff")]
    c, dotenv, d = build_env(tmp_path, accts, {"mff": {"secrets_prefix": "COPY_MFF1"}})
    tt.write_store(d / "mff-1" / "tradovate_token.json", "live-tok", "md", time.time() + 3000)
    _, ka = cm.build_account(c.accounts[1], Ledger(tmp_path / "copy.db"), d, http_factory=lambda p: None,
                             dotenv=dotenv, cfg=c)
    assert ka.access_token() == "live-tok" and ka.store == d / "logins" / "mff" / "tradovate_token.json"


def test_implicit_login_does_not_migrate(tmp_path):
    c = load(tmp_path, [acct("a1", 11, secrets_prefix="COPY_A1")])
    d = tmp_path / "copy"
    tt.write_store(d / "a1" / "tradovate_token.json", "tok", "md", time.time() + 3000)
    _, ka = cm.build_account(c.accounts[0], Ledger(tmp_path / "copy.db"), d, http_factory=lambda p: None,
                             dotenv=tmp_path / ".env", cfg=c)
    assert ka.store == d / "a1" / "tradovate_token.json" and ka.access_token() == "tok"


# ---------------------------------------------------------------- login failure isolation

def test_one_logins_token_failure_leaves_the_other_alone(tmp_path):
    clock = Clk()
    http_bad, http_ok = AuthHttp(), AuthHttp()
    http_bad.renew_ok = False
    d = tmp_path / "copy"
    budget = RenewBudget(d / "r.json", clock=clock)
    env_file(tmp_path / ".env")
    kas = {}
    for name, h in (("bad", http_bad), ("good", http_ok)):
        store = d / "logins" / name / "tradovate_token.json"
        tt.write_store(store, f"tok-{name}", "md", NOW + 600)
        kas[name] = LoginKeepalive(name, f"COPY_{name.upper()}", "demo", store, tmp_path / ".env", budget,
                                   http=h, clock=clock)
    res = {n: asyncio.run(k.tick(force=True)) for n, k in kas.items()}
    assert res == {"bad": "failed", "good": "renewed"}
    assert kas["good"].health()["ok"] and kas["good"].access_token() == "renewed-access"
    assert kas["bad"].access_token() == "tok-bad"
    # a dead token marks only that login
    clock.t = NOW + 700
    assert not kas["bad"].health()["ok"] and kas["bad"].access_token() is None
    assert kas["good"].health()["ok"]


def test_shared_login_failure_gates_both_accounts_and_not_another_login(tmp_path):
    from trade_jev.copytrade.executor import CopyExecutor
    accts = [acct("mff-1", 1, login="mff"), acct("mff-2", 2, login="mff"), acct("o", 3, secrets_prefix="COPY_O")]
    c = load(tmp_path, accts, logins={"mff": {"secrets_prefix": "COPY_M"}})
    ex = {a.name: CopyExecutor(a, TradovateBroker(a, lambda: None, http=None), Ledger(tmp_path / f"{a.name}.db"))
          for a in c.accounts}
    dead = {"mff": False, "o": True}
    for a in c.accounts:
        ex[a.name].token_ok = dead[a.login]
    assert ex["mff-1"].gate_reason() and ex["mff-2"].gate_reason()
    assert ex["mff-1"].status_row()["login"] == "mff" and ex["o"].status_row()["login"] == "o"


# ---------------------------------------------------------------- broker: two accounts on one login

class Shared:
    """One login's fake REST: both accounts' positions and orders come back from the same endpoints."""

    def __init__(self):
        self.calls = []
        self.pos = [{"id": 1, "accountId": A_ID, "contractId": 7, "netPos": 5, "netPrice": 100.0},
                    {"id": 2, "accountId": B_ID, "contractId": 7, "netPos": -3, "netPrice": 101.0}]
        self.orders = [
            {"id": 10, "accountId": A_ID, "ordStatus": "Working", "clOrdId": "cl-a", "action": "Sell"},
            {"id": 11, "accountId": B_ID, "ordStatus": "Working", "clOrdId": "cl-b", "action": "Buy"},
            {"id": 12, "accountId": A_ID, "ordStatus": "Filled", "clOrdId": "cl-a2", "action": "Buy"},
            {"id": 13, "accountId": B_ID, "ordStatus": "Working", "clOrdId": "cl-b2", "action": "Buy"}]

    async def __call__(self, method, url, headers, body):
        path = url.partition("/v1")[2]
        self.calls.append((method, path, body))
        key = path.split("?")[0]
        if key == "/position/list":
            return 200, {"d": [dict(r) for r in self.pos]}
        if key == "/order/list":
            return 200, {"d": [dict(o) for o in self.orders]}
        if key == "/account/list":
            return 200, {"d": [{"id": B_ID, "name": "SIM-B"}, {"id": A_ID, "name": "SIM-A"}]}
        if key == "/cashBalance/deps":
            return 200, {"d": [{"realizedPnL": 1.0}]}
        if key == "/order/cancelorder":
            for o in self.orders:
                if o["id"] == body["orderId"]:
                    o["ordStatus"] = "Canceled"
            return 200, {"failureReason": "Success"}
        if key == "/order/liquidateposition":
            for r in self.pos:
                if r["accountId"] == body["accountId"] and r["contractId"] == body["contractId"]:
                    r["netPos"] = 0
            return 200, {"orderId": 99}
        if key == "/order/item":
            return 200, {"ordStatus": "Filled"}
        if key == "/fill/deps":
            return 200, {"d": [{"id": 1, "price": 105.0, "qty": 5}]}
        return 404, {}

    def posts(self, path):
        return [b for m, p, b in self.calls if m == "POST" and p == path]


def broker_for(tmp_path, fake, name, aid):
    a = cfg.Account(name=name, enabled=True, mode="demo", env="demo", account_id=aid, root="MNQ", qty=5,
                    daily_loss_limit=1000.0, daily_profit_stop=490.0, commission=0.5, secrets_prefix="COPY_M",
                    login="mff", explicit_login=True)
    clock = Clock()
    b = TradovateBroker(a, lambda: "tok", lambda: ["tok"], http=fake, ledger=Ledger(tmp_path / f"{name}.db"),
                        sleep=clock.sleep, clock=clock)
    b.set_symbol("MNQZ6")
    return b


def run(coro):
    return asyncio.run(coro)


def test_positions_and_orders_are_each_accounts_own(tmp_path):
    fake = Shared()
    a, b = broker_for(tmp_path, fake, "a", A_ID), broker_for(tmp_path, fake, "b", B_ID)
    pa, pb = run(a.position(True)), run(b.position(True))
    assert (pa.side, pa.qty) == (1, 5) and (pb.side, pb.qty) == (-1, 3)
    assert [o["id"] for o in run(a.orders(True))] == [10] and [o["id"] for o in run(b.orders(True))] == [11, 13]


def test_spec_picks_the_accounts_own_name(tmp_path):
    fake = Shared()
    assert run(broker_for(tmp_path, fake, "a", A_ID)._spec()) == "SIM-A"
    assert run(broker_for(tmp_path, fake, "b", B_ID)._spec()) == "SIM-B"


def test_sweep_cancels_only_its_own_orders(tmp_path):
    fake = Shared()
    a = broker_for(tmp_path, fake, "a", A_ID)
    assert run(a.sweep()) is True
    assert sorted(p["orderId"] for p in fake.posts("/order/cancelorder")) == [10]
    assert [o["ordStatus"] for o in fake.orders if o["accountId"] == B_ID] == ["Working", "Working"]


def test_flatten_liquidates_only_its_own_position_and_sweeps_only_its_orders(tmp_path):
    fake = Shared()
    a = broker_for(tmp_path, fake, "a", A_ID)
    ack = run(a.flatten())
    assert ack.ok
    assert [p["accountId"] for p in fake.posts("/order/liquidateposition")] == [A_ID]
    assert sorted(p["orderId"] for p in fake.posts("/order/cancelorder")) == [10]
    assert [r["netPos"] for r in fake.pos if r["accountId"] == B_ID] == [-3]
    assert all(o["ordStatus"] == "Working" for o in fake.orders if o["accountId"] == B_ID)


def test_flat_account_flatten_never_touches_the_neighbour(tmp_path):
    fake = Shared()
    fake.pos[0]["netPos"] = 0                  # A is flat, B is not
    assert run(broker_for(tmp_path, fake, "a", A_ID).flatten()).ok
    assert fake.posts("/order/liquidateposition") == []
    assert fake.orders[1]["ordStatus"] == "Working" and fake.pos[1]["netPos"] == -3


def test_cash_exit_info_and_fills_target_the_accounts_own_ids(tmp_path):
    fake = Shared()
    b = broker_for(tmp_path, fake, "b", B_ID)
    run(b.cash_realized("2026-11-05"))
    assert ("GET", f"/cashBalance/deps?masterid={B_ID}", None) in fake.calls
    px, why = run(b.exit_info({"tp_id": 11, "side": 1, "tp_px": 1.0}))     # its own order id, filtered upstream
    assert (px, why) == (105.0, "target")
    assert ("GET", "/order/item?id=11", None) in fake.calls and ("GET", "/fill/deps?masterid=11", None) in fake.calls


def test_recover_ignores_the_neighbours_clordid(tmp_path):
    fake = Shared()
    a = broker_for(tmp_path, fake, "a", A_ID)
    rows = run(a._raw_orders(True))
    assert not any(o["clOrdId"] == "cl-b" for o in rows)       # B's order can never be adopted as A's entry


def test_direct_flatten_checks_a_shared_login_once(tmp_path, monkeypatch):
    from tests.copytrade_fakes import FakeTradovate
    accts = [acct("mff-1", 11, login="mff", mode="demo"), acct("mff-2", 12, login="mff", mode="demo")]
    c, dotenv, d = build_env(tmp_path, accts, {"mff": {"secrets_prefix": "COPY_MFF1"}})
    tt.write_store(d / "logins" / "mff" / "tradovate_token.json", "tok", "md", time.time() + 3600)
    ticks = []

    async def tick(self, force=False):
        ticks.append(self.name)
        return "ok"
    monkeypatch.setattr(LoginKeepalive, "tick", tick)
    rc = cm.cmd_flatten_all(c, Ledger(tmp_path / "copy.db"), d, True, isatty=True, ask=lambda p: "FLATTEN",
                            http_factory=lambda pool: FakeTradovate(), dotenv=dotenv)
    assert ticks == ["mff"] and rc in (0, 1)


def test_ledger_adds_the_login_column_to_an_old_db(tmp_path):
    import sqlite3
    p = tmp_path / "old.db"
    con = sqlite3.connect(p)
    con.execute("CREATE TABLE copy_status(name TEXT PRIMARY KEY, day TEXT, updated_at TEXT, mode TEXT, env TEXT, "
                "root TEXT, qty INTEGER, contract TEXT, armed INTEGER, gate_reason TEXT, pos_side INTEGER, "
                "pos_qty INTEGER, avg_px REAL, realized REAL, open_pnl REAL, halted INTEGER, halt_kind TEXT, "
                "token_ok INTEGER, token_expires_at TEXT, entries INTEGER, errors INTEGER, last_error TEXT)")
    con.commit()
    con.close()
    led = Ledger(p)
    led.status({"name": "a", "login": "mff"})
    assert led.statuses()[0]["login"] == "mff"
