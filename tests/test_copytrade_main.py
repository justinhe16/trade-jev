"""The copytrade command line: refusals, the owner-only commands, and that nothing real is ever reached."""

import socket
import time

import pytest

from tests.copytrade_fakes import ACCT_ID, FakeTradovate
from trade_jev.copytrade import __main__ as cm
from trade_jev.copytrade import config as cfg
from trade_jev.copytrade.daemon import take_lock
from trade_jev.copytrade.ledger import Ledger, et_day


def account(**kw):
    base = dict(name="a1", enabled=True, mode="demo", env="demo", account_id=ACCT_ID, root="MNQ", qty=5,
                daily_loss_limit=1000.0, daily_profit_stop=490.0, commission=0.5, secrets_prefix="COPY_A1")
    base.update(kw)
    return cfg.Account(**base)


@pytest.fixture
def env(tmp_path):
    dotenv = tmp_path / ".env"
    dotenv.write_text("COPY_A1_ACCESS_TOKEN=seed\n")
    fake = FakeTradovate()
    store = tmp_path / "copy" / "a1" / "tradovate_token.json"
    from trade_jev import tradovate_token as tt
    tt.write_store(store, "tok-live", "md", time.time() + 3600)
    return dict(db=tmp_path / "copy.db", directory=tmp_path / "copy", dotenv=dotenv, fake=fake,
                http_factory=lambda pool: fake)


def run(env, argv, accounts=None, **kw):
    conf = cfg.CopyConfig(tuple(accounts if accounts is not None else [account()]))
    inject = {"cfg": conf, "db": env["db"], "directory": env["directory"], "dotenv": env["dotenv"],
              "http_factory": env["http_factory"], **kw}
    return cm.main(argv, **inject)


def never(*a, **k):
    raise AssertionError("must not ask")


@pytest.mark.parametrize("acct_kw,kw,msg", [
    ({}, {"isatty": False}, "needs a terminal"),
    ({"env": "live", "mode": "live"}, {"isatty": True}, "only runs against env demo"),
    ({"env": "live", "mode": "dry_run"}, {"isatty": True}, "only runs against env demo"),
    ({"mode": "dry_run", "account_id": 0}, {"isatty": True}, "set account_id first"),
])
def test_rehearse_refusals_never_ask_or_touch_the_network(env, capsys, acct_kw, kw, msg):
    rc = run(env, ["rehearse", "a1"], [account(**acct_kw)], ask=never, **kw)
    assert rc == 2 and msg in capsys.readouterr().out
    assert env["fake"].calls == []


def test_rehearse_needs_the_account_name_typed(env, capsys):
    rc = run(env, ["rehearse", "a1"], isatty=True, ask=lambda prompt: "yes")
    assert rc == 2 and "not confirmed" in capsys.readouterr().out and env["fake"].calls == []


def test_rehearse_unknown_account_is_a_config_error(env, capsys):
    assert run(env, ["rehearse", "nope"], isatty=True, ask=never) == 2
    assert "no account named" in capsys.readouterr().err


def test_without_a_terminal_in_this_test_run_rehearse_refuses_by_default(env, capsys, monkeypatch):
    monkeypatch.setattr("sys.stdin", open("/dev/null"))
    assert run(env, ["rehearse", "a1"], ask=never) == 2


def test_flatten_all_through_the_daemon_when_it_is_down_fails(env, capsys):
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        port = s.getsockname()[1]
    conf = cfg.CopyConfig((account(),), port=port)
    rc = cm.main(["flatten-all"], cfg=conf, db=env["db"], directory=env["directory"])
    assert rc == 1 and "--direct" in capsys.readouterr().out


def test_direct_flatten_refuses_while_the_daemon_runs_or_without_a_terminal_or_confirmation(env, capsys):
    lock = take_lock(env["directory"])
    try:
        assert run(env, ["flatten-all", "--direct"], isatty=True, ask=never) == 1
        assert "daemon is running" in capsys.readouterr().out
    finally:
        lock.close()
    assert run(env, ["flatten-all", "--direct"], isatty=False, ask=never) == 1
    assert "needs a terminal" in capsys.readouterr().out
    assert run(env, ["flatten-all", "--direct"], isatty=True, ask=lambda p: "no") == 1
    assert env["fake"].calls == []


def test_direct_flatten_flattens_every_live_account_after_confirmation(env, capsys):
    env["fake"]._make_position(1, 5, 24500.0)
    rc = run(env, ["flatten-all", "--direct"], accounts=[account(), account(name="d", mode="dry_run", account_id=0,
                                                                              secrets_prefix="COPY_D")],
             isatty=True, ask=lambda p: "FLATTEN")
    out = capsys.readouterr().out
    assert rc == 0 and "a1: flat" in out and "d:" not in out          # the dry-run account has nothing to flatten
    assert env["fake"].pos[99]["netPos"] == 0


def test_clear_halts_clears_todays_halt(env, capsys):
    led = Ledger(env["db"])
    day = et_day(time.time_ns())
    led.halt("a1", day, "day", "x")
    led.halt("a1", "2020-01-01", "day", "old")
    assert run(env, ["clear-halts", "a1"]) == 0
    assert led.active_halt("a1", day) is None and led.active_halt("a1", "2020-01-01") is not None
    assert run(env, ["clear-halts", "ghost"]) == 2


def test_preflight_is_read_only(env, capsys):
    rc = run(env, ["preflight", "a1", "--symbol", "MNQZ6"])
    out = capsys.readouterr().out
    assert rc == 0 and "PASS account 11 found: DEMO11" in out and "PASS contract MNQZ6 found" in out
    assert {c[0] for c in env["fake"].calls} == {"GET"}
    assert not [c for c in env["fake"].calls if "/order/" in c[1]]
    env["fake"].read_fail.add("/contract/find")
    assert run(env, ["preflight", "a1"]) == 1


def test_status_prints_rows(env, capsys):
    assert run(env, ["status"]) == 0 and "no copy status rows" in capsys.readouterr().out
    Ledger(env["db"]).status({"name": "a1", "mode": "demo", "env": "demo", "root": "MNQ", "qty": 5, "pos_side": 0,
                              "pos_qty": 0, "realized": 1.0, "open_pnl": 0.0, "entries": 1, "halted": 0,
                              "token_ok": 1, "updated_at": "x"})
    assert run(env, ["status"]) == 0 and "a1: demo/demo MNQ x5" in capsys.readouterr().out


def test_bad_accounts_file_is_exit_2(env, capsys, tmp_path, monkeypatch):
    p = tmp_path / "bad.yaml"
    p.write_text("accounts:\n  - name: x\n")
    monkeypatch.setenv("TRADE_JEV_ACCOUNTS", str(p))
    assert cm.main(["status"], db=env["db"], directory=env["directory"]) == 2
    assert "accounts.yaml" in capsys.readouterr().err


def test_run_refuses_to_start_a_second_daemon(env, capsys):
    lock = take_lock(env["directory"])
    try:
        assert run(env, ["run"]) == 1
        assert "holds the lock" in capsys.readouterr().err
    finally:
        lock.close()


def test_dry_run_accounts_build_a_null_broker_and_the_real_transport_is_unreachable(env, tmp_path):
    from trade_jev.copytrade.broker import NullBroker
    led = Ledger(env["db"])
    broker, ka = cm.build_account(account(mode="dry_run"), led, env["directory"], dotenv=env["dotenv"])
    assert broker.dry and isinstance(broker.inner, NullBroker)
    live_broker, _ = cm.build_account(account(), led, env["directory"], dotenv=env["dotenv"])
    import asyncio
    with pytest.raises(AssertionError, match="real transport"):       # the autouse guard: no test can go out
        asyncio.run(live_broker.http("GET", "https://x", {}, None))
