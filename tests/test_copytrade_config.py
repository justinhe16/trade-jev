import pytest
import yaml

from trade_jev.copytrade import config as cfg

GOOD = {"name": "a1", "enabled": True, "env": "demo", "account_id": 11, "root": "MNQ", "qty": 5,
        "daily_loss_limit": 1000, "daily_profit_stop": 490, "commission": 0.5, "secrets_prefix": "COPY_A1"}


def write(tmp_path, accounts, **top):
    p = tmp_path / "accounts.yaml"
    p.write_text(yaml.safe_dump({"accounts": accounts, **top}))
    return p


def acct(**kw):
    return {**GOOD, **kw}


def test_absent_file_means_no_accounts(tmp_path):
    c = cfg.load(tmp_path / "nope.yaml")
    assert c.accounts == () and c.enabled() == [] and c.port == 8768
    assert cfg.load().accounts == ()   # the autouse fixture points the default at a missing temp file


def test_default_mode_is_dry_run_and_defaults(tmp_path):
    (a,) = cfg.load(write(tmp_path, [GOOD])).accounts
    assert a.mode == "dry_run" and a.slip_ticks == 4 and a.tp_points is None and a.point_value == 2.0
    assert cfg.load(write(tmp_path, [acct(root="NQ")])).accounts[0].point_value == 20.0


@pytest.mark.parametrize("kw", [
    {"mode": "live", "env": "demo"}, {"mode": "demo", "env": "live"}, {"env": "paper"}, {"mode": "go"},
    {"root": "ES"}, {"qty": 0}, {"qty": 1.5}, {"secrets_prefix": "TRADOVATE"}, {"secrets_prefix": "TRADOVATE_X"},
    {"secrets_prefix": "lower"}, {"daily_loss_limit": -1}, {"bogus": 1}, {"name": "bad name"},
    {"mode": "demo", "env": "demo", "account_id": 0}, {"slip_ticks": -1}, {"enabled": "yes"},
])
def test_rejects_bad_accounts(tmp_path, kw):
    with pytest.raises(cfg.ConfigError):
        cfg.load(write(tmp_path, [acct(**kw)]))


def test_mode_env_pairs_accepted(tmp_path):
    ok = [acct(mode="live", env="live"), acct(name="a2", mode="demo", env="demo", account_id=12,
                                              secrets_prefix="COPY_A2"),
          acct(name="a3", mode="dry_run", env="live", account_id=13, secrets_prefix="COPY_A3")]
    assert [a.mode for a in cfg.load(write(tmp_path, ok)).accounts] == ["live", "demo", "dry_run"]


def test_duplicates_rejected(tmp_path):
    for kw in ({"name": "a1"}, {"secrets_prefix": "COPY_A1"}, {"account_id": 11}):
        other = acct(name="a2", account_id=12, secrets_prefix="COPY_A2")
        other.update(kw)
        with pytest.raises(cfg.ConfigError, match="duplicate"):
            cfg.load(write(tmp_path, [GOOD, other]))


def test_top_level_validation(tmp_path):
    for top in ({"port": 0}, {"port": "x"}, {"kill_status_url": "https://example.com"}, {"nope": 1}):
        with pytest.raises(cfg.ConfigError):
            cfg.load(write(tmp_path, [GOOD], **top))
    p = tmp_path / "bad.yaml"
    p.write_text("accounts: [")
    with pytest.raises(cfg.ConfigError):
        cfg.load(p)


def test_example_file_loads():
    c = cfg.load(cfg.ROOT / "accounts.example.yaml")
    (a,) = c.accounts
    assert (a.qty, a.root, a.daily_loss_limit, a.daily_profit_stop, a.mode) == (5, "MNQ", 1000.0, 490.0, "dry_run")
    assert c.port == 8768 and c.kill_status_url.endswith("/api/status")


def test_symbol_for():
    assert cfg.symbol_for("MNQ", "NQZ6") == "MNQZ6"
    assert cfg.symbol_for("NQ", "NQH7") == "NQH7"
    for bad in ("MNQZ6", "NQ", "ESZ6", "NQZ26", "NQX6", "", None, "NQZ6 "):
        with pytest.raises(cfg.ConfigError):
            cfg.symbol_for("MNQ", bad)
    with pytest.raises(cfg.ConfigError):
        cfg.symbol_for("ES", "NQZ6")
