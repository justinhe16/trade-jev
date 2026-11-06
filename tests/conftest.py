"""Every test runs with the Tradovate token store and .env pointed at temp paths, so no test can read or
write the real data/tradovate_token*.json or the real .env, and no token / host variable leaks in from the
machine running the tests."""

import pytest


@pytest.fixture(autouse=True)
def isolated_tradovate_token_files(monkeypatch, tmp_path_factory):
    d = tmp_path_factory.mktemp("tvtoken")
    monkeypatch.setenv("TRADOVATE_TOKEN_STORE", str(d / "tradovate_token.json"))
    monkeypatch.setenv("TRADE_JEV_DOTENV", str(d / "tradovate.env"))   # a file that does not exist until a test writes it
    for name in ("TRADOVATE_ACCESS_TOKEN", "TRADOVATE_MD_ACCESS_TOKEN", "TRADOVATE_AUTH_HOST"):
        monkeypatch.delenv(name, raising=False)
    return d


@pytest.fixture(autouse=True)
def no_real_tradovate_or_copy_state(monkeypatch, tmp_path_factory):
    """Copy trading tests (and every other test) can never reach Tradovate or read real state: the real HTTP
    transports raise, and the accounts file, copy DB, copy dir and Resonant DB point at temp paths."""
    from trade_jev import tradovate, tradovate_keepalive
    from trade_jev.copytrade import __main__ as cm
    from trade_jev.copytrade import broker as cb

    async def boom(*a, **k):
        raise AssertionError("a test tried to reach Tradovate over the real transport")

    monkeypatch.setattr(tradovate, "default_http", boom)
    monkeypatch.setattr(tradovate_keepalive, "default_http", boom)
    monkeypatch.setattr(cb, "make_http", lambda *a, **k: boom)
    monkeypatch.setattr(cm, "make_http", lambda *a, **k: boom)
    d = tmp_path_factory.mktemp("copy")
    monkeypatch.setenv("TRADE_JEV_ACCOUNTS", str(d / "accounts.yaml"))
    monkeypatch.setenv("TRADE_JEV_COPY_DB", str(d / "copy.db"))
    monkeypatch.setenv("TRADE_JEV_COPY_DIR", str(d / "copy"))
    monkeypatch.setattr(cm, "RESONANT_DB", str(d / "resonant.db"))
