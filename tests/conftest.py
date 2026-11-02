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
