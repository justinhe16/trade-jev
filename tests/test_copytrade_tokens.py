import asyncio
import json
import os

from trade_jev import tradovate_token as tt
from trade_jev.copytrade.tokens import FAIL_BACKOFF_S, TICK_S, AccountKeepalive, RenewBudget

NOW = 1_800_000_000.0


def iso(ts):
    return tt.iso_utc(ts)


class AuthHttp:
    """A fake auth server: renewaccesstoken and accesstokenrequest."""

    def __init__(self):
        self.calls = []
        self.renew_ok = True
        self.login_ok = False

    async def __call__(self, method, url, headers, body):
        self.calls.append((method, url, dict(headers), body))
        if url.endswith("/auth/renewaccesstoken"):
            if not self.renew_ok:
                return 401, {"errorText": "Access is denied"}
            return 200, {"accessToken": "renewed-access", "mdAccessToken": "renewed-md",
                         "expirationTime": iso(NOW + 4800)}
        if url.endswith("/auth/accesstokenrequest"):
            if not self.login_ok:
                return 200, {"errorText": "Incorrect username or password"}
            return 200, {"accessToken": "login-access", "mdAccessToken": "login-md",
                         "expirationTime": iso(NOW + 4800), "userId": 1}
        return 404, {}

    def n(self, suffix):
        return len([c for c in self.calls if c[1].endswith(suffix)])


class Clk:
    def __init__(self, t=NOW):
        self.t = t

    def __call__(self):
        return self.t


def env_file(path, **kw):
    path.write_text("".join(f"{k}={v}\n" for k, v in kw.items()))
    os.utime(path, (NOW, NOW))


def make(tmp_path, name="a1", prefix="COPY_A1", env="demo", http=None, clock=None, budget=None, **dot):
    dotenv = tmp_path / ".env"
    if dot:
        env_file(dotenv, **dot)
    clock = clock or Clk()
    return AccountKeepalive(name, prefix, env, tmp_path / "copy" / name / "tradovate_token.json", dotenv,
                            budget or RenewBudget(tmp_path / "copy" / "renew.json", clock=clock),
                            http=http or AuthHttp(), clock=clock)


def tick(ka, **kw):
    return asyncio.run(ka.tick(**kw))


def test_prefix_isolation_reads_only_its_own_variables(tmp_path):
    env_file(tmp_path / ".env", COPY_A1_ACCESS_TOKEN="a1-access", COPY_A1_MD_ACCESS_TOKEN="a1-md",
             COPY_A1_AUTH_HOST="a1.example.com", COPY_B_ACCESS_TOKEN="b-access", TRADOVATE_ACCESS_TOKEN="main-access",
             TRADOVATE_AUTH_HOST="main.example.com")
    a = make(tmp_path)
    b = make(tmp_path, name="b", prefix="COPY_B")
    assert (a._seed().access, a._seed().md, a._host()) == ("a1-access", "a1-md", "a1.example.com")
    assert (b._seed().access, b._host()) == ("b-access", tt.DEFAULT_AUTH_HOST)
    none = make(tmp_path, name="c", prefix="COPY_C")
    assert none._seed() is None and "main-access" not in str(none.secrets())
    assert "a1-access" in a.secrets() and "b-access" not in a.secrets() and "main-access" not in a.secrets()


def test_a_token_is_renewed_into_its_own_store_and_the_main_store_is_never_touched(
        tmp_path, isolated_tradovate_token_files):
    http = AuthHttp()
    ka = make(tmp_path, http=http, COPY_A1_ACCESS_TOKEN="seed-access", COPY_A1_MD_ACCESS_TOKEN="seed-md")
    assert tick(ka) == "adopted-env"
    assert http.calls[0][2] == {"Authorization": "Bearer seed-access"}
    assert http.calls[0][1] == f"https://{tt.DEFAULT_AUTH_HOST}/v1/auth/renewaccesstoken"
    store = tmp_path / "copy" / "a1" / "tradovate_token.json"
    assert json.loads(store.read_text())["accessToken"] == "renewed-access"
    assert (tmp_path / "copy" / "a1" / "tradovate_token_status.json").exists()
    assert (tmp_path / "copy" / "a1" / ".tradovate_token.lock").exists()
    assert ka.access_token() == "renewed-access" and ka.health()["ok"]
    # the main paper store, status, lock and the real data/ directory: nothing created or changed
    assert list(isolated_tradovate_token_files.iterdir()) == []
    assert not (tt.ROOT / "data" / "tradovate_token.json").exists()
    assert ka.store != tt.token_store_path() and ka.store.parent == tmp_path / "copy" / "a1"


def test_health_never_contains_a_token_and_reports_expiry(tmp_path):
    ka = make(tmp_path, COPY_A1_ACCESS_TOKEN="seed-access")
    assert ka.health()["ok"] is False and ka.access_token() is None
    tick(ka)
    h = ka.health()
    assert h["ok"] and h["expires_at"] and "renewed" not in json.dumps(h)


def test_renew_budget_survives_restarts_and_rolls_hourly(tmp_path):
    clk = Clk()
    path = tmp_path / "budget.json"
    b = RenewBudget(path, clock=clk)
    assert all(b.take() for _ in range(6)) and not b.take()
    assert not RenewBudget(path, clock=clk).take()          # a restart does not reset it
    assert RenewBudget(path, clock=clk).used() == 6
    clk.t += 3601
    assert RenewBudget(path, clock=clk).take()


def test_renewal_stops_when_the_budget_is_used_up_without_any_http(tmp_path):
    clk, http = Clk(), AuthHttp()
    budget = RenewBudget(tmp_path / "budget.json", clock=clk)
    for _ in range(6):
        budget.take()
    ka = make(tmp_path, http=http, clock=clk, budget=budget, COPY_A1_ACCESS_TOKEN="seed-access")
    assert tick(ka) == "failed" and http.calls == []
    assert "budget" in tt.read_status(tt.status_path(ka.store))["last_error"]


def test_failing_account_backs_off_fifteen_minutes_and_a_healthy_one_ticks_every_minute(tmp_path):
    sleeps = []

    async def sleep(s):
        sleeps.append(s)
    http = AuthHttp()
    http.renew_ok = False
    ka = make(tmp_path, http=http, COPY_A1_ACCESS_TOKEN="seed-access")
    ka.sleep = sleep
    asyncio.run(ka.run(max_ticks=3))
    assert sleeps == [FAIL_BACKOFF_S, FAIL_BACKOFF_S] and FAIL_BACKOFF_S == 900.0
    ok = make(tmp_path, name="ok", prefix="COPY_OK", COPY_OK_ACCESS_TOKEN="seed-access")
    ok.sleep = sleep
    sleeps.clear()
    asyncio.run(ok.run(max_ticks=3))
    assert sleeps == [TICK_S, TICK_S] and TICK_S == 60.0


PW = dict(COPY_A1_USERNAME="u", COPY_A1_PASSWORD="pw-SECRET", COPY_A1_CID="7", COPY_A1_SEC="sec-SECRET",
          COPY_A1_DEVICE_ID="dev")


def test_password_path_makes_exactly_one_attempt_per_env_mtime(tmp_path):
    http = AuthHttp()
    ka = make(tmp_path, http=http, **PW)
    assert tick(ka) == "failed" and http.n("/auth/accesstokenrequest") == 1
    tick(ka)
    tick(ka)
    assert http.n("/auth/accesstokenrequest") == 1
    ka2 = make(tmp_path, http=http, **PW)                        # a daemon restart: still the same .env mtime
    os.utime(tmp_path / ".env", (NOW, NOW))
    tick(ka2)
    assert http.n("/auth/accesstokenrequest") == 1
    os.utime(tmp_path / ".env", (NOW + 100, NOW + 100))          # the owner edited .env
    http.login_ok = True
    assert tick(ka2) == "renewed" and http.n("/auth/accesstokenrequest") == 2
    assert ka2.access_token() == "login-access"
    body = [c for c in http.calls if c[1].endswith("accesstokenrequest")][0][3]
    assert body["name"] == "u" and body["cid"] == 7
    status = tt.read_status(tt.status_path(ka.store))
    assert "pw-SECRET" not in json.dumps(status) and "sec-SECRET" not in json.dumps(status)


def test_password_path_penalty_is_a_failure_not_a_retry(tmp_path):
    class Penalty(AuthHttp):
        async def __call__(self, method, url, headers, body):
            self.calls.append((method, url, headers, body))
            return 200, {"p-ticket": "t", "p-time": 3}
    http = Penalty()
    ka = make(tmp_path, http=http, **PW)
    assert tick(ka) == "failed" and http.n("/auth/accesstokenrequest") == 1


def test_password_path_needs_all_five_variables(tmp_path):
    http = AuthHttp()
    partial = dict(PW)
    del partial["COPY_A1_SEC"]
    ka = make(tmp_path, http=http, **partial)
    assert tick(ka) in ("no-tokens", "failed") and http.n("/auth/accesstokenrequest") == 0


def test_a_dead_store_without_fresh_tokens_makes_no_http_and_records_it(tmp_path):
    http = AuthHttp()
    ka = make(tmp_path, http=http)
    tt.write_store(ka.store, "old", "old-md", NOW - 10)
    assert tick(ka) == "expired" and http.calls == [] and ka.health() == {
        "ok": False, "expires_at": iso(NOW - 10), "error": "token expired"}
