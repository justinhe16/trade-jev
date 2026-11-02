"""Token mode building blocks: mode selection, the token store (atomic, 0600, compatible with the temporary
keep-alive's file), token selection, the lock, and the keep-alive renewer. No network, no real files: the
autouse fixture in conftest.py points the store and .env at temp paths."""

import asyncio
import json
import os
import stat
import time

import pytest

from trade_jev import tradovate as tv
from trade_jev import tradovate_keepalive as ka
from trade_jev import tradovate_token as tt

SECRETS = ["ACC-OLD", "MD-OLD", "ACC-NEW", "MD-NEW", "ACC-ENV", "MD-ENV", "ACC-BAD"]


@pytest.fixture(autouse=True)
def no_network(monkeypatch):
    async def boom(*a, **k):
        raise AssertionError("a test reached the real HTTP layer")
    monkeypatch.setattr(ka, "default_http", boom)
    monkeypatch.setattr(tv, "default_http", boom)


@pytest.fixture
def store(isolated_tradovate_token_files):
    return isolated_tradovate_token_files / "tradovate_token.json"


@pytest.fixture
def dotenv(isolated_tradovate_token_files):
    return isolated_tradovate_token_files / "tradovate.env"      # == $TRADE_JEV_DOTENV (absent until written)


def put_env(path, access="ACC-ENV", md="MD-ENV", age_s=0.0, extra=""):
    path.write_text(f"TRADOVATE_ACCESS_TOKEN={access}\nTRADOVATE_MD_ACCESS_TOKEN={md}\n{extra}")
    t = time.time() - age_s
    os.utime(path, (t, t))


def put_store(path, access="ACC-OLD", md="MD-OLD", left_s=4800.0, age_s=0.0):
    path.unlink(missing_ok=True)                    # write_store refuses to shorten a store: start clean
    assert tt.write_store(path, access, md, time.time() + left_s)
    t = time.time() - age_s
    os.utime(path, (t, t))


class RenewHttp:
    """GET renewaccesstoken fake: answers from a queue, records the bearer token used."""

    def __init__(self, *answers):
        self.answers, self.bearers, self.urls = list(answers), [], []

    async def __call__(self, method, url, headers, body):
        assert method == "GET" and url.endswith("/auth/renewaccesstoken") and body is None
        self.bearers.append(headers["Authorization"].removeprefix("Bearer "))
        self.urls.append(url)
        a = self.answers.pop(0)
        if isinstance(a, Exception):
            raise a
        return a


def renewed(access="ACC-NEW", md="MD-NEW", left_s=4800.0):
    return 200, {"accessToken": access, "mdAccessToken": md, "expirationTime": tt.iso_utc(time.time() + left_s),
                 "userStatus": "Active"}


def keepalive(store, dotenv, http, host=None, clock=time.time, sleep=None):
    return ka.Keepalive(store, dotenv, host=host, http=http, clock=clock, sleep=sleep or (lambda s: asyncio.sleep(0)))


def tick(k, force=False):
    return asyncio.run(k.tick(force))


def status(store):
    return json.loads(tt.status_path(store).read_text())


def assert_no_secrets(*paths_or_text):
    for x in paths_or_text:
        text = x.read_text() if hasattr(x, "read_text") else str(x)
        assert not any(s in text for s in SECRETS), text


# ---------------------------------------------------------------- mode selection, paths

def test_auth_mode_selection(store):
    assert tt.auth_mode({}) == "password"
    assert tt.auth_mode({"TRADOVATE_ACCESS_TOKEN": "  "}) == "password"          # blank is not set
    assert tt.auth_mode({"TRADOVATE_ACCESS_TOKEN": "ACC-ENV"}) == "token"
    assert tt.auth_mode({"TRADOVATE_MD_ACCESS_TOKEN": "MD-ENV"}) == "password"    # the access token decides
    put_store(store)
    assert tt.auth_mode({}) == "token"                                            # a store file is enough
    assert tv.missing_credentials({}) == []                                       # token mode needs no password


def test_config_in_token_mode_and_auth_host(store):
    c = tv.TradovateConfig.from_env({"TRADOVATE_ACCESS_TOKEN": "ACC-ENV", "TRADOVATE_MD_ACCESS_TOKEN": "MD-ENV"})
    assert c.mode == "token" and c.base == "https://live.tradovateapi.com/v1"
    assert "ACC-ENV" not in repr(c) and "MD-ENV" not in repr(c)
    assert {"ACC-ENV", "MD-ENV"} <= set(c.secrets())
    h = tv.TradovateConfig.from_env({"TRADOVATE_ACCESS_TOKEN": "x", "TRADOVATE_AUTH_HOST": "demo.tradovateapi.com"})
    assert h.base == "https://demo.tradovateapi.com/v1"
    p = tv.TradovateConfig.from_env({"TRADOVATE_USERNAME": "u", "TRADOVATE_PASSWORD": "p", "TRADOVATE_CID": "1",
                                     "TRADOVATE_SEC": "s", "TRADOVATE_DEVICE_ID": "d"})
    assert p.mode == "password" and p.base == "https://live.tradovateapi.com/v1"
    d = tv.TradovateConfig.from_env({"TRADOVATE_USERNAME": "u", "TRADOVATE_PASSWORD": "p", "TRADOVATE_CID": "1",
                                     "TRADOVATE_SEC": "s", "TRADOVATE_DEVICE_ID": "d", "TRADOVATE_ENV": "demo"})
    assert d.base == "https://demo.tradovateapi.com/v1"


def test_paths_next_to_the_store(store):
    assert tt.token_store_path({}) == store
    assert tt.status_path(store).name == "tradovate_token_status.json" and tt.status_path(store).parent == store.parent
    assert tt.lock_path(store).name == ".tradovate_token.lock"
    assert tt.dotenv_path({}) == store.parent / "tradovate.env" and not tt.dotenv_path({}).exists()


def test_default_paths_are_gitignored_names():
    ignore = (tt.ROOT / ".gitignore").read_text().split()
    assert "data/tradovate_token*" in ignore and "data/.tradovate_token*" in ignore


# ---------------------------------------------------------------- expiry parsing

@pytest.mark.parametrize("s, want", [
    ("2026-11-02T20:00:00Z", 1793649600.0),
    ("2026-11-02T20:00:00+00:00", 1793649600.0),
    ("2026-11-02T20:00:00", 1793649600.0),                       # naive = UTC
    ("2026-11-02T20:00:00.250Z", 1793649600.25),
    ("2026-11-02T16:00:00-04:00", 1793649600.0),
    ("garbage", None), ("", None), (None, None), (12345, None),
])
def test_parse_expiration(s, want):
    assert tt.parse_expiration(s) == want


def test_iso_utc_roundtrip():
    t = 1793649600.25
    assert tt.iso_utc(t) == "2026-11-02T20:00:00.250Z" and tt.parse_expiration(tt.iso_utc(t)) == t


# ---------------------------------------------------------------- the store

def test_write_store_is_atomic_0600_and_exactly_the_documented_keys(store):
    assert tt.write_store(store, "ACC-OLD", "MD-OLD", time.time() + 4800)
    assert stat.S_IMODE(store.stat().st_mode) == 0o600
    d = json.loads(store.read_text())
    assert tuple(d) == tt.STORE_KEYS == ("accessToken", "mdAccessToken", "expirationTime")
    assert d["accessToken"] == "ACC-OLD" and tt.parse_expiration(d["expirationTime"]) > time.time()
    assert [p.name for p in store.parent.iterdir() if p.name.endswith(".tmp")] == []
    ts = tt.read_store(store)
    assert (ts.access, ts.md, ts.source) == ("ACC-OLD", "MD-OLD", "store") and not ts.dead(time.time())
    assert "ACC-OLD" not in repr(ts)


def test_write_store_failure_leaves_the_old_store_and_no_temp_file(store, monkeypatch):
    put_store(store)
    before = store.read_text()

    def boom(*a):
        raise OSError("disk full")
    monkeypatch.setattr(os, "replace", boom)
    with pytest.raises(OSError):
        tt.write_store(store, "ACC-NEW", "MD-NEW", time.time() + 9000)
    assert store.read_text() == before
    assert [p.name for p in store.parent.iterdir() if p.name.endswith(".tmp")] == []


def test_write_store_refuses_to_replace_newer_tokens_with_older(store):
    put_store(store, "ACC-NEW", "MD-NEW", left_s=4800)
    assert tt.write_store(store, "ACC-OLD", "MD-OLD", time.time() + 100) is False
    assert tt.read_store(store).access == "ACC-NEW"
    assert tt.write_store(store, "ACC-OLD", "MD-OLD", time.time() + 9000) is True


def test_the_temporary_keepalives_file_is_read_as_is(store):
    # the format the temporary keep-alive already writes: same keys, Tradovate's own timestamp form
    exp = tt.iso_utc(time.time() + 3000)
    store.write_text(json.dumps({"accessToken": "ACC-OLD", "mdAccessToken": "MD-OLD", "expirationTime": exp,
                                 "somethingElse": 1}))
    ts = tt.read_store(store)
    assert (ts.access, ts.md) == ("ACC-OLD", "MD-OLD") and ts.expires_at == tt.parse_expiration(exp)
    assert tt.auth_mode({}) == "token"


def test_store_reading_is_tolerant(store):
    assert tt.read_store(store) is None                                        # missing
    store.write_text("{not json")
    assert tt.read_store(store) is None
    store.write_text("[1]")
    assert tt.read_store(store) is None
    store.write_text(json.dumps({"accessToken": "  ", "mdAccessToken": "m", "expirationTime": "x"}))
    assert tt.read_store(store) is None                                        # no access token
    store.write_text(json.dumps({"accessToken": "ACC-OLD", "expirationTime": "garbage"}))
    ts = tt.read_store(store)
    assert ts.md == "" and ts.dead(time.time())                                # a bad expiry is dead
    store.write_text(json.dumps({"accessToken": "ACC-OLD"}))
    assert tt.read_store(store).dead(time.time())


# ---------------------------------------------------------------- the .env seed and selection

def test_seed_is_read_fresh_from_the_file_not_from_os_environ(dotenv, monkeypatch):
    monkeypatch.setenv("TRADOVATE_ACCESS_TOKEN", "STALE-FROM-IMPORT")
    assert tt.read_seed({"TRADOVATE_ACCESS_TOKEN": "STALE-FROM-IMPORT"}, dotenv).access == "STALE-FROM-IMPORT"  # no file
    put_env(dotenv, "ACC-ENV", "MD-ENV")
    s = tt.read_seed({"TRADOVATE_ACCESS_TOKEN": "STALE-FROM-IMPORT"}, dotenv)
    assert (s.access, s.md, s.source) == ("ACC-ENV", "MD-ENV", "env") and s.written_at == pytest.approx(dotenv.stat().st_mtime)
    dotenv.write_text("OTHER=1\n")
    assert tt.read_seed({}, dotenv) is None
    assert tt.read_dotenv_setting("TRADOVATE_AUTH_HOST", {}, dotenv) == ""
    dotenv.write_text("TRADOVATE_AUTH_HOST=demo.tradovateapi.com\n")
    assert tt.read_dotenv_setting("TRADOVATE_AUTH_HOST", {}, dotenv) == "demo.tradovateapi.com"


def ts(access, md, left_s, source="store", age_s=0.0):
    return tt.TokenSet(access, md, None if left_s is None else time.time() + left_s, source, time.time() - age_s)


def test_select_tokens_matrix():
    now = time.time()
    good, dead = ts("S", "m", 3000), ts("D", "m", -60, age_s=7000)
    fresh = ts("E", "m", None, "env", age_s=10)
    old_seed = ts("E", "m", None, "env", age_s=tt.TOKEN_LIFETIME_S + 60)
    assert tt.select_tokens(good, fresh, now) is good                          # a valid store wins
    assert tt.select_tokens(good, None, now) is good
    assert tt.select_tokens(dead, fresh, now) is fresh                         # dead store, fresh seed
    assert tt.select_tokens(None, fresh, now) is fresh
    assert tt.select_tokens(dead, old_seed, now) is dead                       # a stale .env is not trusted
    assert tt.select_tokens(dead, None, now) is dead and tt.select_tokens(dead, None, now).dead(now)
    assert tt.select_tokens(None, None, now) is None
    assert tt.select_tokens(None, old_seed, now) is None
    near = ts("S", "m", 300, age_s=600)                                        # inside a 15 min margin, .env newer
    assert tt.select_tokens(near, fresh, now, margin=900) is fresh
    assert tt.select_tokens(near, None, now, margin=900) is near


def test_seed_freshness_rules():
    now = time.time()
    store_new = ts("S", "m", 3000, age_s=60)
    assert not tt.seed_is_fresh(ts("S", "m", None, "env", age_s=1), store_new, now)      # same token as the store
    assert not tt.seed_is_fresh(ts("E", "m", None, "env", age_s=600), store_new, now)    # .env older than the store
    assert tt.seed_is_fresh(ts("E", "m", None, "env", age_s=5), store_new, now)          # .env edited after it
    assert not tt.seed_is_fresh(ts("E", "m", None, "env", age_s=6000), ts("S", "m", 3000, age_s=9000), now)  # > 90 min old
    mapping = tt.TokenSet("E", "m", None, "env", None)                                   # env mapping: no mtime
    assert tt.seed_is_fresh(mapping, None, now) and tt.seed_is_fresh(mapping, ts("D", "m", -5), now)
    assert not tt.seed_is_fresh(mapping, store_new, now)


def test_token_skip_reason(store, dotenv):
    assert tt.token_skip_reason() == "tradovate token expired"                 # nothing at all
    put_store(store)
    assert tt.token_skip_reason() is None
    put_store(store, left_s=-100)
    assert tt.token_skip_reason() == tt.EXPIRED_REASON == "tradovate token expired"
    put_env(dotenv, "ACC-ENV", "MD-ENV", age_s=10_000)                         # pasted long ago: dead too
    assert tt.token_skip_reason() == "tradovate token expired"
    put_env(dotenv, "ACC-ENV", "MD-ENV", age_s=5)                              # just pasted
    assert tt.token_skip_reason() is None


# ---------------------------------------------------------------- the lock

def test_lock_excludes_across_handles_sync_and_async(store):
    path = tt.lock_path(store)
    with tt.StoreLock(path):
        with pytest.raises(tt.LockTimeout):
            tt.StoreLock(path, timeout=0.2, poll=0.02).__enter__()

        async def second():
            async with tt.StoreLock(path, timeout=0.2, poll=0.02):
                pass
        with pytest.raises(tt.LockTimeout):
            asyncio.run(second())
    with tt.StoreLock(path, timeout=0.2):                                       # released
        pass


def test_async_lock_waits_without_blocking_the_loop_then_proceeds(store):
    path, order = tt.lock_path(store), []

    async def holder():
        async with tt.StoreLock(path):
            order.append("held")
            await asyncio.sleep(0.3)
            order.append("released")

    async def waiter():
        await asyncio.sleep(0.05)
        async with tt.StoreLock(path, timeout=5, poll=0.02):
            order.append("waiter")

    async def go():
        await asyncio.gather(holder(), waiter())
    asyncio.run(go())
    assert order == ["held", "released", "waiter"]


# ---------------------------------------------------------------- status file

def test_status_update_merges_redacts_clips_and_holds_no_tokens(store):
    sp = tt.status_path(store)
    tt.update_status(sp, {"last_ok_at": "t1", "writer": "x"})
    tt.update_status(sp, {"last_error": "renew failed ACC-OLD " + "x" * 500}, secrets=["ACC-OLD"])
    d = json.loads(sp.read_text())
    assert d["last_ok_at"] == "t1" and "ACC-OLD" not in d["last_error"] and "***" in d["last_error"]
    assert len(d["last_error"]) <= tt.ERROR_CLIP
    tt.record_failure(sp, time.time(), "again", writer="keepalive")
    tt.record_failure(sp, time.time(), "again", writer="keepalive")
    assert json.loads(sp.read_text())["consecutive_failures"] == 2
    tt.record_ok(sp, time.time(), time.time() + 100, source="store", writer="keepalive")
    d = json.loads(sp.read_text())
    assert d["consecutive_failures"] == 0 and d["last_error"] is None and d["last_ok_at"] != "t1"
    assert set(d) >= {"last_ok_at", "expires_at", "last_error", "consecutive_failures", "checked_at", "source", "writer"}


# ---------------------------------------------------------------- the keep-alive

def test_renewal_success_writes_store_and_status(store, dotenv):
    put_store(store, left_s=600)                                               # under 30 min: due
    http = RenewHttp(renewed())
    assert tick(keepalive(store, dotenv, http)) == "renewed"
    assert http.bearers == ["ACC-OLD"] and http.urls == ["https://live.tradovateapi.com/v1/auth/renewaccesstoken"]
    ts_ = tt.read_store(store)
    assert (ts_.access, ts_.md) == ("ACC-NEW", "MD-NEW") and stat.S_IMODE(store.stat().st_mode) == 0o600
    st = status(store)
    assert st["consecutive_failures"] == 0 and st["last_error"] is None and st["writer"] == "keepalive"
    assert st["expires_at"] == tt.iso_utc(ts_.expires_at) and st["last_ok_at"] and st["checked_at"]
    assert_no_secrets(tt.status_path(store))


def test_not_due_means_no_http_but_the_status_is_kept_fresh(store, dotenv):
    put_store(store, left_s=4000)                                              # > 30 min left
    tt.record_failure(tt.status_path(store), time.time(), "old trouble", writer="keepalive")
    http = RenewHttp()
    assert tick(keepalive(store, dotenv, http)) == "ok" and http.bearers == []
    st = status(store)
    assert st["consecutive_failures"] == 0 and st["last_error"] is None and st["checked_at"]


def test_a_forced_tick_renews_even_when_not_due(store, dotenv):
    put_store(store, left_s=4000)
    http = RenewHttp(renewed())
    assert tick(keepalive(store, dotenv, http), force=True) == "renewed" and http.bearers == ["ACC-OLD"]


def test_a_missing_md_token_in_the_answer_keeps_the_old_one(store, dotenv):
    put_store(store, left_s=600)
    http = RenewHttp((200, {"accessToken": "ACC-NEW", "expirationTime": tt.iso_utc(time.time() + 4800)}))
    assert tick(keepalive(store, dotenv, http)) == "renewed"
    assert tt.read_store(store).md == "MD-OLD"


def test_renewal_failure_is_recorded_redacted_and_recovers(store, dotenv):
    put_store(store, left_s=600)
    http = RenewHttp((401, {"errorText": "Token ACC-OLD expired"}), tv.TradovateError("OSError: ACC-OLD reset"),
                     renewed())
    k = keepalive(store, dotenv, http)
    assert tick(k) == "failed"
    st = status(store)
    assert st["consecutive_failures"] == 1 and "***" in st["last_error"] and st["last_error_at"]
    assert tt.read_store(store).access == "ACC-OLD"                            # the store is untouched
    assert tick(k) == "failed" and status(store)["consecutive_failures"] == 2
    assert_no_secrets(tt.status_path(store))
    assert tick(k) == "renewed"                                                # recovered
    st = status(store)
    assert st["consecutive_failures"] == 0 and st["last_error"] is None
    assert tt.read_store(store).access == "ACC-NEW"


def test_a_dead_store_is_expired_without_any_http(store, dotenv):
    put_store(store, left_s=-60)
    http = RenewHttp()
    k = keepalive(store, dotenv, http)
    assert tick(k) == "expired" and tick(k) == "expired" and http.bearers == []
    st = status(store)
    assert st["last_error"] == "token expired" and st["consecutive_failures"] == 2


def test_no_tokens_anywhere(store, dotenv):
    assert tick(keepalive(store, dotenv, RenewHttp())) == "no-tokens"
    st = status(store)
    assert "no tokens" in st["last_error"] and st["consecutive_failures"] == 1


def test_fresh_env_tokens_replace_a_dead_store_automatically(store, dotenv):
    put_store(store, left_s=-600, age_s=3000)
    k = keepalive(store, dotenv, RenewHttp((200, renewed("ACC-ENV", "MD-ENV")[1])))
    assert tick(k) == "expired"
    put_env(dotenv, "ACC-ENV", "MD-ENV", age_s=1)                              # the owner pastes fresh tokens
    http = RenewHttp(renewed("ACC-NEW", "MD-NEW"))
    k.http = http
    assert tick(k) == "adopted-env" and http.bearers == ["ACC-ENV"]
    assert tt.read_store(store).access == "ACC-NEW"
    st = status(store)
    assert st["source"] == "env" and st["consecutive_failures"] == 0 and st["last_error"] is None
    assert tick(k) == "ok"                                                     # and it is not retried


def test_env_tokens_edited_after_a_valid_store_are_picked_up_too(store, dotenv):
    put_store(store, left_s=4000, age_s=300)
    put_env(dotenv, "ACC-ENV", "MD-ENV", age_s=10)
    http = RenewHttp(renewed("ACC-NEW", "MD-NEW"))
    assert tick(keepalive(store, dotenv, http)) == "adopted-env" and http.bearers == ["ACC-ENV"]


def test_bad_env_tokens_are_ignored_while_the_store_is_valid_and_tried_once(store, dotenv):
    put_store(store, left_s=4000, age_s=300)
    put_env(dotenv, "ACC-BAD", "MD-ENV", age_s=10)                             # e.g. an old token after an unrelated edit
    http = RenewHttp((401, {"errorText": "bad ACC-BAD"}))
    k = keepalive(store, dotenv, http)
    assert tick(k) == "ok" and http.bearers == ["ACC-BAD"]
    assert status(store)["consecutive_failures"] == 0                          # not a failure: the store is fine
    assert tick(k) == "ok" and len(http.bearers) == 1                          # not retried every minute
    assert tt.read_store(store).access == "ACC-OLD"


def test_bad_env_tokens_with_a_dead_store_count_as_a_failure(store, dotenv):
    put_store(store, left_s=-100, age_s=3000)
    put_env(dotenv, "ACC-BAD", "MD-ENV", age_s=10)
    http = RenewHttp((401, {"errorText": "bad ACC-BAD"}))
    k = keepalive(store, dotenv, http)
    assert tick(k) == "failed"
    st = status(store)
    assert st["consecutive_failures"] == 1 and "rejected" in st["last_error"] and st["source"] == "env"
    assert_no_secrets(tt.status_path(store))
    assert tick(k) == "expired" and len(http.bearers) == 1


def test_a_newer_store_written_by_another_renewer_is_adopted_with_no_http(store, dotenv):
    put_store(store, left_s=1000)                                              # due
    k = keepalive(store, dotenv, RenewHttp())
    put_store(store, "ACC-NEW", "MD-NEW", left_s=4800)                         # the session renewed it first
    assert tick(k) == "ok" and k.http.bearers == []


def test_host_comes_from_dotenv_then_the_default(store, dotenv, monkeypatch):
    put_store(store, left_s=600)
    dotenv.write_text("TRADOVATE_AUTH_HOST=demo.tradovateapi.com\n")
    http = RenewHttp(renewed(), renewed())
    k = keepalive(store, dotenv, http)
    assert tick(k) == "renewed" and http.urls[0].startswith("https://demo.tradovateapi.com/")
    assert status(store)["host"] == "demo.tradovateapi.com"
    put_store(store, left_s=600)
    assert tick(keepalive(store, dotenv, http, host="h.example")) == "renewed" and http.urls[1].startswith("https://h.example/")


def test_lock_busy_is_a_failure_not_a_crash(store, dotenv, monkeypatch):
    put_store(store, left_s=600)
    monkeypatch.setattr(tt, "LOCK_TIMEOUT_S", 0.1)
    orig = tt.StoreLock.__init__
    monkeypatch.setattr(tt.StoreLock, "__init__", lambda self, path, timeout=0.1, poll=0.02: orig(self, path, timeout, poll))
    with tt.StoreLock(tt.lock_path(store)):
        assert tick(keepalive(store, dotenv, RenewHttp())) == "failed"
    assert "busy" in status(store)["last_error"]


def test_run_loop_forces_the_first_tick_then_polls(store, dotenv):
    sleeps = []

    async def sleep(s):
        sleeps.append(s)
    assert (ka.POLL_S, ka.RETRY_S, ka.RENEW_BEFORE_S, ka.START_SKIP_S) == (60, 120, 1800, 120)
    put_store(store, left_s=4000, age_s=3000)                                   # written long ago, not due
    http = RenewHttp(renewed(left_s=4100))
    asyncio.run(keepalive(store, dotenv, http, sleep=sleep).run(max_ticks=3))
    assert http.bearers == ["ACC-OLD"]                                          # tick 1 forced, ticks 2 and 3 not due
    assert sleeps == [ka.POLL_S, ka.POLL_S]                                     # no sleep after the last tick


def test_run_loop_retries_a_failed_renewal_after_2_minutes(store, dotenv):
    sleeps = []

    async def sleep(s):
        sleeps.append(s)
    put_store(store, left_s=1000, age_s=3000)                                   # due
    http = RenewHttp((500, {}), (500, {}), renewed())
    asyncio.run(keepalive(store, dotenv, http, sleep=sleep).run(max_ticks=3))
    assert http.bearers == ["ACC-OLD"] * 3 and sleeps == [ka.RETRY_S, ka.RETRY_S]
    assert tt.read_store(store).access == "ACC-NEW" and status(store)["consecutive_failures"] == 0


def test_run_loop_skips_the_forced_renewal_when_the_store_is_very_fresh(store, dotenv):
    put_store(store, left_s=4800, age_s=10)
    http = RenewHttp()
    asyncio.run(keepalive(store, dotenv, http).run(max_ticks=1))
    assert http.bearers == []


def test_run_loop_survives_a_crashing_tick(store, dotenv):
    sleeps = []

    async def sleep(s):
        sleeps.append(s)
    k = keepalive(store, dotenv, RenewHttp(), sleep=sleep)

    async def bad(force=False):
        raise ValueError("bug")
    k.tick = bad
    asyncio.run(k.run(max_ticks=2))
    assert sleeps == [ka.RETRY_S]


def test_main_once_exit_codes_and_no_secrets_in_output(store, dotenv, monkeypatch, capsys):
    put_store(store, left_s=4000)
    monkeypatch.setattr(ka, "default_http", RenewHttp(renewed()))
    assert ka.main(["--once"]) == 0
    out = capsys.readouterr().out
    assert "renewed" in out and not any(s in out for s in SECRETS)
    monkeypatch.setattr(ka, "default_http", RenewHttp((401, {"errorText": "no ACC-NEW"})))
    assert ka.main(["--once"]) == 1
    put_store(store, left_s=-100)
    assert ka.main(["--once"]) == 1
    assert not any(s in capsys.readouterr().out for s in SECRETS)
    store.unlink()
    assert ka.main(["--once"]) == 1                                             # no tokens at all: 1, never a crash
    put_env(dotenv, "ACC-ENV", "MD-ENV", age_s=1)
    monkeypatch.setattr(ka, "default_http", RenewHttp(renewed()))
    assert ka.main(["--once"]) == 0                                             # adopted-env
