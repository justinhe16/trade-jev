"""Tradovate token keep-alive: renews the web-session tokens 24/7 so they never lapse.

    uv run python -m trade_jev.tradovate_keepalive          # the launchd service (com.tradejev.tvtoken)
    uv run python -m trade_jev.tradovate_keepalive --once   # one check / renewal, exit 0 if healthy

Tokens live about 80 minutes and die if nobody renews them (a weekend, a reboot, a stopped service), so this
runs all the time, not only in market hours. It checks every minute and renews when under 30 minutes remain
(about every 50 minutes), and once at start unless the store was written in the last 2 minutes (a launchd
restart must not hammer Tradovate).

It reads and writes data/tradovate_token.json (the token store) and writes
data/tradovate_token_status.json (last_ok_at, expires_at, last_error, ... never a token) for the Resonant
extension. The race rule with the live session (everything under one file lock, the store re-read inside
it) is documented in tradovate_token.py.

Each tick: re-read .env (fresh; os.environ is stale).
  - .env holds tokens that are fresh (different from the store's, and the store is dead or .env was edited
    after it) and not tried yet: renew with them, write the store (`adopted-env`). A seed that fails is
    ignored (it is remembered, so it is not retried every minute) unless the store is dead too.
  - the store is dead: record `token expired` in the status file, no HTTP (a dead token cannot renew);
    this is what makes the extension alert, and a fresh paste into .env recovers it by itself.
  - the store is valid and near expiry (or forced): renew with it.
Tokens never appear in the log.
"""

from __future__ import annotations

import argparse
import asyncio
import hashlib
import os
import sys
import time
from pathlib import Path

from trade_jev import tradovate_token as tt
from trade_jev.tradovate import DEFAULT_LIFETIME_S, TradovateError, default_http, redact, renew_request

POLL_S = 60
RENEW_BEFORE_S = 1800
RETRY_S = 120
START_SKIP_S = 120
OK_RESULTS = ("renewed", "adopted-env", "ok")


def _log(msg: str) -> None:
    print(f"[tvtoken] {time.strftime('%F %T')} {msg}", flush=True)


TRANSIENT_HTTP = (408, 429)  # timeout / rate limit: retry, never blacklist a seed


class Rejected(RuntimeError):
    """A definitive answer: HTTP 4xx (not 408/429) or errorText. Transport errors, 5xx, 408 and 429 are plain
    RuntimeError (retried)."""


class Keepalive:
    def __init__(self, store, dotenv, host: str | None = None, http=None, clock=time.time,
                 sleep=asyncio.sleep):
        self.store, self.dotenv, self.host = Path(store), Path(dotenv) if dotenv else None, host
        self.http = http or default_http
        self.clock, self.sleep = clock, sleep
        self._tried: set[str] = set()   # sha256(seed access) + .env mtime of the seeds already used / rejected

    # -- helpers

    def _host(self) -> str:
        return (self.host or tt.read_dotenv_setting("TRADOVATE_AUTH_HOST", os.environ, self.dotenv)
                or tt.DEFAULT_AUTH_HOST)

    def _seed(self):
        return tt.read_seed(os.environ, self.dotenv)

    @staticmethod
    def _seed_key(seed: tt.TokenSet) -> str:
        return f"{hashlib.sha256(seed.access.encode()).hexdigest()}:{seed.written_at}"

    async def _renew(self, host: str, access: str, md: str, secrets: list[str]):
        """(expires_at, md, access) on success, else raises RuntimeError(redacted text)."""
        try:
            status, d = await renew_request(self.http, f"https://{host}/v1", access)
        except TradovateError as e:
            raise RuntimeError(f"renew failed: {redact(e, secrets)}") from None
        if status >= 400 or d.get("errorText") or not d.get("accessToken"):
            text = redact(f"renew failed: {d.get('errorText') or 'HTTP ' + str(status)}", secrets)
            transient = status >= 500 or status in TRANSIENT_HTTP or (status < 400 and not d.get("errorText"))
            raise (RuntimeError if transient else Rejected)(text)
        exp = tt.parse_expiration(d.get("expirationTime")) or self.clock() + DEFAULT_LIFETIME_S
        return exp, d.get("mdAccessToken") or md, d["accessToken"]

    def _persist(self, access: str, md: str, exp: float) -> float:
        """Write the store; on refusal (the store on disk expires later) keep what is on disk. Returns the
        expiry of the store as it is on disk."""
        if tt.write_store(self.store, access, md, exp):
            return exp
        cur = tt.read_store(self.store)
        _log("store write refused: the store on disk expires later; keeping it")
        return cur.expires_at if cur and cur.expires_at else exp

    # -- one tick

    async def tick(self, force: bool = False) -> str:
        """"renewed" | "adopted-env" | "ok" | "failed" | "expired" | "no-tokens"."""
        status_file, host = tt.status_path(self.store), self._host()
        try:
            async with tt.StoreLock(tt.lock_path(self.store)):
                return await self._tick_locked(force, status_file, host)
        except tt.LockTimeout as e:
            tt.record_failure(status_file, self.clock(), f"token store busy: {e}", writer="keepalive", host=host)
            _log("store lock busy")
            return "failed"

    async def _tick_locked(self, force: bool, status_file, host: str) -> str:
        now = self.clock()
        store, seed = tt.read_store(self.store), self._seed()
        secrets = [t for ts in (store, seed) if ts for t in (ts.access, ts.md)]

        if seed is not None and tt.seed_is_fresh(seed, store, now) and self._seed_key(seed) not in self._tried:
            try:
                exp, md, access = await self._renew(host, seed.access, seed.md, secrets)
            except RuntimeError as e:
                if isinstance(e, Rejected):  # only a definitive rejection blacklists the seed
                    self._tried.add(self._seed_key(seed))
                if store is not None and not store.dead(now):
                    _log("tokens in .env were rejected; ignored (the store is still valid)")
                else:
                    tt.record_failure(status_file, now, f"tokens in .env rejected: {e}", writer="keepalive",
                                      host=host, secrets=secrets, source="env")
                    _log("tokens in .env rejected and the store is dead")
                    return "failed"
            else:
                exp = self._persist(access, md, exp)
                tt.record_ok(status_file, now, exp, source="env", writer="keepalive", host=host)
                _log(f"renewed from .env tokens; expires {tt.iso_utc(exp)}")
                return "adopted-env"

        if store is None:
            tt.record_failure(status_file, now, "no tokens: paste an access token + md token into .env",
                              writer="keepalive", host=host)
            _log("no tokens in the store or .env")
            return "no-tokens"
        if store.dead(now):
            tt.record_failure(status_file, now, "token expired", writer="keepalive", host=host)
            _log("store token expired; waiting for fresh tokens in .env")
            return "expired"

        failing = int(tt.read_status(status_file).get("consecutive_failures") or 0) > 0
        if force or failing or store.dead(now, RENEW_BEFORE_S):  # failing: retry even if not due
            try:
                exp, md, access = await self._renew(host, store.access, store.md, secrets)
            except RuntimeError as e:
                tt.record_failure(status_file, now, str(e), writer="keepalive", host=host, secrets=secrets)
                _log(f"renewal failed: {e}")
                return "failed"
            exp = self._persist(access, md, exp)
            tt.record_ok(status_file, now, exp, source="store", writer="keepalive", host=host)
            _log(f"renewed; expires {tt.iso_utc(exp)}")
            return "renewed"

        # healthy and not due: only checked_at / expires_at / host move. Failures and last_error are cleared
        # by a successful renewal or adoption only (a revoked token with a future expiry must still alert).
        tt.update_status(status_file, {"checked_at": tt.iso_utc(now), "expires_at": tt.iso_utc(store.expires_at),
                                       "host": host})
        return "ok"

    # -- loop

    async def run(self, max_ticks: int | None = None) -> None:
        n = 0
        while max_ticks is None or n < max_ticks:
            store = tt.read_store(self.store)
            first = n == 0
            recent = store is not None and store.written_at is not None and self.clock() - store.written_at < START_SKIP_S
            try:
                result = await self.tick(force=first and not recent)
            except asyncio.CancelledError:
                raise
            except Exception as e:  # noqa: BLE001 — a bug or a full disk must not end the service
                _log(f"tick crashed: {type(e).__name__}")
                result = "failed"
            n += 1
            if max_ticks is not None and n >= max_ticks:
                break
            await self.sleep(RETRY_S if result == "failed" else POLL_S)


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description="Keep the Tradovate web-session tokens alive.")
    ap.add_argument("--once", action="store_true", help="one tick (forced renewal), exit 0 if healthy")
    args = ap.parse_args(argv)
    ka = Keepalive(tt.token_store_path(), tt.dotenv_path())
    _log(f"start: store {ka.store}, host {ka._host()}")
    try:
        if args.once:
            result = asyncio.run(ka.tick(force=True))
            print(result)
            return 0 if result in OK_RESULTS else 1
        asyncio.run(ka.run())
    except KeyboardInterrupt:
        pass
    return 0


if __name__ == "__main__":
    sys.exit(main())
