"""Per-account Tradovate token upkeep for the copy daemon.

`AccountKeepalive` is the paper keep-alive (tradovate_keepalive.Keepalive) with three differences: it reads
`<PREFIX>_ACCESS_TOKEN` / `<PREFIX>_MD_ACCESS_TOKEN` / `<PREFIX>_AUTH_HOST` from .env (fresh each tick), its store,
status and lock files live in data/copy/<name>/ (the main data/tradovate_token*.json are never touched), and it
is deliberately slow: a tick every 60 s, 15 minutes between attempts while failing.

Tradovate rate limits are per IP /24 and shared with the paper account: renewaccesstoken 15/hour,
accesstokenrequest 5 failed/hour. So `RenewBudget` caps the daemon at 6 HTTP renewals per hour across all
accounts (persisted, so a restart loop cannot burn it), and the password path (all five `<PREFIX>_USERNAME`,
`_PASSWORD`, `_CID`, `_SEC`, `_DEVICE_ID` set) makes exactly one login attempt per .env modification time.
A token problem blocks that account's entries only, and clears by itself; exits and flatten still try.
"""

from __future__ import annotations

import asyncio
import json
import os
import time
from pathlib import Path

from dotenv import dotenv_values

from trade_jev import tradovate_token as tt
from trade_jev.tradovate import TradovateAuth, TradovateConfig, TradovateError, redact
from trade_jev.tradovate_keepalive import Keepalive

TICK_S = 60.0
FAIL_BACKOFF_S = 900.0
RENEWS_PER_HOUR = 6
PASSWORD_VARS = ("USERNAME", "PASSWORD", "CID", "SEC", "DEVICE_ID")


class RenewBudget:
    """At most `limit` HTTP renewals per rolling hour for the whole daemon, persisted in a small JSON file."""

    def __init__(self, path, limit: int = RENEWS_PER_HOUR, clock=time.time):
        self.path, self.limit, self.clock = Path(path), limit, clock

    def _load(self) -> list[float]:
        try:
            d = json.loads(self.path.read_text())
            return [float(t) for t in d] if isinstance(d, list) else []
        except (OSError, ValueError, TypeError):
            return []

    def used(self) -> int:
        now = self.clock()
        return len([t for t in self._load() if now - t < 3600])

    def take(self) -> bool:
        now = self.clock()
        live = [t for t in self._load() if now - t < 3600]
        if len(live) >= self.limit:
            return False
        live.append(now)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        tmp = self.path.with_suffix(".tmp")
        tmp.write_text(json.dumps(live))
        os.replace(tmp, self.path)
        return True


class AccountKeepalive(Keepalive):
    def __init__(self, name: str, prefix: str, env: str, store, dotenv, budget: RenewBudget, http=None,
                 clock=time.time, sleep=asyncio.sleep):
        super().__init__(store, dotenv, None, http, clock, sleep)
        self.name, self.prefix, self.env, self.budget = name, prefix, env, budget
        self.fail_backoff = FAIL_BACKOFF_S
        self._attempt_file = Path(store).with_name("password_attempt.json")

    # -- .env access (fresh each time)

    def _vals(self) -> dict:
        if self.dotenv and Path(self.dotenv).is_file():
            try:
                return {k: (v or "").strip() for k, v in dotenv_values(self.dotenv).items()}
            except OSError:
                return {}
        return {k: v.strip() for k, v in os.environ.items() if k.startswith(self.prefix + "_")}

    def _env_mtime(self) -> float | None:
        try:
            return Path(self.dotenv).stat().st_mtime if self.dotenv else None
        except OSError:
            return None

    def _host(self) -> str:
        h = self._vals().get(f"{self.prefix}_AUTH_HOST")
        return h or tt.DEFAULT_AUTH_HOST   # the web app logs in on the live host, demo accounts included

    def _seed(self):
        v = self._vals()
        access = v.get(f"{self.prefix}_ACCESS_TOKEN", "")
        if not access:
            return None
        return tt.TokenSet(access, v.get(f"{self.prefix}_MD_ACCESS_TOKEN", ""), None, "env", self._env_mtime())

    # -- renew budget

    async def _renew(self, host, access, md, secrets):
        if not self.budget.take():
            raise RuntimeError(f"renew budget used up ({self.budget.limit}/hour for the whole daemon)")
        return await super()._renew(host, access, md, secrets)

    # -- access for the broker and the status

    def access_token(self) -> str | None:
        st = tt.read_store(self.store)
        return st.access if st is not None and not st.dead(self.clock()) else None

    def secrets(self) -> list[str]:
        out = []
        for ts in (tt.read_store(self.store), self._seed()):
            if ts:
                out += [ts.access, ts.md]
        v = self._vals()
        out += [v.get(f"{self.prefix}_{k}", "") for k in ("PASSWORD", "SEC", "ACCESS_TOKEN", "MD_ACCESS_TOKEN")]
        return [s for s in out if s]

    def health(self) -> dict:
        """{ok, expires_at, error}: never a token."""
        st = tt.read_store(self.store)
        status = tt.read_status(tt.status_path(self.store))
        now = self.clock()
        if st is None:
            return {"ok": False, "expires_at": None, "error": status.get("last_error") or "no tokens"}
        if st.dead(now):
            return {"ok": False, "expires_at": tt.iso_utc(st.expires_at), "error": "token expired"}
        err = status.get("last_error")
        failing = int(status.get("consecutive_failures") or 0) >= 2 and st.dead(now, 600)
        return {"ok": not failing, "expires_at": tt.iso_utc(st.expires_at), "error": err if failing else None}

    # -- password path: one attempt per .env mtime

    def _password_vars(self) -> dict | None:
        v = self._vals()
        got = {k: v.get(f"{self.prefix}_{k}", "") for k in PASSWORD_VARS}
        return got if all(got.values()) else None

    def _attempted(self, mtime) -> bool:
        try:
            return json.loads(self._attempt_file.read_text()).get("mtime") == mtime
        except (OSError, ValueError, AttributeError):
            return False

    async def _password_login(self) -> str:
        pw, mtime = self._password_vars(), self._env_mtime()
        if pw is None or mtime is None or self._attempted(mtime):
            return "skipped"
        self._attempt_file.parent.mkdir(parents=True, exist_ok=True)
        self._attempt_file.write_text(json.dumps({"mtime": mtime}))   # recorded BEFORE the attempt
        cfg = TradovateConfig(pw["USERNAME"], pw["PASSWORD"], pw["CID"], pw["SEC"], pw["DEVICE_ID"], env=self.env,
                              mode="password", auth_host=self._vals().get(f"{self.prefix}_AUTH_HOST") or None)
        auth = TradovateAuth(cfg, http=self.http, sleep=self.sleep, clock=self.clock)
        status_file, secrets = tt.status_path(self.store), self.secrets()
        try:
            d = await auth._post_login(auth._login_body())   # exactly one POST: a penalty is a failure
            if "p-ticket" in d or "p-time" in d:
                raise TradovateError("login penalised (p-ticket): not retried")
            auth._apply(d, full=True)
        except Exception as e:  # noqa: BLE001
            tt.record_failure(status_file, self.clock(), f"password login failed: {redact(e, secrets)}",
                              writer="copy", host=cfg.base, secrets=secrets)
            return "failed"
        exp = self._persist(auth.access_token, auth.md_token or "", auth.expires_at)
        tt.record_ok(status_file, self.clock(), exp, source="password", writer="copy", host=cfg.base)
        return "renewed"

    async def tick(self, force: bool = False) -> str:
        result = await super().tick(force)
        if result in ("no-tokens", "expired", "failed") and self._password_vars() is not None:
            r = await self._password_login()
            if r != "skipped":
                return r
        return result

    # -- loop

    async def run(self, max_ticks: int | None = None) -> None:
        n = 0
        while max_ticks is None or n < max_ticks:
            store = tt.read_store(self.store)
            recent = store is not None and store.written_at is not None and self.clock() - store.written_at < 120
            try:
                result = await self.tick(force=(n == 0 and not recent))
            except asyncio.CancelledError:
                raise
            except Exception:  # noqa: BLE001 — a bug or a full disk must not end the daemon
                result = "failed"
            n += 1
            if max_ticks is not None and n >= max_ticks:
                break
            await self.sleep(self.fail_backoff if result in ("failed", "expired", "no-tokens") else TICK_S)
