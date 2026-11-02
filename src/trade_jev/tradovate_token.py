"""Tradovate token mode: the token store, the status file and the lock that keeps renewers apart.

Why token mode. The owner's account is a My Funded Futures simulated account; the API username / password
login is refused for it. MFF approved this method instead: log in on trader.tradovate.com, copy the web
session's `accessToken` and `mdAccessToken` into .env, and keep them alive by renewing them
(GET /auth/renewaccesstoken answers with both tokens and an `expirationTime` about 80 minutes ahead).

Files (all under data/, gitignored; override the store with $TRADOVATE_TOKEN_STORE):
  tradovate_token.json         the token store {accessToken, mdAccessToken, expirationTime}: written
                               atomically (temp file in the same directory, fsync, os.replace), mode 0600.
                               Every successful renewal rewrites it. A temporary keep-alive script writes
                               this exact format too, so the two are interchangeable.
  tradovate_token_status.json  last_ok_at, expires_at, last_error, last_error_at, consecutive_failures,
                               checked_at, source, writer, host. NEVER a token. The Resonant extension
                               reads it (trade_jev_status, the `watch` job).
  .tradovate_token.lock        sidecar lock file (flock) for the read-check-renew-write cycle.

Which tokens win (`select_tokens`): a still-valid store; else the .env seed if it is fresh (see
`seed_is_fresh`); else a dead store (reported as expired); else nothing. `.env` is re-read from the file on
every call: src/trade_jev/__init__.py loads it into os.environ once at import, so os.environ goes stale when
the owner pastes new tokens into a running machine. A fresh .env token means: different from the store's,
and either the store is missing / dead or .env was modified after the store was last written, and .env was
modified within the last 90 minutes (a token pasted earlier than that is dead anyway).

Assumption: renewaccesstoken does NOT invalidate the previous access token (it stays usable until its own
expiry). So a session may keep using a token it adopted until its own margin, and a renewer that loses a
race costs nothing. If that ever proved false the lock + adopt-from-store rule still keeps renewers apart.

Token mode is sticky: once data/tradovate_token.json exists the mode stays "token" even if the .env tokens
are removed. To go back to password mode delete the store.

Race rule (who renews). Two processes renew: the keep-alive (`python -m trade_jev.tradovate_keepalive`,
launchd, 24/7) and the live session (in-process, during the session). Renewal may invalidate the token it
renewed, so two uncoordinated renewers could each use a token the other just replaced. So every
read-check-renew-write cycle runs under `StoreLock`, and under the lock the store is re-read first: if the
store's token is valid beyond the caller's margin it is adopted with no HTTP call. The keep-alive renews
when under 30 minutes remain (about every 50 minutes); the session's margin is 15 minutes
(tradovate.RENEW_MARGIN_S), so while the keep-alive is healthy the session only ever adopts the store's
tokens, and it renews itself (writing the store) only when the keep-alive is not doing its job.

stdlib + python-dotenv only; this module does not import tradovate.py.
"""

from __future__ import annotations

import asyncio
import fcntl
import json
import os
import tempfile
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path

from dotenv import dotenv_values

ROOT = Path(__file__).resolve().parents[2]
STORE_ENV = "TRADOVATE_TOKEN_STORE"      # test / emergency override of the store path
DOTENV_ENV = "TRADE_JEV_DOTENV"          # test override of the .env path
DEFAULT_AUTH_HOST = "live.tradovateapi.com"
TOKEN_LIFETIME_S = 90 * 60
EXPIRED_REASON = "tradovate token expired"
STORE_KEYS = ("accessToken", "mdAccessToken", "expirationTime")
LOCK_TIMEOUT_S = 20.0
LOCK_POLL_S = 0.1
ERROR_CLIP = 200


class LockTimeout(TimeoutError):
    pass


# ---------------------------------------------------------------- paths and mode

def _setting(env, name: str) -> str:
    """env[name], falling back to os.environ for the test-only path overrides."""
    return (env.get(name) or os.environ.get(name) or "").strip()


def token_store_path(env=os.environ) -> Path:
    v = _setting(env, STORE_ENV)
    return Path(v).expanduser() if v else ROOT / "data" / "tradovate_token.json"


def status_path(store: Path) -> Path:
    return Path(store).with_name("tradovate_token_status.json")


def lock_path(store: Path) -> Path:
    return Path(store).with_name(".tradovate_token.lock")


def status_lock_path(status: Path) -> Path:
    """A separate, short-lived lock for the status file's read-modify-write (never nested in StoreLock's file)."""
    return Path(status).with_name(".tradovate_token_status.lock")


def dotenv_path(env=os.environ) -> Path:
    v = _setting(env, DOTENV_ENV)
    return Path(v).expanduser() if v else ROOT / ".env"


def auth_mode(env=os.environ) -> str:
    """"token" when TRADOVATE_ACCESS_TOKEN is set or a token store file exists, else "password"."""
    if (env.get("TRADOVATE_ACCESS_TOKEN") or "").strip() or token_store_path(env).exists():
        return "token"
    return "password"


# ---------------------------------------------------------------- time

def parse_expiration(s) -> float | None:
    """ISO timestamp (Z, +00:00, or naive = UTC) -> epoch seconds; None for anything else."""
    if not isinstance(s, str) or not s.strip():
        return None
    try:
        dt = datetime.fromisoformat(s.strip().replace("Z", "+00:00"))
    except ValueError:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc).timestamp()


def iso_utc(ts: float) -> str:
    return datetime.fromtimestamp(ts, timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")


# ---------------------------------------------------------------- token sets

@dataclass(frozen=True)
class TokenSet:
    access: str = field(repr=False)
    md: str = field(repr=False)
    expires_at: float | None          # epoch; None = unknown (an .env seed)
    source: str                       # "store" | "env"
    written_at: float | None = None   # file mtime

    def dead(self, now: float, margin: float = 0.0) -> bool:
        """Expired, or expiring within `margin` seconds. An unknown expiry is not dead."""
        return self.expires_at is not None and self.expires_at <= now + margin


def read_store(path) -> TokenSet | None:
    """The store, or None when it is missing or unreadable. Extra keys are ignored; a bad or missing
    expiry makes the set dead (expires_at 0)."""
    path = Path(path)
    try:
        d = json.loads(path.read_text())
        mtime = path.stat().st_mtime
    except (OSError, ValueError):
        return None
    if not isinstance(d, dict):
        return None
    access = d.get("accessToken")
    if not isinstance(access, str) or not access.strip():
        return None
    md = d.get("mdAccessToken")
    exp = parse_expiration(d.get("expirationTime"))
    return TokenSet(access.strip(), md.strip() if isinstance(md, str) else "",
                    exp if exp is not None else 0.0, "store", mtime)


def write_store(path, access: str, md: str, expires_at: float) -> bool:
    """Atomically write the store (0600). Refuses (False) when the existing store expires later: a
    slower renewer must never replace newer tokens with older ones."""
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    cur = read_store(path)
    if cur is not None and cur.expires_at is not None and cur.expires_at > expires_at:
        return False
    payload = {"accessToken": access, "mdAccessToken": md or "", "expirationTime": iso_utc(expires_at)}
    assert tuple(payload) == STORE_KEYS
    fd, tmp = tempfile.mkstemp(dir=path.parent, prefix=".tradovate_token.", suffix=".tmp")
    try:
        os.fchmod(fd, 0o600)
        with os.fdopen(fd, "w") as f:
            json.dump(payload, f)
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp, path)
        os.chmod(path, 0o600)
    except BaseException:
        try:
            os.unlink(tmp)
        except OSError:
            pass
        raise
    return True


def _dotenv_file(env, dotenv) -> Path:
    return Path(dotenv).expanduser() if dotenv else dotenv_path(env)


def read_dotenv_setting(name: str, env=os.environ, dotenv=None) -> str:
    """`name` from the .env file (fresh read), else from env; "" when unset."""
    p = _dotenv_file(env, dotenv)
    if p.is_file():
        try:
            v = dotenv_values(p).get(name)
        except OSError:
            v = None
        if v and v.strip():
            return v.strip()
    return (env.get(name) or "").strip()


def read_seed(env=os.environ, dotenv=None) -> TokenSet | None:
    """The tokens pasted into .env (fresh read of the file, with its mtime), else the env mapping."""
    p = _dotenv_file(env, dotenv)
    if p.is_file():
        try:
            vals = dotenv_values(p)
            mtime = p.stat().st_mtime
        except OSError:
            return None
        access = (vals.get("TRADOVATE_ACCESS_TOKEN") or "").strip()
        md = (vals.get("TRADOVATE_MD_ACCESS_TOKEN") or "").strip()
        written = mtime
    else:
        access = (env.get("TRADOVATE_ACCESS_TOKEN") or "").strip()
        md = (env.get("TRADOVATE_MD_ACCESS_TOKEN") or "").strip()
        written = None
    if not access:
        return None
    return TokenSet(access, md, None, "env", written)


def seed_is_fresh(seed: TokenSet | None, store: TokenSet | None, now: float) -> bool:
    if seed is None or not seed.access:
        return False
    if store is not None and store.access == seed.access:
        return False
    store_ok = store is not None and not store.dead(now)
    if seed.written_at is None:  # the env mapping carries no mtime: only a stand-in for a missing / dead store
        return not store_ok
    if now - seed.written_at > TOKEN_LIFETIME_S:
        return False
    return (not store_ok) or store.written_at is None or seed.written_at > store.written_at


def select_tokens(store: TokenSet | None, seed: TokenSet | None, now: float,
                  margin: float = 0.0) -> TokenSet | None:
    """A valid store; else a fresh seed; else a dead store; else None."""
    if store is not None and not store.dead(now, margin):
        return store
    if seed_is_fresh(seed, store, now):
        return seed
    return store


def token_skip_reason(env=os.environ, now: float | None = None, dotenv=None) -> str | None:
    """EXPIRED_REASON when token mode has no usable token (dead store, no fresh .env tokens), else None."""
    now = time.time() if now is None else now
    sel = select_tokens(read_store(token_store_path(env)), read_seed(env, dotenv), now)
    if sel is None or sel.dead(now):
        return EXPIRED_REASON
    return None


# ---------------------------------------------------------------- lock

class StoreLock:
    """flock on a sidecar file. Polls LOCK_NB so an async caller never blocks the event loop; raises
    LockTimeout after `timeout` seconds. Usable as `with` and `async with`."""

    def __init__(self, path, timeout: float = LOCK_TIMEOUT_S, poll: float = LOCK_POLL_S):
        self.path, self.timeout, self.poll = Path(path), timeout, poll
        self._fd: int | None = None

    def _open(self) -> int:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        return os.open(self.path, os.O_RDWR | os.O_CREAT, 0o600)

    def _try(self, fd: int) -> bool:
        try:
            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
            return True
        except BlockingIOError:
            return False

    def __enter__(self):
        fd = self._open()
        deadline = time.monotonic() + self.timeout
        while not self._try(fd):
            if time.monotonic() >= deadline:
                os.close(fd)
                raise LockTimeout(f"token store lock busy for {self.timeout:g}s")
            time.sleep(self.poll)
        self._fd = fd
        return self

    def __exit__(self, *exc):
        fd, self._fd = self._fd, None
        if fd is not None:
            try:
                fcntl.flock(fd, fcntl.LOCK_UN)
            finally:
                os.close(fd)

    async def __aenter__(self):
        fd = self._open()
        deadline = time.monotonic() + self.timeout
        try:
            while not self._try(fd):
                if time.monotonic() >= deadline:
                    raise LockTimeout(f"token store lock busy for {self.timeout:g}s")
                await asyncio.sleep(self.poll)
        except BaseException:
            os.close(fd)
            raise
        self._fd = fd
        return self

    async def __aexit__(self, *exc):
        self.__exit__(*exc)


# ---------------------------------------------------------------- status file

def read_status(path) -> dict:
    try:
        d = json.loads(Path(path).read_text())
    except (OSError, ValueError):
        return {}
    return d if isinstance(d, dict) else {}


def _redact(text, secrets) -> str:
    text = str(text)
    for s in sorted({s for s in secrets if s}, key=len, reverse=True):
        text = text.replace(s, "***")
    return text


def update_status(path, updates: dict, secrets=(), increment: str | None = None) -> dict:
    """Merge `updates` into the status file (atomic); `increment` names a counter to add 1 to. The
    read-modify-write runs under a short lock on a separate lock file (best effort: if it stays busy the
    write goes ahead unlocked). `last_error` is redacted and clipped; no token ever goes in."""
    path = Path(path)
    try:
        with StoreLock(status_lock_path(path), timeout=5.0):
            return _update_status(path, updates, secrets, increment)
    except LockTimeout:
        return _update_status(path, updates, secrets, increment)


def _update_status(path: Path, updates: dict, secrets, increment) -> dict:
    cur = read_status(path)
    cur.update(updates)
    if increment:
        cur[increment] = int(cur.get(increment) or 0) + 1
    if cur.get("last_error"):
        cur["last_error"] = " ".join(_redact(cur["last_error"], secrets).split())[:ERROR_CLIP]
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp = tempfile.mkstemp(dir=path.parent, prefix=".tradovate_token_status.", suffix=".tmp")
    try:
        with os.fdopen(fd, "w") as f:
            json.dump(cur, f, indent=1)
            f.flush()
            os.fsync(f.fileno())
        os.chmod(tmp, 0o644)  # no secrets in it
        os.replace(tmp, path)
    except BaseException:
        try:
            os.unlink(tmp)
        except OSError:
            pass
        raise
    return cur


def record_ok(path, now: float, expires_at: float, *, source: str, writer: str, host: str | None = None) -> None:
    update_status(path, {"last_ok_at": iso_utc(now), "expires_at": iso_utc(expires_at), "last_error": None,
                         "consecutive_failures": 0, "checked_at": iso_utc(now), "source": source,
                         "writer": writer, "host": host})


def record_failure(path, now: float, error, *, writer: str, host: str | None = None, secrets=(),
                   source: str | None = None) -> None:
    upd = {"last_error": str(error), "last_error_at": iso_utc(now),
           "checked_at": iso_utc(now), "writer": writer, "host": host}
    if source:
        upd["source"] = source
    update_status(path, upd, secrets, increment="consecutive_failures")
