"""accounts.yaml: strict validation. An absent file means no accounts (no behaviour change anywhere)."""

from __future__ import annotations

import os
import re
from dataclasses import dataclass
from pathlib import Path

import yaml

from trade_jev import ROOT

ACCOUNTS_ENV = "TRADE_JEV_ACCOUNTS"      # test / emergency override of the accounts.yaml path
COPY_DB_ENV = "TRADE_JEV_COPY_DB"
COPY_DIR_ENV = "TRADE_JEV_COPY_DIR"      # per-account token stores and the daemon lock
DEFAULT_KILL_URL = "http://127.0.0.1:7777/api/status"
DEFAULT_PORT = 8768
MODES = ("dry_run", "demo", "live")
ENVS = ("demo", "live")
ROOTS = ("MNQ", "NQ")
POINT_VALUE = {"MNQ": 2.0, "NQ": 20.0}
DEFAULT_SLIP_TICKS = 4            # 1 point: $10 on 5 MNQ (the limit derives from the NQ quote)
SYMBOL_RE = re.compile(r"^NQ([HMUZ]\d)$")
PREFIX_RE = re.compile(r"^[A-Z][A-Z0-9_]*$")
NAME_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_-]{0,30}$")
KNOWN = {"name", "enabled", "mode", "env", "account_id", "root", "qty", "daily_loss_limit", "daily_profit_stop",
         "commission", "tp_points", "sl_points", "slip_ticks", "secrets_prefix"}


class ConfigError(ValueError):
    pass


@dataclass(frozen=True)
class Account:
    name: str
    enabled: bool
    mode: str
    env: str
    account_id: int
    root: str
    qty: int
    daily_loss_limit: float
    daily_profit_stop: float
    commission: float
    secrets_prefix: str
    tp_points: float | None = None
    sl_points: float | None = None
    slip_ticks: int = DEFAULT_SLIP_TICKS

    @property
    def point_value(self) -> float:
        return POINT_VALUE[self.root]


@dataclass(frozen=True)
class CopyConfig:
    accounts: tuple[Account, ...] = ()
    kill_status_url: str = DEFAULT_KILL_URL
    port: int = DEFAULT_PORT

    def enabled(self) -> list[Account]:
        return [a for a in self.accounts if a.enabled]

    def get(self, name: str) -> Account:
        for a in self.accounts:
            if a.name == name:
                return a
        raise ConfigError(f"no account named {name!r} in accounts.yaml")


def accounts_path(env=os.environ) -> Path:
    v = (env.get(ACCOUNTS_ENV) or "").strip()
    return Path(v).expanduser() if v else ROOT / "accounts.yaml"


def copy_db_path(env=os.environ) -> Path:
    v = (env.get(COPY_DB_ENV) or "").strip()
    return Path(v).expanduser() if v else ROOT / "data" / "copy.db"


def copy_dir(env=os.environ) -> Path:
    v = (env.get(COPY_DIR_ENV) or "").strip()
    return Path(v).expanduser() if v else ROOT / "data" / "copy"


def symbol_for(root: str, paper_symbol: str) -> str:
    """The paper session's NQZ6 becomes MNQZ6 (root MNQ) or stays NQZ6 (root NQ). Anything that is not
    NQ + month code (H M U Z) + one digit is refused: ConfigError."""
    m = SYMBOL_RE.match(str(paper_symbol or ""))
    if not m or root not in ROOTS:
        raise ConfigError(f"refusing symbol {paper_symbol!r} for root {root!r}")
    return f"{root}{m.group(1)}"


def _num(raw: dict, key: str, where: str, *, positive=True, required=True, default=None):
    v = raw.get(key, default)
    if v is None and not required:
        return None
    if isinstance(v, bool) or not isinstance(v, (int, float)):
        raise ConfigError(f"{where}: {key} must be a number")
    if positive and v <= 0:
        raise ConfigError(f"{where}: {key} must be positive")
    if not positive and v < 0:
        raise ConfigError(f"{where}: {key} must not be negative")
    return v


def parse_account(raw: dict, idx: int) -> Account:
    where = f"accounts[{idx}]"
    if not isinstance(raw, dict):
        raise ConfigError(f"{where} must be a mapping")
    unknown = set(raw) - KNOWN
    if unknown:
        raise ConfigError(f"{where}: unknown key(s) {sorted(unknown)}")
    name = raw.get("name")
    if not isinstance(name, str) or not NAME_RE.match(name):
        raise ConfigError(f"{where}: name must be letters, digits, - or _")
    where = f"account {name}"
    enabled = raw.get("enabled", False)
    if not isinstance(enabled, bool):
        raise ConfigError(f"{where}: enabled must be true or false")
    mode = raw.get("mode", "dry_run")
    if mode not in MODES:
        raise ConfigError(f"{where}: mode must be one of {MODES}")
    env = raw.get("env")
    if env not in ENVS:
        raise ConfigError(f"{where}: env must be one of {ENVS}")
    if mode == "live" and env != "live":
        raise ConfigError(f"{where}: mode live requires env live")
    if mode == "demo" and env != "demo":
        raise ConfigError(f"{where}: mode demo requires env demo")
    aid = raw.get("account_id")
    if isinstance(aid, bool) or not isinstance(aid, int) or aid < 0:
        raise ConfigError(f"{where}: account_id must be an integer")
    if mode != "dry_run" and aid == 0:
        raise ConfigError(f"{where}: account_id is required for mode {mode}")
    root = raw.get("root")
    if root not in ROOTS:
        raise ConfigError(f"{where}: root must be one of {ROOTS}")
    qty = _num(raw, "qty", where)
    if int(qty) != qty:
        raise ConfigError(f"{where}: qty must be a whole number")
    prefix = raw.get("secrets_prefix")
    if not isinstance(prefix, str) or not PREFIX_RE.match(prefix):
        raise ConfigError(f"{where}: secrets_prefix must be UPPER_SNAKE_CASE")
    if prefix == "TRADOVATE" or prefix.startswith("TRADOVATE_"):
        raise ConfigError(f"{where}: secrets_prefix must not be TRADOVATE (that is the paper feed's login)")
    slip = raw.get("slip_ticks", DEFAULT_SLIP_TICKS)
    if isinstance(slip, bool) or not isinstance(slip, int) or slip < 0:
        raise ConfigError(f"{where}: slip_ticks must be a whole number")
    return Account(
        name=name, enabled=enabled, mode=mode, env=env, account_id=aid, root=root, qty=int(qty),
        daily_loss_limit=float(_num(raw, "daily_loss_limit", where)),
        daily_profit_stop=float(_num(raw, "daily_profit_stop", where)),
        commission=float(_num(raw, "commission", where, positive=False)),
        secrets_prefix=prefix, slip_ticks=slip,
        tp_points=_num(raw, "tp_points", where, required=False),
        sl_points=_num(raw, "sl_points", where, required=False))


def load(path: Path | None = None) -> CopyConfig:
    """The parsed accounts.yaml; an absent file is `CopyConfig()` (no accounts). Anything wrong raises ConfigError."""
    p = Path(path) if path else accounts_path()
    if not p.is_file():
        return CopyConfig()
    try:
        doc = yaml.safe_load(p.read_text()) or {}
    except (OSError, yaml.YAMLError) as e:
        raise ConfigError(f"{p.name} unreadable: {type(e).__name__}") from None
    if not isinstance(doc, dict):
        raise ConfigError(f"{p.name} must be a mapping")
    unknown = set(doc) - {"accounts", "kill_status_url", "port"}
    if unknown:
        raise ConfigError(f"{p.name}: unknown key(s) {sorted(unknown)}")
    raw = doc.get("accounts") or []
    if not isinstance(raw, list):
        raise ConfigError("accounts must be a list")
    accts = tuple(parse_account(r, i) for i, r in enumerate(raw))
    for what, vals in (("name", [a.name for a in accts]), ("secrets_prefix", [a.secrets_prefix for a in accts]),
                       ("account_id", [a.account_id for a in accts if a.account_id])):
        dup = {v for v in vals if vals.count(v) > 1}
        if dup:
            raise ConfigError(f"duplicate {what}: {sorted(dup)}")
    port = doc.get("port", DEFAULT_PORT)
    if isinstance(port, bool) or not isinstance(port, int) or not 1 <= port <= 65535:
        raise ConfigError("port must be 1..65535")
    url = doc.get("kill_status_url", DEFAULT_KILL_URL)
    if not isinstance(url, str) or not url.startswith("http://127.0.0.1"):
        raise ConfigError("kill_status_url must be a loopback http://127.0.0.1... URL")
    return CopyConfig(accts, url, port)
