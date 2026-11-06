"""Copy trading commands.

  uv run python -m trade_jev.copytrade run                 the daemon (launchd com.tradejev.copy)
  uv run python -m trade_jev.copytrade status              every account's status row from data/copy.db
  uv run python -m trade_jev.copytrade preflight NAME      read-only checks: account/list, contract/find
  uv run python -m trade_jev.copytrade clear-halts NAME    clear today's day halt for an account
  uv run python -m trade_jev.copytrade flatten-all [--direct]
        halt every account for the ET day and flatten; the daemon does it, --direct (daemon down, owner only)
        does it from this terminal
  uv run python -m trade_jev.copytrade rehearse NAME       owner only, at a terminal, demo env only: places ONE
        real order on the demo account to check the API assumptions (printed pass/fail)
"""

from __future__ import annotations

import argparse
import asyncio
import json
import signal
import sys
import time
import urllib.error
import urllib.request
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

from trade_jev import tradovate_token as tt
from trade_jev.copytrade import config as cfgmod
from trade_jev.copytrade.broker import NullBroker, TradovateBroker, make_http
from trade_jev.copytrade.daemon import FLATTEN_ALL_HEADER, AlreadyRunning, Daemon, lock_held, take_lock, db_kill_status
from trade_jev.copytrade.ledger import Ledger, et_day
from trade_jev.copytrade.tokens import AccountKeepalive, RenewBudget
from trade_jev.executor.broker import DryRun

RESONANT_DB = "~/.resonant/resonant.db"   # opened read-only by the kill watcher's fallback, never written


def build_account(acct, ledger, directory, http_factory=None, dotenv=None, budget=None, pools=None):
    """(broker, keepalive) for one account. dry_run accounts get a NullBroker and a keepalive only when the
    prefix has tokens (so the panel can show token health)."""
    store = directory / acct.name / "tradovate_token.json"
    dotenv = dotenv if dotenv is not None else tt.dotenv_path()
    pool = ThreadPoolExecutor(max_workers=2, thread_name_prefix=f"copy-{acct.name}")
    if pools is not None:
        pools[acct.name] = pool
    http = (http_factory or make_http)(pool)
    ka = AccountKeepalive(acct.name, acct.secrets_prefix, acct.env, store, dotenv,
                          budget or RenewBudget(directory / "renew_budget.json"), http=http)
    if acct.mode == "dry_run":
        return DryRun(NullBroker()), (ka if ka._seed() or store.exists() else None)

    def emit(kind, detail, notify=False):
        ledger.event(acct.name, kind, detail, notify=notify)
    return TradovateBroker(acct, ka.access_token, ka.secrets, http=http, ledger=ledger, emit=emit), ka


async def run_daemon(cfg, ledger, directory, port=None) -> int:
    pools: dict = {}
    budget = RenewBudget(directory / "renew_budget.json")
    dotenv = tt.dotenv_path()

    # one build per account: the keepalive the broker reads is the one the daemon runs
    built = {a.name: build_account(a, ledger, directory, dotenv=dotenv, budget=budget, pools=pools)
             for a in cfg.enabled()}
    d = Daemon(cfg, ledger, lambda a, ka: built[a.name][0], lambda a: built[a.name][1],
               kill_db=lambda: db_kill_status(Path(RESONANT_DB).expanduser()))
    await d.start(port or cfg.port)
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, lambda: asyncio.ensure_future(d.stop()))   # stop, never flatten
    print(f"[copy] running: {[a.name for a in cfg.enabled()]} on 127.0.0.1:{port or cfg.port}", flush=True)
    await d._stop.wait()
    return 0


def _print(*a):
    print(*a, flush=True)


def cmd_status(cfg, ledger) -> int:
    rows = ledger.statuses()
    if not rows:
        _print("no copy status rows yet (is the daemon running?)")
    for r in rows:
        _print(f"{r['name']}: {r['mode']}/{r['env']} {r['root']} x{r['qty']} pos {r['pos_side']:+d}x{r['pos_qty']} "
               f"realized {r['realized']} open {r['open_pnl']} entries {r['entries']} "
               f"{'HALTED ' + str(r['halt_kind']) if r['halted'] else ''} gate: {r['gate_reason'] or 'open'} "
               f"token {'ok' if r['token_ok'] else 'BAD'} updated {r['updated_at']}")
    return 0


def cmd_clear_halts(cfg, ledger, name) -> int:
    cfg.get(name)
    n = ledger.clear_halts(name, et_day(time.time_ns()))
    ledger.event(name, "halts_cleared", f"{n} halt(s) cleared by the owner", notify=False)
    _print(f"cleared {n} halt(s) for {name} today (the daemon notices within a few seconds)")
    return 0


def _post_flatten_all(port: int, timeout: float = 30.0) -> dict:
    req = urllib.request.Request(f"http://127.0.0.1:{port}/flatten-all", b"{}", {
        "Content-Type": "application/json", FLATTEN_ALL_HEADER: "flatten-all"})
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return json.loads(r.read())


def cmd_flatten_all(cfg, ledger, directory, direct, *, isatty=None, ask=input, http_factory=None,
                    dotenv=None) -> int:
    if not direct:
        try:
            out = _post_flatten_all(cfg.port)
        except (urllib.error.URLError, OSError) as e:
            _print(f"the copy daemon is not reachable ({type(e).__name__}); with it down use --direct (owner only)")
            return 1
        _print(json.dumps(out))
        return 0 if out.get("ok") else 1
    isatty = sys.stdin.isatty() if isatty is None else isatty
    if lock_held(directory):
        _print("refusing --direct: the daemon is running; run without --direct")
        return 1
    if not isatty:
        _print("refusing --direct: needs a terminal")
        return 1
    live = [a for a in cfg.enabled() if a.mode != "dry_run"]
    if ask(f"flatten {[a.name for a in live]} directly? type FLATTEN: ").strip() != "FLATTEN":
        _print("not confirmed")
        return 1
    rc = 0
    for a in live:
        broker, ka = build_account(a, ledger, directory, http_factory, dotenv)

        async def go():
            if ka is not None:   # the daemon may have been down for hours: seed from .env, renew or log in first
                try:
                    await ka.tick()
                except Exception:  # noqa: BLE001
                    pass
            return await broker.flatten()
        try:
            ack = asyncio.run(go())
            err = None if ack.ok else str(ack.error)
        except Exception as e:  # noqa: BLE001
            err = f"{type(e).__name__}"
        if err is None:
            _print(f"{a.name}: flat")
        else:
            _print(f"{a.name}: FAILED: {err}. CLOSE IT BY HAND in the Tradovate UI (trader.tradovate.com): "
                   "flatten the position and cancel its orders.")
        ledger.event(a.name, "flatten_all_direct", "ok" if err is None else err, notify=False)
        rc |= 0 if err is None else 1
    return rc


def cmd_preflight(cfg, ledger, directory, name, symbol=None, http_factory=None, dotenv=None) -> int:
    """Read-only: account/list (the id is there, with its name) and contract/find. Places nothing."""
    a = cfg.get(name)
    broker, ka = build_account(a, ledger, directory, http_factory, dotenv)
    if a.mode == "dry_run" and not isinstance(broker, TradovateBroker):
        broker = TradovateBroker(a, ka.access_token if ka else (lambda: None), ka.secrets if ka else (lambda: ()),
                                 http=(http_factory or make_http)(None), ledger=None)
    ok = True

    async def go():
        nonlocal ok
        try:
            spec = await broker._spec()
            _print(f"PASS account {a.account_id} found: {spec}")
        except Exception as e:  # noqa: BLE001
            ok = False
            _print(f"FAIL account/list: {broker._red(e)}")
        sym = symbol or a.root + "Z6"
        try:
            await broker._contract_ok(sym)
            _print(f"PASS contract {sym} found")
        except Exception as e:  # noqa: BLE001
            ok = False
            _print(f"FAIL contract/find {sym}: {broker._red(e)}")
    asyncio.run(go())
    return 0 if ok else 1


def cmd_rehearse(cfg, ledger, directory, name, *, isatty=None, ask=input, http_factory=None, dotenv=None,
                 sleep=time.sleep) -> int:
    """OWNER ONLY. Places one real order on a DEMO account to check the plan's API assumptions."""
    isatty = sys.stdin.isatty() if isatty is None else isatty
    a = cfg.get(name)
    if not isatty:
        _print("refusing: rehearse needs a terminal (it places a real demo order)")
        return 2
    if a.env != "demo" or a.mode == "live":
        _print("refusing: rehearse only runs against env demo")
        return 2
    if a.mode == "dry_run" and a.account_id == 0:
        _print("refusing: set account_id first (preflight prints it)")
        return 2
    if ask(f"this places a real order on demo account {a.name}. Type the account name to continue: ").strip() != a.name:
        _print("not confirmed")
        return 2
    px = float(ask("current touch price of the contract (e.g. 24500.25): ").strip())
    broker, ka = build_account(a, ledger, directory, http_factory, dotenv)
    sym = ask(f"contract (e.g. {a.root}Z6): ").strip()
    broker = TradovateBroker(a, ka.access_token, ka.secrets, http=(http_factory or make_http)(None), ledger=None)
    results: list[tuple[str, bool, str]] = []

    def check(label, ok, detail=""):
        results.append((label, ok, detail))
        _print(f"{'PASS' if ok else 'FAIL'} {label} {detail}")

    async def go():
        broker.set_symbol(sym)
        try:
            check("flat before", not await broker.position(True))
            ack = await broker.enter(1, 1, px + 1.0, 40, 40)       # 10 pt TP and SL, marketable IOC limit
            check("OSO entry with IOC parent accepted and filled", ack.ok, str(ack.error or ""))
            if ack.ok:
                check("children verified live after modifyorder (OCO kept)", bool(ack.info.get("modified")))
                live = await broker.orders(True)
                check("two live bracket children", len(live) == 2, f"{len(live)} live")
            fa = await broker.flatten()
            check("liquidate + sweep leaves flat, no orders", fa.ok, str(fa.error or ""))
            check("no live orders left", not await broker.orders(True))
            probe = await broker.probe_unfilled_ioc(px - 100.0)
            if "error" in probe:
                check("IOC parent expires unfilled (probe)", False, probe["error"])
            else:
                check("IOC parent expired unfilled", probe["parent"] in ("Canceled", "Expired"), str(probe["parent"]))
                _print(f"INFO Tradovate {'KEPT' if probe['children_live'] else 'cancelled'} the OSO children "
                       f"when the IOC parent expired unfilled ({probe['children']}); the daemon sweeps either way")
                check("sweep leaves no live orders after the unfilled probe", probe["swept"])
        except Exception as e:  # noqa: BLE001
            check("rehearsal ran", False, broker._red(e))
    asyncio.run(go())
    return 0 if all(ok for _, ok, _ in results) else 1


def main(argv=None, **inject) -> int:
    ap = argparse.ArgumentParser(prog="python -m trade_jev.copytrade", description=__doc__.split("\n")[0])
    sub = ap.add_subparsers(dest="cmd", required=True)
    sub.add_parser("run")
    sub.add_parser("status")
    for n in ("preflight", "clear-halts", "rehearse"):
        sp = sub.add_parser(n)
        sp.add_argument("name")
        if n == "preflight":
            sp.add_argument("--symbol")
    fa = sub.add_parser("flatten-all")
    fa.add_argument("--direct", action="store_true")
    args = ap.parse_args(argv)
    try:
        cfg = inject.get("cfg") or cfgmod.load()
    except cfgmod.ConfigError as e:
        print(f"accounts.yaml: {e}", file=sys.stderr)
        return 2
    directory = inject.get("directory") or cfgmod.copy_dir()
    ledger = Ledger(inject.get("db") or cfgmod.copy_db_path())
    common = {k: inject[k] for k in ("http_factory", "dotenv") if k in inject}
    try:
        if args.cmd == "run":
            if not cfg.enabled():
                print("[copy] no enabled accounts in accounts.yaml; idling", flush=True)
                while True:
                    time.sleep(3600)
            try:
                lock = take_lock(directory)
            except AlreadyRunning as e:
                print(f"[copy] {e}", file=sys.stderr)
                return 1
            try:
                return asyncio.run(run_daemon(cfg, ledger, directory))
            finally:
                lock.close()
        if args.cmd == "status":
            return cmd_status(cfg, ledger)
        if args.cmd == "clear-halts":
            return cmd_clear_halts(cfg, ledger, args.name)
        if args.cmd == "flatten-all":
            return cmd_flatten_all(cfg, ledger, directory, args.direct, **{k: v for k, v in inject.items()
                                   if k in ("isatty", "ask", "http_factory", "dotenv")})
        if args.cmd == "preflight":
            return cmd_preflight(cfg, ledger, directory, args.name, args.symbol, **common)
        if args.cmd == "rehearse":
            return cmd_rehearse(cfg, ledger, directory, args.name, **{k: v for k, v in inject.items()
                                if k in ("isatty", "ask", "http_factory", "dotenv", "sleep")})
    except cfgmod.ConfigError as e:
        print(str(e), file=sys.stderr)
        return 2
    return 0


if __name__ == "__main__":
    sys.exit(main())
