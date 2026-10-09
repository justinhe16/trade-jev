"""Executor: places the live runner's intents at a broker, inside hard limits. Always starts disarmed;
arm it on the Live page (http://localhost:8765/live).

  uv run python -m trade_jev.executor --broker fake --allow-replay      # with a --from-file rehearsal
  uv run python -m trade_jev.executor --broker NAME --dry-run           # a private backend: highlight, never click
  uv run python -m trade_jev.executor --broker NAME --account ACCT      # a private backend: real orders

Real broker backends are not in this repo. `--broker NAME` loads `private/NAME.py` (gitignored), which
must define `async def create(profile: Path, account: str | None) -> Broker` (see executor/broker.py).

Pair it with:  uv run python -m trade_jev.live ... --exec-url http://127.0.0.1:8766
"""

from __future__ import annotations

import argparse
import asyncio
import importlib.util
import threading
from datetime import datetime
from pathlib import Path

from trade_jev import ROOT
from trade_jev.data import ET
from trade_jev.executor.broker import DryRun, FakeBroker
from trade_jev.executor.core import Executor, Limits
from trade_jev.executor.server import Reporter, make_server


PRIVATE = ROOT / "private"


def load_backend(name: str):
    path = PRIVATE / f"{name}.py"
    if not path.exists():
        have = sorted(p.stem for p in PRIVATE.glob("*.py") if p.stem != "probe") if PRIVATE.exists() else []
        raise SystemExit(f"no broker backend private/{name}.py (have: {have or 'none'}); "
                         f"use --broker fake, or add a private backend")
    spec = importlib.util.spec_from_file_location(f"private_{name}", path)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


async def amain() -> None:
    ap = argparse.ArgumentParser(description="Place the live runner's intents at a broker.")
    ap.add_argument("--broker", default="fake", help="'fake', or the name of a backend in private/")
    ap.add_argument("--dry-run", action="store_true", help="locate and highlight, never click")
    ap.add_argument("--port", type=int, default=8766)
    ap.add_argument("--live-url", default="http://127.0.0.1:8765")
    ap.add_argument("--qty", type=int, default=1, help="contracts per entry (NQ)")
    ap.add_argument("--max-daily-loss", type=float, default=2000.0, help="$; flatten and halt for the day")
    ap.add_argument("--max-orders", type=int, default=40, help="entries (new positions) per day")
    ap.add_argument("--slip-ticks", type=int, default=2, help="marketable limit: ask + N ticks (bid − N)")
    ap.add_argument("--profile", default=None,
                    help="browser profile for a web backend (keeps the login); default ~/.trade-jev-<broker>")
    ap.add_argument("--account", default=None, help="account name the backend's preflight must see")
    ap.add_argument("--allow-replay", action="store_true", help="act on intents from --from-file rehearsals")
    args = ap.parse_args()

    lim = Limits(qty=args.qty, max_daily_loss=args.max_daily_loss, max_orders=args.max_orders,
                 slip_ticks=args.slip_ticks, allow_replay=args.allow_replay)
    if args.broker == "fake":
        broker = FakeBroker()
    else:
        profile = Path(args.profile or Path.home() / f".trade-jev-{args.broker}").expanduser()
        broker = await load_backend(args.broker).create(profile, args.account)
    if args.dry_run:
        broker = DryRun(broker)

    day = datetime.now(ET).date().isoformat()
    log = ROOT / "runs" / "executor" / f"{day}.jsonl"
    log.parent.mkdir(parents=True, exist_ok=True)
    ex = Executor(broker, lim, Reporter(args.live_url), log)
    if hasattr(broker, "preflight") or hasattr(getattr(broker, "inner", None), "preflight"):
        pf = await (broker.preflight() if hasattr(broker, "preflight") else broker.inner.preflight())
        ex.preflight = pf
        if not pf.get("ok"):
            await ex.halt(f"preflight failed: {pf.get('problems')}")
    ex.log("start", broker=broker.name, limits=lim.__dict__)

    loop = asyncio.get_running_loop()
    srv = make_server(ex, loop, args.port)
    threading.Thread(target=srv.serve_forever, daemon=True).start()
    print(f"executor ({broker.name}) on http://127.0.0.1:{args.port} · reporting to {args.live_url} · "
          f"log {log.relative_to(ROOT)}", flush=True)
    print(f"limits: {lim.qty} contract(s), −${lim.max_daily_loss:,.0f}/day, {lim.max_orders} orders, "
          f"limit = touch ± {lim.slip_ticks} ticks · DISARMED until you arm it on the Live page", flush=True)
    n = 0
    while True:
        await asyncio.sleep(1)
        await ex.watchdog_tick()
        n += 1
        if n % 2 == 0:
            await ex._push_status()


def main() -> None:
    try:
        asyncio.run(amain())
    except KeyboardInterrupt:
        print("\nexecutor stopped (any open position keeps its broker-side bracket).")


if __name__ == "__main__":
    main()
