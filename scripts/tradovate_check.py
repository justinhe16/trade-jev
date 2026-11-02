"""Smoke test for the Tradovate source (run by hand, with .env filled in).

  uv run python scripts/tradovate_check.py --seconds 30
  uv run python scripts/tradovate_check.py --env demo --symbol NQZ6

Logs in, resolves the contract, subscribes the DOM and the tick chart for N seconds and prints what came
back: token expiry (time only), user status, which md host and token kind authorized, DOM update rate,
book depth seen, trade ticks and the buy / sell / unknown aggressor split. Exit 0 only if DOM updates
arrived. It calls auth endpoints, GET contract/find and the md/* market-data endpoints only: never an
order, position or account endpoint. It never prints a secret.

Note: Tradovate allows 2 concurrent sessions per user. Running this while the daily job is live may kick
one of them; run it outside 09:15-16:00 ET.
"""

from __future__ import annotations

import argparse
import asyncio
import os
import sys
import time
from datetime import datetime

from trade_jev import tradovate as tv
from trade_jev.live import front_month, now_et


async def run(argv: list[str] | None = None, env=os.environ) -> int:
    ap = argparse.ArgumentParser(description="Tradovate source smoke test (read-only market data).")
    ap.add_argument("--seconds", type=float, default=30.0)
    ap.add_argument("--symbol", default=None, help="default: the NQ front month, e.g. NQZ6")
    ap.add_argument("--env", choices=sorted(tv.REST), default=None, help="default: $TRADOVATE_ENV, else live")
    args = ap.parse_args(argv)

    env = dict(env)
    if args.env:
        env["TRADOVATE_ENV"] = args.env
    missing = tv.missing_credentials(env)
    if missing:
        print("FAIL: tradovate credentials missing: " + ", ".join(missing))
        return 2
    try:
        cfg = tv.TradovateConfig.from_env(env)
    except tv.TradovateError as e:
        print(f"FAIL: {e}")
        return 2
    symbol = args.symbol or front_month(now_et())
    print(f"env {cfg.env}: REST {cfg.base}; md hosts {', '.join(u.split('/')[2] for u in cfg.md_candidates())}; "
          f"symbol {symbol}")

    auth = tv.TradovateAuth(cfg)
    try:
        await auth.login()
    except tv.ApiKeyRejected as e:
        print(f"FAIL: {e}")
        print(f"      {e.hint}")
        return 3
    except tv.BadCredentials as e:
        print(f"FAIL: {e}")
        return 3
    except tv.TradovateError as e:
        print(f"FAIL: {auth.redact(e)}")
        return 3
    exp = datetime.fromtimestamp(auth.expires_at).strftime("%H:%M:%S")
    print(f"login ok; token expires {exp} (local); user status {auth.user_status}; "
          f"hasMarketData {auth.has_market_data}")

    src = tv.TradovateSource(symbol, 0, config=cfg, auth=auth)
    rows, t0 = 0, time.monotonic()
    gen = src.batches()
    try:
        async with asyncio.timeout(args.seconds):
            async for b in gen:
                rows += len(b)
    except TimeoutError:
        pass
    except tv.TradovateError as e:
        print(f"FAIL: {auth.redact(e)}")
        return 3
    finally:
        await gen.aclose()
    secs = max(time.monotonic() - t0, 1e-9)
    st = src.stats()
    print(f"md socket: host {st['md_host']}, token kind {st['md_token_kind']}; contract id {st['contract_id']}")
    print(f"DOM: {st['dom_updates']} updates ({st['dom_updates'] / secs:.1f}/s); levels per side: "
          f"max {st['max_bid_levels']}/{st['max_ask_levels']} (bid/ask), median "
          f"{st['median_bid_levels']:g}/{st['median_ask_levels']:g}")
    print(f"ticks: {st['ticks']} (buy {src.buys} / {src.buy_volume} lots, sell {src.sells} / {src.sell_volume}, "
          f"unknown {src.unknown} / {src.unknown_volume}); rows {rows}; reconnects {st['reconnects']}")
    if not st["dom_updates"]:
        print("FAIL: no DOM updates: likely no CME market-data entitlement for API access yet")
        return 4
    print("OK")
    return 0


if __name__ == "__main__":
    sys.exit(asyncio.run(run()))
