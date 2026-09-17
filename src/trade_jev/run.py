"""Run: backtest days while calling Jev; stores answers + trades in runs/<id>/.

  uv run python -m trade_jev.run --days 2026-06-23                     # jev + baselines, one day
  uv run python -m trade_jev.run --all                                 # all 15 days
  uv run python -m trade_jev.run --days 2026-06-23 --policies hold,random,imbalance   # no API key needed
"""

from __future__ import annotations

import argparse
import asyncio
import csv
import json
import time
from dataclasses import asdict, fields
from datetime import datetime

from trade_jev import ROOT
from trade_jev.data import decision_grid, list_days, load_day
from trade_jev.harness import Config, DayResult, Trade, equity_curve, run_day
from trade_jev.metrics import summarize
from trade_jev.policies import BASELINES, Gated, JevPolicy, JsonlCache, RateLimiter
from trade_jev.settings import DEFAULT


def build_policies(names: list[str], args) -> tuple[list, object | None]:
    out, client = [], None
    for n in names:
        if n == "jev":
            from typesafe_sdk import AsyncTypeSafeClient
            client = AsyncTypeSafeClient()
            jev = JevPolicy(client, JsonlCache(ROOT / "cache" / "jev.jsonl"),
                            RateLimiter(args.rps), encoder=args.encoder, model=args.model)
            out.append(Gated(jev, args.min_conf, args.agree, args.min_hold)
                       if args.agree > 1 or args.min_conf > 0 or args.min_hold > 0 else jev)
        else:
            out.append(BASELINES[n]())
    return out, client


async def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--days", default="", help="comma-separated YYYY-MM-DD")
    ap.add_argument("--all", action="store_true")
    ap.add_argument("--policies", default="jev,hold,random,imbalance")
    ap.add_argument("--encoder", default="raw_l10")
    ap.add_argument("--model", default="jev-latest")
    ap.add_argument("--rps", type=float, default=15.0, help="Jev requests/sec (limit is 20)")
    ap.add_argument("--parallel-days", type=int, default=3, help="days held in memory at once")
    ap.add_argument("--run-id", default=None)
    ap.add_argument("--min-conf", type=float, default=DEFAULT.min_conf, help="Jev: ignore answers below this prob")
    ap.add_argument("--agree", type=int, default=DEFAULT.agree, help="Jev: consecutive agreeing answers to act")
    ap.add_argument("--min-hold", type=float, default=DEFAULT.min_hold_s, help="Jev: min seconds before reversing")
    for f in fields(Config):
        ap.add_argument(f"--{f.name.replace('_', '-')}", type=type(f.default), default=f.default)
    args = ap.parse_args()

    cfg = Config(**{f.name: getattr(args, f.name) for f in fields(Config)})
    available = list_days()
    days = sorted(available) if args.all else [d for d in args.days.split(",") if d]
    if not days:
        ap.error("pass --days or --all")
    missing = [d for d in days if d not in available]
    if missing:
        ap.error(f"no data for {missing}; have {sorted(available)}")

    policies, client = build_policies(args.policies.split(","), args)
    run_id = args.run_id or datetime.now().strftime("%Y%m%d-%H%M%S")
    out = ROOT / "runs" / run_id
    out.mkdir(parents=True, exist_ok=True)
    (out / "config.json").write_text(json.dumps(
        {"config": asdict(cfg), "days": days, "policies": [p.name for p in policies],
         "encoder": args.encoder, "model": args.model,
         "gate": {"min_conf": args.min_conf, "agree": args.agree, "min_hold_s": args.min_hold}},
        indent=2))

    results: dict[str, list[DayResult]] = {p.name: [] for p in policies}
    book_rows_seen = snapshots = 0
    t_start = time.time()
    sem = asyncio.Semaphore(args.parallel_days)

    async def one_day(d: str) -> None:
        async with sem:
            t0 = time.time()
            grid = decision_grid(d, cfg.start_et, cfg.end_et, cfg.cadence_s)
            day = await asyncio.to_thread(load_day, d, grid)
            # early closes (e.g. Juneteenth) end before end_et — never decide on a stale book
            grid = grid[(grid >= day.ts[0]) & (grid <= day.ts[-1])]
            nonlocal book_rows_seen, snapshots
            snapshots += len(grid)
            book_rows_seen += int(day.row_at(grid[-1]) - day.row_at(grid[0]) + 1) if len(grid) else 0
            print(f"[{d}] loaded {len(day.ts):,} rows in {time.time() - t0:.0f}s; "
                  f"{len(grid)} decisions × {len(policies)} policies", flush=True)
            day_res = await asyncio.gather(*(run_day(day, grid, p, cfg) for p in policies))
            eq_dir = out / "equity"
            eq_dir.mkdir(exist_ok=True)
            eq = {"day": d, "symbol": day.symbol, "policies": {}}
            for p, r in zip(policies, day_res):
                results[p.name].append(r)
                curve = equity_curve(day, grid, r.trades)
                eq["t_ns"], eq["mid"] = curve["t_ns"], curve["mid"]
                eq["policies"][p.name] = {"equity": curve["equity"], "position": curve["position"]}
                print(f"[{d}] {p.name:>16}: {len(r.trades):4d} trades  ${r.pnl:>10,.2f}", flush=True)
            (eq_dir / f"{d}.json").write_text(json.dumps(eq))
            del day

    try:
        await asyncio.gather(*(one_day(d) for d in days))
    finally:
        if client is not None:
            await client.aclose()

    half = len(days) // 2
    splits = {"all": days} if len(days) < 4 else {"all": days, "dev": days[:half], "holdout": days[half:]}
    summary = {}
    for name, rs in results.items():
        pdir = out / name
        pdir.mkdir(exist_ok=True)
        with (pdir / "decisions.jsonl").open("w") as fh:
            for r in sorted(rs, key=lambda r: r.day):
                for dec in r.decisions:
                    fh.write(json.dumps(dec) + "\n")
        with (pdir / "trades.csv").open("w", newline="") as fh:
            w = csv.DictWriter(fh, fieldnames=[f.name for f in fields(Trade)])
            w.writeheader()
            for r in sorted(rs, key=lambda r: r.day):
                w.writerows(asdict(t) for t in r.trades)
        summary[name] = {s: summarize([r for r in rs if r.day in ds]) for s, ds in splits.items()}
    (out / "summary.json").write_text(json.dumps(summary, indent=2))

    wall_s = round(time.time() - t_start, 1)
    jev = [getattr(p, "inner", p) for p in policies]
    jev = [p for p in jev if isinstance(p, JevPolicy)]
    record = {
        "run_id": run_id,
        "finished_at": datetime.now().isoformat(timespec="seconds"),
        "wall_time_s": wall_s,
        "days": days,
        "n_days": len(days),
        "cadence_s": cfg.cadence_s,
        "snapshots_decided": snapshots,
        "book_updates_replayed": book_rows_seen,
        "jev_api_calls": sum(p.api_calls for p in jev),
        "jev_cache_hits": sum(p.cache_hits for p in jev),
        "jev_cost_usd": sum(m["all"]["api_cost_usd"] for n, m in summary.items() if n.startswith("jev")),
        "policies": {
            n: {"net_pnl": m["all"]["net_pnl"], "trades": m["all"]["trades"],
                "win_rate": m["all"]["win_rate"], "max_drawdown": m["all"]["max_drawdown"],
                "actions": m["all"]["actions"],
                **({"holdout_net_pnl": m["holdout"]["net_pnl"]} if "holdout" in m else {})}
            for n, m in summary.items()
        },
        "config": asdict(cfg),
        "encoder": args.encoder if jev else None,
        "model": args.model if jev else None,
        "gate": {"min_conf": args.min_conf, "agree": args.agree, "min_hold_s": args.min_hold}
        if jev else None,
    }
    (out / "results.json").write_text(json.dumps(record, indent=2))
    with (ROOT / "runs" / "index.jsonl").open("a") as fh:
        fh.write(json.dumps(record) + "\n")

    print(f"\nrun → {out}")
    print(f"{record['n_days']} days · {record['snapshots_decided']:,} snapshots decided · "
          f"{record['book_updates_replayed']:,} book updates · {wall_s}s · "
          f"Jev calls: {record['jev_api_calls']:,} (+{record['jev_cache_hits']:,} cached) · "
          f"${record['jev_cost_usd']:.4f}")
    hdr = f"{'policy':>16} {'split':>8} {'net $':>11} {'trades':>7} {'win%':>6} {'sharpe':>7} {'maxDD':>9} {'actions'}"
    print(hdr)
    for name, by in summary.items():
        for s, m in by.items():
            wr = f"{m['win_rate'] * 100:.0f}" if m["win_rate"] is not None else "-"
            print(f"{name:>16} {s:>8} {m['net_pnl']:>11,.2f} {m['trades']:>7} {wr:>6} "
                  f"{str(m['sharpe_daily_ann']):>7} {m['max_drawdown']:>9,.0f} {m['actions']}")

    if jev:
        print("\nview it: uv run python -m trade_jev.view")

if __name__ == "__main__":
    asyncio.run(main())
