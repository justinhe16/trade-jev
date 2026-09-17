"""Numbers behind results/FINDINGS.md, reproducible from stored runs (no Jev calls).

  uv run python scripts/findings.py --tune runs/<a> runs/<b> --test runs/<c> [--grid runs/replays/grid-<ts>.csv]

--tune: runs whose days were used to pick settings; --test: runs on days never used for tuning.
"""

from __future__ import annotations

import argparse
import asyncio
import json
from collections import Counter
from pathlib import Path

import numpy as np
import pandas as pd

from trade_jev.replay import Settings, load_market, load_runs, replay_day

NAMED = {
    "naive (every answer, 20/40 exits)": Settings(0.0, 1, 0, 20, 40),
    "first base (0.8 / 2 / 0s / 200)": Settings(0.8, 2, 0, 200, 0),
    "chosen (0.5 / 2 / 120s / 400)": Settings(0.5, 2, 120, 400, 0),
    "registered (0.7 / 4 / 30s / 200 / 100)": Settings(0.7, 4, 30, 200, 100),
    "every answer, no exits": Settings(0.0, 1, 0, 0, 0),
}


def live_table(run_dirs: list[Path]) -> pd.DataFrame:
    rows = []
    for rd in run_dirs:
        summ = json.loads((rd / "summary.json").read_text())
        for pol, by in summ.items():
            for day, pnl in by["all"]["daily_pnl"].items():
                rows.append({"run": rd.name, "day": day, "policy": pol.split("[")[0], "pnl": pnl})
    return rows and pd.DataFrame(rows).pivot_table(index=["day", "run"], columns="policy", values="pnl").reset_index()


def answer_stats(run_dirs: list[Path]) -> pd.DataFrame:
    rows = []
    for day, rd in sorted(load_runs(run_dirs).items()):
        ds = sorted(rd.answers.values(), key=lambda d: d["t_ns"])
        raw = [d.get("raw_action") or d["action"] for d in ds]
        top = np.array([d["probs"].get(r, 0) for d, r in zip(ds, raw)])
        flips = sum({a, b} == {"BUY", "SELL"} for a, b in zip(raw, raw[1:]))
        c = Counter(raw)
        rows.append({"day": day, "n": len(ds), "buy": c["BUY"], "sell": c["SELL"], "hold": c["HOLD"],
                     "median_top_p": round(float(np.median(top)), 2),
                     "share_top_ge_0.8": round(float((top >= 0.8).mean()), 2),
                     "flip_rate": round(flips / max(len(raw) - 1, 1), 2)})
    return pd.DataFrame(rows)


async def named_replays(run_dirs: list[Path]) -> tuple[pd.DataFrame, pd.DataFrame]:
    rows, moves = [], []
    for day, rd in sorted(load_runs(run_dirs).items()):
        mkt, grid = load_market(rd)
        m0, m1 = float(mkt.mid(int(mkt.row_at(grid[0])))), float(mkt.mid(int(mkt.row_at(grid[-1]))))
        moves.append({"day": day, "open_to_close_pts": round(m1 - m0, 2)})
        for name, s in NAMED.items():
            r = await replay_day(rd, mkt, grid, s)
            rows.append({"day": day, "settings": name, "pnl": r.pnl, "trades": len(r.trades),
                         "wins": sum(t.pnl > 0 for t in r.trades)})
    return pd.DataFrame(rows), pd.DataFrame(moves)


def summarize_named(df: pd.DataFrame) -> pd.DataFrame:
    g = df.groupby("settings")
    out = pd.DataFrame({
        "total": g.pnl.sum(), "mean_day": g.pnl.mean(), "worst_day": g.pnl.min(),
        "pos_days": g.pnl.apply(lambda s: int((s > 0).sum())), "days": g.pnl.size(),
        "trades": g.trades.sum(), "win_rate": g.wins.sum() / g.trades.sum().replace(0, np.nan),
    })
    return out.round(2).sort_values("total", ascending=False)


def grid_oos(grid_csv: Path, tune_days: list[str], test_days: list[str]) -> None:
    df = pd.read_csv(grid_csv)
    keys = ["min_conf", "agree", "min_hold", "stop", "target"]
    tune = df[[f"pnl_{d}" for d in tune_days]].sum(axis=1)
    test = df[[f"pnl_{d}" for d in test_days]].sum(axis=1)
    allp = df[[c for c in df if c.startswith("pnl_")]]
    print(f"\n== Grid: {len(df)} settings · tune {len(tune_days)} days · test {len(test_days)} days ==")
    print(f"share of settings profitable on test days: {(test > 0).mean():.0%}; median test total ${test.median():,.0f}")
    print(f"corr(tune total, test total) across settings: {np.corrcoef(tune, test)[0, 1]:.2f}")
    top = tune.nlargest(10).index
    print("top-10 on tune days → their test-day totals:")
    for i in top:
        cfg = " ".join(f"{k}={df.at[i, k]}" for k in keys)
        rank = int((test > test[i]).sum()) + 1
        print(f"  {cfg:<52} tune ${tune[i]:>9,.0f}  test ${test[i]:>9,.0f}  (test rank {rank}/{len(df)})")
    best = test.idxmax()
    print("best on test days (hindsight): " + " ".join(f"{k}={df.at[best, k]}" for k in keys) + f"  ${test[best]:,.0f}")
    print("\nmean 15-day total by level:")
    tot = allp.sum(axis=1)
    for k in keys:
        print(f"  {k:<9} " + "  ".join(f"{lv}: ${v:,.0f}" for lv, v in df.assign(t=tot).groupby(k)["t"].mean().items()))


async def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--tune", nargs="+", type=Path, required=True)
    ap.add_argument("--test", nargs="+", type=Path, required=True)
    ap.add_argument("--grid", type=Path)
    a = ap.parse_args()
    pd.set_option("display.width", 200)
    pd.set_option("display.max_columns", 20)

    print("== Live runs: net $ per day (Jev under that run's settings vs baselines) ==")
    live = live_table(a.tune + a.test)
    print(live.to_string(index=False))
    for name, runs in (("tune", a.tune), ("test", a.test)):
        lt = live[live.run.isin([r.name for r in runs])]
        print(f"{name} totals: " + "  ".join(f"{c} ${lt[c].sum():,.0f}" for c in ("jev", "imbalance", "random", "hold")))

    print("\n== Jev answers per day ==")
    print(answer_stats(a.tune + a.test).to_string(index=False))

    for name, runs in (("tune", a.tune), ("test", a.test)):
        named, moves = await named_replays(runs)
        print(f"\n== Named settings replayed on {name} days ==")
        print(summarize_named(named).to_string())
        if name == "test":
            chosen = named[named.settings == "chosen (0.5 / 2 / 120s / 400)"].set_index("day")
            m = moves.set_index("day").join(chosen)
            m["abs_move"] = m.open_to_close_pts.abs()
            print("\nchosen settings vs market move on test days:")
            print(m[["open_to_close_pts", "pnl", "trades"]].to_string())
            print(f"corr(|open→close move|, chosen P&L): {np.corrcoef(m.abs_move, m.pnl)[0, 1]:.2f}")

    if a.grid:
        tune_days = sorted(load_runs(a.tune))
        test_days = sorted(load_runs(a.test))
        grid_oos(a.grid, tune_days, test_days)


if __name__ == "__main__":
    asyncio.run(main())
