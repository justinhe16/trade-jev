"""Rank replay_grid.py results with overfitting checks.

  uv run python scripts/analyze_replays.py runs/replays/grid-<ts>.csv
"""

from __future__ import annotations

import sys

import numpy as np
import pandas as pd

KEYS = ["min_conf", "agree", "min_hold", "stop", "target"]

df = pd.read_csv(sys.argv[1])
pnl_cols = [c for c in df if c.startswith("pnl_")]
tr_cols = [c for c in df if c.startswith("trades_")]
days = [c[4:] for c in pnl_cols]
P = df[pnl_cols].to_numpy()

df["total"] = P.sum(1)
df["worst_day"] = P.min(1)
df["pos_days"] = (P > 0).sum(1)
df["trades"] = df[tr_cols].sum(axis=1)
df["per_trade"] = df["total"] / df["trades"].replace(0, np.nan)

# neighborhood robustness: mean total over configs one grid step away in a single param
levels = {k: sorted(df[k].unique()) for k in KEYS}
idx = {tuple(r): i for i, r in enumerate(df[KEYS].itertuples(index=False, name=None))}
nb = []
for row in df[KEYS].itertuples(index=False, name=None):
    vals = []
    for j, k in enumerate(KEYS):
        pos = levels[k].index(row[j])
        for q in (pos - 1, pos + 1):
            if 0 <= q < len(levels[k]):
                alt = list(row)
                alt[j] = levels[k][q]
                vals.append(df["total"].iat[idx[tuple(alt)]])
    nb.append(np.mean(vals))
df["neighbors_avg"] = nb

pd.set_option("display.width", 200)
pd.set_option("display.max_columns", 30)
show = KEYS + pnl_cols + ["total", "worst_day", "pos_days", "trades", "neighbors_avg"]
fmt = df[show].copy()
fmt.columns = [c.replace("pnl_2026-", "") for c in fmt.columns]

print(f"{len(df)} settings · days {days}\n")
print("== Top 10 by total P&L ==")
print(fmt.sort_values("total", ascending=False).head(10).to_string(index=False))

print("\n== Top 10 among configs profitable every day (ranked by worst day) ==")
all_pos = fmt[df["pos_days"] == len(days)]
print(f"{len(all_pos)} configs are positive on all {len(days)} days")
print(all_pos.sort_values("worst_day", ascending=False).head(10).to_string(index=False))

print("\n== Top 10 by robustness (neighbors' average total, needs >= 20 trades) ==")
print(fmt[df["trades"] >= 20].sort_values("neighbors_avg", ascending=False).head(10).to_string(index=False))

print("\n== Leave-one-day-out: pick best on the other days, score on the held-out day ==")
for i, d in enumerate(days):
    others = np.delete(P, i, axis=1).sum(1)
    best = int(np.argmax(others))
    held = P[best, i]
    rank = int((P[:, i] > held).sum()) + 1
    cfg = ", ".join(f"{k}={df[k].iat[best]}" for k in KEYS)
    print(f"  hold out {d}: picked [{cfg}] (others ${others[best]:,.0f}) → held-out ${held:,.0f} "
          f"(rank {rank}/{len(df)}; median config ${np.median(P[:, i]):,.0f})")

print("\n== Marginal effect: mean total by each param level ==")
for k in KEYS:
    g = df.groupby(k)["total"].agg(["mean", "median", "max"]).round(0)
    print(f"\n{k}\n{g.to_string()}")
