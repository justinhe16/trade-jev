"""Replay a full grid of settings over one or more runs to find settings that hold up. No Jev calls.

  uv run python scripts/replay_grid.py runs/<id> [runs/<id> ...]   → runs/replays/grid-<ts>.csv
  uv run python scripts/analyze_replays.py runs/replays/grid-<ts>.csv
"""

from __future__ import annotations

import asyncio
import csv
import itertools
import sys
import time
from concurrent.futures import ProcessPoolExecutor
from datetime import datetime
from pathlib import Path

from trade_jev.replay import RunDay, Settings, load_market, load_runs, replay_day

ROOT = Path(__file__).resolve().parents[1]
GRID = {
    "min_conf": [0.0, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9],
    "agree": [1, 2, 3, 4],
    "min_hold": [0, 30, 60, 120, 300],
    "stop": [0, 100, 200, 400],
    "target": [0, 100, 200],
}
KEYS = list(GRID)


def replay_all(rd: RunDay) -> list[tuple]:
    day, grid = load_market(rd)

    async def go():
        rows = []
        for combo in itertools.product(*GRID.values()):
            r = await replay_day(rd, day, grid, Settings(*combo))
            rows.append((*combo, len(r.trades), round(r.pnl, 2)))
        return rows

    return asyncio.run(go())


def main(run_dirs: list[Path]) -> None:
    runs = load_runs(run_dirs)
    days = sorted(runs)
    n = len(list(itertools.product(*GRID.values())))
    print(f"{n} settings × {len(days)} days {days}", flush=True)
    t0 = time.time()
    with ProcessPoolExecutor(max_workers=min(4, len(days))) as ex:
        per_day = dict(zip(days, ex.map(replay_all, [runs[d] for d in days])))
    print(f"{n * len(days):,} replays in {time.time() - t0:.0f}s", flush=True)

    out = ROOT / "runs" / "replays" / f"grid-{datetime.now():%Y%m%d-%H%M%S}.csv"
    out.parent.mkdir(parents=True, exist_ok=True)
    with out.open("w", newline="") as fh:
        w = csv.writer(fh)
        w.writerow([*KEYS, *[f"pnl_{d}" for d in days], *[f"trades_{d}" for d in days]])
        for i in range(n):
            combo = per_day[days[0]][i][:len(KEYS)]
            w.writerow([*combo, *[per_day[d][i][-1] for d in days], *[per_day[d][i][-2] for d in days]])
    print(out)


if __name__ == "__main__":
    main([Path(p) for p in sys.argv[1:]])
