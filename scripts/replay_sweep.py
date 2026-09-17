"""Quick replay table: a handful of settings over one or more runs, per day. No Jev calls.

  uv run python scripts/replay_sweep.py runs/<id> [runs/<id> ...]
"""

from __future__ import annotations

import asyncio
import itertools
import sys
from pathlib import Path

from trade_jev.replay import Settings, load_market, load_runs, replay_day

EXITS = [(20, 40), (200, 0), (0, 0)]  # (stop_ticks, target_ticks); 0 = off
CONF, AGREE, HOLD = (0.0, 0.5, 0.8), (1, 2, 3), (0, 60)


async def main(run_dirs: list[Path]) -> None:
    rows = []
    for day_s, rd in sorted(load_runs(run_dirs).items()):
        day, grid = load_market(rd)
        for (stop, tgt), conf, agree, hold in itertools.product(EXITS, CONF, AGREE, HOLD):
            r = await replay_day(rd, day, grid, Settings(conf, agree, hold, stop, tgt))
            rows.append((day_s, f"{stop}/{tgt}", conf, agree, hold, len(r.trades),
                         sum(t.reason == "jev" for t in r.trades),
                         sum(t.pnl > 0 for t in r.trades), r.pnl))

    print(f"{'day':>10} {'stop/tgt':>8} {'conf':>4} {'agree':>5} {'hold':>5} {'trades':>6} "
          f"{'reversals':>9} {'win%':>5} {'net $':>10}")
    for day_s, ex, conf, agree, hold, n, rev, wins, pnl in rows:
        wr = f"{wins / n * 100:.0f}" if n else "-"
        print(f"{day_s:>10} {ex:>8} {conf:>4} {agree:>5} {hold:>4}s {n:>6} {rev:>9} {wr:>5} {pnl:>10,.0f}")


if __name__ == "__main__":
    asyncio.run(main([Path(p) for p in sys.argv[1:]]))
