"""Check that the viewer's in-browser replay (viz/replay.js) matches Python replays.

  uv run python scripts/check_viewer.py runs/<id> [runs/<id> ...]

With stops off, P&L should match exactly (stops fill at their price in the viewer).
"""

import asyncio
import json
import subprocess
import sys
import tempfile
from pathlib import Path

from trade_jev.replay import Settings, load_market, load_runs, replay_day
from trade_jev.view import build_day

ROOT = Path(__file__).resolve().parents[1]
CASES = [(0.0, 1, 0, 200, 0), (0.8, 2, 0, 200, 0), (0.5, 2, 60, 0, 0), (0.0, 1, 0, 20, 40), (0.6, 3, 30, 100, 80)]
JS = r"""
const fs = require('fs');
const { replay } = require('./viz/replay.js');
const days = JSON.parse(fs.readFileSync(process.argv[1], 'utf8'));
const out = {};
for (const d of days) JSON.parse(process.argv[2]).forEach((c, i) => {
  const [minConf, agree, minHold, stop, target] = c;
  const r = replay(d, {minConf, agree, minHold, stop, target, commission: d.commission},
                   {tick: d.tick, pointValue: d.point_value});
  out[d.day + ' #' + i] = [r.stats.trades, Math.round(r.stats.net)];
});
console.log(JSON.stringify(out));
"""


async def main(run_dirs: list[Path]) -> None:
    py, payloads = {}, []
    for day_s, rd in sorted(load_runs(run_dirs).items()):
        day, grid = load_market(rd)
        payloads.append(build_day("check", rd))
        for i, c in enumerate(CASES):
            r = await replay_day(rd, day, grid, Settings(*c))
            py[f"{day_s} #{i}"] = (c, len(r.trades), round(r.pnl))

    with tempfile.NamedTemporaryFile("w", suffix=".json") as fh:
        json.dump(payloads, fh)
        fh.flush()
        res = subprocess.run(["node", "-e", JS, fh.name, json.dumps(CASES)],
                             capture_output=True, text=True, cwd=ROOT, check=True)
    js = json.loads(res.stdout)
    print(f"{'day [conf, agree, hold, stop, target]':<40} {'py trades':>9} {'js trades':>9} {'py $':>9} {'js $':>9}")
    for key, (c, n, pnl) in py.items():
        jn, jp = js[key]
        flag = "" if (n, pnl) == (jn, jp) else "  <-- differs"
        label = f"{key.split(' #')[0]} {list(c)}"
        print(f"{label:<40} {n:>9} {jn:>9} {pnl:>9} {jp:>9}{flag}")


if __name__ == "__main__":
    asyncio.run(main([Path(p) for p in sys.argv[1:]]))
