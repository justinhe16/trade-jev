"""Write the committed data sample in data/sample/: 10 synthetic L10 book snapshots.

  uv run python scripts/make_sample.py

The numbers are made up (seeded random walk); only the shape matches the real Databento
day files described in data/DATA.md, so the pipeline can be smoke-tested without licensed data.
"""

from __future__ import annotations

import json
from pathlib import Path

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq

from trade_jev.data import LEVELS, TICK, et_to_ns

ROOT = Path(__file__).resolve().parents[1]
OUT = ROOT / "data" / "sample"
DAY, SYMBOL, START_ET, N, EVERY_S = "2026-06-23", "NQU6", "09:30:00", 10, 15
SCHEMA = pa.schema([
    ("ts_event", pa.timestamp("ns", tz="UTC")),
    ("instrument_id", pa.int64()),
    ("bid_px", pa.list_(pa.float64())),
    ("bid_sz", pa.list_(pa.int64())),
    ("ask_px", pa.list_(pa.float64())),
    ("ask_sz", pa.list_(pa.int64())),
    ("trade_delta", pa.int64()),
])


def main() -> None:
    rng = np.random.default_rng(7)
    t0 = et_to_ns(DAY, START_ET)
    ts = t0 + np.arange(N, dtype=np.int64) * EVERY_S * 10**9 - rng.integers(1_000, 900_000, N)  # just before each tick
    bid_ticks = 80_000 + np.cumsum(rng.integers(-12, 13, N))  # 20,000.00 ± a few points
    spread = rng.integers(1, 5, N)
    rows = []
    for i in range(N):
        b0, a0 = int(bid_ticks[i]), int(bid_ticks[i] + spread[i])
        rows.append({
            "ts_event": int(ts[i]),
            "instrument_id": 0,  # synthetic
            "bid_px": [(b0 - k) * TICK for k in range(LEVELS)],
            "bid_sz": rng.integers(1, 10, LEVELS).tolist(),
            "ask_px": [(a0 + k) * TICK for k in range(LEVELS)],
            "ask_sz": rng.integers(1, 10, LEVELS).tolist(),
            "trade_delta": int(rng.choice([0, 0, 0, 1, -1, 2, -3])),
        })
    table = pa.Table.from_pylist(rows, schema=SCHEMA)

    OUT.mkdir(parents=True, exist_ok=True)
    stem = f"GLBX.MDP3__{SYMBOL}__mbp-10__rth__{DAY}__sample"
    pq.write_table(table, OUT / f"{stem}.parquet")
    (OUT / f"{stem}.json").write_text(json.dumps({
        "dataset": "GLBX.MDP3", "symbol": SYMBOL, "schema": "mbp-10", "day": DAY, "session": "rth",
        "rows": N, "synthetic": True,
        "note": f"made-up values in the real shape; {N} snapshots every {EVERY_S}s from {START_ET} ET",
    }, indent=2))
    with (OUT / "sample.jsonl").open("w") as fh:  # same rows, human-readable
        for r in table.to_pylist():
            r["ts_event"] = r["ts_event"].isoformat()
            fh.write(json.dumps(r) + "\n")
    print(f"wrote {N} synthetic rows → {OUT}")


if __name__ == "__main__":
    main()
