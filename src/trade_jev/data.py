"""Load one cached MBP-10 day into compact numpy arrays.

A busy day is ~30M rows; holding the full L10 book for every row would be ~10 GB.
We keep only what the backtest needs:
  * every row: ts (int64 ns), best bid/ask (int32 ticks), trade_delta
  * full L10 book: only at the requested decision rows
"""

from __future__ import annotations

import json
import os
import re
from dataclasses import dataclass
from datetime import date, datetime, time
from pathlib import Path
from zoneinfo import ZoneInfo

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq

from trade_jev import ROOT

TICK = 0.25
POINT_VALUE = 20.0  # NQ: $20 / point  →  $5 / tick
LEVELS = 10
ET = ZoneInfo("America/New_York")

# Databento day files (see data/DATA.md). Default: data/databento/ (gitignored); override in .env.
DATA_DIR = Path(os.environ.get("TRADE_JEV_DATA", ROOT / "data" / "databento")).expanduser()

_NAME = re.compile(r"GLBX\.MDP3__(?P<sym>\w+)__mbp-10__rth__(?P<day>\d{4}-\d{2}-\d{2})__\w+\.parquet")


def list_days(data_dir: Path = DATA_DIR) -> dict[str, tuple[str, Path]]:
    """{'2026-06-23': ('NQU6', path)} for every non-empty cached day."""
    out: dict[str, tuple[str, Path]] = {}
    for p in sorted(data_dir.glob("*.parquet")):
        m = _NAME.fullmatch(p.name)
        if not m:
            continue
        meta = p.with_suffix(".json")
        if meta.exists() and json.loads(meta.read_text()).get("rows", 1) == 0:
            continue
        out[m["day"]] = (m["sym"], p)
    return out


def et_to_ns(day: str, hhmmss: str) -> int:
    dt = datetime.combine(date.fromisoformat(day), time.fromisoformat(hhmmss), ET)
    return int(dt.timestamp() * 1e9)


def ns_to_et(ns: int) -> str:
    return datetime.fromtimestamp(ns / 1e9, ET).strftime("%H:%M:%S")


@dataclass
class Day:
    day: str
    symbol: str
    ts: np.ndarray        # int64 ns, per row
    bid: np.ndarray       # int32 ticks, best bid per row
    ask: np.ndarray       # int32 ticks, best ask per row
    cum_delta: np.ndarray  # int64, cumulative trade_delta per row
    book_rows: np.ndarray  # row indices that have a full book below
    books: np.ndarray     # (len(book_rows), 4, LEVELS): bid_px, bid_sz, ask_px, ask_sz (px in ticks)

    def row_at(self, t_ns: int | np.ndarray) -> np.ndarray | int:
        """Index of the last row with ts <= t (no look-ahead). -1 if before first row."""
        return np.searchsorted(self.ts, t_ns, side="right") - 1

    def book(self, row: int) -> np.ndarray:
        k = np.searchsorted(self.book_rows, row)
        assert self.book_rows[k] == row, f"no full book stored for row {row}"
        return self.books[k]

    def mid(self, row: int | np.ndarray) -> np.ndarray | float:
        return (self.bid[row] + self.ask[row]) * TICK / 2


def _ticks(a: pa.Array) -> np.ndarray:
    return np.rint(a.to_numpy(zero_copy_only=False) / TICK).astype(np.int32)


def load_day(day: str, decision_times_ns, data_dir: Path = DATA_DIR) -> Day:
    """Load `day`; store full L10 books at the rows seen at each decision time."""
    symbol, path = list_days(data_dir)[day]
    pf = pq.ParquetFile(path)

    ts = pf.read(columns=["ts_event"]).column("ts_event").combine_chunks().cast(pa.int64()).to_numpy()
    rows = np.searchsorted(ts, np.asarray(decision_times_ns), side="right") - 1
    book_rows = np.unique(rows[rows >= 0])

    bids, asks, deltas, books = [], [], [], []
    off = 0
    for g in range(pf.num_row_groups):
        t = pf.read_row_group(g, columns=["bid_px", "bid_sz", "ask_px", "ask_sz", "trade_delta"])
        n = t.num_rows
        cols = {}
        for c in ("bid_px", "bid_sz", "ask_px", "ask_sz"):
            la = t.column(c).combine_chunks()
            vals = la.values
            if len(vals) != n * LEVELS:
                raise ValueError(f"{path.name} rg{g}: {c} is not {LEVELS} levels on every row")
            cols[c] = (_ticks(vals) if c.endswith("px") else vals.to_numpy()).reshape(n, LEVELS)
        bids.append(cols["bid_px"][:, 0].copy())
        asks.append(cols["ask_px"][:, 0].copy())
        deltas.append(t.column("trade_delta").combine_chunks().to_numpy())
        lo, hi = np.searchsorted(book_rows, [off, off + n])
        local = book_rows[lo:hi] - off
        books.append(np.stack([cols["bid_px"][local], cols["bid_sz"][local],
                               cols["ask_px"][local], cols["ask_sz"][local]], axis=1))
        off += n

    return Day(
        day=day, symbol=symbol, ts=ts,
        bid=np.concatenate(bids), ask=np.concatenate(asks),
        cum_delta=np.cumsum(np.concatenate(deltas)),
        book_rows=book_rows,
        books=np.concatenate(books) if books else np.zeros((0, 4, LEVELS), np.int64),
    )


def decision_grid(day: str, start_et: str, end_et: str, cadence_s: float) -> np.ndarray:
    step = int(cadence_s * 1e9)
    return np.arange(et_to_ns(day, start_et), et_to_ns(day, end_et), step, dtype=np.int64)


def secs(s: float) -> int:
    return int(round(s * 1e9))
