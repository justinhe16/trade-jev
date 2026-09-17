# Data

Backtests read NQ order-book data from Databento: one Parquet file per trading day. That data is licensed, so it isn't in git. This page covers where to put it and how it was pulled.

## Layout

```
data/
  DATA.md          ← this file
  databento/       ← put the day files here (gitignored)
  sample/          ← 10 synthetic snapshots in the same shape (committed)
```

To keep the files somewhere else, set `TRADE_JEV_DATA=/path/to/files` in `.env`.

## Day files

Each trading day is one Parquet file plus a JSON sidecar, named:

```
GLBX.MDP3__{symbol}__mbp-10__rth__{YYYY-MM-DD}__{anything}.parquet
GLBX.MDP3__{symbol}__mbp-10__rth__{YYYY-MM-DD}__{anything}.json   # {"dataset", "symbol", "schema", "day", "session", "rows"}
```

A day whose sidecar says `"rows": 0` (a weekend or holiday) is skipped. Every level list must have exactly 10 entries.

| Column | Type | Meaning |
|---|---|---|
| `ts_event` | timestamp (ns, UTC) | Exchange event time, sorted ascending |
| `instrument_id` | int64 | Databento instrument id |
| `bid_px`, `ask_px` | list of 10 float64 | Price per level, best first (NQ tick = 0.25) |
| `bid_sz`, `ask_sz` | list of 10 int64 | Resting contracts per level |
| `trade_delta` | int64 | `+size` if a buyer hit the ask, `−size` if a seller hit the bid, `0` if the update isn't a trade |

The original set: 15 trading days, 2026-06-08 → 06-26, 08:30–17:00 ET, about 244M rows and 3.7 GB. `NQM6` covers Jun 8–12 and `NQU6` covers Jun 15 onward, after the roll.

## How it was pulled

This comes from the [Databento](https://databento.com) Historical API, using the `databento` Python client, one request per day:

- **Dataset:** `GLBX.MDP3`, CME Globex MDP 3.0
- **Schema:** `mbp-10`, the top 10 price levels. There's one record per book or trade event.
- **Symbols:** `stype_in="raw_symbol"` with the front-month contract, e.g. `NQU6`
- **Window:** 08:30–17:00 America/New_York, sent as UTC. That's 12:30–21:00Z during daylight time.
- **Cost:** check it first with `metadata.get_cost`, since the data is billed per GB

```python
import databento as db
import pyarrow as pa, pyarrow.parquet as pq

UNDEF = 2**63 - 1
client = db.Historical(key="YOUR_DATABENTO_KEY")
q = dict(dataset="GLBX.MDP3", schema="mbp-10", stype_in="raw_symbol", symbols="NQU6",
         start="2026-06-23T12:30:00Z", end="2026-06-23T21:00:00Z")
print("quote $", client.metadata.get_cost(**q))

rows = []
for r in client.timeseries.get_range(**q):
    if not hasattr(r, "levels"):          # skip symbol mappings / system messages
        continue
    lv = [l for l in r.levels]
    rows.append({
        "ts_event": r.ts_event,
        "instrument_id": r.instrument_id,
        "bid_px": [l.bid_px * 1e-9 for l in lv if l.bid_px != UNDEF],
        "bid_sz": [l.bid_sz for l in lv if l.bid_px != UNDEF],
        "ask_px": [l.ask_px * 1e-9 for l in lv if l.ask_px != UNDEF],
        "ask_sz": [l.ask_sz for l in lv if l.ask_px != UNDEF],
        "trade_delta": (r.size if str(r.side) == "B" else -r.size if str(r.side) == "A" else 0)
                       if str(r.action) == "T" else 0,
    })
pq.write_table(pa.Table.from_pylist(rows), "data/databento/GLBX.MDP3__NQU6__mbp-10__rth__2026-06-23__v1.parquet")
```

This is a sketch of what the original downloader did; that downloader lived in a separate project, not this repo.
- **Speed:** a busy day has 20–30M records, so for real use, stream to Parquet in batches rather than building one list.
- **Empty levels:** unset levels are dropped here, but the loader expects all 10 levels on every row, which holds for liquid NQ hours.
- **Sidecar:** write the JSON sidecar next to each Parquet file.

## Sample

`data/sample/` has 10 snapshots with **made-up numbers** in exactly this shape, built by `scripts/make_sample.py`. Use it to try the pipeline without a Databento account:

```bash
TRADE_JEV_DATA=data/sample uv run python -m trade_jev.run --days 2026-06-23 --policies hold,random,imbalance
```
