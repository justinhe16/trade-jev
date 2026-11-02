# trade-jev

Tests Jev (TypeSafe) as a BUY / SELL / HOLD trader on NQ L10 order-book data from Databento (15 trading days, Jun 8–26 2026). **Findings: [`results/FINDINGS.md`](results/FINDINGS.md).** Published Jev answers and results are in [`results/`](results/); where the market data goes and how it was pulled is in [`data/DATA.md`](data/DATA.md).

**Concepts**
- **Run:** a backtest that calls Jev and stores its answers and trades in `runs/<id>/`. It's the only step that costs money.
- **Settings:** harness parameters: cutoff, agreeing answers, min hold, stop, target, commission.
- **Replay:** a run's stored answers re-scored under new settings. It needs no API calls.
- **View:** one local web app that replays any mix of runs, days and settings live.
- **Live:** the same harness on a live Databento feed: Jev signals, paper trades and a live page. It places no orders.

Flow: **run once → replay many times → view**. Then **live** for forward testing on new days.

## 0) Setup

Install dependencies, add your API key, and point at the data. Databento day files go in `data/databento/` (gitignored), or anywhere set by `TRADE_JEV_DATA`; see [`data/DATA.md`](data/DATA.md). No data yet? Use the synthetic sample.

```bash
uv sync
echo "TYPESAFE_API_KEY=..." >> .env
echo "TRADE_JEV_DATA=/path/to/databento/files" >> .env        # optional; default data/databento/
TRADE_JEV_DATA=data/sample uv run python -m trade_jev.run --days 2026-06-23 --policies hold,random,imbalance   # smoke test
```

**In git:** code, docs, `data/sample/` (synthetic), `results/` (published runs), `runs/index.jsonl`. **Local only:** Databento data (incl. `data/live/` recordings), `runs/<id>/` (includes the order books Jev saw), `cache/`, `data/paper.db`, `logs/`, `.env`.

## 1) Run

Asks Jev every 15s and trades 1 contract next to the hold / random / imbalance baselines. It prints Jev calls and P&L, saves to `runs/<id>/`, and appends to `runs/index.jsonl`, which the viewer lists.

```bash
uv run python -m trade_jev.run --days 2026-06-23                                   # one day
uv run python -m trade_jev.run --days 2026-06-08,2026-06-09,2026-06-10             # several days
uv run python -m trade_jev.run --all                                               # all 15 days
uv run python -m trade_jev.run --days 2026-06-23 --policies hold,random,imbalance  # baselines only, no API
```

## 2) Run settings

Defaults are the registered setting from [`results/FINDINGS.md`](results/FINDINGS.md): cutoff 0.7, 4 agreeing answers, 30s min hold, 200-tick stop, 100-tick target (`0` = off; defined in `src/trade_jev/settings.py`). Settings change which positions Jev sees, so a new run calls Jev again.

```bash
uv run python -m trade_jev.run --days 2026-06-23 \
  --min-conf 0.6 --agree 2 --min-hold 60 --stop-ticks 400 --target-ticks 0 \
  --cadence-s 15 --latency-ms 250 --commission 2.5
```

## 3) Replay

Replays one or more runs under many settings at once to find settings that hold up across days. It's free but approximate, because Jev answered with the run's own positions.

```bash
uv run python scripts/replay_sweep.py runs/<id> [runs/<id> ...]         # quick per-day table
uv run python scripts/replay_grid.py runs/<id> [runs/<id> ...]          # 1,920 settings → runs/replays/grid-<ts>.csv
uv run python scripts/analyze_replays.py runs/replays/grid-<ts>.csv     # rank + overfitting checks
uv run python scripts/findings.py --tune runs/<a> --test runs/<b> --grid runs/replays/grid-<ts>.csv  # tune vs test report
```

## 4) View

A local web app that lists every run in `runs/index.jsonl`. Pick any mix of runs and days, replay them live with the sliders, save presets to compare, press play, and click any point to see what Jev saw.

```bash
uv run python -m trade_jev.view                  # → http://localhost:8765
uv run python -m trade_jev.view --port 9000 --no-open
```

Links keep the view: `?sel=<run>@<day>,<run>@<day>&minConf=0.6&agree=2&minHold=60&stop=400&target=0` plus `&play=1` (autoplay) or `&at=11:02` (jump to a time).

## 5) Publish

Copies a run's Jev answers, probabilities, trades and results to `results/<id>/` for sharing. Order books and prices are stripped, since they're licensed Databento data.

```bash
uv run python scripts/publish_run.py runs/<id> [runs/<id> ...]
```

## 6) Live

Streams NQ L10 from Databento, asks Jev on the backtest's clock (09:30:00 ET + every 15s), applies the registered filters, and keeps a paper position with the backtest's fills and costs. Hold, random and imbalance baselines run alongside. One local server at `http://localhost:8765` serves **Replay**, **Live** and **Docs**. The Docs tab ([`viz/docs.html`](viz/docs.html)) covers use cases, costs and next steps.

```bash
echo "DATABENTO_API_KEY=..." >> .env
uv run python -m trade_jev.live                                    # start before 09:30 ET → /live; Ctrl-C to stop
uv run python -m trade_jev.live --no-jev                           # feed + baselines only
uv run python -m trade_jev.live --from-file 2026-06-23 --speed 60 --stored-answers runs/<id>   # rehearsal, no subscription
uv run python scripts/check_live_parity.py runs/<id> [runs/<id> ...]   # live engine == backtest, trade for trade
```

A session writes `runs/live-<day>/`, the same layout as a run plus `manual_fills.jsonl`, and records the feed to `data/live/` in the day-file format. It then opens in Replay and works with the replay scripts. The only built-in difference from the backtest is that the order goes out when Jev answers, not at the decision time; each decision records `jev_ms` and `send_ns`.

### Executor (automation)

`trade_jev.executor` places the live runner's signals at a broker, inside hard limits (1 contract, daily loss halt, entry cap, heartbeat and read-back checks; see the Docs tab). It starts disarmed; arm it on the Live page. `--broker fake` is built in for rehearsals and tests. Real broker backends are kept out of this repo: `--broker NAME` loads `private/NAME.py` (gitignored), which implements the interface in `src/trade_jev/executor/broker.py`. Add `--dry-run` to highlight without placing orders. Web-based backends need the `browser` extra (`uv sync --extra browser`).

```bash
uv run python -m trade_jev.executor --broker fake --allow-replay
uv run python -m trade_jev.live --from-file 2026-06-23 --speed 60 --stored-answers runs/<id> --exec-url http://127.0.0.1:8766
```

## 7) Live paper trading

Every weekday the Mini runs one live session with no orders: Jev with the registered filters (`settings.DEFAULT`: cutoff 0.7, 4 agreeing, 30s min hold, 200-tick stop / 100-tick target) next to the hold / random / imbalance baselines, on the live Databento NQ feed. Each session is recorded in a local SQLite file, and the viewer's **Paper** tab shows it.

**Keys** in `.env` (never committed): `DATABENTO_API_KEY` (live mbp-10 feed) and `TYPESAFE_API_KEY` (Jev).

**Install** on the Mini, from the deploy checkout (`/Users/justinhe/trade-jev`):

```bash
uv sync
scripts/install_launchd.sh      # copies deploy/launchd/*.plist to ~/Library/LaunchAgents and loads them
```

- `com.tradejev.paper` runs `scripts/run_paper_day.sh` on weekdays at 06:15 local time (09:15 ET on a Pacific-time Mac). The script works out ET itself: it waits until 09:15 ET if started early and does nothing after 15:55 ET. It then runs `python -m trade_jev.live --exit-at-close --no-open --port 8767`, which exits 0 once the session's outputs are written. Weekends and CME full-closure holidays (`src/trade_jev/tradingdays.py`, 2026–2027; add the next year before it starts) exit 0 and record a `holiday` row. On the shortened holiday sessions (13:00 ET halt) the session closes when the feed goes quiet.
- `com.tradejev.view` keeps the viewer up on `http://127.0.0.1:8765`.
- Logs: `logs/paper-<day>.log` (one per ET day), `logs/view.log`. While a session runs, its own Live page is at `http://localhost:8767/live`.
- The Mac must stay awake through the session (System Settings → Energy: prevent automatic sleeping). The script holds `caffeinate -i` while it runs.

**The record:** `data/paper.db` (override with `TRADE_JEV_PAPER_DB`), SQLite in WAL mode, created on first use. It has four tables: `paper_sessions` (one row per day: `running | done | crashed | holiday | aborted`, with a heartbeat at least every 60s while running), `paper_trades` (every policy's trades, written as they close), `paper_decisions` (the Jev decision log) and `paper_summary` (per-policy totals, written at the end). A `running` row with a stale heartbeat means the process died. The schema is in `src/trade_jev/paperdb.py`. The usual `runs/live-<day>/` files are still written.

**Behaviour of `trade_jev.live` (also when run by hand):**
- On a weekend or holiday it exits 0 and writes a `holiday` row.
- A live session closes 2 minutes after its end even if the feed has gone quiet.
- SIGTERM (`launchctl kill SIGTERM`) closes the session like Ctrl-C does. It writes the summary, marks the session `aborted` and exits 0; Ctrl-C exits 130.
- Jev errors (bad key, outage) count as HOLD and show in the decision log's error column.

**View:** `http://127.0.0.1:8765/paper` shows a per-day table (status, Jev P&L against the baselines, trades, wins, drawdown, Jev cost) and a drill-down with each session's trades and Jev decisions. It updates every 5s while a session runs. `uv run python -m trade_jev.view` serves it standalone, with no run or live session needed.

```bash
uv run python -m trade_jev.live --exit-at-close --no-open             # one session by hand, as the schedule runs it
uv run python -m trade_jev.live --from-file 2026-06-23 --speed 0 --paper-db --exit-at-close   # rehearsal into the DB
sqlite3 data/paper.db "select day, status, heartbeat_at from paper_sessions order by day desc limit 5"
```

## 8) Test

Unit tests, plus a check that the page's replays match the Python replays.

```bash
uv run pytest -q
uv run python scripts/check_viewer.py runs/<id>
uv run python scripts/check_live_parity.py runs/<id>
uv run python scripts/make_sample.py        # regenerate the synthetic sample
```
