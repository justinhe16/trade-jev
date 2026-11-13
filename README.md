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

`trade_jev.executor` places the live runner's signals at a broker (this is the single-broker, one-contract automation; the multi-account mirror of the paper position is `trade_jev.copytrade`, see "Copy trading". Both receive the same intents; `intent` and `heartbeat` messages now also carry a per-process `boot` id, and heartbeats the paper `bid` and `ask`), inside hard limits (1 contract, daily loss halt, entry cap, heartbeat and read-back checks; see the Docs tab). It starts disarmed; arm it on the Live page. `--broker fake` is built in for rehearsals and tests. Real broker backends are kept out of this repo: `--broker NAME` loads `private/NAME.py` (gitignored), which implements the interface in `src/trade_jev/executor/broker.py`. Add `--dry-run` to highlight without placing orders. Web-based backends need the `browser` extra (`uv sync --extra browser`).

```bash
uv run python -m trade_jev.executor --broker fake --allow-replay
uv run python -m trade_jev.live --from-file 2026-06-23 --speed 60 --stored-answers runs/<id> --exec-url http://127.0.0.1:8766
```

## 7) Live paper trading

Every weekday the Mini runs one live session that never places an order (copy trading, below, is a separate daemon that mirrors its positions): Jev with the registered filters (`settings.DEFAULT`: cutoff 0.7, 4 agreeing, 30s min hold, 200-tick stop / 100-tick target) next to the hold / random / imbalance baselines, on a live NQ L2 feed (Databento by default, or Tradovate: see "Tradovate source" below). Each session is recorded in a local SQLite file, and the viewer's **Paper** tab shows it.

**Keys** in `.env` (never committed; `.env.example` lists every name): `TYPESAFE_API_KEY` (Jev) and, for the feed, `DATABENTO_API_KEY` or the `TRADOVATE_*` set. `TRADE_JEV_SOURCE` = `databento` (default) | `tradovate` | `off` picks the feed.

**Install** on the Mini, from the deploy checkout (`/Users/justinhe/trade-jev`):

```bash
uv sync
scripts/install_launchd.sh      # copies deploy/launchd/*.plist to ~/Library/LaunchAgents and loads them
```

- `com.tradejev.paper` runs `scripts/run_paper_day.sh` on weekdays at 06:15 local time (09:15 ET on a Pacific-time Mac). The script works out ET itself: it waits until 09:15 ET if started early and does nothing after 15:55 ET. It then runs `python -m trade_jev.live --exit-at-close --no-open --port 8767`, which exits 0 once the session's outputs are written. Weekends and CME full-closure holidays (`src/trade_jev/tradingdays.py`, 2026–2027; add the next year before it starts) exit 0 and record a `holiday` row. On the shortened holiday sessions (13:00 ET halt) the session closes when the feed goes quiet.
- `com.tradejev.view` keeps the viewer up on `http://127.0.0.1:8765`.
- `com.tradejev.copy` (only when `accounts.yaml` exists; see "Copy trading") is the copy-trading daemon. Without `accounts.yaml` nothing about copy trading is installed or started and the paper session is unchanged.
- Logs: `logs/paper-<day>.log` (one per ET day), `logs/view.log`. While a session runs, its own Live page is at `http://localhost:8767/live`.
- The Mac must stay awake through the session (System Settings → Energy: prevent automatic sleeping). The script holds `caffeinate -i` while it runs.

**The record:** `data/paper.db` (override with `TRADE_JEV_PAPER_DB`), SQLite in WAL mode, created on first use. It has six tables: `paper_sessions` (one row per day: `running | done | crashed | holiday | aborted | skipped`, with a heartbeat at least every 60s while running, and the feed in `source`), `paper_trades` (every policy's trades, written as they close), `paper_decisions` (the Jev decision log), `paper_summary` (per-policy totals, written at the end), and two chart-only tables the engine never reads back: `paper_bars` (1-minute OHLC of the mid from every quote, for the Resonant `chart` panel; backfill a recorded day with `uv run python -m trade_jev.paperdb bars --day YYYY-MM-DD`) and `paper_open` (Jev's open position). A `running` row with a stale heartbeat means the process died. The schema is in `src/trade_jev/paperdb.py`. The usual `runs/live-<day>/` files are still written.

**Behaviour of `trade_jev.live` (also when run by hand):**
- On a weekend or holiday it exits 0 and writes a `holiday` row.
- With `TRADE_JEV_SOURCE=off`, or `tradovate` and any `TRADOVATE_*` credential missing, it exits 0 and writes a `skipped` row with the reason (names only, never values) and starts no feed.
- A live session closes 2 minutes after its end even if the feed has gone quiet.
- SIGTERM (`launchctl kill SIGTERM`) closes the session like Ctrl-C does. It writes the summary, marks the session `aborted` and exits 0; Ctrl-C exits 130.
- Jev errors (bad key, outage) count as HOLD and show in the decision log's error column.

**View:** `http://127.0.0.1:8765/paper` shows a per-day table (status, Jev P&L against the baselines, trades, wins, drawdown, Jev cost) and a drill-down with each session's trades and Jev decisions. It updates every 5s while a session runs. `uv run python -m trade_jev.view` serves it standalone, with no run or live session needed.

```bash
uv run python -m trade_jev.live --exit-at-close --no-open             # one session by hand, as the schedule runs it
uv run python -m trade_jev.live --from-file 2026-06-23 --speed 0 --paper-db --exit-at-close   # rehearsal into the DB
sqlite3 data/paper.db "select day, status, heartbeat_at from paper_sessions order by day desc limit 5"
```

### Copy trading

Paper trading stays exactly as it is (the main `TRADOVATE_*` login, 1 NQ, it never places an order) and is the **signal source**. Every account in `accounts.yaml` mirrors Jev's paper position changes at its own contract and size (e.g. 5 MNQ), with the same TP/SL in points. `accounts.yaml` is gitignored; copy `accounts.example.yaml`. **No `accounts.yaml` (or none with `enabled: true`) means no behaviour change at all**: `scripts/run_paper_day.sh` then omits `--exec-url`, and `install_launchd.sh` does not install `com.tradejev.copy`.

```
paper session (live.py) --intents/heartbeats--> copy daemon (python -m trade_jev.copytrade run, :8768)
                                                  one worker per account --> Tradovate REST (that account's own token)
                                                  data/copy.db  --> Resonant panel "Copy accounts", job copy-watch, tool trade_jev_copy_flatten_all
```

**Accounts.** Per account: `mode` is `dry_run` (default: records "would enter" rows and places no orders; if the prefix has a token in `.env` it still makes token-renewal calls so the panel can show token health), `demo` or `live`; `mode live` requires `env: live`, `mode demo` requires `env: demo` (`dry_run` takes either). `root` is `MNQ` or `NQ` (the paper symbol `NQZ6` becomes `MNQZ6`; anything not `NQ` + `H|M|U|Z` + a digit is refused). Credentials come from `<secrets_prefix>_*` in `.env` (names in `.env.example`); the prefix must not be `TRADOVATE`. The entry is a marketable IOC limit at the NQ touch plus/minus `slip_ticks` (default 4 ticks = 1 point, $10 on 5 MNQ, because the NQ quote prices an MNQ order), with TP and SL attached in the same `placeoso` request and then moved to the real fill (`modifyorder`); a miss is recorded and never chased.

**What it does.** Entries need: account enabled, startup clean, Resonant not paused or `autonomy: off`, a fresh `LIVE` paper heartbeat, a valid token, no halt. Exits are never gated: paper exiting, a reversal, the kill switch, a lost heartbeat and the 15:56 ET end-of-day guard all flatten (liquidate, cancel every live order, read back). If the account stops out first it stays flat until paper's next signal. Per account, per ET day: `daily_loss_limit` and `daily_profit_stop` (realized + open, from the account's own fill ledger, open marked at paper's bid/ask) flatten and halt the day; so do a failed flatten, a read-back mismatch, a bracket missing after a fill, 3 rejects in a row, the entry cap and the flatten-all tool. A halt lasts until the next ET day or `python -m trade_jev.copytrade clear-halts NAME`. Link, token and kill problems block entries only and clear by themselves. The one exception is a `connection` halt (a flatten read-back or position read that failed on the network: DNS, timeout, HTTP 5xx or 429): while one is active the daemon probes the account every 5 s, and after 5 minutes of unbroken good fresh position reads on a flat account (no position, no working orders) with a live paper link and a good token it clears that halt alone and texts `connection back for 5 min: resumed`; any failed read restarts the 5 minutes, at most 3 times per account per day (then it texts that the halt stays until you clear it). It never clears any other halt, and a halt of another kind raised meanwhile stays on top. Failed broker reads are logged (once per episode, not texted, then a `position reads recovered after N failures over Xs` line) so an outage shows in `logs/copy.log`. A crash, hang or dead token in one account never touches the others.

**An entry that does not fill.** The entry is an IOC limit, and on some accounts an IOC that does not cross still reads `Working` for a few seconds. After the cancel the daemon re-polls the parent (up to 3 s, then cancels once more) until it reads terminal. A clean miss (`Canceled`, `Expired` or `Rejected`, position flat, reads succeeding) sweeps the bracket children, logs a non-texting `entry_missed` event (`no fill at the limit; cancelled; still copying`), and does not halt: the account stays flat until paper's next signal and the next entry goes out normally. If the broker reads fail on the network while resolving it, the entry's fate is unknown, so the bracket is left in place and the account takes a `connection` halt (probe, flatten if needed, auto-resume after 5 good minutes). Only a parent that reads `Filled` (or stays `Working` after two cancels) while the position reads flat is ambiguous: the bracket is kept, the day halts, and it texts.

**Not supported / read this.**
- **Manual trading on these accounts is not supported.** The daemon flattens any position it did not open, and halts the account for the day.
- **A daemon restart while in a position flattens it** (startup rule), and so does a paper restart (a new `boot`). SIGTERM stops the daemon without flattening.
- The Live page shows the runner as "disarmed" for copy accounts: the daemon arms itself from the heartbeat (mode `live`, status `LIVE`, fresh) and ignores the runner's `armed`.
- Anything on this machine that can reach `127.0.0.1:8768` as this user can post an intent. The daemon only accepts a loopback `Host` and `Content-Type: application/json` (so a web page cannot), and a fresh `sent_wall_ns` and the matching `boot` raise the bar; they do not stop a deliberate local process.
- Texts come from the Resonant job `copy-watch` (5 minute minimum, so up to 5 minutes late) and not during a kill or when Resonant is down. Defences that do not need a text: the account's own TP/SL at the broker, and the daemon's flatten.

**Kill switch.** `resonant kill` writes Resonant's database directly and `ProcessTracker.paused` stops the extension's tools, so the daemon watches it itself every 3 s: Resonant's `GET http://127.0.0.1:7777/api/status` (`paused`, `autonomy`), and if that fails `kv.paused` in `~/.resonant/resonant.db` opened read-only. `paused` flattens every account at once; `autonomy: off` blocks entries and holds; both sources unavailable blocks entries, holds and texts once. `trade_jev_copy_flatten_all` (tool, L2) halts every account for the day and flattens, but cannot run while Resonant is killed; the terminal fallback is `uv run python -m trade_jev.copytrade flatten-all --direct` (daemon down, owner only).

**Commands** (`uv run python -m trade_jev.copytrade ...`): `run`, `status`, `preflight NAME` (read-only: `account/list`, `contract/find`), `clear-halts NAME`, `flatten-all [--direct]`, `rehearse NAME` (owner only, needs a terminal, `env: demo` only, places ONE real demo order and prints pass/fail for the API assumptions below, plus a second probe that answers whether Tradovate cancels the OSO children when an IOC parent expires unfilled; the daemon sweeps either way). Tokens: each account has its own store in `data/copy/<name>/` (the main `data/tradovate_token*.json` is never touched); renewals are capped at 6 per hour for the whole daemon (Tradovate limits are per IP and shared with the paper login), and the password path (all five `<PREFIX>_USERNAME/_PASSWORD/_CID/_SEC/_DEVICE_ID` set) makes one login attempt per `.env` modification.

**Rollout.** (1) Merge, then in `/Users/justinhe/trade-jev`: `git pull && uv sync` (PyYAML is a new dependency and the paper job runs `uv run --frozen`) before the next 06:15 run. (2) Dry run: `cp accounts.example.yaml accounts.yaml`, first account enabled with `mode: dry_run`, `scripts/install_launchd.sh`, read `logs/copy.log`; the next session's panel shows "would enter" rows matching paper. (3) Demo: a demo account with its own login, `preflight`, then by hand in market hours `rehearse` and resolve every failed line; `mode: demo`, 1 MNQ; `launchctl kickstart -k gui/$(id -u)/com.tradejev.copy`; run several sessions and the drills (`resonant kill`/resume, the flatten-all tool, SIGTERM the daemon in a position, stop paper in a position). (4) Live: the live login, `env: live`, `mode: live`, `qty: 1`, limits, `preflight`, restart, watch the first entry in the Tradovate UI; raise to 5 by editing `qty` and restarting while flat. **Rollback:** `enabled: false` or `mode: dry_run` and restart, or `launchctl bootout gui/$(id -u)/com.tradejev.copy`; removing `accounts.yaml` makes the next paper session omit `--exec-url`.

**`customTag50`.** If NinjaTrader configured an operator allow-list for your login (some prop firms do), every order must carry `customTag50`; this daemon does not send it, so every entry and liquidation would be rejected. That fails safe (3 rejects halt the account for the day), but liquidation would fail too: check with `rehearse` on demo and ask the firm. `logs/copy.log` carries the daemon's and the accounts' event lines (the `would enter` detail is in `data/copy.db` and the panel).

**Tradovate API assumptions** (what is documented and what is not) are listed in the pull request that added this and checked by `rehearse`: unverified are IOC on the OSO parent, child quantity on a partial fill, child status before the fill, that modifying a child keeps its OCO link, that liquidating cancels the brackets (the sweep covers either), and that the demo REST accepts a token renewed on the live auth host. `clOrdId` server-side de-duplication is never relied on.

**Ledger.** `data/copy.db` (`TRADE_JEV_COPY_DB`; SQLite, WAL, the daemon is the only writer): `copy_status` (one row per account, every 5 s), `copy_signals` (one per paper intent per account, by `(boot, seq)`), `copy_orders` (written before every POST), `copy_fills`, `copy_trades`, `copy_events` (`notify=1` rows are the texts), `copy_halts`. The Resonant extension reads it read-only.

### Tradovate source

`TRADE_JEV_SOURCE=tradovate` (or `--source tradovate`) streams the book from Tradovate instead of Databento: the L2 DOM (top 10 per side, a full snapshot about every 50 ms) plus the tick chart for trades, through the same engine, paper book, DB, viewer and extension. Paper only: the code calls the auth endpoints, `GET contract/find` and the `md/*` market-data endpoints, never an order, position or account endpoint.

**Setup**
- Tradovate API access needs a LIVE account with more than $1,000 equity and an API Access subscription. Generate an API key in the Tradovate web app (Settings > API Access): it gives the `cid` and `sec`.
- `.env`: `TRADOVATE_USERNAME`, `TRADOVATE_PASSWORD`, `TRADOVATE_CID`, `TRADOVATE_SEC`, `TRADOVATE_DEVICE_ID` (any stable UUID: `uuidgen`). Optional: `TRADOVATE_ENV` = `live` | `demo` (default `live`), `TRADOVATE_APP_ID` (default `trade-jev`), `TRADOVATE_APP_VERSION` (default `1.0`), `TRADOVATE_MD_URL` (replaces the websocket URL, disables the host fallback).
- For a simulated or evaluation account (for example a funded-trader evaluation) use `TRADOVATE_ENV=demo`: login, renew and contract lookup then go to `demo.tradovateapi.com`. Prop-firm evaluation accounts may not offer API Access at all: if login says `this account has no API key: cid/sec rejected`, ask the firm or Tradovate. A wrong username or password is reported separately (`bad credentials`).
- The first login from a new device can demand a captcha: log in once by hand in the Tradovate web app (same device), wait about an hour, then retry. A short rate limit ("penalty") of up to 120 s is waited out and retried once; a longer one is reported as an error.
- Using a My Funded Futures (or other prop-firm) simulated account where the API login is refused: use token mode, below.
- Switch the feed in `.env`: `TRADE_JEV_SOURCE=databento | tradovate | off`. `off`, or `tradovate` with a credential missing, makes the daily job write a `skipped` row (reason in the viewer and extension) and exit 0. An explicit `--source` on the command line wins over `TRADE_JEV_SOURCE` (the env value, including `off`, is only the default).

#### Token mode (My Funded Futures)

The owner's account is an MFF simulated account; its API username / password login is refused, and MFF approved this method for their sim account: log in on trader.tradovate.com and use the web session's tokens. Nothing logs in with a password in this mode.

1. Log in at https://trader.tradovate.com, open the browser dev tools (Network), and copy `accessToken` and `mdAccessToken` from the `accesstokenrequest` response into `.env` as `TRADOVATE_ACCESS_TOKEN` and `TRADOVATE_MD_ACCESS_TOKEN`. Close the tab; do not log out of the web app (logging out ends the session and kills the tokens).
2. Token mode is on when `TRADOVATE_ACCESS_TOKEN` is set or `data/tradovate_token.json` exists; `TRADOVATE_USERNAME` etc. are then not needed and count as configured for `run_paper_day.sh`. `TRADOVATE_AUTH_HOST` (default `live.tradovateapi.com`, where the web app logs in even for demo accounts) picks the REST host.
3. `GET /auth/renewaccesstoken` with the access token returns both tokens and an `expirationTime` about 80 minutes ahead. The keep-alive, `uv run python -m trade_jev.tradovate_keepalive` (launchd `com.tradejev.tvtoken`, installed by `scripts/install_launchd.sh`, log `logs/tvtoken.log`), runs 24/7: it renews at start and then whenever under 30 minutes remain (about every 50 minutes). **If the renewer stops for more than about 80 minutes (reboot, stopped service, closed laptop) the tokens expire** and you paste fresh ones into `.env`; the keep-alive re-reads `.env` and picks them up on its own (tokens in `.env` newer than a dead store, and edited in the last 90 minutes).
4. Files (all gitignored, never commit them): `data/tradovate_token.json` (the token store `{accessToken, mdAccessToken, expirationTime}`, written atomically with mode 0600 after every renewal) and `data/tradovate_token_status.json` (`last_ok_at`, `expires_at`, `last_error`, `consecutive_failures`, `checked_at`, ...: never a token). The store is used while valid; otherwise fresh `.env` tokens; a dead store with no fresh `.env` tokens skips the day with the reason `tradovate token expired`.
5. Race avoidance: the keep-alive and the live session both renew, and a renewal may invalidate the old token. Every read-check-renew-write runs under a file lock (`data/.tradovate_token.lock`), and the store is re-read inside the lock: a token that is still valid beyond the caller's margin is adopted without any HTTP call. The keep-alive's margin is 30 minutes, the session's 15 minutes, so with the keep-alive healthy the session only adopts the store's tokens, and it renews (and writes the store) by itself only if the keep-alive is not doing its job. Details in the `tradovate_token.py` docstring.
6. The Resonant extension shows the token in `trade_jev_status` (`token` block) and its `watch` job (every 15 minutes, any time of day) raises an urgent `trade-jev/token` report, "Tradovate token expired - log into trader.tradovate.com and paste a fresh access token + md token into ~/trade-jev/.env", when renewal is failing or the token expires within 10 minutes; it clears when healthy again.
7. Assumption: `renewaccesstoken` does not invalidate the previous access token, so a session may keep using a token it adopted until its own 15-minute margin. Token mode is sticky: once `data/tradovate_token.json` exists the mode stays token mode even if the `.env` tokens are removed; to go back to password mode, delete the store. The `watch` job also alerts (same `trade-jev/token` key) when the status file's `checked_at` is older than 20 minutes (the renewer stopped); a vanished status file is silent and never sends a clear.
8. The websocket authorizes with the `mdAccessToken` and waits for the authorize answer (200) before it subscribes: a subscription sent earlier is silently dropped by Tradovate.

`uv run python scripts/tradovate_check.py` in token mode renews once (never printing a token), authorizes the md socket, subscribes quote + DOM + ticks and reports `renew ok; token expires ...; user status ...; md token present|absent`, levels per side, DOM update rate, ticks and quotes; exit 3 if the token is expired or refused.

**Smoke test** (run outside 09:15-16:00 ET): `uv run python scripts/tradovate_check.py --seconds 30` (`--env demo|live`, `--symbol NQZ6`). It logs in, resolves the contract, subscribes the DOM and ticks, and prints the env and hosts used, token expiry (time only), user status, which md host and token kind authorized, DOM update rate, levels per side, tick count and the buy / sell / unknown split. It exits non-zero with a one-line reason: missing credentials (names), captcha, no API key, bad credentials, or `no DOM updates: likely no CME market-data entitlement for API access yet`.

**Caveats**
- Licence: CME market-data entitlement for API (non-display) use is not confirmed with Tradovate. Until it is, expect login to work but no DOM data. A session that sees no book rows all day ends `crashed` with that reason.
- Tradovate allows 2 concurrent sessions per user. Running `tradovate_check.py` while the daily job is live may kick a session.
- Tokens live 90 minutes; they are renewed about 15 minutes before expiry (a failed renewal triggers a full login). The websocket reconnects with backoff (1, 2, 4 ... 30 s), re-authorizes, resubscribes and ignores the history Tradovate replays.
- Hosts and tokens that are not documented are tried in order and logged (never the token): the md socket authorizes with `mdAccessToken` first, then `accessToken`. Only `md.tradovateapi.com` is documented, for both environments; with `TRADOVATE_ENV=demo` the one fallback is `md-demo.tradovateapi.com`.
- No catch-up replay. Databento replays the session so far after a late start; Tradovate cannot. If the first row arrives after the 09:28 ET open, the engine skips the grid points it missed and warms up for 60 s on live data: every decision until then is HOLD without a Jev call, recorded as `raw_action = WARMUP`, `action = HOLD` in `paper_decisions` (the viewer shows them as "warmup", not as errors). The log prints `late start: warming up 60s ... decisions until HH:MM:SS ET are HOLD` and the session's settings record `late_start_warmup_s` and `warmup_until_et`.
- Rows: each DOM snapshot is a row with delta 0 (identical consecutive snapshots are dropped); each trade tick after the end-of-history marker is a row with the latest book and delta = +size or -size. Row times are the event times, forced non-decreasing; trades before the first book are dropped.
- Aggressor rule per trade: if the tick carries the ask and price >= ask, buy; else if it carries the bid and price <= bid, sell; else the same test against the latest DOM best ask / bid; else the tick rule against the previous trade price (up buy, down sell, unchanged the previous side; the very first trade with nothing to go on counts 0).
- The recorded feed (`data/live/`) keeps the Databento file layout, so `load_day` reads it. The sidecar JSON key `"source": "trade_jev.live"` was replaced by `"writer": "trade_jev.live"` plus `"feed": "databento" | "tradovate"` (nothing in the repo read the old key; update any outside tool that does).
- Fail fast: if the md socket refuses (non-200 on authorize for every host / token, or on subscribeDOM / getChart) on at least 5 attempts spanning at least 5 minutes and no book data has ever arrived (so a brief refusal at the 09:15 start does not lose the day: launchd does not restart the job), the session ends `crashed` with `Tradovate market data refused: <server text> - likely no CME market-data entitlement for API access`, so the extension alerts early. The reconnect backoff only resets after a connection has delivered data for 60 s.
- Update the deploy checkout after merging: `uv sync` (adds `websockets`), then `resonant ext update trade-jev` for the extension.

## 8) Test

Unit tests, plus a check that the page's replays match the Python replays.

```bash
uv run pytest -q
uv run python scripts/check_viewer.py runs/<id>
uv run python scripts/check_live_parity.py runs/<id>
uv run python scripts/make_sample.py        # regenerate the synthetic sample
```
