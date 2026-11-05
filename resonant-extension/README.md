# trade-jev Resonant extension

Lets [Resonant](https://github.com/justinhe16/resonant) answer "is trade-jev running?", "how did Jev do this week vs hold?" and "show me today's trades". It can start or stop the paper session, texts you if the session dies during market hours, and puts each day's result in the hourly pulse after the close.

It is **paper only**. It reads the paper runner's SQLite DB read-only and runs `launchctl` on the paper job. It never places orders.

## Install (owner, on the Mini's terminal)

Commit the extension first: `path:` installs only tracked, unchanged files. Then:

```sh
resonant ext validate /Users/justinhe/trade-jev/resonant-extension
resonant ext add path:/Users/justinhe/trade-jev/resonant-extension
```

`ext add` shows the review screen. Type `trade-jev` to confirm. After a later change, run `resonant ext update trade-jev` (the token alert needs this update: it changes the `watch` schedule and `trade_jev_status`; so does the live Jev visibility below: it adds `trade_jev_decisions`, job descriptions and `launch_agents`, so the review screen asks again; the `paper` dashboard panel below also needs one: the review screen lists `+ panel paper`; so does the `chart` panel: `+ panel chart`). The update only swaps the extension's files: the running paper session (`com.tradejev.paper`) is not touched and needs no restart.

Each job's first run after install is a silent baseline. Run `/ext run trade-jev/watch` once before relying on it.

## Tools

| Tool | Effect / level | What it does |
|---|---|---|
| `trade_jev_status` | read, L0 | Today's session (ET date): `running`, `done`, `crashed`, `aborted`, `holiday`, `skipped` (the runner chose not to trade, e.g. `TRADE_JEV_SOURCE=off` or Tradovate credentials missing: the summary reads `skipped: <reason>`) or **`stale`**. The session block also carries `source` (`databento` or `tradovate`, null on a DB from before the column existed). Stale means status is `running` but the heartbeat is more than 3 min old, so the process is likely dead. Also shows Jev's position (its latest decision), realized P&L, trade count and wins so far, a **`jev_live`** block (see [Is Jev making model calls?](#is-jev-making-model-calls); the summary gets `; Jev: live model calls (1224 calls, 0 errors, avg 123 ms, last 14:36:45 ET)`), and whether the launchd job is loaded (`launchctl print gui/<uid>/com.tradejev.paper`, best effort). Every answer also has a **`token`** block with the Tradovate token's health (expires in N min, last OK, consecutive renewal failures, last error; never a token), read from the keep-alive's status file, and the summary gets ` \| Tradovate token: <problem>` when it needs the owner. |
| `trade_jev_pnl {date}` | read, L0 | Jev vs the baselines: pnl, trades, wins, max drawdown, Jev calls and cost, and `jev_minus_baseline`. Ranges add a per-day list. A one-day summary names the day (`Paper NQZ6 Tue 11/3: …`) and ends ` (crashed: <error>)` for a crashed day. |
| `trade_jev_trades {date, policy, limit}` | read, L0 | The latest `limit` trades (oldest first, ET times) for one policy, plus the total count. |
| `trade_jev_decisions {date, limit}` | read, L0 | The latest `limit` Jev decisions (oldest first, ET): `raw` (Jev's answer), `act` (after the filters), `pos`, `p` (probabilities), `ms` (latency), `err`, plus the total and a `live` block (the same stats as `jev_live`) for the latest session in the range. |
| `trade_jev_start` | write, **L2** | `launchctl kickstart gui/<uid>/com.tradejev.paper`. It does nothing if the job is already running. If the job isn't loaded it refuses and does nothing. |
| `trade_jev_stop` | write, **L2** | `launchctl kill SIGTERM gui/<uid>/com.tradejev.paper`. It does nothing if the job isn't running. |

Every manifest arg is required, because the Resonant SDK marks each declared arg `required`. "Not given" therefore has explicit values:

- **`date`**: `""` or `latest` (the latest session day that was not a holiday or skipped), `today`, `yesterday`, `YYYY-MM-DD`, `week` (Monday through today, ET), `month` (the 1st through today), or `YYYY-MM-DD..YYYY-MM-DD` (inclusive, at most 366 days).
- **`policy`**: `""` or `jev` means the Jev policy. Anything else matches a policy name, case-insensitive (`hold`, `random`, `imbalance`).
- **`limit`**: `0` means 20. The maximum is 50 (trades and decisions).

Results stay under 4 KB: long lists drop their oldest entries and report `omitted`.

**Which policy is Jev?** Any policy whose name contains `jev`, in any case (`is_jev()` in `cli.py`, the only place that rule lives). Every other policy is a baseline. If several match, the first by name is the headline one.

**Where the P&L comes from.** pnl, trades and wins come from `paper_trades` closed trades (`exit_ns` set), so they are current even mid-session. Drawdown, Jev calls and Jev cost come from `paper_summary` when it has a row. `paper_summary` is only written at session end, so for a session without one (still running, or killed before writing it) the Jev policy's `jev_calls` is the count of model calls in its `paper_decisions` log, marked `jev_calls_from: "decisions"` (a Jev that has decisions but no closed trade yet still gets its row) (or `"summary+decisions"` when a day mixes both); `jev_cost_usd` stays null there, because the log has no token counts. A policy with summary rows but no trade rows uses the summary's numbers. A day with several sessions (a restart after a crash) is summed. `holiday` and `skipped` sessions are ignored; `pnl` for a day that was only skipped answers `skipped: <reason>`.

### Is Jev making model calls?

`paper_decisions` is written as each 15 s decision is made, so it answers this mid-session (the 8765 viewer is not attached to the live session, and the session's own view on 8767 isn't something the brain should curl). `jev_live` (in `trade_jev_status`) and `live` (in `trade_jev_decisions`) hold, for one session's Jev policy:

- `decisions`, `warmup` (late-start warm-up rows, `raw_action = WARMUP`), `model_calls` (answered), `errors`, `last_error` (`HH:MM:SS <error>`)
- `avg_ms`, `p95_ms` over the model calls; `last_decision_et`, `last_decision_age_s`
- `raw` and `gated`: the action mix before and after the filters (`raw_action` vs `action`)
- `model` (`paper_sessions.jev_model`) and **`calls_mode`**

The signal is `jev_ms`: live.py records the wall time of the policy call (`Guarded.last_ms`) on every Jev decision. Stored answers (`--stored-answers`) and jsonl cache hits answer from memory in well under 1 ms; a real Jev API call takes about 90 ms or more (avg ~123 ms on the Mini). So a **model call** is an *answered* call: a decision with no error and `jev_ms >= 20` (`CALL_MS` in `cli.py`); failed attempts are counted in `errors`, so attempts = `model_calls + errors`. This log-based count is not the same definition as `paper_summary.jev_calls` (non-cached answers that reported tokens), which is why pnl marks it with `jev_calls_from`. `p95_ms` is nearest-rank. `calls_mode` judges the latest 20 decisions (5 minutes), so it is about now:

| `calls_mode` | Means |
|---|---|
| `live model calls` | at least one recent decision answered after a network round trip |
| `replay/no calls` | the session plays stored answers (`jev_model` is `stored:<run>`), or every recent answer came from memory |
| `calls failing` | every recent decision recorded an error (a timeout or API error is a HOLD) |
| `warming up` / `no decisions yet` | a late start's warm-up, or nothing logged yet |

### Why start and stop are L2

Resonant's gate weighs the level against who chose the action ([docs/extensions.md §4](https://github.com/justinhe16/resonant/blob/main/docs/extensions.md)):

- At L2, a command you type (`/trade-jev stop`) **acts, then reports**.
- When the model chooses the tool from a chat message ("kill trade-jev for today"), it **asks first**, and a plain `yes` approves. You can reply `yes always` to stop being asked in chat.

L3 would act on a model's guess without asking. A misread "stop" would then end the day's session unasked, and the session can't be resumed mid-day without losing its state. The Resonant evals (`prog-start`, `prog-stop`) also expect both tools to ask. L1 (always a `yes <code>`) would be too strict for a reversible action.

The spec does not forbid running `launchctl` from `cli.py`. A tool's argv[0] must be a `.py` file in the extension, which `./cli.py` is, and `cli.py` runs `/bin/launchctl` by absolute path. The deferred extension sandbox design in Resonant's docs would deny `launchctl`. If that sandbox ships, start and stop would need an allowance or would have to move.

If `launchctl` times out (10 s), start and stop return `"unknown"` with exit 0 and don't fail, because the action may have gone through. Resonant takes exit 1 to mean nothing happened.

If the plist sets `KeepAlive`, launchd restarts the job after SIGTERM. In that case the runner should treat SIGTERM as "aborted for today" and exit cleanly.

## Jobs

| Job | When (America/New_York) | Reports |
|---|---|---|
| `close-summary` | 16:10 weekdays (catch-up 6 h) | `trade-jev/pnl/<day>`, urgency **normal** (the hourly pulse). The title always names its day, so an open item is never read as the current session: `Paper NQ Mon 11/2: Jev +$420 (6 trades), hold −$110, random +$30` for a finished day; otherwise the end comes first, `Paper NQZ6 Mon 11/2: crashed — BentoError: A live data license is required…` (the error's first line), or `stale (likely dead)`, `aborted`, `still running`, followed by `; <P&L>` only if something traded. The report's ttl runs to the next trading day's 09:30 ET open (`18h` from Monday 16:10, `66h` from Friday), so once the next session is up the previous day's item is closed and the morning briefing can't list it as today's. `state` holds the day, the status, Jev's P&L rounded to $50 and Jev's trade count, so a re-run on the same day isn't news. Nothing is reported on a holiday or a day with no session. A caught-up run reports the day it was due for (`RESONANT_JOB_DUE_AT`). |
| `watch` | at :05, :20, :35 and :50 of every hour, every day (`5,20,35,50 * * * *`). The token check runs on every run; the paper-session check inside it only runs between 09:30 and 16:00 on NYSE trading days, so its first check is 09:35 and its last is 15:50. | Two keys, both urgency **urgent** (texted at once). **`trade-jev/token`**, any hour of any day, titled `Tradovate token expired — log into trader.tradovate.com and paste a fresh access token + md token into ~/trade-jev/.env`, while the keep-alive's status file (`data/tradovate_token_status.json`) shows renewal failing (2 or more consecutive failures), a token that expires within 10 min or already expired, or an unreadable file; one `clear` once healthy again. No status file means token mode isn't in use: silent. **`trade-jev/down`**, while today's latest session is `crashed` or stale, or when no session exists by 09:35 (caught at the 09:35 run). Once healthy again it sends one `clear` (texted as ✓). `done`, `aborted` (stopped on purpose), `holiday` and `skipped` are healthy (a `skipped` row also counts as the runner having run, so there is no missing-by-09:35 alert). After `trade_jev_stop`, watch stays quiet for the rest of that ET day even if the runner never wrote `aborted`. The marker is `$RESONANT_EXT_DATA/stopped_day`, and `trade_jev_start` removes it. The clear is sent only after a down report, which is remembered in `$RESONANT_EXT_DATA/watch_state.json`, so a healthy day sends nothing. The down and token states are remembered independently in the same file (read-modify-write). |

Each job also has a one-line `description`, and the manifest lists the repo's three launchd agents under `launch_agents` (`com.tradejev.paper`, `com.tradejev.view`, `com.tradejev.tvtoken`, from `deploy/launchd`). Both are display-only: the Resonant dashboard's diagram shows them, and Resonant never installs, loads or reads the agents.

The manifest grants `report: {max_urgency: urgent}` and a 5-minute `health` check (`./cli.py health`). The check is healthy when the config parses and the DB, if present, opens read-only. A missing DB is healthy.

Trading days: weekdays minus the NYSE full closures for 2026–2027 built into `cli.py`. Add more in `config.json`.

## Dashboard panels

The manifest declares two Resonant panels, `paper` and `chart`.

### `paper`

`paper` (title "Paper trading"): `./cli.py panel paper` prints one JSON object in Resonant's panel format ([docs/extensions.md §6a](https://github.com/justinhe16/resonant/blob/main/docs/extensions.md)), and the dashboard draws it, so the local paper page (`:8765/paper`, `:8767/live`) can be read on a phone. It is an L0 read like the tools: it reads `paper.db` (read-only) and the token status file, runs `launchctl print` on the three agents, and never acts. It runs every minute 09:00-16:59 ET on weekdays (`active: "*/1 9-16 * * 1-5"`, which covers the 09:15-16:15 session because cron only has whole hours) and every 15 minutes otherwise (`idle`), with `max_bytes: 24576`. `local_url` is the session's live view (`http://127.0.0.1:8767/live`) while a session runs, else `http://127.0.0.1:8765/paper`.

Everything comes from the tools' own helpers (`cmd_status`, `cmd_pnl`, `policy_stats`, `launchd_state`), so the panel and the chat answers agree. The cumulative curve and the recent-trades table need every closed trade of the day (`trades` returns one policy and at most 50 rows), which is the panel's one extra query (`closed_trades`).

**Today.** With no session today (a weekend, a holiday, before the start), the P&L widgets show the latest trading day and say so (`last session Fri 11/6`).

- Stats: **Jev P&L**, **Jev trades / wins**, **Model calls** (avg and p95 ms), **Call errors** (last error), **Calls mode** (`jev_live.calls_mode`), **Session** (`running`, `done`, `crashed` with the error's first line, `stale`, `aborted`, `skipped`, or `no session` with the last session day), **Token expires in** (Tradovate, from the keep-alive's status file; `n/a` when token mode isn't in use).
- Series: **Cumulative P&L** through the day, one line per policy (Jev, imbalance, random, then other baselines, at most 4): 0 at the 09:30 open, then a point at each exit. Over 240 points it is thinned to 240 evenly spaced points, so the running totals stay exact instead of being bucket-averaged by Resonant.
- Table: **Recent trades**, the latest 20 closed trades of every policy (time ET, policy, side, points, $, why).
- Status: `com.tradejev.paper`, `com.tradejev.view`, `com.tradejev.tvtoken` (down when not loaded; warn when the always-on viewer or keep-alive isn't running, or the paper job isn't during session hours), and the **data feed**, judged by the session heartbeat (up while running, down when stale).

**History.**

- Series: **Daily P&L** per policy for the last 30 trading days (bars).
- Table: the latest 30 **sessions** (day, status, Jev $, trades, Jev calls, the error's first line).

**Health.** `bad` when today's session crashed or went stale, no session is up during session hours (unless stopped with `trade_jev_stop`), every recent Jev call fails, or the Tradovate token is expired, failing renewal or unreadable. `warn` when the token is expiring or its renewer stopped, an agent is down or not running when it should be, or Jev has call errors in a running session. Otherwise `ok`.

### `chart`

`chart` (title "Jev trades", `./cli.py panel chart`) is listed after `paper`, so it lands in the right-hand column of `#/ext/trade-jev`. Same refresh, tz and `timeout_s` as `paper`; `max_bytes: 32768`.

- One `price` widget: **1-minute candles of the top-of-book mid** of the session's contract (the session's `symbol`, e.g. NQZ6), 09:30 to 15:59 ET (about 390 candles), titled e.g. `NQZ6 1-min mid, today`. The recordings carry signed trade volume but no trade prices, so the candles are mid, not last trade.
- **Jev's trades only** (the `jev[...]` policy) from `paper_trades`: side, entry and exit time and price, $ P&L after commission, and why (`target`, `stop`, `reversal` (the runner's `jev` reason: Jev's own order closed it), `eod`). The newest 100 (the widget's cap). While the session is running (fresh heartbeat), Jev's open position (`paper_open`) is an open trade.
- **No session today** (weekend, holiday, or before 09:30 when today's session has no candles yet): the latest earlier session, titled e.g. `NQZ6 1-min mid, Tue 11/3 (last session)`.
- **Lag:** a minute's candle is written when the next minute's first quote arrives, so the chart is up to a minute behind, and from 09:30 to 09:31 it still shows the last session.
- **Crashed session:** the candles up to the crash, plus a text widget with the error's first line; health `bad` (also when the session is stale) while it is today's session.
- **A session from before `paper_bars`** (no bars recorded): a line of the last 15 s decision mid per minute, titled `mid (15 s samples; no 1-min bars)`. Not candles: on 11-03/11-04, OHLC built from 15 s samples had about 60% of the true range and 17 of 42 Jev fills fell outside their candle. Backfill a recorded day instead with `uv run python -m trade_jev.paperdb bars --day 2026-11-04` (from the repo root; it reads `data/live/…__live.parquet` and writes that day's `paper_bars`).

**Where the candles come from.** The runner writes them: `paper_bars(session_id, t_s, o, h, l, c)`, the minute's mid OHLC from every quote, inserted as each minute completes (the last at session end), through the same queued writer as the rest of the paper record. Nothing in the engine reads them back, and a failing write is logged and skipped. The panel's query is an indexed lookup of about 400 rows, which takes milliseconds at the end of a full day (the budget is 5 s). The Parquet recording can't be used instead: this directory is stdlib only, and the file has no footer until the session closes.

## Data source and config

The default DB is `/Users/justinhe/trade-jev/data/paper.db`, opened as `file:…?mode=ro`. If the DB or its `paper_sessions` table doesn't exist yet, the tools answer "no paper sessions yet" rather than failing.

Resonant runs the extension with `HOME` set to its data dir, not your home directory. To override defaults, write `$RESONANT_EXT_DATA/config.json` (normally `~/.resonant/ext-data/trade-jev/config.json`). Every key is optional:

```json
{"db_path": "/Users/justinhe/trade-jev/data/paper.db",
 "token_status_path": "/Users/justinhe/trade-jev/data/tradovate_token_status.json",
 "launchd_label": "com.tradejev.paper",
 "holidays": ["2026-11-27"]}
```

Schema expected (written by the paper runner):

- `paper_sessions(session_id PK, day (ET date), symbol, status running|done|crashed|holiday|aborted|skipped, source (nullable; the feed), started_at, ended_at, heartbeat_at (ISO UTC, at least every 60 s while running), settings_json, jev_model, run_dir, error)`
- `paper_trades(id, session_id, policy, side, entry_ns, entry_px, exit_ns, exit_px, reason jev|stop|target|eod, points, pnl)`
- `paper_decisions(session_id, policy, t_ns, time_et, mid, position, action, raw_action, probs_json, jev_ms, error)`
- `paper_summary(session_id, policy, trades, wins, pnl, max_drawdown, jev_calls, jev_cost_usd)`, PK `(session_id, policy)`
- `paper_bars(session_id, t_s (minute start, unix s), o, h, l, c)`, PK `(session_id, t_s)` (chart only; may be absent on an older DB)
- `paper_open(session_id, policy, side long|short, entry_ns, entry_px)`, PK `(session_id, policy)`: a row while that policy is in a position (chart only; may be absent)

## Constraints it follows

- Stdlib only, run as `python -I -S cli.py <cmd>` with `PATH=/usr/bin:/bin`.
- No dependency files in this directory.
- Prints one JSON object on stdout and exits 0. On failure it prints one stderr line and exits 1, and nothing changed.
- `emit.py` and `panel.py` are unmodified copies of `resonant_sdk/emit.py` and `resonant_sdk/panel.py`. Copy them again if the SDK changes.

## Tests

From the repo root:

```sh
uv run pytest -q                      # whole repo, including these
uv run pytest -q resonant-extension   # just the extension
```

The tests build a temp DB with the schema above, fake `launchctl` and `report()`, and run the CLI under `python -I -S` with a scratch environment. One test validates `resonant.yaml` with the real parser (`resonant_sdk.manifest.load_manifest`) through `uv run --frozen --no-sync --project $RESONANT_REPO` (default `/Users/justinhe/resonant`; it installs nothing and never reads `~/.resonant`), and is skipped where there is no Resonant checkout. `test_trade_jev_panel.py` checks the panel on a running day, a crashed day, a weekend with no session and no DB, keeps a heavy 60-day history under the 24 KB budget, and runs every output through the same checkout's `docs/panel.schema.json` and core's own `normalize()` with the manifest's `max_bytes` (skipped likewise). `test_trade_jev_chart.py` does the same for `chart`: a running day with an open trade, a full day, a crashed day, a weekend, before the open, a restart, the decision-mid fallback, OHLC validity, and a large day (60 earlier sessions, 3200 trades, bars from 08:00 to 17:00) that must stay under 32 KB after normalize and run in under 5 s. They use no network and never run the real `launchctl`. The test modules have no `__init__.py` and use unique names, so they don't collide with the repo's `tests` package.
