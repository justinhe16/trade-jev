# trade-jev Resonant extension

Lets [Resonant](https://github.com/justinhe16/resonant) answer "is trade-jev running?", "how did Jev do this week vs hold?" and "show me today's trades". It can start or stop the paper session, texts you if the session dies during market hours, and puts each day's result in the hourly pulse after the close.

It is **paper only**. It reads the paper runner's SQLite DB read-only and runs `launchctl` on the paper job. It never places orders.

## Install (owner, on the Mini's terminal)

Commit the extension first: `path:` installs only tracked, unchanged files. Then:

```sh
resonant ext validate /Users/justinhe/trade-jev/resonant-extension
resonant ext add path:/Users/justinhe/trade-jev/resonant-extension
```

`ext add` shows the review screen. Type `trade-jev` to confirm. After a later change, run `resonant ext update trade-jev` (the token alert needs this update: it changes the `watch` schedule and `trade_jev_status`; so does the live Jev visibility below: it adds `trade_jev_decisions`, job descriptions and `launch_agents`, so the review screen asks again). The update only swaps the extension's files: the running paper session (`com.tradejev.paper`) is not touched and needs no restart.

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

**Where the P&L comes from.** pnl, trades and wins come from `paper_trades` closed trades (`exit_ns` set), so they are current even mid-session. Drawdown, Jev calls and Jev cost come from `paper_summary` when it has a row. `paper_summary` is only written at session end, so for a session without one (still running, or killed before writing it) the Jev policy's `jev_calls` is the count of model calls in its `paper_decisions` log, marked `jev_calls_from: "decisions"` (or `"summary+decisions"` when a day mixes both); `jev_cost_usd` stays null there, because the log has no token counts. A policy with summary rows but no trade rows uses the summary's numbers. A day with several sessions (a restart after a crash) is summed. `holiday` and `skipped` sessions are ignored; `pnl` for a day that was only skipped answers `skipped: <reason>`.

### Is Jev making model calls?

`paper_decisions` is written as each 15 s decision is made, so it answers this mid-session (the 8765 viewer is not attached to the live session, and the session's own view on 8767 isn't something the brain should curl). `jev_live` (in `trade_jev_status`) and `live` (in `trade_jev_decisions`) hold, for one session's Jev policy:

- `decisions`, `warmup` (late-start warm-up rows, `raw_action = WARMUP`), `model_calls`, `errors`, `last_error` (`HH:MM:SS <error>`)
- `avg_ms`, `p95_ms` over the model calls; `last_decision_et`, `last_decision_age_s`
- `raw` and `gated`: the action mix before and after the filters (`raw_action` vs `action`)
- `model` (`paper_sessions.jev_model`) and **`calls_mode`**

The signal is `jev_ms`: live.py records the wall time of the policy call (`Guarded.last_ms`) on every Jev decision. Stored answers (`--stored-answers`) and jsonl cache hits answer from memory in well under 1 ms; a real Jev API call takes about 90 ms or more (avg ~123 ms on the Mini). So a **model call** is a decision with no error and `jev_ms >= 20` (`CALL_MS` in `cli.py`). `calls_mode` judges the latest 20 decisions (5 minutes), so it is about now:

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

## Constraints it follows

- Stdlib only, run as `python -I -S cli.py <cmd>` with `PATH=/usr/bin:/bin`.
- No dependency files in this directory.
- Prints one JSON object on stdout and exits 0. On failure it prints one stderr line and exits 1, and nothing changed.
- `emit.py` is an unmodified copy of `resonant_sdk/emit.py`. Copy it again if the SDK changes.

## Tests

From the repo root:

```sh
uv run pytest -q                      # whole repo, including these
uv run pytest -q resonant-extension   # just the extension
```

The tests build a temp DB with the schema above, fake `launchctl` and `report()`, and run the CLI under `python -I -S` with a scratch environment. One test validates `resonant.yaml` with the real parser (`resonant_sdk.manifest.load_manifest`) through `uv run --frozen --no-sync --project $RESONANT_REPO` (default `/Users/justinhe/resonant`; it installs nothing and never reads `~/.resonant`), and is skipped where there is no Resonant checkout. They use no network and never run the real `launchctl`. The test modules have no `__init__.py` and use unique names, so they don't collide with the repo's `tests` package.
