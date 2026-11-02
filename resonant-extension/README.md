# trade-jev Resonant extension

Lets [Resonant](https://github.com/justinhe16/resonant) answer "is trade-jev running?", "how did Jev do this week vs hold?" and "show me today's trades". It can start or stop the paper session, texts you if the session dies during market hours, and puts each day's result in the hourly pulse after the close.

It is **paper only**. It reads the paper runner's SQLite DB read-only and runs `launchctl` on the paper job. It never places orders.

## Install (owner, on the Mini's terminal)

Commit the extension first: `path:` installs only tracked, unchanged files. Then:

```sh
resonant ext validate /Users/justinhe/trade-jev/resonant-extension
resonant ext add path:/Users/justinhe/trade-jev/resonant-extension
```

`ext add` shows the review screen. Type `trade-jev` to confirm. After a later change, run `resonant ext update trade-jev`.

Each job's first run after install is a silent baseline. Run `/ext run trade-jev/watch` once before relying on it.

## Tools

| Tool | Effect / level | What it does |
|---|---|---|
| `trade_jev_status` | read, L0 | Today's session (ET date): `running`, `done`, `crashed`, `aborted`, `holiday` or **`stale`**. Stale means status is `running` but the heartbeat is more than 3 min old, so the process is likely dead. Also shows Jev's position (its latest decision), realized P&L, trade count and wins so far, and whether the launchd job is loaded (`launchctl print gui/<uid>/com.tradejev.paper`, best effort). |
| `trade_jev_pnl {date}` | read, L0 | Jev vs the baselines: pnl, trades, wins, max drawdown, Jev calls and cost, and `jev_minus_baseline`. Ranges add a per-day list. |
| `trade_jev_trades {date, policy, limit}` | read, L0 | The latest `limit` trades (oldest first, ET times) for one policy, plus the total count. |
| `trade_jev_start` | write, **L2** | `launchctl kickstart gui/<uid>/com.tradejev.paper`. It does nothing if the job is already running. If the job isn't loaded it refuses and does nothing. |
| `trade_jev_stop` | write, **L2** | `launchctl kill SIGTERM gui/<uid>/com.tradejev.paper`. It does nothing if the job isn't running. |

Every manifest arg is required, because the Resonant SDK marks each declared arg `required`. "Not given" therefore has explicit values:

- **`date`**: `""` or `latest` (the latest non-holiday session day), `today`, `yesterday`, `YYYY-MM-DD`, `week` (Monday through today, ET), `month` (the 1st through today), or `YYYY-MM-DD..YYYY-MM-DD` (inclusive, at most 366 days).
- **`policy`**: `""` or `jev` means the Jev policy. Anything else matches a policy name, case-insensitive (`hold`, `random`, `imbalance`).
- **`limit`**: `0` means 20. The maximum is 50.

Results stay under 4 KB: long lists drop their oldest entries and report `omitted`.

**Which policy is Jev?** Any policy whose name contains `jev`, in any case (`is_jev()` in `cli.py`, the only place that rule lives). Every other policy is a baseline. If several match, the first by name is the headline one.

**Where the P&L comes from.** pnl, trades and wins come from `paper_trades` closed trades (`exit_ns` set), so they are current even mid-session. Drawdown, Jev calls and Jev cost come from `paper_summary` when it has a row. A policy with summary rows but no trade rows uses the summary's numbers. A day with several sessions (a restart after a crash) is summed. `holiday` sessions are ignored.

### Why start and stop are L2

Resonant's gate weighs the level against who chose the action ([docs/extensions.md §4](https://github.com/justinhe16/resonant/blob/main/docs/extensions.md)):

- At L2, a command you type (`/trade-jev stop`) **acts, then reports**.
- When the model chooses the tool from a chat message ("kill trade-jev for today"), it **asks first**, and a plain `yes` approves. You can reply `yes always` to stop being asked in chat.

L3 would act on a model's guess without asking. A misread "stop" would then end the day's session unasked, and the session can't be resumed mid-day without losing its state. The Resonant evals (`prog-start`, `prog-stop`) also expect both tools to ask. L1 (always a `yes <code>`) would be too strict for a reversible action.

The spec does not forbid running `launchctl` from `cli.py`. A tool's argv[0] must be a `.py` file in the extension, which `./cli.py` is, and `cli.py` runs `/bin/launchctl` by absolute path. The deferred extension sandbox design in Resonant's docs would deny `launchctl`. If that sandbox ships, start and stop would need an allowance or would have to move.

If the plist sets `KeepAlive`, launchd restarts the job after SIGTERM. In that case the runner should treat SIGTERM as "aborted for today" and exit cleanly.

## Jobs

| Job | When (America/New_York) | Reports |
|---|---|---|
| `close-summary` | 16:10 weekdays (catch-up 6 h) | `trade-jev/pnl/<day>`, urgency **normal** (the hourly pulse). Title example: `Paper NQ: Jev +$420 (6 trades), hold −$110, random +$30`, with ` (crashed)` and similar appended when the day didn't finish `done`. `state` holds the day, the status, Jev's P&L rounded to $50 and Jev's trade count, so a re-run on the same day isn't news. Nothing is reported on a holiday or a day with no session. A caught-up run reports the day it was due for (`RESONANT_JOB_DUE_AT`). |
| `watch` | every 15 min, 09:00–16:45 weekdays. It checks only between 09:30 and 16:00 on NYSE trading days. | `trade-jev/down`, urgency **urgent** (texted at once), while today's latest session is `crashed` or stale, or when no session exists by 09:35 (first caught at the 09:45 run). Once healthy again it sends one `clear` (texted as ✓). `done`, `aborted` (stopped on purpose) and `holiday` are healthy. The clear is sent only after a down report, which is remembered in `$RESONANT_EXT_DATA/watch_state.json`, so a healthy day sends nothing. |

The manifest grants `report: {max_urgency: urgent}` and a 5-minute `health` check (`./cli.py health`). The check is healthy when the config parses and the DB, if present, opens read-only. A missing DB is healthy.

Trading days: weekdays minus the NYSE full closures for 2026–2027 built into `cli.py`. Add more in `config.json`.

## Data source and config

The default DB is `/Users/justinhe/trade-jev/data/paper.db`, opened as `file:…?mode=ro`. If the DB or its `paper_sessions` table doesn't exist yet, the tools answer "no paper sessions yet" rather than failing.

Resonant runs the extension with `HOME` set to its data dir, not your home directory. To override defaults, write `$RESONANT_EXT_DATA/config.json` (normally `~/.resonant/ext-data/trade-jev/config.json`). Every key is optional:

```json
{"db_path": "/Users/justinhe/trade-jev/data/paper.db",
 "launchd_label": "com.tradejev.paper",
 "holidays": ["2026-11-27"]}
```

Schema expected (written by the paper runner):

- `paper_sessions(session_id PK, day (ET date), symbol, status running|done|crashed|holiday|aborted, started_at, ended_at, heartbeat_at (ISO UTC, at least every 60 s while running), settings_json, jev_model, run_dir, error)`
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

The tests build a temp DB with the schema above, fake `launchctl` and `report()`, and run the CLI under `python -I -S` with a scratch environment. They use no network and never run the real `launchctl`. The test modules have no `__init__.py` and use unique names, so they don't collide with the repo's `tests` package.
