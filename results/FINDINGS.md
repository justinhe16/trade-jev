# Findings: Jev as an NQ scalping policy

Sept 2026. 15 trading days of NQ L10 data (Jun 8–26 2026), 22,401 Jev calls in total, **$0.91** in API cost.

## TL;DR

**Registered setting (now the project default):** cutoff **0.7** / **4** agreeing answers / **30s** min hold / **200**-tick stop / **100**-tick target, defined in `src/trade_jev/settings.py`.

Replayed on all 15 days' stored Jev answers, it made **+$20,795**. It was **positive on 12 of 15 days**, its **worst day was −$1,065**, and it made 178 trades (about 12 per day). Over the same days the imbalance rule lost $21,820 and random lost $11,230.

- **A) Is Jev + harness feasible?**
  - **Engineering: yes.** It's cheap (about 6¢ per trading day), fast, reliable, and fully reproducible.
  - **As a signal: promising but unproven.**
    - The registered setting is the first result that is both positive and steady: a mean of +$1,386/day, t = 3.1, and a bootstrap 90% interval of +$10.5k to +$32.3k.
    - But it was picked *with hindsight*: it ranked #1 of 1,920 settings on consistency, over the same 15 days.
    - It has **never run live**, and it sits in a narrow area of settings: a lower cutoff or no target turns it into a loss.
- **B) Did replaying settings improve the choices?** Yes, dramatically.
  - Every-answer setups lose $15–35k a day.
  - Replays showed which filters matter: a high cutoff, several agreeing answers, a wide stop and a modest target.
  - They also showed that picking the top *total* doesn't carry over to new days. The steady setting we registered only became visible once all 15 days were in.

## Setup

| | |
|---|---|
| Data | Databento `GLBX.MDP3` `mbp-10`, NQM6→NQU6, decisions 09:30–15:55 ET (see [`data/DATA.md`](../data/DATA.md)) |
| Model | `jev-1.13.0` (every call), one BUY / SELL / HOLD Choice per decision |
| What Jev sees | Raw numbers: L10 ladder, mid now / 15s / 60s ago, net aggressor volume over 15s / 60s, time, our position |
| Harness | 1 NQ contract, a decision every 15s, 250ms latency, fills at bid/ask, $2.50/side commission, flat at 15:55 |
| Settings | cutoff / agreeing answers / min hold / stop ticks / target ticks (`0` = off) |
| Baselines | hold (never trades), random (trades 20% of decisions), imbalance (L10 depth imbalance + 60s flow) |
| Runs | [`20260917-172739`](20260917-172739/) Jun 23 (pilot) · [`20260917-174009`](20260917-174009/) Jun 8–10 · [`20260917-holdout11`](20260917-holdout11/) 11 new days |

## The registered setting

### Per day (replayed, net of costs)

| Day | 06-08 | 06-09 | 06-10 | 06-11 | 06-12 | 06-15 | 06-16 | 06-17 | 06-18 | 06-19 | 06-22 | 06-23 | 06-24 | 06-25 | 06-26 |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| P&L $ | −15 | 0 | +495 | +1,845 | +1,930 | +1,935 | +5,355 | +1,945 | +395 | +495 | +460 | +490 | +4,815 | −1,065 | +1,715 |
| Trades | 3 | 0 | 1 | 16 | 13 | 11 | 17 | 10 | 16 | 1 | 7 | 29 | 28 | 10 | 16 |

### Compared with the other settings and the baselines

| | 4 tuning days | 11 new days | All 15 | Winning days | Worst day | Trades |
|---|---|---|---|---|---|---|
| **Registered** (0.7 / 4 / 30s / 200 / 100) | +$970 | **+$19,825** | **+$20,795** | **12 / 15** | **−$1,065** | 178 |
| Chosen earlier (0.5 / 2 / 120s / 400 / off), ran live on the 11 days | +$36,305 | +$7,835 | +$44,140 | 8 / 15 | −$13,630 | 1,148 |
| First base (0.8 / 2 / 0s / 200 / off) | +$21,720 | +$1,220 | +$22,940 | 7 / 15 | −$8,200 | 141 |
| Every answer, no exits | +$29,615 | −$158,205 | −$128,590 | 3 / 15 | −$37,950 | 6,941 |
| Imbalance baseline (live) | +$7,360 | −$29,180 | −$21,820 | 4 / 15 | −$10,390 | — |
| Random baseline (live) | −$7,725 | −$3,505 | −$11,230 | 6 / 15 | −$15,760 | — |

### Why it's steady

- **Four agreeing answers at ≥ 0.7** means Jev has to say the same thing for a full minute before we act, which filters out its snapshot-to-snapshot flipping.
- **The 100-tick target (25 points, $500) takes profits** instead of waiting for Jev to reverse; 76% of trades win on the 11 new days.
- **The 200-tick stop limits losers** without firing on one-update gaps in the book.
- **It trades about 12 times a day**, so costs stay small.

### Why it's not proven yet

- **Hindsight selection.** It is #1 of 1,920 settings by winning days on the *same* 15 days. With that many tries, something will look steady by chance.
  - A walk-forward test (pick the most consistent setting on all earlier days, trade the next day) never picked it and lost **$12,210** over 11 days.
  - Picking by total lost $37,220.
- **Narrow area.** One step away in either direction changes the result a lot:

  | Change | 15-day total | Winning days |
  |---|---|---|
  | cutoff 0.6 | −$9,810 | 5 |
  | cutoff 0.8 | +$17,600 | 10 |
  | 3 agreeing | +$7,970 | 9 |
  | min hold 0–60s | +$20,795 | 12 |
  | min hold 120s | +$19,665 | 11 |
  | stop 100 | +$8,315 | 8 |
  | stop 400 | +$21,965 | 10 |
  | stop off | +$8,425 | 9 |
  | target off | −$13,110 | 5 |
  | target 200 | +$15,465 | 9 |

  Min hold doesn't matter, and a higher cutoff or wider stop is fine. A lower cutoff, fewer agreeing answers, or no target breaks it.
- **Replay only.** Jev never saw the positions this setting would hold (see the caveat below), so a live run could differ.
- **Short, one-regime sample.** 15 days, and most of the profit came from 4 days (Jun 16, 24, 15, 17).

## A) Is Jev + harness feasible?

**Engineering: yes.**
- **Cost:** about 935 input tokens per call, $0.04 per 1M tokens, so about **$0.06 per day** of 15s decisions.
- **Speed and reliability:** 11 days ran in 25 minutes (about 11 calls/s, 4 days in parallel), with no errors or retries.
- **Output:** typed, with a probability for each answer, so filtering on confidence is trivial.
- **Reproducibility:** every answer is saved, so replays cost nothing. Replaying a run under its own settings reproduces its live P&L exactly.

**As a signal: raw Jev answers have no edge; the registered filter looks promising but is unproven.**
- **Unfiltered, Jev reacts to the latest snapshot.** On most days, 29–42% of consecutive answers flip straight between BUY and SELL, and acting on each one pays the spread every 30s.
- **It leans short.** Jev answered SELL 1.6× as often as BUY. The 11 new days fell 599 points, so short-leaning setups benefited: always-short made about +$11.7k and always-long about −$12.3k.
  - The registered setting made money on both up days (06-11, 06-12, 06-15) and down days (06-16, 06-17, 06-24), so it isn't just the short tilt.
- **The earlier live choice (0.5 / 2 / 120s / 400) made +$7,835 on the 11 new days,** but one day (+$23,670) supplied all of it (t = 0.24).

## B) How replaying settings changed the choices

| Step | What we learned | Change |
|---|---|---|
| Pilot, Jun 23: every answer, 20-tick stop / 40-tick target | −$33,380 on 1,219 trades. 826 stops fired after a median of 3.4s, triggered by one-update gaps in the book | Stop 200 ticks, target off |
| Sweep on Jun 23 | 2 agreeing answers with a 0.8 cutoff turned −$13k into +$5.9k | First base: 0.8 / 2 / 0s / 200 / off |
| Live, Jun 8–10 on the first base | 26 trades; profit came from positions held until the close | — |
| Grid on 4 days | "Target off" looked best; picking by top total overfit | Chosen: 0.5 / 2 / 120s / 400 / off |
| Live, 11 new days on the chosen settings | +$7,835, but carried by one day | — |
| Grid on all 15 days | Settings ranked by 4-day results didn't predict the next 11 (correlation −0.14). Ranking by *winning days* surfaced a steady area: a high cutoff, 4 agreeing answers, and a modest target | **Registered: 0.7 / 4 / 30s / 200 / 100** |

What held up across all 15 days (average 15-day total by setting):
- **Cutoff:** higher is better, from −$58k with none to about $0 at 0.8.
- **Agreeing answers:** more is better, from −$57k with 1 to −$11k with 4.
- **Min hold:** longer helps up to 120s.
- **Stop:** roughly neutral.
- **Target:** off is best *on average*, but in the steady area (4 agreeing, ≥ 0.7) a 100-tick target is what makes it steady.

## Caveat: Jev's answers depend on our position

Same model and same kind of market, but very different confidence depending on the position shown in its state:

| Position shown to Jev | Median confidence | Share ≥ 0.8 | Share HOLD |
|---|---|---|---|
| Flat | 0.87–0.92 | 58–75% | 0% |
| Long | 0.60–0.65 | 15–38% | 3–7% |
| Short | 0.47–0.54 | **0%** | 5–38% |

- **High cutoffs are hard to reach while short.** While short, only 6% of answers reach 0.7, and none reach 0.8. The 0.8 first base sat short for 96% of the Jun 8–10 run.
  - At 0.7, the registered setting gets reversal signals rarely (3.6% of answers while short), so its 100-tick target and 200-tick stop do most of the exiting.
- **Replays of other settings are estimates.** A replay reuses answers Jev gave while holding the original run's position. The registered setting is mostly flat (about 12 trades a day, closed by the target or stop), and Jev is *more* confident when flat. So a live run could trade more often than the replay shows.

## Next steps

1. **Run the registered setting live on new days.** It's the default now: `uv run python -m trade_jev.run --days …`. Ideally use new days, since all 15 here were used to choose it; rerunning them live costs about $0.93 and would at least remove the replay approximation.
2. **Split the question:** ask about entry only while flat, and handle exit with its own question (or just the stop/target), so position stops distorting the answers.
3. **Send Jev labeled features instead of raw numbers.** Its notes say it "is not a calculator".
4. **Test on more data:** months of data, compared against always-short / always-long, with settings chosen on one period and tested on the next.

## Reproduce

```bash
uv run python scripts/replay_grid.py runs/20260917-172739 runs/20260917-174009 runs/20260917-holdout11
uv run python scripts/analyze_replays.py runs/replays/grid-<ts>.csv
uv run python scripts/findings.py --tune runs/20260917-172739 runs/20260917-174009 \
  --test runs/20260917-holdout11 --grid runs/replays/grid-<ts>.csv
```

These need the local runs and Databento data. The published answers, trades and summaries for each run are in this folder, and the viewer can replay the registered setting on any day.
