"""Replays: a run's stored Jev answers + settings → trades and P&L. No Jev calls.

Vocabulary
- run:      a backtest that calls Jev (`python -m trade_jev.run`); stored in runs/<id>/.
- settings: harness params (cutoff, agree, min hold, stop, target, commission).
- replay:   one run's stored answers re-scored under one set of settings.

Caveat: Jev answered with the run's own positions in its state, so a replay whose settings
hold different positions approximates what Jev would have said.
"""

from __future__ import annotations

import dataclasses
import json
from dataclasses import dataclass
from pathlib import Path

import numpy as np

from trade_jev.data import Day, decision_grid, load_day
from trade_jev.encode import Context
from trade_jev.harness import Config, DayResult, run_day
from trade_jev.policies import Decision, Gated
from trade_jev.settings import Settings  # noqa: F401  (re-exported for scripts)


@dataclass
class RunDay:
    """One day of stored answers, with the config of the run that produced them."""
    day: str
    config: Config
    answers: dict[int, dict]  # t_ns → decision record from decisions.jsonl


def load_runs(run_dirs: list[Path]) -> dict[str, RunDay]:
    """Stored answers by day across one or more runs (a later run wins on overlapping days)."""
    out: dict[str, RunDay] = {}
    for rd in run_dirs:
        cfg = Config(**json.loads((rd / "config.json").read_text())["config"])
        days: dict[str, dict[int, dict]] = {}
        for line in next(rd.glob("jev*/decisions.jsonl")).open():
            d = json.loads(line)
            days.setdefault(d["day"], {})[d["t_ns"]] = d
        for day, answers in days.items():
            out[day] = RunDay(day, cfg, answers)
    return out


def load_market(rd: RunDay) -> tuple[Day, np.ndarray]:
    """The day's book data and the decision times the run used."""
    c = rd.config
    grid = decision_grid(rd.day, c.start_et, c.end_et, c.cadence_s)
    day = load_day(rd.day, grid)
    return day, grid[(grid >= day.ts[0]) & (grid <= day.ts[-1])]


class StoredAnswers:
    """Policy that plays back what Jev said in the run (before any gating)."""
    name = "replay"

    def __init__(self, answers: dict[int, dict]):
        self.answers = answers

    async def __call__(self, ctx: Context) -> Decision:
        a = self.answers[ctx.t_ns]
        return Decision(a.get("raw_action") or a["action"], a["probs"])


async def replay_day(rd: RunDay, day: Day, grid: np.ndarray, s: Settings) -> DayResult:
    cfg = dataclasses.replace(rd.config, stop_ticks=s.stop_ticks, target_ticks=s.target_ticks)
    policy = Gated(StoredAnswers(rd.answers), s.min_conf, s.agree, s.min_hold_s)
    return await run_day(day, grid, policy, cfg)
