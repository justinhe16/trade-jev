"""Harness settings: the one place defaults live.

The default is the registered "most consistent" setting from the 15-day replay grid
(results/FINDINGS.md): positive on 12 of 15 days, worst day −$1,065.

The bracket moved from 25/50 points to 40/75 on 2026-11-11 (owner): on the 62 live paper entries
of 11/3-11/10, ignoring Jev's exits, 40/75 was the best TP/SL cell on t per day (3.0, positive on 6 of 6 days).
"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class Settings:
    min_conf: float = 0.7    # ignore Jev answers below this probability
    agree: int = 4           # consecutive same-side answers needed to act
    min_hold_s: float = 30   # seconds before a position may reverse
    stop_ticks: int = 300    # 75 points; 0 = off
    target_ticks: int = 160  # 40 points; 0 = off

    def label(self) -> str:
        return (f"conf≥{self.min_conf} agree={self.agree} hold={self.min_hold_s}s "
                f"stop={self.stop_ticks or 'off'} target={self.target_ticks or 'off'}")


DEFAULT = Settings()
