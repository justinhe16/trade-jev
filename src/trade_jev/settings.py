"""Harness settings: the one place defaults live.

The default is the registered "most consistent" setting from the 15-day replay grid
(results/FINDINGS.md): positive on 12 of 15 days, worst day −$1,065.
"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class Settings:
    min_conf: float = 0.7    # ignore Jev answers below this probability
    agree: int = 4           # consecutive same-side answers needed to act
    min_hold_s: float = 30   # seconds before a position may reverse
    stop_ticks: int = 200    # 0 = off
    target_ticks: int = 100  # 0 = off

    def label(self) -> str:
        return (f"conf≥{self.min_conf} agree={self.agree} hold={self.min_hold_s}s "
                f"stop={self.stop_ticks or 'off'} target={self.target_ticks or 'off'}")


DEFAULT = Settings()
