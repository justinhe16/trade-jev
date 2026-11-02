"""Which days the NQ regular session trades: weekdays, minus CME equity-futures full closures.

Only full closures are listed. On the other US holidays (MLK, Presidents, Memorial, Juneteenth,
July 4, Labor, Thanksgiving) CME equity futures trade a shortened session that halts at 13:00 ET;
the live runner treats those as trading days and closes when the feed stops (see live.py).

Hard-coded from the CME holiday calendar. Add the next year before it starts; `COVERED_YEARS`
is checked so an uncovered year warns instead of silently trading a closed day.
"""

from __future__ import annotations

from datetime import date

FULL_CLOSURES = {
    "2026-01-01": "New Year's Day",
    "2026-04-03": "Good Friday",
    "2026-12-25": "Christmas Day",
    "2027-01-01": "New Year's Day",
    "2027-03-26": "Good Friday",
    "2027-12-24": "Christmas Day (observed)",
}
COVERED_YEARS = {2026, 2027}


def closed_reason(day: str | date) -> str | None:
    """Why `day` has no regular session ("weekend" or the holiday's name), or None if it trades."""
    d = date.fromisoformat(day) if isinstance(day, str) else day
    if d.weekday() >= 5:
        return "weekend"
    return FULL_CLOSURES.get(d.isoformat())


def is_trading_day(day: str | date) -> bool:
    return closed_reason(day) is None


def covered(day: str | date) -> bool:
    d = date.fromisoformat(day) if isinstance(day, str) else day
    return d.year in COVERED_YEARS
