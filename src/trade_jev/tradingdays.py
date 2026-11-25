"""Which days the NQ regular session trades: weekdays, minus CME equity-futures full closures.

Full closures skip the day. On the other US holidays (MLK, Presidents, Memorial, Juneteenth, July 4,
Labor, Thanksgiving) CME equity futures trade a shortened session that halts at 13:00 ET, and the day
after Thanksgiving and Christmas Eve halt at 13:15 ET. Those are EARLY_HALTS: the paper runner stops
deciding and the copy daemon flattens FLATTEN_BEFORE_HALT_MIN before the halt. MFF does not
auto-liquidate at a holiday close, and a position still open when the market halts breaches the account.

Hard-coded from the CME holiday calendar. Add the next year before it starts; `COVERED_YEARS`
is checked so an uncovered year warns instead of silently trading a closed day.
"""

from __future__ import annotations

from datetime import date, datetime, timedelta

FULL_CLOSURES = {
    "2026-01-01": "New Year's Day",
    "2026-04-03": "Good Friday",
    "2026-12-25": "Christmas Day",
    "2027-01-01": "New Year's Day",
    "2027-03-26": "Good Friday",
    "2027-12-24": "Christmas Day (observed)",
}
EARLY_HALTS = {   # CME equity futures halt, ET
    "2026-01-19": "13:00:00", "2026-02-16": "13:00:00", "2026-05-25": "13:00:00", "2026-06-19": "13:00:00",
    "2026-07-03": "13:00:00", "2026-09-07": "13:00:00", "2026-11-26": "13:00:00", "2026-11-27": "13:15:00",
    "2026-12-24": "13:15:00",
    "2027-01-18": "13:00:00", "2027-02-15": "13:00:00", "2027-05-31": "13:00:00", "2027-06-18": "13:00:00",
    "2027-07-05": "13:00:00", "2027-09-06": "13:00:00", "2027-11-25": "13:00:00", "2027-11-26": "13:15:00",
}
FLATTEN_BEFORE_HALT_MIN = 15
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


def early_halt_et(day: str | date) -> str | None:
    """The CME halt time (ET, "HH:MM:SS") of a shortened holiday session, or None on a normal day."""
    d = date.fromisoformat(day) if isinstance(day, str) else day
    return EARLY_HALTS.get(d.isoformat())


def session_end_et(day: str | date, default: str) -> str:
    """`default` on a normal day; FLATTEN_BEFORE_HALT_MIN before the halt on an early-halt day (if earlier)."""
    halt = early_halt_et(day)
    if halt is None:
        return default
    end = (datetime.strptime(halt, "%H:%M:%S") - timedelta(minutes=FLATTEN_BEFORE_HALT_MIN)).strftime("%H:%M:%S")
    return min(end, default)
