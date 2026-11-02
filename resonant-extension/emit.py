"""``emit()``: send a structured event from an extension process to Resonant.

Resonant runs extension commands with ``RESONANT_EVENTS_FD=3``: fd 3 is a pipe for events,
one JSON object per line, ``{type, text, dedupe_key, data?}``. Events reach the daemon only
when the run succeeds (exit 0 with one JSON object on stdout). The daemon stores each one
as ``ext.<name>.<type>`` with dedupe key ``ext:<name>:<dedupe_key>``, so re-emitting the
same key is harmless; notifications are sent for the types in ``notify.on``.

Extensions run as ``python -I -S``, so this package isn't importable from them: this file
is stdlib-only, and an extension copies it next to its script. Returning
``{"events": [...]}`` in the result JSON is equivalent.

    from emit import emit
    emit("ci_failed", "resonant#40 CI failed (lint)", "resonant#40:run:123:failed")

``report()`` is ``emit()`` for the pulse: an item the owner may want to hear about (an
incident, a PnL figure), sent as an ``item`` event. Resonant keeps one row per ``key`` and
mentions it again only when its fingerprint changes: the hash of ``state`` when you give
one, else of ``title``. So keep ``state`` coarse (bucket the PnL, leave timestamps out).
``urgency`` is clamped to the ``report.max_urgency`` your manifest was approved for.

    from emit import report
    report("dori/incident/inc-412", "Dori failing: checkout 5xx", urgency="urgent",
           state={"status": "triaging"})
    report("dori/incident/inc-412", "Dori recovered", state={"status": "resolved"}, clear=True)

``quiet=True`` says there is nothing worth saying right now (every count is 0, an empty
digest): Resonant closes the item without a word, so the briefing and the hourly pulse
leave it out, and the next report that isn't quiet opens it again as news. Use ``clear``
instead when the owner should hear that something is over. Daemons older than ``quiet``
ignore the field (the item is then shown as before).

    report("github/summary", "0 open PRs", urgency="low", state=counts, quiet=True)
"""

from __future__ import annotations

import json
import os
import re
import time
from typing import Any

EVENTS_FD_ENV = "RESONANT_EVENTS_FD"
MAX_DEDUPE_KEY = 200
URGENCIES = ("urgent", "normal", "low")
MIN_TTL_S = 60
MAX_TTL_S = 7 * 24 * 3600
MAX_STATE_BYTES = 2000  # canonical JSON of `state`; it is only hashed, so keep it coarse
_ITEM_KEY = re.compile(r"^[a-z0-9/._:-]{1,120}$")
_TTL = re.compile(r"^(\d+)([smh])$")
_TTL_UNIT_S = {"s": 1, "m": 60, "h": 3600}


def emit(type: str, text: str, dedupe_key: str, data: dict[str, Any] | None = None) -> bool:
    """Write one event line. False when not running under Resonant (no events fd)."""
    if not dedupe_key or len(dedupe_key) > MAX_DEDUPE_KEY:
        raise ValueError(f"dedupe_key must be 1..{MAX_DEDUPE_KEY} characters")
    fd_text = os.environ.get(EVENTS_FD_ENV)
    if not fd_text or not fd_text.isdigit():
        return False
    event: dict[str, Any] = {"type": type, "text": text, "dedupe_key": dedupe_key}
    if data is not None:
        event["data"] = data
    line = (json.dumps(event, separators=(",", ":")) + "\n").encode()
    try:
        os.write(int(fd_text), line)
    except OSError:
        return False
    return True


def report(
    key: str,
    title: str,
    *,
    urgency: str = "normal",
    state: dict[str, Any] | None = None,
    clear: bool = False,
    ttl: str = "24h",
    quiet: bool = False,
) -> bool:
    """Report a pulse item. False when not running under Resonant (no events fd).

    ``key`` is 1..120 characters of ``[a-z0-9/._:-]``. ``ttl`` (``30m``, ``24h``; 1 minute to
    ``168h``, 7 days) is how long the item stays open without a new report. ``clear=True`` says
    the thing is over; a clear, like an urgency change, always counts as news, and is
    then dropped. ``quiet=True``: nothing to say (all-zero counts, an empty section); the
    item is closed silently and left out of the briefing and the pulse, never sent.
    ``state`` is at most 2,000 bytes of JSON. Every call gets a unique
    dedupe key, so a repeat is never dropped at ingest: Resonant decides whether it is news.
    """
    if not _ITEM_KEY.fullmatch(key):
        raise ValueError("key must be 1..120 characters of [a-z0-9/._:-]")
    if urgency not in URGENCIES:
        raise ValueError(f"urgency must be one of {URGENCIES}")
    m = _TTL.fullmatch(ttl)
    if not m or not MIN_TTL_S <= int(m.group(1)) * _TTL_UNIT_S[m.group(2)] <= MAX_TTL_S:
        raise ValueError("ttl must look like 30m or 24h, and be between 1m and 168h")
    if state is not None:
        try:
            canonical = json.dumps(state, sort_keys=True, separators=(",", ":"), allow_nan=False)
        except (TypeError, ValueError) as e:
            raise ValueError(f"state must be JSON-serializable: {e}") from e
        if len(canonical.encode()) > MAX_STATE_BYTES:
            raise ValueError(f"state must be at most {MAX_STATE_BYTES} bytes of JSON")
    data: dict[str, Any] = {
        "key": key,
        "urgency": urgency,
        "state": state,
        "clear": clear,
        "ttl": ttl,
        "quiet": quiet,
    }
    # `item:` + key (<= 120) + `:` + time_ns in hex (<= 16) stays well under the 200 limit.
    return emit("item", title, f"item:{key}:{time.time_ns():x}", data)
