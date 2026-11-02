#!/bin/bash
# One unattended live paper session (Jev + baselines, no orders), started by launchd on weekdays.
#
# launchd's StartCalendarInterval is in the Mac's local time (06:15 on a Pacific-time Mini = 09:15 ET).
# This script works out ET itself, so it is right in any time zone:
#   before 09:15 ET  → sleeps until 09:15 ET (gives up if that is more than 4 hours away)
#   09:15–15:55 ET   → runs the session (a late start on Databento replays the session so far)
#   after 15:55 ET   → nothing to do
# Weekends and CME holidays are handled by trade_jev.live (it records a `holiday` row and exits 0).
#
# The feed is picked by TRADE_JEV_SOURCE in .env: databento (default) | tradovate | off. trade_jev.live reads
# .env itself (the same way it reads DATABENTO_API_KEY), so nothing is passed here:
#   off, or tradovate with any TRADOVATE_* credential missing → a `skipped` row with the reason, exit 0,
#     no feed started (the extension's watch treats that as healthy)
#   tradovate in token mode (TRADOVATE_ACCESS_TOKEN set, or data/tradovate_token.json exists) counts as
#     configured credentials; if the token store is dead and .env holds no fresh tokens the day is skipped
#     with the reason "tradovate token expired" (the keep-alive, com.tradejev.tvtoken, normally prevents it)
#   tradovate late start (after 09:28 ET) → no catch-up replay exists: 60s of warm-up on live data,
#     every decision HOLD until then
# A lock keeps a second copy (a missed job fired on wake, a manual run) from starting a duplicate.
#
# Logs: logs/paper-<ET date>.log. The session is recorded in data/paper.db; view it at /paper.
set -euo pipefail

REPO="${TRADE_JEV_REPO:-/Users/justinhe/trade-jev}"
UV="${UV:-$(command -v uv || true)}"
PORT="${TRADE_JEV_PAPER_PORT:-8767}"  # the live page of this session; the KeepAlive viewer has 8765

cd "$REPO"
mkdir -p logs
exec >>"logs/paper-$(TZ=America/New_York date +%F).log" 2>&1
echo "=== $(date '+%F %T %Z') / $(TZ=America/New_York date '+%T ET') run_paper_day.sh"

if [ -z "$UV" ] || [ ! -x "$UV" ]; then
    echo "uv not found (set UV=/path/to/uv; scripts/install_launchd.sh does this)"
    exit 1
fi

et_minutes() { local t; t=$(TZ=America/New_York date +%H%M); echo $(( 10#${t:0:2} * 60 + 10#${t:2:2} )); }
START=$(( 9 * 60 + 15 ))   # 09:15 ET: 15 min before the first decision, enough to subscribe
LAST=$(( 15 * 60 + 55 ))   # 15:55 ET: decisions end

now=$(et_minutes)
if (( now >= LAST )); then
    echo "after 15:55 ET; nothing to do"
    exit 0
fi
if (( now < START )); then
    wait_min=$(( START - now ))
    if (( wait_min > 240 )); then
        echo "more than 4 hours before 09:15 ET; is the schedule right? nothing to do"
        exit 0
    fi
    echo "waiting ${wait_min} min until 09:15 ET"
    sleep $(( wait_min * 60 ))
fi

# lockf -t 0: exit at once if another session holds the lock. caffeinate -i: no idle sleep meanwhile.
/usr/bin/lockf -t 0 "$REPO/logs/paper.lock" \
    /usr/bin/caffeinate -i \
    "$UV" run --frozen python -m trade_jev.live --exit-at-close --no-open --port "$PORT" && status=0 || status=$?
echo "=== $(date '+%F %T %Z') exit $status"
exit $status
