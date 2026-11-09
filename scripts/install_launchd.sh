#!/bin/bash
# Install the launchd agents (run on the Mini, from the deploy checkout):
#   com.tradejev.paper  weekdays 06:15 local → scripts/run_paper_day.sh (one live paper session)
#   com.tradejev.view   the viewer, kept alive on http://127.0.0.1:8765 (Replay / Live / Paper / Docs)
#   com.tradejev.tvtoken the Tradovate token keep-alive (token mode, 24/7; logs/tvtoken.log). Stop any
#                       temporary keep-alive that writes data/tradovate_token.json BEFORE running this, so
#                       two renewers never run side by side.
#   com.tradejev.copy  the copy-trading daemon (logs/copy.log): ONLY when accounts.yaml exists (copy trading is
#                       off, and nothing about it is installed, without it). Re-run after editing accounts.yaml
#                       to pick up a new account; a restart while in a position flattens it.
# Copies deploy/launchd/*.plist to ~/Library/LaunchAgents with the path of uv filled in, then
# (re)bootstraps them. Re-run after changing a plist. Uninstall:
#   launchctl bootout gui/$(id -u)/com.tradejev.paper; launchctl bootout gui/$(id -u)/com.tradejev.view; launchctl bootout gui/$(id -u)/com.tradejev.tvtoken
#   launchctl bootout gui/$(id -u)/com.tradejev.copy      # if installed
#   rm ~/Library/LaunchAgents/com.tradejev.{paper,view,tvtoken,copy}.plist
set -euo pipefail

REPO=/Users/justinhe/trade-jev
UV="$(command -v uv)" || { echo "uv is not on PATH"; exit 1; }
UV="$(cd "$(dirname "$UV")" && pwd -P)/$(basename "$UV")"
DEST="$HOME/Library/LaunchAgents"
DOMAIN="gui/$(id -u)"

[ "$(TZ=America/New_York date +%H)" != "$(date +%H)" ] || echo "note: this Mac is on Eastern time; the 06:15 schedule makes the script wait until 09:15 ET"
mkdir -p "$DEST" "$REPO/logs"
AGENTS="com.tradejev.paper com.tradejev.view com.tradejev.tvtoken"
if [ -f "$REPO/accounts.yaml" ]; then
    AGENTS="$AGENTS com.tradejev.copy"
else
    echo "note: no accounts.yaml: copy trading is off (com.tradejev.copy not installed)"
fi
for name in $AGENTS; do
    sed -e "s#__UV__#$UV#g" -e "s#__UV_DIR__#$(dirname "$UV")#g" "$REPO/deploy/launchd/$name.plist" > "$DEST/$name.plist"
    plutil -lint "$DEST/$name.plist"
    launchctl bootout "$DOMAIN/$name" 2>/dev/null || true
    # bootout returns before the old job is gone; bootstrapping too soon fails with "5: Input/output error"
    for _ in $(seq 20); do launchctl print "$DOMAIN/$name" >/dev/null 2>&1 || break; sleep 0.5; done
    ok=""
    for _ in 1 2 3 4 5; do
        if launchctl bootstrap "$DOMAIN" "$DEST/$name.plist" 2>/dev/null; then ok=1; break; fi
        sleep 1
    done
    [ -n "$ok" ] || launchctl bootstrap "$DOMAIN" "$DEST/$name.plist"   # last try, error shown
    echo "loaded $name"
done
echo "uv: $UV"
echo "viewer: http://127.0.0.1:8765/paper   logs: $REPO/logs/"
echo "token keep-alive: $REPO/logs/tvtoken.log   status: $REPO/data/tradovate_token_status.json"
[ -f "$REPO/accounts.yaml" ] && echo "copy daemon: $REPO/logs/copy.log   status: uv run python -m trade_jev.copytrade status"
echo "check: launchctl print $DOMAIN/com.tradejev.paper | grep -E 'state|last exit'"
