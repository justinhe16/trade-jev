#!/bin/bash
# Install the three launchd agents (run on the Mini, from the deploy checkout):
#   com.tradejev.paper  weekdays 06:15 local → scripts/run_paper_day.sh (one live paper session)
#   com.tradejev.view   the viewer, kept alive on http://127.0.0.1:8765 (Replay / Live / Paper / Docs)
#   com.tradejev.tvtoken the Tradovate token keep-alive (token mode, 24/7; logs/tvtoken.log). Stop any
#                       temporary keep-alive that writes data/tradovate_token.json BEFORE running this, so
#                       two renewers never run side by side.
# Copies deploy/launchd/*.plist to ~/Library/LaunchAgents with the path of uv filled in, then
# (re)bootstraps them. Re-run after changing a plist. Uninstall:
#   launchctl bootout gui/$(id -u)/com.tradejev.paper; launchctl bootout gui/$(id -u)/com.tradejev.view; launchctl bootout gui/$(id -u)/com.tradejev.tvtoken
#   rm ~/Library/LaunchAgents/com.tradejev.{paper,view,tvtoken}.plist
set -euo pipefail

REPO=/Users/justinhe/trade-jev
UV="$(command -v uv)" || { echo "uv is not on PATH"; exit 1; }
UV="$(cd "$(dirname "$UV")" && pwd -P)/$(basename "$UV")"
DEST="$HOME/Library/LaunchAgents"
DOMAIN="gui/$(id -u)"

[ "$(TZ=America/New_York date +%H)" != "$(date +%H)" ] || echo "note: this Mac is on Eastern time; the 06:15 schedule makes the script wait until 09:15 ET"
mkdir -p "$DEST" "$REPO/logs"
for name in com.tradejev.paper com.tradejev.view com.tradejev.tvtoken; do
    sed -e "s#__UV__#$UV#g" -e "s#__UV_DIR__#$(dirname "$UV")#g" "$REPO/deploy/launchd/$name.plist" > "$DEST/$name.plist"
    plutil -lint "$DEST/$name.plist"
    launchctl bootout "$DOMAIN/$name" 2>/dev/null || true
    launchctl bootstrap "$DOMAIN" "$DEST/$name.plist"
    echo "loaded $name"
done
echo "uv: $UV"
echo "viewer: http://127.0.0.1:8765/paper   logs: $REPO/logs/"
echo "token keep-alive: $REPO/logs/tvtoken.log   status: $REPO/data/tradovate_token_status.json"
echo "check: launchctl print $DOMAIN/com.tradejev.paper | grep -E 'state|last exit'"
