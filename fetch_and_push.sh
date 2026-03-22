#!/bin/bash
set -euo pipefail

LOGFILE="/var/log/valentina-fetch.log"
WORKDIR="/opt/valentina-release-radar"
LOCKFILE="/tmp/valentina-fetch.lock"

# Prevent concurrent runs (shops every 4h + spotify 3x/day could overlap)
if [ -f "$LOCKFILE" ]; then
    pid=$(cat "$LOCKFILE" 2>/dev/null)
    if kill -0 "$pid" 2>/dev/null; then
        echo "[$(date -Iseconds)] Skipped: another fetch is running (PID $pid)" >> "$LOGFILE"
        exit 0
    fi
    rm -f "$LOCKFILE"
fi
echo $$ > "$LOCKFILE"
trap 'rm -f "$LOCKFILE"' EXIT

# Sources: use argument if provided, otherwise all 17
SOURCES="${1:-bandcamp,spotify,discogs,hardwax,boomkat,juno,clone,rushhour,deejay,phonica,redeye,traxsource,decks,piccadilly,honestjons,norman,bandcamp_daily}"

echo "========================================" >> "$LOGFILE"
echo "[$(date -Iseconds)] Starting fetch (sources: $SOURCES)..." >> "$LOGFILE"

cd "$WORKDIR"

# Load credentials
set -a
source .env
set +a

# Pull latest
git pull --rebase >> "$LOGFILE" 2>&1

# Fetch specified sources
python3 fetch_multi.py --sources "$SOURCES" >> "$LOGFILE" 2>&1

# Also add network_data.json if spotify resolved new IDs
git add releases.json last_checked.json bandcamp_labels.json network_data.json 2>/dev/null
if ! git diff --cached --quiet; then
    git config user.name "valentina-bot"
    git config user.email "valentina-bot@hetzner"
    git commit -m "chore: update releases [skip ci]" >> "$LOGFILE" 2>&1
    git push >> "$LOGFILE" 2>&1
    echo "[$(date -Iseconds)] Pushed new releases" >> "$LOGFILE"
else
    echo "[$(date -Iseconds)] No new releases" >> "$LOGFILE"
fi

echo "[$(date -Iseconds)] Done" >> "$LOGFILE"
