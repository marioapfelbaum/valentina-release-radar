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

# Stash unstaged changes (e.g. network_data.json modified by running crawler) before pull
STASHED=false
if ! git diff --quiet 2>/dev/null; then
    git stash push -q >> "$LOGFILE" 2>&1
    STASHED=true
    echo "[$(date -Iseconds)] Stashed unstaged changes before pull" >> "$LOGFILE"
fi

# Pull latest
git pull --rebase >> "$LOGFILE" 2>&1

# Restore stashed changes
if [ "$STASHED" = true ]; then
    git stash pop -q >> "$LOGFILE" 2>&1 || true
    echo "[$(date -Iseconds)] Restored stashed changes" >> "$LOGFILE"
fi

# Fetch specified sources
python3 fetch_multi.py --sources "$SOURCES" >> "$LOGFILE" 2>&1

# Generate slim artist index for frontend (network_data.json itself is gitignored)
python3 generate_artist_index.py >> "$LOGFILE" 2>&1 || true

git add releases.json last_checked.json bandcamp_labels.json network_artists.json 2>/dev/null
if ! git diff --cached --quiet; then
    git config user.name "valentina-bot"
    git config user.email "valentina-bot@hetzner"
    git commit -m "chore: update releases" >> "$LOGFILE" 2>&1
    git push >> "$LOGFILE" 2>&1
    echo "[$(date -Iseconds)] Pushed new releases" >> "$LOGFILE"
else
    echo "[$(date -Iseconds)] No new releases" >> "$LOGFILE"
fi

echo "[$(date -Iseconds)] Done" >> "$LOGFILE"
