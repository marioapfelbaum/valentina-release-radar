#!/bin/bash
set -euo pipefail

LOGFILE="/var/log/valentina-fetch.log"
WORKDIR="/opt/valentina-release-radar"

echo "========================================" >> "$LOGFILE"
echo "[$(date -Iseconds)] Starting fetch..." >> "$LOGFILE"

cd "$WORKDIR"

# Load credentials
set -a
source .env
set +a

# Pull latest
git pull --rebase >> "$LOGFILE" 2>&1

# Fetch all 6 sources
python3 fetch_multi.py --sources bandcamp,spotify,discogs,hardwax,boomkat,juno,clone,rushhour >> "$LOGFILE" 2>&1

# Commit and push if changes
git add releases.json last_checked.json bandcamp_labels.json 2>/dev/null
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
