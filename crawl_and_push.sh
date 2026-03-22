#!/bin/bash
set -euo pipefail

LOGFILE="/var/log/valentina-crawl.log"
WORKDIR="/opt/valentina-release-radar"
LOCKFILE="/tmp/valentina-crawl.lock"
FETCH_LOCKFILE="/tmp/valentina-fetch.lock"
TIME_BUDGET="${1:-180}"  # Default: 3 Stunden

# Prevent concurrent crawler runs
if [ -f "$LOCKFILE" ]; then
    pid=$(cat "$LOCKFILE" 2>/dev/null)
    if kill -0 "$pid" 2>/dev/null; then
        echo "[$(date -Iseconds)] Skipped: crawler already running (PID $pid)" >> "$LOGFILE"
        exit 0
    fi
    rm -f "$LOCKFILE"
fi
echo $$ > "$LOCKFILE"
trap 'rm -f "$LOCKFILE"' EXIT

# Wait if a fetch is running (don't fight over network_data.json)
if [ -f "$FETCH_LOCKFILE" ]; then
    fetch_pid=$(cat "$FETCH_LOCKFILE" 2>/dev/null)
    if kill -0 "$fetch_pid" 2>/dev/null; then
        echo "[$(date -Iseconds)] Waiting for fetch to finish (PID $fetch_pid)..." >> "$LOGFILE"
        # Wait max 30 minutes
        for i in $(seq 1 360); do
            sleep 5
            kill -0 "$fetch_pid" 2>/dev/null || break
        done
    fi
fi

echo "========================================" >> "$LOGFILE"
echo "[$(date -Iseconds)] Starting crawler (time-budget: ${TIME_BUDGET}m)..." >> "$LOGFILE"

cd "$WORKDIR"

# Load credentials
set -a
source .env
set +a

# Pull latest
git pull --rebase >> "$LOGFILE" 2>&1

# Run crawler
python3 crawler.py --resume --max-depth 2 --time-budget "$TIME_BUDGET" >> "$LOGFILE" 2>&1

# Commit and push network data
git add network_data.json seed_data.json reference_labels.txt reference_artists.txt 2>/dev/null
if ! git diff --cached --quiet; then
    git config user.name "valentina-bot"
    git config user.email "valentina-bot@hetzner"
    git commit -m "chore: update network data" >> "$LOGFILE" 2>&1
    # Retry push (fetch might have pushed during crawl)
    for i in 1 2 3; do
        git pull --rebase >> "$LOGFILE" 2>&1 && git push >> "$LOGFILE" 2>&1 && break
        echo "[$(date -Iseconds)] Push attempt $i failed, retrying..." >> "$LOGFILE"
        sleep 5
    done
    echo "[$(date -Iseconds)] Pushed network update" >> "$LOGFILE"
else
    echo "[$(date -Iseconds)] No network changes" >> "$LOGFILE"
fi

echo "[$(date -Iseconds)] Crawler done" >> "$LOGFILE"
