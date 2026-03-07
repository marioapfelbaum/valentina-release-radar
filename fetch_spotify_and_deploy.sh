#!/bin/bash
# Fetch Spotify releases and deploy to Netlify
# Scheduled to run after Spotify rate limit expires
set -e
DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$DIR"

echo "$(date): Starting Spotify fetch..."

# Quick connectivity test (1 artist only)
python3 -u -c "
from sources.spotify_source import SpotifyFetcher
from datetime import datetime, timedelta
sp = SpotifyFetcher()
r = sp.fetch_by_artist('Peggy Gou', datetime.now() - timedelta(days=365))
print(f'Test: {len(r)} releases')
if not r:
    print('ERROR: Spotify still rate-limited or broken')
    exit(1)
"

if [ $? -ne 0 ]; then
    echo "$(date): Spotify not ready yet, aborting."
    exit 1
fi

echo "$(date): Spotify OK, running full fetch..."
python3 -u fetch_multi.py --sources spotify --months 12

echo "$(date): Deploying to Netlify..."
rm -rf dist && mkdir -p dist
cp release_radar.html event_radar.html releases.json dist/
cp events.json dist/ 2>/dev/null || true
cp network_explorer.html dist/ 2>/dev/null || true
npx netlify deploy --prod --dir=dist

echo "$(date): Done! Spotify releases are now live."
