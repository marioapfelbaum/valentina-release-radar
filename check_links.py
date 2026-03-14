#!/usr/bin/env python3
"""
check_links.py — Link Availability Proxy for Valentina Release Radar
====================================================================
Runs on port 8081. The HTML app calls /check?url=<encoded_url> to verify
if a shop/streaming URL returns results (HTTP 200 with non-empty body).

Usage:
  python check_links.py          # Starts on port 8081
  python check_links.py 8082     # Custom port

CORS headers are included so the browser can call this from localhost:8080.
"""

import sys
import os
import json
import urllib.parse
import math
from http.server import HTTPServer, BaseHTTPRequestHandler
from pathlib import Path

try:
    import requests
except ImportError:
    print("pip install requests")
    sys.exit(1)

PORT = int(sys.argv[1]) if len(sys.argv) > 1 else 8081

# User-Agent to avoid bot blocks
UA = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"

# Per-site result detection: some sites always return 200 but show "no results"
# We check if the response body contains certain markers indicating actual results
SITE_CHECKS = {
    "bandcamp.com": {"method": "GET", "fail_markers": ["didn't return any results", "0 results"], "min_length": 5000},
    "beatport.com": {"method": "GET", "fail_markers": ['"count":0', "No results found", "We couldn't find"], "min_length": 2000},
    "juno.co.uk": {"method": "GET", "fail_markers": ["did not match any products", "0 results"], "min_length": 3000},
    "deejay.de": {"method": "GET", "fail_markers": ["Keine Ergebnisse", "no results", "0 Treffer"], "min_length": 2000},
    "discogs.com": {"method": "GET", "fail_markers": [], "min_length": 1000},
    "spotify.com": {"method": "GET", "fail_markers": [], "min_length": 500},
    "soundcloud.com": {"method": "GET", "fail_markers": ["We couldn't find", "Sorry, we couldn't find"], "min_length": 2000},
    "youtube.com": {"method": "GET", "fail_markers": [], "min_length": 1000},
    "google.com": {"method": "GET", "fail_markers": [], "min_length": 1000},
}


def get_site_key(url):
    """Extract the domain key for site-specific checks."""
    parsed = urllib.parse.urlparse(url)
    host = parsed.hostname or ""
    for key in SITE_CHECKS:
        if key in host:
            return key
    return None


def check_url(url):
    """Check if a URL has actual search results."""
    try:
        site = get_site_key(url)
        config = SITE_CHECKS.get(site, {"method": "GET", "fail_markers": [], "min_length": 500})

        r = requests.get(url, headers={"User-Agent": UA}, timeout=8, allow_redirects=True)

        if r.status_code >= 400:
            return False

        body = r.text
        if len(body) < config["min_length"]:
            return False

        for marker in config["fail_markers"]:
            if marker.lower() in body.lower():
                return False

        return True

    except Exception:
        return False


EVENTS_FILE = Path(__file__).parent / "events.json"

# Cache events in memory (reloaded on request)
_events_cache = None
_events_mtime = 0


def load_events_cached():
    """Load events.json with mtime-based cache."""
    global _events_cache, _events_mtime
    try:
        mtime = EVENTS_FILE.stat().st_mtime
        if _events_cache is None or mtime > _events_mtime:
            with open(EVENTS_FILE) as f:
                _events_cache = json.load(f)
            _events_mtime = mtime
        return _events_cache
    except (FileNotFoundError, json.JSONDecodeError):
        return []


def haversine(lat1, lon1, lat2, lon2):
    R = 6371
    dLat = math.radians(lat2 - lat1)
    dLon = math.radians(lon2 - lon1)
    a = math.sin(dLat/2)**2 + math.cos(math.radians(lat1))*math.cos(math.radians(lat2))*math.sin(dLon/2)**2
    return R * 2 * math.atan2(math.sqrt(a), math.sqrt(1-a))


class Handler(BaseHTTPRequestHandler):
    def _json_response(self, data, status=200):
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Access-Control-Allow-Origin", "*")
        self.end_headers()
        self.wfile.write(json.dumps(data, ensure_ascii=False).encode())

    def do_GET(self):
        # CORS
        if self.path == "/favicon.ico":
            self.send_response(204)
            self.end_headers()
            return

        parsed = urllib.parse.urlparse(self.path)
        params = urllib.parse.parse_qs(parsed.query)

        if parsed.path == "/check" and "url" in params:
            url = params["url"][0]
            available = check_url(url)
            self._json_response({"url": url, "available": available})

        elif parsed.path == "/batch" and "urls" in params:
            urls = params["urls"]
            results = {}
            for u in urls:
                results[u] = check_url(u)
            self._json_response(results)

        elif parsed.path == "/events":
            # /events?artist=NAME — events for a specific artist
            events = load_events_cached()
            artist = params.get("artist", [None])[0]
            if artist:
                events = [e for e in events if e.get("artist","").lower() == artist.lower()]
            self._json_response(events)

        elif parsed.path == "/events/near":
            # /events/near?lat=X&lon=Y&radius=100
            events = load_events_cached()
            try:
                lat = float(params.get("lat", [0])[0])
                lon = float(params.get("lon", [0])[0])
                radius = float(params.get("radius", [100])[0])
            except (ValueError, IndexError):
                self._json_response({"error": "lat, lon, radius required"}, 400)
                return

            nearby = []
            for e in events:
                elat = e.get("latitude")
                elon = e.get("longitude")
                if elat and elon:
                    dist = haversine(lat, lon, float(elat), float(elon))
                    if dist <= radius:
                        nearby.append({**e, "distance_km": round(dist, 1)})
            nearby.sort(key=lambda x: x["distance_km"])
            self._json_response(nearby)

        elif parsed.path == "/events/refresh":
            # Trigger re-read of events file
            global _events_cache, _events_mtime
            _events_cache = None
            _events_mtime = 0
            events = load_events_cached()
            self._json_response({"status": "refreshed", "count": len(events)})

        else:
            self._json_response({
                "status": "ok",
                "usage": {
                    "/check?url=...": "Check URL availability",
                    "/batch?urls=...&urls=...": "Batch URL check",
                    "/events": "All events",
                    "/events?artist=NAME": "Events for artist",
                    "/events/near?lat=X&lon=Y&radius=100": "Nearby events",
                    "/events/refresh": "Force reload events.json",
                }
            })

    def do_OPTIONS(self):
        self.send_response(204)
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Methods", "GET, OPTIONS")
        self.send_header("Access-Control-Allow-Headers", "*")
        self.end_headers()

    def log_message(self, format, *args):
        # Quieter logging
        pass


if __name__ == "__main__":
    server = HTTPServer(("", PORT), Handler)
    print(f"Link Checker + Events Proxy auf Port {PORT}")
    print(f"  /check?url=<encoded_url>")
    print(f"  /batch?urls=<url1>&urls=<url2>")
    print(f"  /events                          — Alle Events")
    print(f"  /events?artist=NAME              — Events für Artist")
    print(f"  /events/near?lat=X&lon=Y&radius=N — Events in der Nähe")
    print(f"  /events/refresh                  — Events neu laden")
    print(f"  Ctrl+C zum Beenden")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\nBeendet.")
