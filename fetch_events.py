#!/usr/bin/env python3
"""
fetch_events.py — Valentina Event Radar
========================================
Fetches upcoming events/gigs for artists in network_data.json.

Sources:
  1. Resident Advisor (ra.co) — primary
  2. Songkick API — fallback
  3. Bandsintown API — fallback

Output: events.json

Usage:
  python fetch_events.py                   # Fetch events for all network artists
  python fetch_events.py --limit 100       # Only first 100 artists
  python fetch_events.py --resume          # Resume from checkpoint
  python fetch_events.py --daemon          # Run daily refresh loop
"""

import json
import os
import sys
import time
import signal
import hashlib
import argparse
import re
import urllib.parse
from datetime import datetime, timedelta
from pathlib import Path

try:
    import requests
except ImportError:
    print("pip install requests")
    sys.exit(1)

# ─────────────────────────────────────────────────
# CONFIG
# ─────────────────────────────────────────────────

BASE_DIR = Path(__file__).parent
NETWORK_FILE = BASE_DIR / "network_data.json"
EVENTS_FILE = BASE_DIR / "events.json"
CHECKPOINT_FILE = BASE_DIR / ".events_checkpoint.json"
ENV_FILE = BASE_DIR / ".env"

UA = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
RATE_LIMIT = 2.0  # seconds between requests
MAX_MONTHS_AHEAD = 6

# ─────────────────────────────────────────────────
# ENV
# ─────────────────────────────────────────────────

def load_env():
    if ENV_FILE.exists():
        for line in ENV_FILE.read_text().splitlines():
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                k, v = line.split("=", 1)
                os.environ.setdefault(k.strip(), v.strip())

load_env()

BANDSINTOWN_API_KEY = os.getenv("BANDSINTOWN_API_KEY", "")
SONGKICK_API_KEY = os.getenv("SONGKICK_API_KEY", "")

# ─────────────────────────────────────────────────
# DATA
# ─────────────────────────────────────────────────

def load_network():
    if not NETWORK_FILE.exists():
        print(f"❌ {NETWORK_FILE} nicht gefunden")
        sys.exit(1)
    with open(NETWORK_FILE) as f:
        data = json.load(f)
    artists = []
    if isinstance(data, dict) and "artists" in data:
        for aid, info in data["artists"].items():
            if isinstance(info, dict):
                artists.append({
                    "name": info.get("name", ""),
                    "discogs_id": info.get("discogs_id"),
                    "network_id": aid,
                })
    return artists


def load_events():
    if EVENTS_FILE.exists():
        try:
            with open(EVENTS_FILE) as f:
                return json.load(f)
        except (json.JSONDecodeError, IOError):
            return []
    return []


def save_events(events):
    # Remove past events
    today = datetime.now().strftime("%Y-%m-%d")
    events = [e for e in events if e.get("date", "") >= today]
    # Sort by date
    events.sort(key=lambda e: e.get("date", ""))
    # Dedup
    seen = set()
    unique = []
    for e in events:
        key = f"{e['artist'].lower()}_{e['venue'].lower()}_{e['date']}"
        if key not in seen:
            seen.add(key)
            unique.append(e)
    tmp = EVENTS_FILE.with_suffix(".tmp")
    with open(tmp, "w") as f:
        json.dump(unique, f, ensure_ascii=False, indent=2)
    os.replace(tmp, EVENTS_FILE)
    return unique


def load_checkpoint():
    if CHECKPOINT_FILE.exists():
        try:
            with open(CHECKPOINT_FILE) as f:
                return json.load(f)
        except:
            pass
    return {"processed": []}


def save_checkpoint(cp):
    with open(CHECKPOINT_FILE, "w") as f:
        json.dump(cp, f)


def clear_checkpoint():
    if CHECKPOINT_FILE.exists():
        CHECKPOINT_FILE.unlink()


# ─────────────────────────────────────────────────
# RESIDENT ADVISOR (GraphQL API)
# ─────────────────────────────────────────────────

RA_GQL = "https://ra.co/graphql"
RA_HEADERS = {
    "Content-Type": "application/json",
    "User-Agent": UA,
    "Referer": "https://ra.co/",
}

# Cache: artist_name_lower → ra_id
_ra_id_cache = {}


def ra_search_artist(session, artist_name):
    """Search for an artist on RA, return their RA ID or None."""
    key = artist_name.lower().strip()
    if key in _ra_id_cache:
        return _ra_id_cache[key]

    try:
        q = {
            "query": 'query($term: String!) { search(searchTerm: $term, limit: 5, indices: [ARTIST]) { id value searchType contentUrl } }',
            "variables": {"term": artist_name}
        }
        resp = session.post(RA_GQL, json=q, headers=RA_HEADERS, timeout=15)
        if resp.status_code != 200:
            return None

        data = resp.json()
        results = data.get("data", {}).get("search", [])
        if not results:
            _ra_id_cache[key] = None
            return None

        # Find best match
        for r in results:
            if r.get("value", "").lower().strip() == key:
                _ra_id_cache[key] = r["id"]
                return r["id"]

        # Fallback to first result if it's an artist
        first = results[0]
        _ra_id_cache[key] = first["id"]
        return first["id"]

    except Exception:
        return None


def fetch_ra_events(session, artist_name):
    """Fetch upcoming events for an artist from RA GraphQL API."""
    ra_id = ra_search_artist(session, artist_name)
    if not ra_id:
        return []

    try:
        q = {
            "query": '''query($id: ID!) {
                artist(id: $id) {
                    id name upcomingEventsCount
                    events(type: LATEST, limit: 30) {
                        id title date startTime cost
                        venue { name area { name country { urlCode } } }
                        artists { name }
                        contentUrl attending
                    }
                }
            }''',
            "variables": {"id": str(ra_id)}
        }
        resp = session.post(RA_GQL, json=q, headers=RA_HEADERS, timeout=15)
        if resp.status_code != 200:
            return []

        data = resp.json()
        artist_data = data.get("data", {}).get("artist")
        if not artist_data:
            return []

        raw_events = artist_data.get("events", [])
        events = []
        now = datetime.now()
        max_date = now + timedelta(days=MAX_MONTHS_AHEAD * 30)

        for ev in raw_events:
            date_str = ev.get("date", "")
            if not date_str:
                continue

            try:
                dt = datetime.fromisoformat(date_str.replace("Z", ""))
            except ValueError:
                continue

            # Only future events
            if dt < now:
                continue
            if dt > max_date:
                continue

            date = dt.strftime("%Y-%m-%d")

            # Time from startTime
            time_str = ""
            st = ev.get("startTime", "")
            if st:
                try:
                    st_dt = datetime.fromisoformat(st.replace("Z", ""))
                    time_str = st_dt.strftime("%H:%M")
                except ValueError:
                    pass

            venue = ev.get("venue") or {}
            venue_name = venue.get("name", "")
            area = venue.get("area") or {}
            city = area.get("name", "")
            country_data = area.get("country") or {}
            country = country_data.get("urlCode", "")

            lineup = [a.get("name", "") for a in (ev.get("artists") or []) if isinstance(a, dict)]

            event_id = f"ra_{ev.get('id', '')}"

            events.append({
                "id": event_id,
                "artist": artist_name,
                "event_name": ev.get("title", ""),
                "venue": venue_name,
                "city": city,
                "country": country,
                "date": date,
                "time": time_str,
                "lineup": lineup,
                "url": f"https://ra.co/events/{ev.get('id', '')}",
                "source": "resident_advisor",
                "ticket_url": "",
                "price": ev.get("cost", ""),
                "attending": ev.get("attending", 0),
            })

        return events

    except Exception:
        return []


# ─────────────────────────────────────────────────
# BANDSINTOWN
# ─────────────────────────────────────────────────

def fetch_bandsintown_events(session, artist_name):
    """Fetch events from Bandsintown API (free tier)."""
    events = []

    # Bandsintown v3 API — free with app_id
    app_id = BANDSINTOWN_API_KEY or "valentina_release_radar"
    encoded = urllib.parse.quote(artist_name)
    url = f"https://rest.bandsintown.com/artists/{encoded}/events"

    try:
        resp = session.get(url, params={"app_id": app_id, "date": "upcoming"},
                          headers={"Accept": "application/json"}, timeout=15)
        if resp.status_code != 200:
            return events

        data = resp.json()
        if not isinstance(data, list):
            return events

        max_date = (datetime.now() + timedelta(days=MAX_MONTHS_AHEAD * 30)).strftime("%Y-%m-%d")

        for ev in data:
            date_str = ev.get("datetime", "")
            if not date_str:
                continue

            try:
                dt = datetime.fromisoformat(date_str.replace("Z", "+00:00"))
                date = dt.strftime("%Y-%m-%d")
                time_str = dt.strftime("%H:%M")
            except:
                continue

            if date > max_date:
                continue

            venue = ev.get("venue", {})
            lineup = [a.get("name", "") for a in ev.get("lineup", []) if isinstance(a, dict)]

            event_id = f"bit_{ev.get('id', hashlib.md5(f'{artist_name}{date}'.encode()).hexdigest()[:8])}"

            events.append({
                "id": event_id,
                "artist": artist_name,
                "event_name": ev.get("title", ""),
                "venue": venue.get("name", ""),
                "city": venue.get("city", ""),
                "country": venue.get("country", ""),
                "date": date,
                "time": time_str,
                "lineup": lineup or [artist_name],
                "url": ev.get("url", ""),
                "source": "bandsintown",
                "ticket_url": ev.get("offers", [{}])[0].get("url", "") if ev.get("offers") else "",
                "price": "",
                "latitude": float(venue.get("latitude", 0)) or None,
                "longitude": float(venue.get("longitude", 0)) or None,
            })

    except Exception as e:
        pass

    return events


# ─────────────────────────────────────────────────
# SONGKICK
# ─────────────────────────────────────────────────

def fetch_songkick_events(session, artist_name):
    """Fetch events from Songkick API."""
    if not SONGKICK_API_KEY:
        return []

    events = []
    try:
        # Search for artist
        resp = session.get(
            "https://api.songkick.com/api/3.0/search/artists.json",
            params={"apikey": SONGKICK_API_KEY, "query": artist_name},
            timeout=15
        )
        if resp.status_code != 200:
            return events

        data = resp.json()
        results = data.get("resultsPage", {}).get("results", {}).get("artist", [])
        if not results:
            return events

        sk_id = results[0].get("id")
        if not sk_id:
            return events

        # Get upcoming events
        resp2 = session.get(
            f"https://api.songkick.com/api/3.0/artists/{sk_id}/calendar.json",
            params={"apikey": SONGKICK_API_KEY},
            timeout=15
        )
        if resp2.status_code != 200:
            return events

        ev_data = resp2.json()
        for ev in ev_data.get("resultsPage", {}).get("results", {}).get("event", []):
            start = ev.get("start", {})
            date = start.get("date", "")
            time_str = start.get("time", "")

            venue = ev.get("venue", {})
            location = ev.get("location", {})

            event_id = f"sk_{ev.get('id', '')}"
            lineup_raw = ev.get("performance", [])
            lineup = [p.get("displayName", "") for p in lineup_raw]

            events.append({
                "id": event_id,
                "artist": artist_name,
                "event_name": ev.get("displayName", ""),
                "venue": venue.get("displayName", ""),
                "city": location.get("city", ""),
                "country": "",
                "date": date,
                "time": time_str or "",
                "lineup": lineup,
                "url": ev.get("uri", ""),
                "source": "songkick",
                "ticket_url": "",
                "price": "",
                "latitude": venue.get("lat"),
                "longitude": venue.get("lng"),
            })

    except Exception:
        pass

    return events


# ─────────────────────────────────────────────────
# MAIN FETCH
# ─────────────────────────────────────────────────

def fetch_events_for_artist(session, artist_name):
    """Fetch events from all sources, merge, deduplicate."""
    all_events = []

    # Primary: Resident Advisor GraphQL API
    ra_events = fetch_ra_events(session, artist_name)
    all_events.extend(ra_events)
    time.sleep(RATE_LIMIT)

    # Fallback: Bandsintown (may need API key)
    if not ra_events:
        bit_events = fetch_bandsintown_events(session, artist_name)
        all_events.extend(bit_events)
        if bit_events:
            time.sleep(RATE_LIMIT)

    # Fallback: Songkick (needs API key)
    if not all_events:
        sk_events = fetch_songkick_events(session, artist_name)
        all_events.extend(sk_events)
        if sk_events:
            time.sleep(RATE_LIMIT)

    # Dedup: same artist + same venue + same date = 1 event
    seen = set()
    unique = []
    for e in all_events:
        key = f"{e['artist'].lower()}_{e.get('venue','').lower()}_{e['date']}"
        if key not in seen:
            seen.add(key)
            unique.append(e)

    return unique


# ─────────────────────────────────────────────────
# GEOCODING (for "Near Me" feature)
# ─────────────────────────────────────────────────

CITY_COORDS = {
    # Pre-populated coords for common cities (avoids geocoding API)
    "berlin": (52.52, 13.405),
    "london": (51.507, -0.128),
    "amsterdam": (52.37, 4.895),
    "paris": (48.857, 2.352),
    "barcelona": (41.389, 2.159),
    "lisbon": (38.722, -9.139),
    "lisboa": (38.722, -9.139),
    "lissabon": (38.722, -9.139),
    "tbilisi": (41.693, 44.802),
    "kyiv": (50.45, 30.524),
    "bucharest": (44.426, 26.102),
    "bukarest": (44.426, 26.102),
    "cluj": (46.77, 23.59),
    "cluj-napoca": (46.77, 23.59),
    "new york": (40.713, -74.006),
    "nyc": (40.713, -74.006),
    "tokyo": (35.676, 139.65),
    "ibiza": (38.907, 1.432),
    "offenbach": (50.1, 8.77),
    "münchen": (48.135, 11.582),
    "munich": (48.135, 11.582),
    "hamburg": (53.551, 9.993),
    "köln": (50.938, 6.96),
    "cologne": (50.938, 6.96),
    "weimar": (50.98, 11.33),
    "basel": (47.56, 7.59),
    "zürich": (47.377, 8.54),
    "zurich": (47.377, 8.54),
    "st. gallen": (47.424, 9.376),
    "brüssel": (50.85, 4.35),
    "brussels": (50.85, 4.35),
    "oslo": (59.913, 10.752),
    "rom": (41.902, 12.496),
    "rome": (41.902, 12.496),
    "mailand": (45.464, 9.19),
    "milan": (45.464, 9.19),
    "porto": (41.158, -8.629),
    "faro": (37.019, -7.93),
    "sevilla": (37.389, -5.984),
    "madrid": (40.417, -3.704),
    "marseille": (43.297, 5.381),
    "lyon": (45.764, 4.835),
    "santiago": (-33.449, -70.669),
    "são paulo": (-23.551, -46.634),
    "mexico city": (19.433, -99.133),
    "cdmx": (19.433, -99.133),
    "bogotá": (4.711, -74.072),
    "medellín": (6.244, -75.574),
    "buenos aires": (-34.604, -58.382),
}


def enrich_coords(events):
    """Add latitude/longitude to events that don't have them."""
    for e in events:
        if e.get("latitude") and e.get("longitude"):
            continue
        city = e.get("city", "").lower().strip()
        if city in CITY_COORDS:
            e["latitude"], e["longitude"] = CITY_COORDS[city]


# ─────────────────────────────────────────────────
# DAEMON + MAIN
# ─────────────────────────────────────────────────

_shutdown = False

def _handle_signal(sig, frame):
    global _shutdown
    _shutdown = True
    print(f"\n⏹ Signal empfangen — beende...")


def run_fetch(artists, resume=False, limit=0):
    """Single pass: fetch events for all artists."""
    session = requests.Session()

    if limit > 0:
        artists = artists[:limit]

    # Load checkpoint
    cp = load_checkpoint() if resume else {"processed": []}
    processed = set(cp["processed"])

    remaining = [a for a in artists if a["name"].lower() not in processed]
    print(f"🎫 {len(remaining)} Artists zu prüfen ({len(processed)} bereits done)")

    existing_events = load_events()
    new_count = 0

    for i, artist in enumerate(remaining):
        if _shutdown:
            break

        name = artist["name"]
        pct = round((i + 1) / len(remaining) * 100) if remaining else 0
        print(f"[{pct:3d}%] {i+1}/{len(remaining)} — {name}", end="", flush=True)

        events = fetch_events_for_artist(session, name)

        if events:
            # Add network_id
            for e in events:
                e["artist_id"] = artist.get("network_id", "")

            existing_events.extend(events)
            new_count += len(events)
            print(f" — ✓ {len(events)} events")
        else:
            print(f" — keine events")

        processed.add(name.lower())

        if (i + 1) % 20 == 0:
            cp["processed"] = list(processed)
            save_checkpoint(cp)
            saved = save_events(existing_events)
            print(f"\n   💾 {len(saved)} events gespeichert\n")

    # Final save
    enrich_coords(existing_events)
    final = save_events(existing_events)
    clear_checkpoint()

    print(f"\n✅ {len(final)} Events in events.json ({new_count} neu)")
    return final


def run_daemon(args):
    """Run daily: fetch events, sleep, repeat."""
    global _shutdown
    signal.signal(signal.SIGINT, _handle_signal)
    signal.signal(signal.SIGTERM, _handle_signal)

    while not _shutdown:
        print(f"\n{'═'*50}")
        print(f"🎫 EVENT FETCH — {datetime.now().strftime('%Y-%m-%d %H:%M')}")
        print(f"{'═'*50}")

        artists = load_network()
        run_fetch(artists, resume=False, limit=args.limit)

        if _shutdown:
            break

        # Sleep until next day
        print(f"\n😴 Nächster Fetch in 24h...")
        for _ in range(86400):
            if _shutdown:
                break
            time.sleep(1)


def main():
    parser = argparse.ArgumentParser(description="Valentina Event Radar — Fetch events")
    parser.add_argument("--limit", type=int, default=0, help="Limit artists")
    parser.add_argument("--resume", action="store_true", help="Resume from checkpoint")
    parser.add_argument("--daemon", action="store_true", help="Run daily loop")
    args = parser.parse_args()

    print("╔══════════════════════════════════════════════╗")
    print("║   VALENTINA EVENT RADAR — Fetch Events        ║")
    print("╚══════════════════════════════════════════════╝")

    if args.daemon:
        run_daemon(args)
    else:
        artists = load_network()
        run_fetch(artists, resume=args.resume, limit=args.limit)


if __name__ == "__main__":
    main()
