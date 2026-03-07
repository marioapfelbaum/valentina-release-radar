#!/usr/bin/env python3
"""
fetch_releases.py — Valentina Release Radar
============================================
Reads artists from network_data.json (crawler output),
fetches their recent releases from Discogs API,
and writes releases.json for the web app.

Usage:
  python fetch_releases.py                    # Default: last 6 months
  python fetch_releases.py --months 3         # Last 3 months
  python fetch_releases.py --months 12        # Last year
  python fetch_releases.py --limit 50         # Only first 50 artists
  python fetch_releases.py --resume           # Resume from checkpoint
  python fetch_releases.py --daemon           # Run forever, rescan all artists in a loop
  python fetch_releases.py --daemon --months 6  # Daemon with 6-month window

Needs: DISCOGS_TOKEN in .env file (same one the crawler uses)
Output: releases.json (same directory)
"""

import json
import os
import sys
import time
import signal
import hashlib
import argparse
from datetime import datetime, timedelta
from pathlib import Path
from collections import defaultdict

try:
    import requests
except ImportError:
    print("❌ requests nicht installiert. Bitte: pip install requests")
    sys.exit(1)

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    # Manual .env parsing fallback
    env_path = Path(__file__).parent / ".env"
    if env_path.exists():
        for line in env_path.read_text().splitlines():
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                k, v = line.split("=", 1)
                os.environ[k.strip()] = v.strip().strip('"').strip("'")

# --- CONFIG ---
DISCOGS_TOKEN = os.getenv("DISCOGS_TOKEN") or os.getenv("DISCOGS_API_TOKEN")
DISCOGS_BASE = "https://api.discogs.com"
HEADERS = {
    "User-Agent": "ValentinaReleaseRadar/1.0",
    "Authorization": f"Discogs token={DISCOGS_TOKEN}" if DISCOGS_TOKEN else "",
}
RATE_LIMIT_DELAY = 1.1  # Discogs: 60 req/min authenticated
CHECKPOINT_FILE = "fetch_releases_checkpoint.json"
OUTPUT_FILE = "releases.json"
NETWORK_FILE = "network_data.json"
LAST_CHECKED_FILE = "last_checked.json"
WHITELIST_FILE = "genre_whitelist.txt"
BLACKLIST_FILE = "genre_blacklist.txt"

# Smart scheduling: how often to re-check artists
RECHECK_DAYS_NO_RELEASE = 7    # Artist had no recent release → check again in 7 days
RECHECK_DAYS_ACTIVE = 1        # Artist had release in last 3 months → daily
ACTIVE_THRESHOLD_DAYS = 90     # "Active" = had a release in the last 90 days

# Genre mapping for Discogs styles
GENRE_MAP = {
    # Minimal / Micro
    "minimal": "Minimal House",
    "minimal house": "Minimal House",
    "minimal techno": "Minimal Techno",
    "micro house": "Microhouse",
    "microhouse": "Microhouse",
    "romanian minimal": "Rominimal",
    # House variants
    "deep house": "Deep House",
    "tech house": "Tech House",
    "progressive house": "Progressive House",
    "future house": "Future House",
    "electro house": "Electro House",
    "afro house": "Afro House",
    "organic house": "Organic House",
    "bass house": "Bass House",
    "funky house": "Funky House",
    "jackin house": "Jackin House",
    "soulful house": "Soulful House",
    "chicago house": "Chicago House",
    "tribal house": "Tribal House",
    "lo-fi house": "Lo-Fi House",
    "house": "House",
    # Techno variants
    "dub techno": "Dub Techno",
    "melodic techno": "Melodic House",
    "melodic house": "Melodic House",
    "hard techno": "Hard Techno",
    "industrial techno": "Hard Techno",
    "peak time techno": "Peak Time Techno",
    "detroit techno": "Detroit Techno",
    "hypnotic": "Hypnotic Techno",
    "techno": "Techno",
    # Mainstage / EDM
    "big room": "Mainstage",
    "mainstage": "Mainstage",
    "edm": "Mainstage",
    "dance-pop": "Dance / Pop",
    # Trance
    "trance": "Trance",
    "progressive trance": "Trance",
    "psy-trance": "Psy Trance",
    "psytrance": "Psy Trance",
    "goa trance": "Psy Trance",
    # Bass / Breaks
    "breaks": "Breaks",
    "drum and bass": "Drum & Bass",
    "drum & bass": "Drum & Bass",
    "jungle": "Drum & Bass",
    "dubstep": "Dubstep",
    "uk garage": "UK Garage",
    "bassline": "UK Garage",
    # Dub / Ambient / Down
    "dub": "Dub Techno",
    "ambient": "Ambient",
    "dark ambient": "Dark Ambient",
    "downtempo": "Downtempo",
    # Electro / Acid
    "electro": "Electro",
    "acid house": "Acid",
    "acid": "Acid",
    # Disco
    "disco": "Disco",
    "nu disco": "Nu Disco",
    "nu-disco": "Nu Disco",
    "italo-disco": "Italo Disco",
    "indie dance": "Indie Dance",
    # Other electronic
    "experimental": "Experimental",
    "idm": "IDM",
    "leftfield": "Leftfield",
    "glitch": "Glitch",
    "electronica": "Electronica",
    "synth-pop": "Synth Pop",
    "synthwave": "Synthwave",
    "ebm": "EBM",
    "noise": "Noise",
    "trip hop": "Trip Hop",
}


def classify_genre(styles, genres):
    """Map Discogs styles/genres to our genre categories."""
    all_tags = [s.lower() for s in (styles or [])] + [g.lower() for g in (genres or [])]
    for tag in all_tags:
        for key, val in GENRE_MAP.items():
            if key in tag:
                return val
    if any("electronic" in t for t in all_tags):
        return "Electronic"
    return "Other"


# ─── GENRE FILTERING ────────────────────────────────────

def load_filter_list(filename):
    """Load genre filter list from text file."""
    path = Path(__file__).parent / filename
    if not path.exists():
        return set()
    return {line.strip().lower() for line in path.read_text().splitlines() if line.strip()}


_genre_whitelist = None
_genre_blacklist = None


def get_genre_filters():
    """Load and cache genre whitelist/blacklist."""
    global _genre_whitelist, _genre_blacklist
    if _genre_whitelist is None:
        _genre_whitelist = load_filter_list(WHITELIST_FILE)
        _genre_blacklist = load_filter_list(BLACKLIST_FILE)
    return _genre_whitelist, _genre_blacklist


def should_skip_artist(artist_info, network_data=None):
    """Check if artist should be skipped based on genre blacklist.
    Returns (skip: bool, reason: str)."""
    whitelist, blacklist = get_genre_filters()
    if not blacklist:
        return False, ""

    # Get genres from network_data if available
    genres = []
    if network_data and isinstance(network_data, dict) and "artists" in network_data:
        for ak, av in network_data["artists"].items():
            if av.get("discogs_id") == artist_info.get("discogs_id"):
                genres = av.get("genres", [])
                break

    if not genres:
        # No genre info — don't skip (fetch anyway)
        return False, ""

    genres_lower = [g.lower().strip() for g in genres]

    # Check blacklist
    for g in genres_lower:
        if g in blacklist:
            return True, g

    return False, ""


# ─── LAST CHECKED TRACKING ─────────────────────────────

def load_last_checked():
    """Load last_checked.json: {artist_name_lower: {checked: ISO, has_recent: bool}}."""
    path = Path(__file__).parent / LAST_CHECKED_FILE
    if path.exists():
        try:
            with open(path) as f:
                return json.load(f)
        except (json.JSONDecodeError, IOError):
            return {}
    return {}


def save_last_checked(data):
    """Save last_checked.json atomically."""
    path = Path(__file__).parent / LAST_CHECKED_FILE
    tmp = path.with_suffix(".tmp")
    with open(tmp, "w") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    os.replace(tmp, path)


def is_check_due(artist_name, last_checked_data):
    """Determine if an artist is due for re-checking based on smart scheduling."""
    key = artist_name.lower().strip()
    info = last_checked_data.get(key)
    if not info:
        return True  # Never checked

    try:
        last_dt = datetime.fromisoformat(info["checked"])
    except (ValueError, KeyError):
        return True

    now = datetime.now()
    days_since = (now - last_dt).days

    if info.get("has_recent", False):
        # Active artist: check daily
        return days_since >= RECHECK_DAYS_ACTIVE
    else:
        # Inactive artist: check weekly
        return days_since >= RECHECK_DAYS_NO_RELEASE


def update_last_checked(last_checked_data, artist_name, has_recent_release):
    """Update last_checked entry for an artist."""
    key = artist_name.lower().strip()
    last_checked_data[key] = {
        "checked": datetime.now().isoformat(),
        "has_recent": has_recent_release,
    }


def is_reissue(notes, formats):
    """Check if a release is a reissue/remaster/repress."""
    text = (notes or "").lower()
    fmt_text = " ".join(str(f) for f in (formats or [])).lower()
    keywords = ["reissue", "remaster", "repress", "re-issue", "re-master", "re-press"]
    return any(k in text or k in fmt_text for k in keywords)


def format_duration(seconds):
    """Format seconds as M:SS."""
    if not seconds:
        return ""
    m, s = divmod(int(seconds), 60)
    return f"{m}:{s:02d}"


def parse_duration_string(dur_str):
    """Parse Discogs duration string like '7:42' to seconds."""
    if not dur_str or ":" not in dur_str:
        return 0
    parts = dur_str.split(":")
    try:
        if len(parts) == 2:
            return int(parts[0]) * 60 + int(parts[1])
        elif len(parts) == 3:
            return int(parts[0]) * 3600 + int(parts[1]) * 60 + int(parts[2])
    except ValueError:
        return 0
    return 0


def load_network_data():
    """Load artists from network_data.json."""
    path = Path(__file__).parent / NETWORK_FILE
    if not path.exists():
        print(f"❌ {NETWORK_FILE} nicht gefunden. Crawler erst abschließen!")
        sys.exit(1)

    with open(path) as f:
        data = json.load(f)

    artists = []

    # Handle various possible structures from the crawler
    if isinstance(data, dict):
        # Structure: { "artists": { "id": { "name": ..., "discogs_id": ... } } }
        if "artists" in data and isinstance(data["artists"], dict):
            for aid, info in data["artists"].items():
                if isinstance(info, dict):
                    artists.append({
                        "name": info.get("name", aid),
                        "discogs_id": info.get("discogs_id") or info.get("discogs_artist_id"),
                        "musicbrainz_id": info.get("musicbrainz_id") or info.get("mbid"),
                    })
                elif isinstance(info, str):
                    artists.append({"name": info, "discogs_id": None, "musicbrainz_id": None})

        # Structure: { "artists": [ { "name": ..., ... } ] }
        elif "artists" in data and isinstance(data["artists"], list):
            for info in data["artists"]:
                if isinstance(info, dict):
                    artists.append({
                        "name": info.get("name", ""),
                        "discogs_id": info.get("discogs_id") or info.get("discogs_artist_id") or info.get("id"),
                        "musicbrainz_id": info.get("musicbrainz_id") or info.get("mbid"),
                    })
                elif isinstance(info, str):
                    artists.append({"name": info, "discogs_id": None, "musicbrainz_id": None})

        # Structure: { "nodes": [...], "edges": [...] } (network graph format)
        elif "nodes" in data:
            for node in data["nodes"]:
                if isinstance(node, dict) and node.get("type") == "artist":
                    artists.append({
                        "name": node.get("name", node.get("label", "")),
                        "discogs_id": node.get("discogs_id"),
                        "musicbrainz_id": node.get("musicbrainz_id"),
                    })

        # Flat dict: { "Artist Name": { ... } }
        else:
            for key, val in data.items():
                if key not in ("metadata", "edges", "labels", "config", "stats"):
                    if isinstance(val, dict):
                        artists.append({
                            "name": val.get("name", key),
                            "discogs_id": val.get("discogs_id"),
                            "musicbrainz_id": val.get("musicbrainz_id"),
                        })

    elif isinstance(data, list):
        for item in data:
            if isinstance(item, dict):
                artists.append({
                    "name": item.get("name", item.get("artist", "")),
                    "discogs_id": item.get("discogs_id"),
                    "musicbrainz_id": item.get("musicbrainz_id"),
                })

    # Deduplicate by name
    seen = set()
    unique = []
    for a in artists:
        name_lower = a["name"].lower().strip()
        if name_lower and name_lower not in seen:
            seen.add(name_lower)
            unique.append(a)

    return unique


def fetch_discogs(endpoint, params=None):
    """Make a rate-limited Discogs API request."""
    if not DISCOGS_TOKEN:
        print("❌ DISCOGS_TOKEN nicht in .env! Bitte setzen.")
        sys.exit(1)

    url = f"{DISCOGS_BASE}{endpoint}"
    time.sleep(RATE_LIMIT_DELAY)

    try:
        r = requests.get(url, headers=HEADERS, params=params, timeout=15)
        if r.status_code == 429:
            # Rate limited — wait and retry
            wait = int(r.headers.get("Retry-After", 30))
            print(f"  ⏳ Rate limit, warte {wait}s...")
            time.sleep(wait + 1)
            r = requests.get(url, headers=HEADERS, params=params, timeout=15)
        if r.status_code == 200:
            return r.json()
        elif r.status_code == 404:
            return None
        else:
            print(f"  ⚠ HTTP {r.status_code} für {endpoint}")
            return None
    except requests.exceptions.RequestException as e:
        print(f"  ⚠ Request error: {e}")
        return None


def search_artist(name):
    """Search Discogs for an artist, return their ID."""
    data = fetch_discogs("/database/search", {
        "q": name,
        "type": "artist",
        "per_page": 5,
    })
    if not data or not data.get("results"):
        return None

    # Try exact match first
    for r in data["results"]:
        if r.get("title", "").lower().strip() == name.lower().strip():
            return r["id"]

    # Fall back to first result
    return data["results"][0]["id"]


def fetch_artist_releases(artist_id, cutoff_date):
    """Fetch releases for an artist from Discogs, filtered by date."""
    releases = []
    page = 1
    max_pages = 5  # Don't go too deep

    while page <= max_pages:
        data = fetch_discogs(f"/artists/{artist_id}/releases", {
            "sort": "year",
            "sort_order": "desc",
            "per_page": 50,
            "page": page,
        })

        if not data or not data.get("releases"):
            break

        for rel in data["releases"]:
            year = rel.get("year", 0)
            if year and year < cutoff_date.year:
                # Too old, stop paginating
                return releases

            # We need the full release details for date, tracklist, etc.
            rel_id = rel.get("id")
            rel_type = rel.get("type", "")

            # Skip masters, only want actual releases
            if rel_type == "master":
                # Get the main release for this master
                master_data = fetch_discogs(f"/masters/{rel_id}")
                if master_data and master_data.get("main_release"):
                    rel_id = master_data["main_release"]
                else:
                    continue

            release_detail = fetch_discogs(f"/releases/{rel_id}")
            if not release_detail:
                continue

            # Parse release date
            released = release_detail.get("released", "")
            if not released:
                released = f"{year}-01-01" if year else ""

            try:
                # Handle various date formats: 2026-02-10, 2026-02, 2026
                if len(released) >= 10:
                    rel_date = datetime.strptime(released[:10], "%Y-%m-%d")
                elif len(released) >= 7:
                    rel_date = datetime.strptime(released[:7], "%Y-%m")
                elif len(released) >= 4:
                    rel_date = datetime.strptime(released[:4], "%Y")
                else:
                    continue
            except ValueError:
                continue

            if rel_date < cutoff_date:
                continue

            # Extract info
            title = release_detail.get("title", "")
            label_info = release_detail.get("labels", [{}])
            label_name = label_info[0].get("name", "") if label_info else ""
            # Clean label name
            if label_name.startswith("Not On Label"):
                label_name = "Self-Released"

            styles = release_detail.get("styles", [])
            genres_raw = release_detail.get("genres", [])
            genre = classify_genre(styles, genres_raw)
            notes = release_detail.get("notes", "")
            formats = release_detail.get("formats", [])
            re = is_reissue(notes, formats)

            # Get total duration from tracklist
            tracklist = release_detail.get("tracklist", [])
            total_seconds = 0
            for track in tracklist:
                dur = track.get("duration", "")
                total_seconds += parse_duration_string(dur)

            # Get artist name from credits (more accurate than search name)
            artists_info = release_detail.get("artists", [])
            artist_name = ", ".join(a.get("name", "") for a in artists_info) if artists_info else ""

            # Generate stable ID
            stable_id = hashlib.md5(f"{rel_id}".encode()).hexdigest()[:8]

            releases.append({
                "id": stable_id,
                "discogs_release_id": rel_id,
                "title": title,
                "artist": artist_name,
                "album": title,  # For singles, album = title
                "label": label_name,
                "genre": genre,
                "duration": format_duration(total_seconds) if total_seconds else "",
                "date": rel_date.strftime("%Y-%m-%d"),
                "re": re,
                "styles": styles,
                "source": "discogs",
                "source_url": release_detail.get("uri", ""),
                "discogs_url": release_detail.get("uri", ""),
            })

        # Check if there are more pages
        pagination = data.get("pagination", {})
        if page >= pagination.get("pages", 1):
            break
        page += 1

    return releases


def load_checkpoint():
    """Load processing checkpoint."""
    path = Path(__file__).parent / CHECKPOINT_FILE
    if path.exists():
        with open(path) as f:
            return json.load(f)
    return {"processed_artists": [], "releases": []}


def save_checkpoint(checkpoint):
    """Save processing checkpoint (atomic write)."""
    path = Path(__file__).parent / CHECKPOINT_FILE
    tmp = path.with_suffix(".tmp")
    with open(tmp, "w") as f:
        json.dump(checkpoint, f, ensure_ascii=False, indent=2)
    os.replace(tmp, path)


def save_releases(releases):
    """Save final releases.json (atomic write)."""
    # Deduplicate by discogs_release_id
    seen = set()
    unique = []
    for r in releases:
        rid = r.get("discogs_release_id", r["id"])
        if rid not in seen:
            seen.add(rid)
            unique.append(r)

    # Sort by date descending
    unique.sort(key=lambda r: r.get("date", ""), reverse=True)

    path = Path(__file__).parent / OUTPUT_FILE
    tmp = path.with_suffix(".tmp")
    with open(tmp, "w") as f:
        json.dump(unique, f, ensure_ascii=False, indent=2)
    os.replace(tmp, path)

    return unique


def load_existing_releases():
    """Load current releases.json from disk (for incremental merge)."""
    path = Path(__file__).parent / OUTPUT_FILE
    if path.exists():
        try:
            with open(path) as f:
                return json.load(f)
        except (json.JSONDecodeError, IOError):
            return []
    return []


def save_releases_incremental(new_releases):
    """Merge new releases into existing releases.json and save atomically."""
    existing = load_existing_releases()

    # Build lookup of existing release IDs
    seen = set()
    merged = []
    for r in existing:
        rid = r.get("discogs_release_id", r["id"])
        if rid not in seen:
            seen.add(rid)
            merged.append(r)

    # Add only truly new releases
    added = 0
    for r in new_releases:
        rid = r.get("discogs_release_id", r["id"])
        if rid not in seen:
            seen.add(rid)
            merged.append(r)
            added += 1

    # Sort by date descending
    merged.sort(key=lambda r: r.get("date", ""), reverse=True)

    path = Path(__file__).parent / OUTPUT_FILE
    tmp = path.with_suffix(".tmp")
    with open(tmp, "w") as f:
        json.dump(merged, f, ensure_ascii=False, indent=2)
    os.replace(tmp, path)

    return merged, added


def load_network_data_safe():
    """Like load_network_data but returns None instead of exiting if file missing."""
    path = Path(__file__).parent / NETWORK_FILE
    if not path.exists():
        return None
    try:
        # Re-use existing parser by temporarily replacing sys.exit
        return load_network_data()
    except (SystemExit, Exception) as e:
        print(f"  ⚠ Fehler beim Laden von {NETWORK_FILE}: {e}")
        return None


# --- DAEMON ---
_shutdown = False


def _handle_signal(sig, frame):
    global _shutdown
    _shutdown = True
    print(f"\n⏹  Signal empfangen — beende nach aktuellem Artist...")


def run_daemon(args):
    """Run in daemon mode: endless loop scanning all artists with smart scheduling."""
    global _shutdown
    signal.signal(signal.SIGINT, _handle_signal)
    signal.signal(signal.SIGTERM, _handle_signal)

    # Load raw network data for genre checks
    raw_network = None
    try:
        with open(Path(__file__).parent / NETWORK_FILE) as f:
            raw_network = json.load(f)
    except Exception:
        pass

    cycle = 0
    while not _shutdown:
        cycle += 1
        cutoff = datetime.now() - timedelta(days=args.months * 30)

        print()
        print(f"{'═' * 50}")
        print(f"🔁 ZYKLUS {cycle} — {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"   Suche Releases seit {cutoff.strftime('%Y-%m-%d')}")
        print(f"{'═' * 50}")

        # Reload network_data.json each cycle (crawler may have added artists)
        artists = load_network_data_safe()
        if artists is None:
            print(f"   ⏳ {NETWORK_FILE} nicht verfügbar, warte 30s...")
            for _ in range(30):
                if _shutdown:
                    break
                time.sleep(1)
            continue

        # Reload raw network for genre checks
        try:
            with open(Path(__file__).parent / NETWORK_FILE) as f:
                raw_network = json.load(f)
        except Exception:
            pass

        if args.limit > 0:
            artists = artists[:args.limit]

        print(f"   📊 {len(artists)} Artists geladen")

        # Load last_checked data for smart scheduling
        last_checked = load_last_checked()

        # Load checkpoint for this cycle
        checkpoint = load_checkpoint() if args.resume or cycle > 1 else {"processed_artists": [], "releases": []}
        processed = set(checkpoint["processed_artists"])

        # Filter: skip blacklisted genres and artists not due for re-check
        remaining = []
        skipped_genre = 0
        skipped_schedule = 0
        for a in artists:
            name = a["name"]
            if name.lower() in processed:
                continue

            # Genre blacklist check
            skip, reason = should_skip_artist(a, raw_network)
            if skip:
                skipped_genre += 1
                continue

            # Smart scheduling: skip if not due
            if not is_check_due(name, last_checked):
                skipped_schedule += 1
                continue

            remaining.append(a)

        print(f"   🔄 {len(remaining)} zu verarbeiten")
        print(f"   ⏭ {skipped_genre} übersprungen (Genre-Blacklist)")
        print(f"   ⏭ {skipped_schedule} übersprungen (noch nicht fällig)")

        if not remaining and not _shutdown:
            # Nothing to do — find when next check is due
            next_due = None
            for a in artists:
                key = a["name"].lower().strip()
                info = last_checked.get(key)
                if not info:
                    continue
                try:
                    last_dt = datetime.fromisoformat(info["checked"])
                    days_wait = RECHECK_DAYS_ACTIVE if info.get("has_recent") else RECHECK_DAYS_NO_RELEASE
                    due_at = last_dt + timedelta(days=days_wait)
                    if next_due is None or due_at < next_due:
                        next_due = due_at
                except (ValueError, KeyError):
                    pass

            if next_due:
                wait_secs = max(60, int((next_due - datetime.now()).total_seconds()))
                wait_mins = wait_secs // 60
                print(f"\n   😴 Alle Artists gecheckt. Nächster Check in {wait_mins} Minuten...")
                for _ in range(min(wait_secs, 3600)):
                    if _shutdown:
                        break
                    time.sleep(1)
            else:
                print(f"\n   😴 Warte 10 Minuten bis zum nächsten Zyklus...")
                for _ in range(600):
                    if _shutdown:
                        break
                    time.sleep(1)
            continue

        for i, artist in enumerate(remaining):
            if _shutdown:
                break

            name = artist["name"]
            pct = round((i + 1) / len(remaining) * 100) if remaining else 0
            print(f"[{pct:3d}%] {i+1}/{len(remaining)} — {name}", end="", flush=True)

            discogs_id = artist.get("discogs_id")
            if not discogs_id:
                discogs_id = search_artist(name)
                if not discogs_id:
                    print(" — ❌ nicht gefunden")
                    processed.add(name.lower())
                    update_last_checked(last_checked, name, False)
                    continue

            releases = fetch_artist_releases(discogs_id, cutoff)

            has_recent = False
            if releases:
                merged, added = save_releases_incremental(releases)
                print(f" — ✓ {len(releases)} releases ({added} neu, {len(merged)} gesamt)")
                # Check if any release is within ACTIVE_THRESHOLD
                now = datetime.now()
                for r in releases:
                    try:
                        rd = datetime.strptime(r["date"], "%Y-%m-%d")
                        if (now - rd).days <= ACTIVE_THRESHOLD_DAYS:
                            has_recent = True
                            break
                    except ValueError:
                        pass
            else:
                print(f" — keine neuen releases")

            processed.add(name.lower())
            update_last_checked(last_checked, name, has_recent)

            # Checkpoint every 20 artists
            if (i + 1) % 20 == 0:
                checkpoint["processed_artists"] = list(processed)
                save_checkpoint(checkpoint)
                save_last_checked(last_checked)
                print(f"\n   💾 Checkpoint gespeichert\n")

        # Save last_checked after processing
        save_last_checked(last_checked)

        if _shutdown:
            # Save what we have before exiting
            checkpoint["processed_artists"] = list(processed)
            save_checkpoint(checkpoint)
            print(f"\n✅ Daemon sauber beendet. Checkpoint gespeichert.")
            break

        # Cycle complete — clear checkpoint for next full scan
        cp_path = Path(__file__).parent / CHECKPOINT_FILE
        if cp_path.exists():
            cp_path.unlink()

        total = load_existing_releases()
        print()
        print(f"✅ Zyklus {cycle} fertig — {len(total)} Releases gesamt")
        print(f"   Nächster Zyklus startet sofort...")


def main():
    parser = argparse.ArgumentParser(description="Valentina Release Radar — Fetch releases")
    parser.add_argument("--months", type=int, default=6, help="How many months back to search (default: 6)")
    parser.add_argument("--limit", type=int, default=0, help="Limit number of artists to process (0 = all)")
    parser.add_argument("--resume", action="store_true", help="Resume from checkpoint")
    parser.add_argument("--daemon", action="store_true", help="Run continuously, re-scanning all artists in a loop")
    args = parser.parse_args()

    print()
    print("╔══════════════════════════════════════════════╗")
    print("║   VALENTINA RELEASE RADAR — Fetch Releases   ║")
    print("╚══════════════════════════════════════════════╝")
    print()

    if not DISCOGS_TOKEN:
        print("❌ Kein DISCOGS_TOKEN in .env gefunden!")
        print("   Bitte DISCOGS_TOKEN=dein_token in .env eintragen.")
        sys.exit(1)

    # Daemon mode
    if args.daemon:
        print(f"🔁 DAEMON MODE — läuft permanent, Ctrl+C zum Beenden")
        print(f"📅 Zeitfenster: {args.months} Monate")
        if args.limit:
            print(f"   (begrenzt auf {args.limit} Artists)")
        run_daemon(args)
        return

    # Load artists
    artists = load_network_data()
    print(f"📊 {len(artists)} Artists aus {NETWORK_FILE} geladen")

    if args.limit > 0:
        artists = artists[:args.limit]
        print(f"   (begrenzt auf {args.limit})")

    cutoff = datetime.now() - timedelta(days=args.months * 30)
    print(f"📅 Suche Releases seit {cutoff.strftime('%Y-%m-%d')} ({args.months} Monate)")

    # Resume support
    checkpoint = load_checkpoint() if args.resume else {"processed_artists": [], "releases": []}
    processed = set(checkpoint["processed_artists"])
    all_releases = checkpoint["releases"]

    remaining = [a for a in artists if a["name"].lower() not in processed]
    print(f"🔄 {len(remaining)} Artists zu verarbeiten ({len(processed)} bereits done)")
    print()

    api_calls = 0
    new_releases = 0

    for i, artist in enumerate(remaining):
        name = artist["name"]
        pct = round((i + 1) / len(remaining) * 100)
        print(f"[{pct:3d}%] {i+1}/{len(remaining)} — {name}", end="", flush=True)

        # Get Discogs artist ID
        discogs_id = artist.get("discogs_id")
        if not discogs_id:
            discogs_id = search_artist(name)
            api_calls += 1
            if not discogs_id:
                print(" — ❌ nicht gefunden")
                processed.add(name.lower())
                continue

        # Fetch releases
        releases = fetch_artist_releases(discogs_id, cutoff)
        api_calls += 1

        if releases:
            all_releases.extend(releases)
            new_releases += len(releases)
            print(f" — ✓ {len(releases)} releases")
        else:
            print(f" — keine neuen releases")

        processed.add(name.lower())

        # Checkpoint every 20 artists
        if (i + 1) % 20 == 0:
            checkpoint["processed_artists"] = list(processed)
            checkpoint["releases"] = all_releases
            save_checkpoint(checkpoint)
            final = save_releases(all_releases)
            print(f"\n   💾 Checkpoint: {len(final)} releases gesamt, {api_calls} API calls\n")

    # Final save
    final = save_releases(all_releases)

    # Clean up checkpoint
    cp_path = Path(__file__).parent / CHECKPOINT_FILE
    if cp_path.exists():
        cp_path.unlink()

    print()
    print("═" * 50)
    print(f"✅ FERTIG!")
    print(f"   {len(final)} Releases in {OUTPUT_FILE}")
    print(f"   {len(processed)} Artists verarbeitet")
    print(f"   {api_calls} Discogs API calls")
    print(f"   Genres: {', '.join(sorted(set(r['genre'] for r in final)))}")
    print()
    print(f"👉 Öffne release_radar.html im Browser!")
    print("═" * 50)


if __name__ == "__main__":
    main()
