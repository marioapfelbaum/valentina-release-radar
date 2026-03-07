#!/usr/bin/env python3
"""
expand_network.py — Netzwerk-Expansion für Valentina Release Radar
===================================================================
Erweitert network_data.json mit neuen Artists aus verschiedenen Quellen.

Modi:
  --from-labels      Alle Artists der Labels aus reference_labels.txt
  --from-favs        Labels aus favs.json → kompletten Roster holen
  --from-clubs       Club-Lineups der letzten 12 Monate
  --from-festivals   Festival-Lineups der letzten 3-5 Jahre
  --from-scenes      Szene-spezifische Quellen (Rominimal, Latam)
  --from-magazines   Charts/Reviews von RA, Groove, Hardwax, Phonica etc.
  --from-artists     Reference artists aus reference_artists.txt als Seeds

Usage:
  python expand_network.py --from-labels
  python expand_network.py --from-labels --resume
  python expand_network.py --from-artists
  python expand_network.py --from-clubs
  python expand_network.py --dry-run --from-labels
"""

import json
import os
import sys
import time
import argparse
import re
from pathlib import Path
from datetime import datetime

try:
    import requests
except ImportError:
    print("pip install requests")
    sys.exit(1)

# ─────────────────────────────────────────────────
# PATHS
# ─────────────────────────────────────────────────

BASE_DIR = Path(__file__).parent
NETWORK_FILE = BASE_DIR / "network_data.json"
CHECKPOINT_FILE = BASE_DIR / ".expand_checkpoint.json"
WHITELIST_FILE = BASE_DIR / "genre_whitelist.txt"
BLACKLIST_FILE = BASE_DIR / "genre_blacklist.txt"
LABELS_FILE = BASE_DIR / "reference_labels.txt"
ARTISTS_FILE = BASE_DIR / "reference_artists.txt"
FAVS_FILE = BASE_DIR / "favs.json"
ENV_FILE = BASE_DIR / ".env"

# ─────────────────────────────────────────────────
# CONFIG
# ─────────────────────────────────────────────────

RATE_LIMIT_DELAY = 2.0  # Seconds between requests
SAVE_INTERVAL = 10       # Save after every N new artists

# ─────────────────────────────────────────────────
# LOAD ENV + TOKEN
# ─────────────────────────────────────────────────

def load_env():
    if ENV_FILE.exists():
        for line in ENV_FILE.read_text().splitlines():
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                k, v = line.split("=", 1)
                os.environ.setdefault(k.strip(), v.strip())

load_env()
DISCOGS_TOKEN = os.getenv("DISCOGS_TOKEN") or os.getenv("DISCOGS_API_TOKEN")

# ─────────────────────────────────────────────────
# DISCOGS API
# ─────────────────────────────────────────────────

class DiscogsAPI:
    BASE = "https://api.discogs.com"

    def __init__(self, token):
        self.session = requests.Session()
        self.session.headers.update({
            "Authorization": f"Discogs token={token}",
            "User-Agent": "ValentinaExpandNetwork/1.0 +https://valentina.studio"
        })
        self.request_count = 0
        self.last_request = 0

    def _get(self, url, params=None):
        elapsed = time.time() - self.last_request
        if elapsed < RATE_LIMIT_DELAY:
            time.sleep(RATE_LIMIT_DELAY - elapsed)
        self.last_request = time.time()
        self.request_count += 1
        try:
            resp = self.session.get(url, params=params, timeout=15)
            if resp.status_code == 429:
                wait = int(resp.headers.get("Retry-After", 60))
                print(f"    ⏳ Rate Limit, warte {wait}s...")
                time.sleep(wait)
                return self._get(url, params)
            if resp.status_code == 404:
                return None
            resp.raise_for_status()
            return resp.json()
        except requests.exceptions.RequestException as e:
            print(f"    ⚠ API Error: {e}")
            return None

    def search_label(self, name):
        data = self._get(f"{self.BASE}/database/search",
                         {"q": name, "type": "label", "per_page": 10})
        if not data or not data.get("results"):
            return None
        # Exact match first
        for r in data["results"]:
            if r.get("title", "").lower().strip() == name.lower().strip():
                return r
        # Fuzzy: first result
        return data["results"][0]

    def search_artist(self, name):
        data = self._get(f"{self.BASE}/database/search",
                         {"q": name, "type": "artist", "per_page": 5})
        if not data or not data.get("results"):
            return None
        for r in data["results"]:
            if r.get("title", "").lower().strip() == name.lower().strip():
                return r
        return data["results"][0]

    def get_label_releases(self, label_id, page=1):
        return self._get(f"{self.BASE}/labels/{label_id}/releases",
                         {"page": page, "per_page": 100})

    def get_artist(self, artist_id):
        return self._get(f"{self.BASE}/artists/{artist_id}")

    def get_label(self, label_id):
        return self._get(f"{self.BASE}/labels/{label_id}")

    def get_artist_releases(self, artist_id, page=1):
        return self._get(f"{self.BASE}/artists/{artist_id}/releases",
                         {"page": page, "per_page": 100, "sort": "year", "sort_order": "desc"})


# ─────────────────────────────────────────────────
# GENRE FILTERING
# ─────────────────────────────────────────────────

def load_lines(path):
    """Load non-empty lines from a text file."""
    if not path.exists():
        return []
    return [l.strip() for l in path.read_text().splitlines() if l.strip()]

def load_genre_sets():
    whitelist = {g.lower() for g in load_lines(WHITELIST_FILE)}
    blacklist = {g.lower() for g in load_lines(BLACKLIST_FILE)}
    return whitelist, blacklist

def is_blacklisted(genres, blacklist):
    """Check if any of the artist's genres are on the blacklist."""
    for g in genres:
        if g.lower().strip() in blacklist:
            return True
    return False

def has_whitelisted(genres, whitelist):
    """Check if any genre matches the whitelist."""
    for g in genres:
        if g.lower().strip() in whitelist:
            return True
    return False


# ─────────────────────────────────────────────────
# NETWORK DATA
# ─────────────────────────────────────────────────

def load_network():
    if NETWORK_FILE.exists():
        with open(NETWORK_FILE) as f:
            return json.load(f)
    return {
        "metadata": {"total_requests": 0, "artists_found": 0, "labels_found": 0},
        "artists": {},
        "labels": {},
        "edges": [],
        "crawled_artists": []
    }

def save_network(data):
    data["metadata"]["last_updated"] = datetime.now().isoformat()
    data["metadata"]["artists_found"] = len(data["artists"])
    data["metadata"]["labels_found"] = len(data["labels"])
    tmp = NETWORK_FILE.with_suffix(".tmp")
    with open(tmp, "w") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    os.replace(tmp, NETWORK_FILE)

def artist_key(discogs_id):
    return f"d_{discogs_id}"

def label_key(discogs_id):
    return f"d_{discogs_id}"

def add_artist(network, discogs_id, name, genres=None, label_ids=None, source=""):
    key = artist_key(discogs_id)
    if key in network["artists"]:
        # Update source if new
        existing = network["artists"][key]
        if source and source not in existing.get("sources", []):
            existing.setdefault("sources", []).append(source)
        if label_ids:
            existing_labels = set(existing.get("label_ids", []))
            for lid in label_ids:
                if lid not in existing_labels:
                    existing.setdefault("label_ids", []).append(lid)
        return False  # not new
    network["artists"][key] = {
        "name": name,
        "depth": 0,
        "genres": genres or [],
        "url": "",
        "discogs_id": discogs_id,
        "mbid": None,
        "spotify_id": None,
        "popularity": None,
        "sources": ["discogs", source] if source else ["discogs"],
        "label_ids": label_ids or [],
        "is_seed": False
    }
    return True  # new

def add_label(network, discogs_id, name, source=""):
    key = label_key(discogs_id)
    if key in network["labels"]:
        existing = network["labels"][key]
        if source and source not in existing.get("sources", []):
            existing.setdefault("sources", []).append(source)
        return False
    network["labels"][key] = {
        "name": name,
        "depth": 0,
        "genres": [],
        "url": "",
        "discogs_id": discogs_id,
        "mbid": None,
        "release_count": 0,
        "profile": "",
        "sources": ["discogs", source] if source else ["discogs"],
        "artist_ids": [],
        "is_seed": False
    }
    return True


# ─────────────────────────────────────────────────
# CHECKPOINT
# ─────────────────────────────────────────────────

def load_checkpoint(mode):
    if CHECKPOINT_FILE.exists():
        with open(CHECKPOINT_FILE) as f:
            cp = json.load(f)
        if cp.get("mode") == mode:
            return cp
    return None

def save_checkpoint(mode, data):
    data["mode"] = mode
    with open(CHECKPOINT_FILE, "w") as f:
        json.dump(data, f)

def clear_checkpoint():
    if CHECKPOINT_FILE.exists():
        CHECKPOINT_FILE.unlink()


# ─────────────────────────────────────────────────
# MODE: --from-labels
# ─────────────────────────────────────────────────

def expand_from_labels(api, network, blacklist, dry_run=False, resume=False):
    """For each label in reference_labels.txt, fetch ALL artists from its roster."""
    labels = load_lines(LABELS_FILE)
    if not labels:
        print("❌ reference_labels.txt leer oder nicht gefunden")
        return 0

    print(f"\n{'='*60}")
    print(f"FROM-LABELS: {len(labels)} Labels aus reference_labels.txt")
    print(f"{'='*60}\n")

    # Resume support
    start_idx = 0
    if resume:
        cp = load_checkpoint("from-labels")
        if cp:
            start_idx = cp.get("label_index", 0)
            print(f"  ↻ Fortsetzen ab Label #{start_idx + 1}")

    total_new = 0
    total_skipped = 0
    total_existing = 0

    for idx, label_name in enumerate(labels):
        if idx < start_idx:
            continue

        print(f"\n[{idx+1}/{len(labels)}] 🏷️  {label_name}")

        # Search for the label on Discogs
        result = api.search_label(label_name)
        if not result:
            print(f"  ⚠ Nicht auf Discogs gefunden")
            continue

        label_id = result["id"]
        label_title = result.get("title", label_name)
        lkey = label_key(label_id)

        # Add label to network
        add_label(network, label_id, label_title, source="reference_labels")

        # Fetch all releases from this label
        page = 1
        label_new = 0
        label_artists_seen = set()

        while True:
            data = api.get_label_releases(label_id, page=page)
            if not data or not data.get("releases"):
                break

            for rel in data["releases"]:
                # Extract artist info
                a_id = rel.get("artist_id") or rel.get("id")
                # The label releases endpoint returns artist as string
                a_name = rel.get("artist", "").strip()

                # Skip "Various" or empty
                if not a_name or a_name.lower() in ("various", "various artists", "unknown"):
                    continue

                # artist field on label releases is the name, not ID
                # We need the actual Discogs artist ID from the stats field or catalog
                # Actually, the releases endpoint for labels returns:
                # {id, status, type, format, artist, title, catno, year, ...}
                # where 'id' is the RELEASE id, not artist id.
                # We need to extract unique artist names and search for them.

                if a_name in label_artists_seen:
                    continue
                label_artists_seen.add(a_name)

                # Check if already in network by name
                existing = False
                for ak, av in network["artists"].items():
                    if av["name"].lower().strip() == a_name.lower().strip():
                        existing = True
                        # Just update label_ids
                        if lkey not in av.get("label_ids", []):
                            av.setdefault("label_ids", []).append(lkey)
                        if "reference_labels" not in av.get("sources", []):
                            av.setdefault("sources", []).append("reference_labels")
                        total_existing += 1
                        break

                if existing:
                    continue

                # Search for artist on Discogs to get their ID and genres
                a_result = api.search_artist(a_name)
                if not a_result:
                    continue

                a_discogs_id = a_result["id"]
                a_genres = [g.lower() for g in a_result.get("genre", [])] + \
                           [s.lower() for s in a_result.get("style", [])]

                # Check blacklist
                if is_blacklisted(a_genres, blacklist):
                    print(f"  ⛔ SKIP {a_name} (blacklisted: {a_genres})")
                    total_skipped += 1
                    continue

                if dry_run:
                    print(f"  + [DRY] {a_name} (genres: {a_genres})")
                    label_new += 1
                    total_new += 1
                    continue

                was_new = add_artist(
                    network, a_discogs_id, a_result.get("title", a_name),
                    genres=a_genres,
                    label_ids=[lkey],
                    source="reference_labels"
                )
                if was_new:
                    label_new += 1
                    total_new += 1
                    print(f"  + {a_result.get('title', a_name)}")

                    if total_new % SAVE_INTERVAL == 0 and not dry_run:
                        save_network(network)
                        save_checkpoint("from-labels", {"label_index": idx})
                        print(f"  💾 Gespeichert ({total_new} neue)")

            # Pagination
            pagination = data.get("pagination", {})
            if page >= pagination.get("pages", 1):
                break
            page += 1

        print(f"  → {len(label_artists_seen)} Artists gefunden, {label_new} neu")

        # Save checkpoint after each label
        if not dry_run and label_new > 0:
            save_network(network)
            save_checkpoint("from-labels", {"label_index": idx + 1})

    print(f"\n{'='*60}")
    print(f"FROM-LABELS Ergebnis:")
    print(f"  Neue Artists:      {total_new}")
    print(f"  Bereits vorhanden: {total_existing}")
    print(f"  Blacklisted/Skip:  {total_skipped}")
    print(f"{'='*60}")

    return total_new


# ─────────────────────────────────────────────────
# MODE: --from-artists
# ─────────────────────────────────────────────────

def expand_from_artists(api, network, blacklist, dry_run=False, resume=False):
    """Add reference artists directly + their label-mates."""
    artists = load_lines(ARTISTS_FILE)
    if not artists:
        print("❌ reference_artists.txt leer oder nicht gefunden")
        return 0

    print(f"\n{'='*60}")
    print(f"FROM-ARTISTS: {len(artists)} Artists aus reference_artists.txt")
    print(f"{'='*60}\n")

    start_idx = 0
    if resume:
        cp = load_checkpoint("from-artists")
        if cp:
            start_idx = cp.get("artist_index", 0)
            print(f"  ↻ Fortsetzen ab Artist #{start_idx + 1}")

    total_new = 0

    for idx, artist_name in enumerate(artists):
        if idx < start_idx:
            continue

        print(f"\n[{idx+1}/{len(artists)}] 🎧 {artist_name}")

        result = api.search_artist(artist_name)
        if not result:
            print(f"  ⚠ Nicht auf Discogs gefunden")
            continue

        a_id = result["id"]
        a_genres = [g.lower() for g in result.get("genre", [])] + \
                   [s.lower() for s in result.get("style", [])]

        was_new = add_artist(
            network, a_id, result.get("title", artist_name),
            genres=a_genres, source="reference_artists"
        )
        if was_new:
            total_new += 1
            print(f"  + Added as seed")

        # Fetch their releases to discover labels
        page_data = api.get_artist_releases(a_id, page=1)
        if page_data and page_data.get("releases"):
            label_ids_seen = set()
            for rel in page_data["releases"][:50]:  # First 50 releases
                lab = rel.get("label")
                if lab and lab.lower() not in ("not on label", "white label", "self-released", "unknown"):
                    # Get label ID from the release stats/resource_url
                    # Unfortunately artist releases don't include label_id directly
                    # We just track the names for now
                    pass

        if not dry_run and total_new % SAVE_INTERVAL == 0 and total_new > 0:
            save_network(network)
            save_checkpoint("from-artists", {"artist_index": idx})

    if not dry_run and total_new > 0:
        save_network(network)

    print(f"\n  → {total_new} neue Artists hinzugefügt")
    return total_new


# ─────────────────────────────────────────────────
# MODE: --from-favs
# ─────────────────────────────────────────────────

def expand_from_favs(api, network, blacklist, dry_run=False, resume=False):
    """Read favs.json, extract labels, fetch full rosters."""
    if not FAVS_FILE.exists():
        print("❌ favs.json nicht gefunden")
        print("   Exportiere Favoriten aus der Web-App zuerst.")
        return 0

    with open(FAVS_FILE) as f:
        favs = json.load(f)

    # Extract unique labels from favorites
    labels_seen = set()
    for fav in favs:
        label = fav.get("label", "").strip()
        if label and label.lower() not in ("not on label", "white label", "self-released", "", "unknown"):
            labels_seen.add(label)

    print(f"\n{'='*60}")
    print(f"FROM-FAVS: {len(labels_seen)} Labels aus {len(favs)} Favoriten")
    print(f"{'='*60}\n")

    # Write temp labels file and use from-labels logic
    total_new = 0
    for i, label_name in enumerate(sorted(labels_seen)):
        print(f"\n[{i+1}/{len(labels_seen)}] 🏷️  {label_name}")

        result = api.search_label(label_name)
        if not result:
            print(f"  ⚠ Nicht auf Discogs gefunden")
            continue

        label_id = result["id"]
        lkey = label_key(label_id)
        add_label(network, label_id, result.get("title", label_name), source="favs")

        # Fetch roster (first page only for favs — speed)
        data = api.get_label_releases(label_id, page=1)
        if not data or not data.get("releases"):
            continue

        artists_seen = set()
        label_new = 0
        for rel in data["releases"]:
            a_name = rel.get("artist", "").strip()
            if not a_name or a_name.lower() in ("various", "various artists"):
                continue
            if a_name in artists_seen:
                continue
            artists_seen.add(a_name)

            # Check if already in network
            exists = any(
                av["name"].lower().strip() == a_name.lower().strip()
                for av in network["artists"].values()
            )
            if exists:
                continue

            a_result = api.search_artist(a_name)
            if not a_result:
                continue

            a_genres = [g.lower() for g in a_result.get("genre", [])] + \
                       [s.lower() for s in a_result.get("style", [])]

            if is_blacklisted(a_genres, blacklist):
                continue

            if not dry_run:
                was_new = add_artist(
                    network, a_result["id"], a_result.get("title", a_name),
                    genres=a_genres, label_ids=[lkey], source="favs"
                )
                if was_new:
                    label_new += 1
                    total_new += 1
                    print(f"  + {a_result.get('title', a_name)}")
            else:
                print(f"  + [DRY] {a_name}")
                total_new += 1

        if label_new > 0 and not dry_run:
            save_network(network)

        print(f"  → {len(artists_seen)} Artists, {label_new} neu")

    print(f"\n  → Gesamt: {total_new} neue Artists aus Favoriten-Labels")
    return total_new


# ─────────────────────────────────────────────────
# MODE: --from-clubs
# ─────────────────────────────────────────────────

CLUBS = {
    # Deutschland
    "panorama_bar": {"name": "Panorama Bar / Berghain", "city": "Berlin", "country": "DE",
                     "ra_id": "64", "ra_slug": "berghain-panorama-bar"},
    "robert_johnson": {"name": "Robert Johnson", "city": "Offenbach", "country": "DE",
                       "ra_id": "2093", "ra_slug": "robert-johnson"},
    "tresor": {"name": "Tresor", "city": "Berlin", "country": "DE",
               "ra_id": "447", "ra_slug": "tresor"},
    "about_blank": {"name": "://about blank", "city": "Berlin", "country": "DE",
                    "ra_id": "10768", "ra_slug": "about-blank"},
    "salon_wild": {"name": "Salon Wild", "city": "Berlin", "country": "DE",
                   "ra_id": "", "ra_slug": "salon-wild"},
    "paloma_bar": {"name": "Paloma Bar", "city": "Berlin", "country": "DE",
                   "ra_id": "", "ra_slug": "paloma-bar"},
    "rote_sonne": {"name": "Rote Sonne", "city": "München", "country": "DE",
                   "ra_id": "3672", "ra_slug": "rote-sonne"},
    "hoer_bar": {"name": "Hör Bar", "city": "München", "country": "DE",
                 "ra_id": "", "ra_slug": ""},
    "golden_pudel": {"name": "Golden Pudel Club", "city": "Hamburg", "country": "DE",
                     "ra_id": "9387", "ra_slug": "golden-pudel-club"},
    "hoppetosse": {"name": "Hoppetosse / Revier Südost", "city": "Berlin", "country": "DE",
                   "ra_id": "", "ra_slug": "revier-suedost"},
    "sameheads": {"name": "Sameheads", "city": "Berlin", "country": "DE",
                  "ra_id": "", "ra_slug": "sameheads"},
    "maschinenraum": {"name": "Maschinenraum", "city": "Weimar", "country": "DE",
                      "ra_id": "", "ra_slug": ""},
    "pollerwiesen": {"name": "PollerWiesen", "city": "Köln", "country": "DE",
                     "ra_id": "", "ra_slug": "pollerwiesen"},
    # Schweiz
    "nordstern": {"name": "Nordstern", "city": "Basel", "country": "CH",
                  "ra_id": "11791", "ra_slug": "nordstern"},
    "zukunft": {"name": "Zukunft", "city": "Zürich", "country": "CH",
                "ra_id": "46880", "ra_slug": "zukunft"},
    "kulm": {"name": "Kulm", "city": "St. Gallen", "country": "CH",
             "ra_id": "", "ra_slug": ""},
    # Niederlande
    "garage_noord": {"name": "Garage Noord", "city": "Amsterdam", "country": "NL",
                     "ra_id": "80059", "ra_slug": "garage-noord-amsterdam"},
    "de_school": {"name": "De School", "city": "Amsterdam", "country": "NL",
                  "ra_id": "73498", "ra_slug": "de-school"},
    # UK
    "fabric": {"name": "Fabric", "city": "London", "country": "UK",
               "ra_id": "428", "ra_slug": "fabric"},
    "phonox": {"name": "Phonox", "city": "London", "country": "UK",
               "ra_id": "72647", "ra_slug": "phonox"},
    # Frankreich
    "concrete": {"name": "Concrete", "city": "Paris", "country": "FR",
                 "ra_id": "47549", "ra_slug": "concrete"},
    # Belgien
    "fuse": {"name": "Fuse", "city": "Brüssel", "country": "BE",
             "ra_id": "1036", "ra_slug": "fuse-brussels"},
    # Spanien
    "nitsa": {"name": "Nitsa", "city": "Barcelona", "country": "ES",
              "ra_id": "2000", "ra_slug": "nitsa-club"},
    # Portugal
    "lux_fragil": {"name": "Lux Frágil", "city": "Lissabon", "country": "PT",
                   "ra_id": "1693", "ra_slug": "lux"},
    # Italien
    "mondo": {"name": "Mondo", "city": "Rom", "country": "IT",
              "ra_id": "", "ra_slug": ""},
    "goa_club": {"name": "Goa Club", "city": "Rom", "country": "IT",
                 "ra_id": "14153", "ra_slug": "goa-club"},
    # Osteuropa
    "bassiani": {"name": "Bassiani", "city": "Tbilisi", "country": "GE",
                 "ra_id": "70092", "ra_slug": "bassiani"},
    "closer": {"name": "Closer", "city": "Kyiv", "country": "UA",
               "ra_id": "68277", "ra_slug": "closer-kyiv"},
    "guesthouse": {"name": "Guesthouse", "city": "Bukarest", "country": "RO",
                   "ra_id": "92456", "ra_slug": "guesthouse"},
    "control_club": {"name": "Control Club", "city": "Bukarest", "country": "RO",
                     "ra_id": "26139", "ra_slug": "control-club"},
    "midi": {"name": "Midi", "city": "Cluj", "country": "RO",
             "ra_id": "85555", "ra_slug": ""},
    "la_gazette": {"name": "La Gazette", "city": "Cluj", "country": "RO",
                   "ra_id": "", "ra_slug": ""},
    "eden": {"name": "Eden", "city": "Cluj", "country": "RO",
             "ra_id": "", "ra_slug": ""},
    # Skandinavien
    "jaeger": {"name": "Jaeger", "city": "Oslo", "country": "NO",
               "ra_id": "68783", "ra_slug": "jaeger"},
    # USA
    "bossa_nova": {"name": "Bossa Nova Civic Club", "city": "NYC", "country": "US",
                   "ra_id": "52474", "ra_slug": "bossa-nova-civic-club"},
    "nowadays": {"name": "Nowadays", "city": "NYC", "country": "US",
                 "ra_id": "81998", "ra_slug": "nowadays"},
    # Japan
    "the_loft": {"name": "The Loft", "city": "Tokyo", "country": "JP",
                 "ra_id": "", "ra_slug": ""},
    # Ibiza
    "dc10": {"name": "Circoloco / DC-10", "city": "Ibiza", "country": "ES",
             "ra_id": "1349", "ra_slug": "dc-10"},
}

FESTIVALS = {
    "sunwaves": {"name": "Sunwaves", "country": "RO", "ra_slug": "sunwaves-festival"},
    "dekmantel": {"name": "Dekmantel", "country": "NL", "ra_slug": "dekmantel-festival"},
    "freerotation": {"name": "Freerotation", "country": "UK", "ra_slug": "freerotation"},
    "houghton": {"name": "Houghton", "country": "UK", "ra_slug": "houghton-festival"},
    "meakusma": {"name": "Meakusma", "country": "BE", "ra_slug": "meakusma"},
    "atonal": {"name": "Atonal", "country": "DE", "ra_slug": "berlin-atonal"},
    "terraforma": {"name": "Terraforma", "country": "IT", "ra_slug": "terraforma"},
    "love_intl": {"name": "Love International", "country": "HR", "ra_slug": "love-international"},
    "dimensions": {"name": "Dimensions", "country": "HR", "ra_slug": "dimensions-festival"},
    "pollerwiesen": {"name": "PollerWiesen", "country": "DE", "ra_slug": "pollerwiesen"},
    "moga": {"name": "MOGA", "country": "MA", "ra_slug": "moga-festival"},
    "labyrinth": {"name": "Labyrinth", "country": "JP", "ra_slug": "labyrinth-festival"},
    "capsule": {"name": "Capsule Festival", "country": "JP", "ra_slug": "capsule-festival"},
    "mutek_mx": {"name": "Mutek México", "country": "MX", "ra_slug": "mutek-mexico"},
    "dekmantel_sp": {"name": "Dekmantel São Paulo", "country": "BR", "ra_slug": "dekmantel-sao-paulo"},
    "sonar_santiago": {"name": "Sonar Santiago", "country": "CL", "ra_slug": "sonar-santiago"},
    "bpm": {"name": "BPM Festival", "country": "PT", "ra_slug": "the-bpm-festival"},
}

# RA has anti-scraping. We try RA first, fallback to Google.
RA_UA = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"


def scrape_ra_venue_events(session, slug, months=12):
    """Try to scrape RA venue page for artist names."""
    if not slug:
        return []

    url = f"https://ra.co/clubs/{slug}/events"
    try:
        resp = session.get(url, headers={"User-Agent": RA_UA}, timeout=15)
        if resp.status_code != 200:
            return []
        # Extract artist names from the page
        # RA uses Next.js / React — data is often in JSON within <script> tags
        # Look for artist names in lineup info
        artists = set()
        # Try JSON-LD or embedded data
        json_matches = re.findall(r'"lineup":\s*\[(.*?)\]', resp.text)
        for m in json_matches:
            names = re.findall(r'"name":\s*"([^"]+)"', m)
            artists.update(names)
        # Also try plain text patterns
        name_matches = re.findall(r'class="[^"]*"[^>]*>([A-Z][a-z]+ [A-Z][a-z]+(?:\s[A-Z][a-z]+)?)</[^>]+>', resp.text)
        # This is fragile — fallback to Google
        return list(artists)
    except Exception as e:
        print(f"    ⚠ RA Scrape Error: {e}")
        return []


def google_search_club_artists(session, club_name, city):
    """Fallback: Google search for club lineups."""
    query = f'site:ra.co "{club_name}" "{city}" lineup 2025 OR 2026'
    try:
        resp = session.get(
            "https://www.google.com/search",
            params={"q": query, "num": 20},
            headers={"User-Agent": RA_UA},
            timeout=15
        )
        if resp.status_code != 200:
            return []
        # Extract potential artist names from Google results
        # This is limited but can find some names
        artists = set()
        # Look for RA event page links and extract names from snippets
        snippets = re.findall(r'<span[^>]*>([^<]+)</span>', resp.text)
        return list(artists)
    except Exception as e:
        print(f"    ⚠ Google Search Error: {e}")
        return []


def expand_from_clubs(api, network, blacklist, dry_run=False, resume=False):
    """Scrape club lineups from RA and add artists."""
    print(f"\n{'='*60}")
    print(f"FROM-CLUBS: {len(CLUBS)} Clubs")
    print(f"{'='*60}\n")

    session = requests.Session()
    total_new = 0

    start_idx = 0
    club_keys = list(CLUBS.keys())
    if resume:
        cp = load_checkpoint("from-clubs")
        if cp:
            start_idx = cp.get("club_index", 0)

    for idx, club_id in enumerate(club_keys):
        if idx < start_idx:
            continue

        club = CLUBS[club_id]
        print(f"\n[{idx+1}/{len(CLUBS)}] 🏠 {club['name']} ({club['city']})")

        slug = club.get("ra_slug", "")
        artists = scrape_ra_venue_events(session, slug)

        if not artists:
            # Fallback: Google search
            print(f"  RA fehlgeschlagen, versuche Google-Suche...")
            artists = google_search_club_artists(session, club["name"], club["city"])

        if not artists:
            print(f"  ⚠ Keine Artists gefunden")
            time.sleep(RATE_LIMIT_DELAY)
            continue

        print(f"  Gefunden: {len(artists)} Artists")
        club_new = 0

        for a_name in artists:
            if not a_name or len(a_name) < 2:
                continue

            # Check if already in network
            exists = any(
                av["name"].lower().strip() == a_name.lower().strip()
                for av in network["artists"].values()
            )
            if exists:
                continue

            a_result = api.search_artist(a_name)
            if not a_result:
                continue

            a_genres = [g.lower() for g in a_result.get("genre", [])] + \
                       [s.lower() for s in a_result.get("style", [])]

            if is_blacklisted(a_genres, blacklist):
                print(f"  ⛔ SKIP {a_name} (blacklisted)")
                continue

            if not dry_run:
                was_new = add_artist(
                    network, a_result["id"], a_result.get("title", a_name),
                    genres=a_genres,
                    source=f"club:{club_id}"
                )
                if was_new:
                    club_new += 1
                    total_new += 1
                    print(f"  + {a_result.get('title', a_name)}")
            else:
                print(f"  + [DRY] {a_name}")
                total_new += 1

        if club_new > 0 and not dry_run:
            save_network(network)
            save_checkpoint("from-clubs", {"club_index": idx + 1})

        print(f"  → {club_new} neue Artists")
        time.sleep(RATE_LIMIT_DELAY)

    print(f"\n  → Gesamt: {total_new} neue Artists aus Clubs")
    return total_new


# ─────────────────────────────────────────────────
# MODE: --from-festivals
# ─────────────────────────────────────────────────

def expand_from_festivals(api, network, blacklist, dry_run=False, resume=False):
    """Scrape festival lineups from RA."""
    print(f"\n{'='*60}")
    print(f"FROM-FESTIVALS: {len(FESTIVALS)} Festivals")
    print(f"{'='*60}\n")

    session = requests.Session()
    total_new = 0

    for idx, (fest_id, fest) in enumerate(FESTIVALS.items()):
        print(f"\n[{idx+1}/{len(FESTIVALS)}] 🎪 {fest['name']} ({fest['country']})")

        slug = fest.get("ra_slug", "")
        artists = scrape_ra_venue_events(session, slug)

        if not artists:
            print(f"  ⚠ Keine Artists gefunden (RA-Scraping fehlgeschlagen)")
            time.sleep(RATE_LIMIT_DELAY)
            continue

        print(f"  Gefunden: {len(artists)} Artists")
        fest_new = 0

        for a_name in artists:
            if not a_name or len(a_name) < 2:
                continue
            exists = any(
                av["name"].lower().strip() == a_name.lower().strip()
                for av in network["artists"].values()
            )
            if exists:
                continue

            a_result = api.search_artist(a_name)
            if not a_result:
                continue

            a_genres = [g.lower() for g in a_result.get("genre", [])] + \
                       [s.lower() for s in a_result.get("style", [])]

            if is_blacklisted(a_genres, blacklist):
                continue

            if not dry_run:
                was_new = add_artist(
                    network, a_result["id"], a_result.get("title", a_name),
                    genres=a_genres, source=f"festival:{fest_id}"
                )
                if was_new:
                    fest_new += 1
                    total_new += 1
                    print(f"  + {a_result.get('title', a_name)}")

        if fest_new > 0 and not dry_run:
            save_network(network)

        print(f"  → {fest_new} neue Artists")
        time.sleep(RATE_LIMIT_DELAY)

    print(f"\n  → Gesamt: {total_new} neue Artists aus Festivals")
    return total_new


# ─────────────────────────────────────────────────
# MODE: --from-scenes
# ─────────────────────────────────────────────────

ROMINIMAL_ARTISTS = [
    "Raresh", "Rhadoo", "Petre Inspirescu", "Pedro", "Praslea", "Barac",
    "Priku", "Arapu", "Cap", "Cezar", "Crihan", "Herodot", "Sepp",
    "Dubfound", "Lizz", "Nu Zau", "Suciu", "Cosmjn", "Vlad Caia", "Sit",
    "Cally", "Mihigh", "Nelu Stanciu", "Livio & Roby", "Premiesku", "Gescu"
]

LATAM_ARTISTS = [
    "Matias Aguayo", "Argenis Brito", "Pier Bucci", "Dinky", "Dandy Jack",
    "Ricardo Tobar", "Alejandro Paz", "Marcelo Rosselot", "Rebolledo", "Verraco"
]

LATAM_LABELS = {
    "chile": ["Mamba Negra", "Discos Porfiados", "Oscilador", "Nehuen Records"],
    "argentina": ["Concepto Hipnótico", "Banlieue Records", "CiCLO"],
    "mexico": ["Duro", "Naafi", "Nórdika"],
    "colombia": ["Periferia"],
    "brazil": ["Nada Records", "Mamba Rec"],
}

ROMINIMAL_LABELS = [
    "Metereze", "Sleep Is Commercial", "Amphia", "Serialism", "SoLenoid",
    "Primăria", "Undercolor", "Cyclic Records", "[a:rpia:r]", "Sacre",
    "Nervmusic", "Playedby"
]


def expand_from_scenes(api, network, blacklist, dry_run=False, resume=False):
    """Add scene-specific artists and label rosters."""
    print(f"\n{'='*60}")
    print(f"FROM-SCENES: Rominimal + Latam")
    print(f"{'='*60}\n")

    total_new = 0

    # --- Rominimal Scene ---
    print(f"🇷🇴 ROMINIMAL SZENE")
    print(f"  Artists: {len(ROMINIMAL_ARTISTS)}")
    for a_name in ROMINIMAL_ARTISTS:
        result = api.search_artist(a_name)
        if not result:
            print(f"  ⚠ {a_name} nicht gefunden")
            continue
        was_new = add_artist(
            network, result["id"], result.get("title", a_name),
            genres=[g.lower() for g in result.get("genre", [])],
            source="scene:rominimal"
        )
        if was_new and not dry_run:
            total_new += 1
            print(f"  + {result.get('title', a_name)}")
        elif was_new:
            print(f"  + [DRY] {a_name}")
            total_new += 1

    # Rominimal labels
    print(f"\n  Labels: {len(ROMINIMAL_LABELS)}")
    for label_name in ROMINIMAL_LABELS:
        result = api.search_label(label_name)
        if not result:
            print(f"  ⚠ Label '{label_name}' nicht gefunden")
            continue

        label_id = result["id"]
        lkey = label_key(label_id)
        add_label(network, label_id, result.get("title", label_name), source="scene:rominimal")

        # Get roster
        data = api.get_label_releases(label_id, page=1)
        if not data or not data.get("releases"):
            continue

        seen = set()
        for rel in data["releases"]:
            a_name = rel.get("artist", "").strip()
            if not a_name or a_name.lower() in ("various", "various artists") or a_name in seen:
                continue
            seen.add(a_name)

            exists = any(
                av["name"].lower().strip() == a_name.lower().strip()
                for av in network["artists"].values()
            )
            if exists:
                continue

            a_result = api.search_artist(a_name)
            if not a_result:
                continue

            a_genres = [g.lower() for g in a_result.get("genre", [])] + \
                       [s.lower() for s in a_result.get("style", [])]
            if is_blacklisted(a_genres, blacklist):
                continue

            if not dry_run:
                was_new = add_artist(
                    network, a_result["id"], a_result.get("title", a_name),
                    genres=a_genres, label_ids=[lkey], source="scene:rominimal"
                )
                if was_new:
                    total_new += 1
                    print(f"  + {a_result.get('title', a_name)} (via {label_name})")

    if not dry_run:
        save_network(network)

    # --- Latam Scene ---
    print(f"\n🌎 LATAM SZENE")
    print(f"  Artists: {len(LATAM_ARTISTS)}")
    for a_name in LATAM_ARTISTS:
        result = api.search_artist(a_name)
        if not result:
            print(f"  ⚠ {a_name} nicht gefunden")
            continue
        was_new = add_artist(
            network, result["id"], result.get("title", a_name),
            genres=[g.lower() for g in result.get("genre", [])],
            source="scene:latam"
        )
        if was_new and not dry_run:
            total_new += 1
            print(f"  + {result.get('title', a_name)}")

    # Latam labels per country
    for country, labels in LATAM_LABELS.items():
        print(f"\n  🏷️  {country.upper()} Labels: {len(labels)}")
        for label_name in labels:
            result = api.search_label(label_name)
            if not result:
                print(f"  ⚠ Label '{label_name}' nicht gefunden")
                continue

            label_id = result["id"]
            lkey = label_key(label_id)
            add_label(network, label_id, result.get("title", label_name),
                      source=f"scene:latam_{country}")

            data = api.get_label_releases(label_id, page=1)
            if not data or not data.get("releases"):
                continue

            seen = set()
            for rel in data["releases"]:
                a_name = rel.get("artist", "").strip()
                if not a_name or a_name.lower() in ("various", "various artists") or a_name in seen:
                    continue
                seen.add(a_name)

                exists = any(
                    av["name"].lower().strip() == a_name.lower().strip()
                    for av in network["artists"].values()
                )
                if exists:
                    continue

                a_result = api.search_artist(a_name)
                if not a_result:
                    continue

                a_genres = [g.lower() for g in a_result.get("genre", [])] + \
                           [s.lower() for s in a_result.get("style", [])]
                if is_blacklisted(a_genres, blacklist):
                    continue

                if not dry_run:
                    was_new = add_artist(
                        network, a_result["id"], a_result.get("title", a_name),
                        genres=a_genres, label_ids=[lkey],
                        source=f"scene:latam_{country}"
                    )
                    if was_new:
                        total_new += 1
                        print(f"  + {a_result.get('title', a_name)} (via {label_name})")

    if not dry_run:
        save_network(network)

    print(f"\n  → Gesamt: {total_new} neue Artists aus Szenen")
    return total_new


# ─────────────────────────────────────────────────
# MODE: --from-magazines
# ─────────────────────────────────────────────────

MAGAZINE_SOURCES = {
    "hardwax": {
        "name": "Hardwax",
        "url": "https://hardwax.com/downloads/minimal/",
        "alt_urls": [
            "https://hardwax.com/this-week/",
            "https://hardwax.com/downloads/deep-house/",
            "https://hardwax.com/downloads/techno/",
        ]
    },
    "phonica": {
        "name": "Phonica Records",
        "url": "https://www.phonicarecords.com/bestsellers",
    },
    "rushhour": {
        "name": "Rush Hour",
        "url": "https://www.rushhour.nl/genre/house-techno",
    },
    "juno": {
        "name": "Juno Recommends",
        "url": "https://www.juno.co.uk/bestsellers/minimal-deep-tech/thisweek/",
    },
}


def scrape_magazine_artists(session, source_key, source_info):
    """Scrape artist names from magazine/record store pages."""
    artists = set()

    urls = [source_info["url"]] + source_info.get("alt_urls", [])
    for url in urls:
        try:
            resp = session.get(url, headers={"User-Agent": RA_UA}, timeout=15)
            if resp.status_code != 200:
                continue

            # Generic extraction: look for patterns like "Artist - Title" or artist names in product listings
            # Each site has different HTML — we use generic heuristics

            # Pattern 1: "Artist — Title" or "Artist - Title"
            matches = re.findall(r'(?:by|artist["\s:]+|class="artist[^"]*"[^>]*>)\s*([A-Za-zÀ-ž][\w\s&.\']+)', resp.text)
            for m in matches:
                name = m.strip()
                if 2 < len(name) < 60 and not any(skip in name.lower() for skip in ("various", "vinyl", "digital", "pre-order", "buy", "cart", "listen")):
                    artists.add(name)

            # Pattern 2: JSON-LD structured data
            ld_matches = re.findall(r'"byArtist":\s*\{[^}]*"name":\s*"([^"]+)"', resp.text)
            artists.update(ld_matches)

            time.sleep(RATE_LIMIT_DELAY)

        except Exception as e:
            print(f"    ⚠ {source_info['name']} Error ({url}): {e}")

    return list(artists)


def expand_from_magazines(api, network, blacklist, dry_run=False, resume=False):
    """Scrape record stores and magazines for artist names."""
    print(f"\n{'='*60}")
    print(f"FROM-MAGAZINES: {len(MAGAZINE_SOURCES)} Quellen")
    print(f"{'='*60}\n")

    session = requests.Session()
    total_new = 0

    for src_key, src_info in MAGAZINE_SOURCES.items():
        print(f"\n📰 {src_info['name']}")

        artists = scrape_magazine_artists(session, src_key, src_info)
        print(f"  Gefunden: {len(artists)} potenzielle Artists")

        src_new = 0
        for a_name in artists:
            exists = any(
                av["name"].lower().strip() == a_name.lower().strip()
                for av in network["artists"].values()
            )
            if exists:
                continue

            a_result = api.search_artist(a_name)
            if not a_result:
                continue

            a_genres = [g.lower() for g in a_result.get("genre", [])] + \
                       [s.lower() for s in a_result.get("style", [])]
            if is_blacklisted(a_genres, blacklist):
                continue

            if not dry_run:
                was_new = add_artist(
                    network, a_result["id"], a_result.get("title", a_name),
                    genres=a_genres, source=f"mag:{src_key}"
                )
                if was_new:
                    src_new += 1
                    total_new += 1
                    print(f"  + {a_result.get('title', a_name)}")

        if src_new > 0 and not dry_run:
            save_network(network)

        print(f"  → {src_new} neue Artists")

    print(f"\n  → Gesamt: {total_new} neue Artists aus Magazinen")
    return total_new


# ─────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Valentina Netzwerk-Expansion")
    parser.add_argument("--from-labels", action="store_true", help="Labels aus reference_labels.txt")
    parser.add_argument("--from-artists", action="store_true", help="Artists aus reference_artists.txt")
    parser.add_argument("--from-favs", action="store_true", help="Labels aus favs.json")
    parser.add_argument("--from-clubs", action="store_true", help="Club-Lineups scrapen")
    parser.add_argument("--from-festivals", action="store_true", help="Festival-Lineups scrapen")
    parser.add_argument("--from-scenes", action="store_true", help="Szene-Quellen (Rominimal, Latam)")
    parser.add_argument("--from-magazines", action="store_true", help="Magazine/Stores scrapen")
    parser.add_argument("--all", action="store_true", help="Alle Modi ausführen")
    parser.add_argument("--dry-run", action="store_true", help="Änderungen nur anzeigen")
    parser.add_argument("--resume", action="store_true", help="Nach Abbruch weitermachen")
    args = parser.parse_args()

    if not any([args.from_labels, args.from_artists, args.from_favs, args.from_clubs,
                args.from_festivals, args.from_scenes, args.from_magazines, args.all]):
        parser.print_help()
        sys.exit(1)

    if not DISCOGS_TOKEN:
        print("❌ DISCOGS_TOKEN nicht in .env!")
        sys.exit(1)

    api = DiscogsAPI(DISCOGS_TOKEN)
    network = load_network()
    _, blacklist = load_genre_sets()

    initial_count = len(network["artists"])
    print(f"🌐 Netzwerk geladen: {initial_count} Artists, {len(network['labels'])} Labels")

    total_new = 0

    if args.from_labels or args.all:
        total_new += expand_from_labels(api, network, blacklist, args.dry_run, args.resume)

    if args.from_artists or args.all:
        total_new += expand_from_artists(api, network, blacklist, args.dry_run, args.resume)

    if args.from_favs or args.all:
        total_new += expand_from_favs(api, network, blacklist, args.dry_run, args.resume)

    if args.from_scenes or args.all:
        total_new += expand_from_scenes(api, network, blacklist, args.dry_run, args.resume)

    if args.from_clubs or args.all:
        total_new += expand_from_clubs(api, network, blacklist, args.dry_run, args.resume)

    if args.from_festivals or args.all:
        total_new += expand_from_festivals(api, network, blacklist, args.dry_run, args.resume)

    if args.from_magazines or args.all:
        total_new += expand_from_magazines(api, network, blacklist, args.dry_run, args.resume)

    # Final save
    if not args.dry_run and total_new > 0:
        save_network(network)
        clear_checkpoint()

    final_count = len(network["artists"])
    print(f"\n{'='*60}")
    print(f"📊 ERGEBNIS")
    print(f"  Vorher:  {initial_count} Artists")
    print(f"  Nachher: {final_count} Artists")
    print(f"  Neu:     {total_new}")
    print(f"  API Calls: {api.request_count}")
    print(f"{'='*60}")


if __name__ == "__main__":
    main()
