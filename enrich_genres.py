#!/usr/bin/env python3
"""
enrich_genres.py — Valentina Genre Enrichment + Cleanup
========================================================
Holt Discogs-Genres für alle Artists im Netzwerk über Release-Suche
und entfernt Artists, die nicht zur Szene passen.

Usage:
  python enrich_genres.py                  # Enrich + Clean
  python enrich_genres.py --enrich-only    # Nur Genres holen
  python enrich_genres.py --clean-only     # Nur aufräumen (Genres müssen vorhanden sein)
  python enrich_genres.py --resume         # Fortsetzen nach Abbruch
  python enrich_genres.py --limit 100      # Nur erste 100 Artists enrichen
"""

import json
import os
import sys
import time
import argparse
from pathlib import Path
from datetime import datetime
from collections import Counter

try:
    import requests
except ImportError:
    print("pip install requests")
    sys.exit(1)

# ── Paths ──
BASE_DIR = Path(__file__).parent
NETWORK_FILE = BASE_DIR / "network_data.json"
BACKUP_FILE = BASE_DIR / "network_data.pre_enrich.json"
CHECKPOINT_FILE = BASE_DIR / ".enrich_checkpoint.json"

# ── Config ──
RATE_LIMIT_DELAY = 1.1  # Discogs: 60 req/min
SAVE_INTERVAL = 50

# Styles/genres that indicate the artist belongs in the minimal/electronic scene.
# If ANY of these appear in an artist's styles, they're relevant.
SCENE_STYLES = {
    "minimal", "microhouse", "micro house", "minimal techno", "minimal house",
    "deep house", "tech house", "dub techno", "dub", "dub house",
    "house", "techno", "acid house", "acid", "electro",
    "ambient", "dark ambient", "downtempo", "experimental",
    "electronica", "idm", "leftfield", "glitch",
    "disco", "nu-disco", "nu disco", "italo-disco", "italo disco",
    "progressive house", "afro house", "tribal house",
    "detroit techno", "chicago house", "balearic",
    "breaks", "breakbeat", "uk garage", "garage house",
    "trip hop", "abstract", "future jazz",
    "drum and bass", "jungle",
    "synthwave", "synth-pop",
    "industrial", "ebm", "noise",
    "trance", "progressive trance", "psy-trance",
}

# Top-level Discogs genres that are clearly NOT electronic
NON_ELECTRONIC_GENRES = {
    "rock", "pop", "hip hop", "classical", "jazz", "blues", "country",
    "folk", "metal", "punk", "reggae", "latin",
    "funk / soul",  # Discogs format
    "funk/soul",
    "stage & screen",
    "children's",
    "brass & military",
    "folk, world, & country",
    "non-music",
}

# ── Load env ──
def load_env():
    env_path = BASE_DIR / ".env"
    if env_path.exists():
        for line in env_path.read_text().splitlines():
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                k, v = line.split("=", 1)
                os.environ.setdefault(k.strip(), v.strip().strip('"').strip("'"))

load_env()
DISCOGS_TOKEN = os.getenv("DISCOGS_TOKEN") or os.getenv("DISCOGS_API_TOKEN")


# ── API ──
class DiscogsAPI:
    BASE = "https://api.discogs.com"

    def __init__(self, token):
        self.session = requests.Session()
        self.session.headers.update({
            "Authorization": f"Discogs token={token}",
            "User-Agent": "ValentinaEnrichGenres/1.0"
        })
        self.request_count = 0
        self.last_request = 0

    def _rate_limit(self):
        elapsed = time.time() - self.last_request
        if elapsed < RATE_LIMIT_DELAY:
            time.sleep(RATE_LIMIT_DELAY - elapsed)
        self.last_request = time.time()
        self.request_count += 1

    def search_releases(self, artist_name, per_page=10):
        """Search for releases by artist — returns genre+style data."""
        self._rate_limit()
        try:
            resp = self.session.get(
                f"{self.BASE}/database/search",
                params={"artist": artist_name, "type": "release", "per_page": per_page},
                timeout=15
            )
            if resp.status_code == 429:
                wait = int(resp.headers.get("Retry-After", 60))
                print(f" ⏳ Rate Limit, warte {wait}s...", end="", flush=True)
                time.sleep(wait + 1)
                return self.search_releases(artist_name, per_page)
            if resp.status_code == 200:
                return resp.json().get("results", [])
            return []
        except requests.exceptions.RequestException as e:
            print(f" ⚠ {e}", end="", flush=True)
            return []


# ── Data ──
def load_network():
    with open(NETWORK_FILE, "r", encoding="utf-8") as f:
        return json.load(f)


def save_network(network):
    network["metadata"]["last_updated"] = datetime.now().isoformat()
    network["metadata"]["artists_found"] = len(network["artists"])
    tmp = NETWORK_FILE.with_suffix(".tmp")
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(network, f, ensure_ascii=False, indent=2)
    os.replace(tmp, NETWORK_FILE)


def backup_network():
    import shutil
    if NETWORK_FILE.exists():
        shutil.copy2(NETWORK_FILE, BACKUP_FILE)
        return True
    return False


def load_checkpoint():
    if CHECKPOINT_FILE.exists():
        with open(CHECKPOINT_FILE) as f:
            return json.load(f)
    return {"enriched": []}


def save_checkpoint(cp):
    with open(CHECKPOINT_FILE, "w") as f:
        json.dump(cp, f)


def clear_checkpoint():
    if CHECKPOINT_FILE.exists():
        CHECKPOINT_FILE.unlink()


def load_lines(path):
    if not path.exists():
        return set()
    return {l.strip().lower() for l in path.read_text().splitlines() if l.strip()}


# ── Genre classification ──
# Discogs top-level genres that DISQUALIFY an artist (mainstream indicators).
# If an artist has ANY of these, they're not underground electronic.
# Exception: "Funk / Soul" and "Jazz" are tolerated (deep house/disco roots).
DISQUALIFYING_GENRES = {"pop", "rock", "hip hop", "classical", "latin",
                        "reggae", "stage & screen", "children's",
                        "brass & military", "non-music",
                        "folk, world, & country", "country", "blues", "metal"}


def classify_artist(genres, styles):
    """
    Classify an artist based on their Discogs genres + styles.
    Returns: 'electronic', 'non_electronic', or 'unknown'

    Conservative approach: only remove artists with ZERO electronic connection.
    """
    if not genres and not styles:
        return "unknown"

    genres_lower = {g.lower().strip() for g in genres}

    # If "Electronic" is present → keep (even Diplo/Madonna — their releases
    # will be filtered by genre in the UI anyway)
    has_electronic = any("electronic" in g for g in genres_lower)
    if has_electronic:
        return "electronic"

    # Check styles for electronic indicators
    all_tags = {t.lower().strip() for t in styles}
    for tag in all_tags:
        for ss in SCENE_STYLES:
            if ss in tag:
                return "electronic"

    # "Funk / Soul" or "Jazz" only → keep (deep house/disco/acid jazz roots)
    if genres_lower <= {"funk / soul", "funk/soul", "jazz"}:
        return "electronic"

    # No electronic connection at all → non_electronic
    return "non_electronic"


# ── Phase 1: Enrich genres via release search ──
def enrich_genres(network, api, resume=False, limit=0):
    """Fetch genres from Discogs release search for all artists."""
    artists = network["artists"]

    # Get artists that need enrichment (no genres yet)
    to_enrich = []
    for akey, adata in artists.items():
        if not adata.get("genres"):
            to_enrich.append((akey, adata.get("name", "?")))

    # Resume support
    enriched_set = set()
    if resume:
        cp = load_checkpoint()
        enriched_set = set(cp.get("enriched", []))
        to_enrich = [(k, n) for k, n in to_enrich if k not in enriched_set]

    if limit > 0:
        to_enrich = to_enrich[:limit]

    total = len(to_enrich)
    print(f"  {total} Artists zu enrichen ({len(enriched_set)} bereits erledigt)")

    if total == 0:
        return 0

    est_mins = int(total * RATE_LIMIT_DELAY / 60)
    print(f"  Geschätzte Dauer: ~{est_mins} Minuten")
    print()

    enriched = 0

    for i, (akey, name) in enumerate(to_enrich):
        pct = round((i + 1) / total * 100)
        print(f"  [{pct:3d}%] {i+1}/{total} — {name}", end="", flush=True)

        # Search for releases by this artist
        results = api.search_releases(name, per_page=10)

        if results:
            # Aggregate genres and styles from all results
            all_genres = set()
            all_styles = set()
            for r in results:
                for g in r.get("genre", []):
                    all_genres.add(g)
                for s in r.get("style", []):
                    all_styles.add(s)

            combined = list(set(
                [g.lower() for g in all_genres] +
                [s.lower() for s in all_styles]
            ))

            if combined:
                artists[akey]["genres"] = combined
                # Also store raw Discogs genres/styles separately for better filtering
                artists[akey]["discogs_genres"] = sorted(all_genres)
                artists[akey]["discogs_styles"] = sorted(all_styles)
                enriched += 1
                # Show classification
                cls = classify_artist(list(all_genres), list(all_styles))
                marker = {"electronic": "✓", "non_electronic": "✕",
                          "mainstream_electronic": "~", "unknown": "?"}[cls]
                top_styles = sorted(all_styles)[:3]
                print(f" — {marker} {', '.join(top_styles) if top_styles else ', '.join(sorted(all_genres)[:2])}")
            else:
                print(f" — keine Genres")
        else:
            print(f" — keine Releases gefunden")

        enriched_set.add(akey)

        # Save periodically
        if (i + 1) % SAVE_INTERVAL == 0:
            save_network(network)
            save_checkpoint({"enriched": list(enriched_set)})
            print(f"\n  💾 Checkpoint: {enriched}/{i+1} enriched, {api.request_count} API calls\n")

    # Final save
    save_network(network)
    save_checkpoint({"enriched": list(enriched_set)})

    print(f"\n  ✅ {enriched} Artists mit Genres angereichert ({api.request_count} API calls)")
    return enriched


# ── Phase 2: Clean non-electronic artists ──
def clean_network(network):
    """Remove artists that don't belong in the electronic scene."""
    artists = network["artists"]
    edges = network.get("edges", [])
    labels = network.get("labels", {})

    # Build protected set (seeds + reference artists)
    ref_artists = load_lines(BASE_DIR / "reference_artists.txt")
    protected = set()
    for akey, adata in artists.items():
        if adata.get("is_seed"):
            protected.add(akey)
        if adata.get("name", "").lower().strip() in ref_artists:
            protected.add(akey)

    before = len(artists)
    removed = []
    removed_keys = set()

    for akey in list(artists.keys()):
        if akey in protected:
            continue

        adata = artists[akey]
        discogs_genres = adata.get("discogs_genres", [])
        discogs_styles = adata.get("discogs_styles", [])

        # Use raw Discogs data if available, otherwise fall back to genres array
        if discogs_genres or discogs_styles:
            cls = classify_artist(discogs_genres, discogs_styles)
        elif adata.get("genres"):
            # genres array contains mixed genres+styles (lowercase)
            cls = classify_artist(adata["genres"], [])
        else:
            continue  # No data → keep

        if cls in ("non_electronic", "mainstream_electronic"):
            removed.append((adata.get("name", "?"), cls,
                            discogs_genres or adata.get("genres", [])))
            removed_keys.add(akey)
            del artists[akey]

    # Clean up edges
    network["edges"] = [
        e for e in edges
        if e.get("artist_id") not in removed_keys
    ]

    # Clean up label artist_ids
    for ldata in labels.values():
        if "artist_ids" in ldata:
            ldata["artist_ids"] = [
                a for a in ldata["artist_ids"] if a not in removed_keys
            ]

    after = len(artists)

    print(f"\n  ═══════════════════════════════════════")
    print(f"  CLEANUP ERGEBNIS:")
    print(f"  Vorher:   {before:,} Artists")
    print(f"  Nachher:  {after:,} Artists")
    print(f"  Entfernt: {before - after:,} Artists")
    print(f"  ═══════════════════════════════════════")

    if removed:
        # Group by classification
        non_elec = [(n, g) for n, c, g in removed if c == "non_electronic"]
        mainstream = [(n, g) for n, c, g in removed if c == "mainstream_electronic"]

        if non_elec:
            print(f"\n  Non-Electronic ({len(non_elec)}):")
            for name, genres in sorted(non_elec)[:20]:
                print(f"    ✕ {name:35s} [{', '.join(str(g) for g in genres[:3])}]")
            if len(non_elec) > 20:
                print(f"    ... und {len(non_elec) - 20} weitere")

        if mainstream:
            print(f"\n  Mainstream Electronic ({len(mainstream)}):")
            for name, genres in sorted(mainstream)[:20]:
                print(f"    ~ {name:35s} [{', '.join(str(g) for g in genres[:3])}]")
            if len(mainstream) > 20:
                print(f"    ... und {len(mainstream) - 20} weitere")

    # Genre stats of remaining
    style_counter = Counter()
    for adata in artists.values():
        for s in adata.get("discogs_styles", []):
            style_counter[s] += 1

    if style_counter:
        print(f"\n  Top Styles im bereinigten Netzwerk:")
        for s, c in style_counter.most_common(25):
            print(f"    {s:30s} {c:>5}")

    return before - after


# ── Main ──
def main():
    parser = argparse.ArgumentParser(description="Valentina Genre Enrichment + Cleanup")
    parser.add_argument("--enrich-only", action="store_true", help="Nur Genres holen")
    parser.add_argument("--clean-only", action="store_true", help="Nur aufräumen")
    parser.add_argument("--resume", action="store_true", help="Fortsetzen nach Abbruch")
    parser.add_argument("--limit", type=int, default=0, help="Limit Artists für Enrichment")
    args = parser.parse_args()

    print()
    print("╔═══════════════════════════════════════════════╗")
    print("║   VALENTINA — Genre Enrichment + Cleanup       ║")
    print("╚═══════════════════════════════════════════════╝")
    print()

    network = load_network()
    artists = network["artists"]
    print(f"  📂 Geladen: {len(artists):,} Artists")

    has_genres = sum(1 for a in artists.values() if a.get("genres"))
    print(f"  📊 Mit Genres: {has_genres:,} / {len(artists):,}")
    print()

    # Backup before changes
    if not args.clean_only:
        if backup_network():
            print(f"  💾 Backup: {BACKUP_FILE.name}")

    # Phase 1: Enrich
    if not args.clean_only:
        if not DISCOGS_TOKEN:
            print("  ❌ Kein DISCOGS_TOKEN in .env!")
            sys.exit(1)

        api = DiscogsAPI(DISCOGS_TOKEN)
        print(f"\n  ── PHASE 1: Genre Enrichment ──")
        enrich_genres(network, api, resume=args.resume, limit=args.limit)

    # Phase 2: Clean
    if not args.enrich_only:
        print(f"\n  ── PHASE 2: Cleanup ──")
        clean_network(network)
        save_network(network)
        clear_checkpoint()

    print(f"\n  ✅ Fertig!")
    print(f"  📊 Netzwerk: {len(network['artists']):,} Artists")


if __name__ == "__main__":
    main()
