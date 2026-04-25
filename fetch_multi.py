#!/usr/bin/env python3
"""
fetch_multi.py — Valentina Multi-Source Release Fetcher
=======================================================
Fetches releases from Beatport, Bandcamp, Spotify, and Discogs,
deduplicates across sources, and writes unified releases.json.

Usage:
  python3 fetch_multi.py                                  # All sources, 6 months
  python3 fetch_multi.py --sources beatport               # Only Beatport
  python3 fetch_multi.py --sources beatport,bandcamp      # Beatport + Bandcamp
  python3 fetch_multi.py --months 3                       # Last 3 months
  python3 fetch_multi.py --browse-only                    # Only genre browse (fast)
  python3 fetch_multi.py --limit 5                        # Limit pages/labels (testing)
  python3 fetch_multi.py --resume                         # Resume from checkpoint

Sources: beatport, bandcamp, spotify, discogs
"""

import argparse
import hashlib
import json
import os
import signal
import sys
import time
from datetime import datetime, timedelta
from difflib import SequenceMatcher
from pathlib import Path

try:
    import requests
except ImportError:
    print("pip install requests")
    sys.exit(1)

# Load .env
_env_path = Path(__file__).parent / ".env"
if _env_path.exists():
    for line in _env_path.read_text().splitlines():
        line = line.strip()
        if line and not line.startswith("#") and "=" in line:
            k, v = line.split("=", 1)
            os.environ.setdefault(k.strip(), v.strip().strip('"').strip("'"))

from sources.beatport import BeatportFetcher
from sources.bandcamp import BandcampFetcher
from sources.spotify_source import SpotifyFetcher
from sources.discogs_source import DiscogsFetcher
from sources.genre_map import BEATPORT_GENRE_MAP, classify_genre

# Optional scrapers — import only when needed (require beautifulsoup4 / cloudscraper)
def _import_hardwax():
    from sources.hardwax import HardwaxFetcher
    return HardwaxFetcher

def _import_boomkat():
    from sources.boomkat import BoomkatFetcher
    return BoomkatFetcher

def _import_juno():
    from sources.juno import JunoFetcher
    return JunoFetcher

def _import_clone():
    from sources.clone import CloneFetcher
    return CloneFetcher

def _import_rushhour():
    from sources.rushhour import RushHourFetcher
    return RushHourFetcher

def _import_deejay():
    from sources.deejay import DeejayFetcher
    return DeejayFetcher

def _import_phonica():
    from sources.phonica import PhonicaFetcher
    return PhonicaFetcher

def _import_redeye():
    from sources.redeye import RedeyeFetcher
    return RedeyeFetcher

def _import_traxsource():
    from sources.traxsource import TraxsourceFetcher
    return TraxsourceFetcher

def _import_decks():
    from sources.decks import DecksFetcher
    return DecksFetcher

def _import_piccadilly():
    from sources.piccadilly import PiccadillyFetcher
    return PiccadillyFetcher

def _import_honestjons():
    from sources.honestjons import HonestJonsFetcher
    return HonestJonsFetcher

def _import_norman():
    from sources.norman import NormanFetcher
    return NormanFetcher

def _import_bandcamp_daily():
    from sources.bandcamp_daily import BandcampDailyFetcher
    return BandcampDailyFetcher

# --- CONFIG ---
OUTPUT_FILE = "releases.json"
NETWORK_FILE = "network_data.json"
CHECKPOINT_FILE = "fetch_multi_checkpoint.json"
LABEL_BLACKLIST_FILE = "label_blacklist.txt"
REFERENCE_LABELS_FILE = "reference_labels.txt"
REFERENCE_ARTISTS_FILE = "reference_artists.txt"

# Graceful shutdown
_shutdown = False
def _handle_signal(sig, frame):
    global _shutdown
    print("\n⚠ Shutdown requested, finishing current task...")
    _shutdown = True
signal.signal(signal.SIGINT, _handle_signal)
signal.signal(signal.SIGTERM, _handle_signal)


# ─── LABEL / ARTIST FILTERING ────────────────────────

def _load_filter_file(filename):
    """Load a text file of names (one per line), ignoring comments and blanks."""
    path = Path(__file__).parent / filename
    if not path.exists():
        return set()
    names = set()
    for line in path.read_text().splitlines():
        line = line.strip()
        if line and not line.startswith("#"):
            names.add(line.lower())
    return names


# Caches
_label_blacklist = None
_reference_labels = None
_network_artists = None


def _get_label_blacklist():
    """Load and cache label blacklist (distributors, spam labels)."""
    global _label_blacklist
    if _label_blacklist is None:
        _label_blacklist = _load_filter_file(LABEL_BLACKLIST_FILE)
        print(f"  📋 Label-Blacklist: {len(_label_blacklist)} Einträge")
    return _label_blacklist


def _get_reference_labels():
    """Load and cache reference labels (trusted/curated labels)."""
    global _reference_labels
    if _reference_labels is None:
        _reference_labels = _load_filter_file(REFERENCE_LABELS_FILE)
        print(f"  📋 Reference Labels: {len(_reference_labels)} Einträge")
    return _reference_labels


def _get_network_artist_names():
    """Load and cache artist names from network_data.json."""
    global _network_artists
    if _network_artists is None:
        _network_artists = set()
        path = Path(__file__).parent / NETWORK_FILE
        if path.exists():
            try:
                with open(path) as f:
                    data = json.load(f)
                for key, info in data.get("artists", {}).items():
                    name = info.get("name", "")
                    if name:
                        _network_artists.add(name.lower().strip())
            except (json.JSONDecodeError, IOError):
                pass
        # Also load reference_artists.txt
        ref_artists = _load_filter_file(REFERENCE_ARTISTS_FILE)
        _network_artists.update(ref_artists)
        print(f"  📋 Netzwerk-Artists: {len(_network_artists)} Einträge")
    return _network_artists


def _is_label_blacklisted(label):
    """Check if a label is in the blacklist (exact or fuzzy)."""
    if not label:
        return False
    label_lower = label.lower().strip()
    blacklist = _get_label_blacklist()

    # Exact match
    if label_lower in blacklist:
        return True

    # Check if blacklist entry is contained in label name (e.g. "LANDR" in "LANDR, Self-Released")
    for bl_entry in blacklist:
        if bl_entry in label_lower or label_lower in bl_entry:
            return True

    return False


def _is_reference_label(label):
    """Check if a label matches a reference label (fuzzy)."""
    if not label:
        return False
    label_lower = label.lower().strip()
    ref_labels = _get_reference_labels()

    # Exact match
    if label_lower in ref_labels:
        return True

    # Fuzzy: check if reference label is contained in label name
    for ref in ref_labels:
        if ref in label_lower or label_lower in ref:
            return True

    return False


def _has_network_artist(artist_str):
    """Check if any artist in the release is in our network.

    Uses fast set-based matching. Splits the artist string into
    individual names and checks each against the network set.
    Only considers names with 4+ characters to avoid false positives.
    """
    import re

    if not artist_str:
        return False

    network = _get_network_artist_names()

    # Split into individual artist names using common separators
    parts = re.split(r'\s*,\s*|\s*&\s*|\s+feat\.?\s+|\s+ft\.?\s+|\s*/\s*', artist_str.lower().strip())

    for part in parts:
        part = part.strip()
        if not part or len(part) < 4:
            continue

        # Direct set lookup (fast!)
        if part in network:
            return True

        # Also check without parenthetical suffixes like "(DE)", "(CA)"
        clean = re.sub(r'\s*\([^)]*\)\s*$', '', part).strip()
        if clean and len(clean) >= 4 and clean in network:
            return True

    return False


def filter_beatport_releases(releases):
    """Filter Beatport releases using label blacklist and network matching.

    A Beatport release is KEPT if:
      1. Its label is in reference_labels.txt, OR
      2. Its artist is in the network (network_data.json / reference_artists.txt)

    A Beatport release is REMOVED if:
      1. Its label is in label_blacklist.txt (distributors/spam), OR
      2. It doesn't match any of the above keep criteria

    Returns:
      (kept, removed_count)
    """
    if not releases:
        return [], 0

    kept = []
    removed_blacklist = 0
    removed_no_match = 0
    kept_label = 0
    kept_artist = 0

    for rel in releases:
        label = rel.get("label", "")
        artist = rel.get("artist", "")

        # Step 1: Check blacklist — instant rejection
        if _is_label_blacklisted(label):
            removed_blacklist += 1
            continue

        # Step 2: Check if label is in reference labels — instant keep
        if _is_reference_label(label):
            kept.append(rel)
            kept_label += 1
            continue

        # Step 3: Check if artist is in the network — keep
        if _has_network_artist(artist):
            # Extra filter: skip compilations with 8+ artists from non-reference labels
            artist_count = len(artist.split(","))
            if artist_count >= 8 and not _is_reference_label(label):
                removed_no_match += 1
                continue
            kept.append(rel)
            kept_artist += 1
            continue

        # Step 4: No match → remove
        removed_no_match += 1

    total_removed = removed_blacklist + removed_no_match
    print(f"  🔍 Beatport Label-Filter:")
    print(f"     ✓ Behalten: {len(kept)} ({kept_label} via Label, {kept_artist} via Artist)")
    print(f"     ✗ Entfernt: {total_removed} ({removed_blacklist} Blacklist, {removed_no_match} kein Match)")

    return kept, total_removed


# ─── DEDUPLICATION ────────────────────────────────────

def normalize_name(name):
    """Normalize artist/title for fuzzy matching."""
    if not name:
        return ""
    n = name.lower().strip()
    # Remove common suffixes
    for suffix in ["(original mix)", "(original)", "(remix)", " ep", " lp",
                   " feat.", " feat ", " ft.", " ft ", " & ", " and "]:
        n = n.replace(suffix, " ")
    # Remove parentheticals
    import re
    n = re.sub(r'\([^)]*\)', '', n)
    # Normalize whitespace
    n = re.sub(r'\s+', ' ', n).strip()
    return n


def similarity(a, b):
    """String similarity ratio (0.0 to 1.0)."""
    if not a or not b:
        return 0.0
    return SequenceMatcher(None, a, b).ratio()


def _normalize_catno(cat):
    """Normalize catalog number for fuzzy matching.
    Strips spaces, dashes, and trailing 'D' (digital variant)."""
    import re
    c = re.sub(r'[\s\-_]', '', cat.lower())
    c = re.sub(r'd$', '', c)
    return c


def are_duplicates(r1, r2):
    """Check if two releases are the same across sources."""
    # Same source + same source_id = obvious duplicate
    if r1.get("source") == r2.get("source"):
        return r1["id"] == r2["id"]

    # Strategy 1: Catalog number + label match (normalized)
    cat1 = (r1.get("catalog_number") or "").strip()
    cat2 = (r2.get("catalog_number") or "").strip()
    if cat1 and cat2 and _normalize_catno(cat1) == _normalize_catno(cat2):
        lab1 = normalize_name(r1.get("label", ""))
        lab2 = normalize_name(r2.get("label", ""))
        if lab1 and lab2 and similarity(lab1, lab2) > 0.7:
            return True

    # Strategy 2: Fuzzy artist + title match within date proximity
    artist1 = normalize_name(r1.get("artist", ""))
    artist2 = normalize_name(r2.get("artist", ""))
    title1 = normalize_name(r1.get("title", ""))
    title2 = normalize_name(r2.get("title", ""))

    if not artist1 or not artist2 or not title1 or not title2:
        return False

    # Check date proximity (within 60 days — shops list releases weeks after actual release date)
    try:
        d1 = datetime.strptime(r1["date"], "%Y-%m-%d")
        d2 = datetime.strptime(r2["date"], "%Y-%m-%d")
        if abs((d1 - d2).days) > 60:
            return False
    except (ValueError, KeyError):
        pass

    # Check artist + title similarity
    artist_sim = similarity(artist1, artist2)
    title_sim = similarity(title1, title2)

    if artist_sim > 0.85 and title_sim > 0.85:
        return True

    # Partial artist match (one contains the other) + high title match
    if (artist1 in artist2 or artist2 in artist1) and title_sim > 0.80:
        return True

    return False


# Source priority for merging (higher = preferred)
SOURCE_PRIORITY = {"hardwax": 6, "boomkat": 5, "clone": 5, "rushhour": 5,
                   "phonica": 5, "redeye": 5, "deejay": 5,
                   "traxsource": 5, "piccadilly": 5, "honestjons": 5,
                   "norman": 5, "decks": 5, "bandcamp_daily": 4,
                   "beatport": 4, "discogs": 4,
                   "bandcamp": 3, "juno": 3, "spotify": 2}


def verify_release_dates(releases, discogs_token):
    """Verify release dates for shop-sourced releases via Discogs search.

    For releases with date_verified=False that haven't been checked yet,
    searches Discogs for the actual release year. Marks releases older
    than 6 months as is_restock=True.
    """
    import requests as req

    to_check = [r for r in releases
                if not r.get("date_verified") and not r.get("discogs_year_checked")]

    if not to_check:
        return releases

    print(f"\n▶ Phase 7b: Discogs Date Verification")
    print(f"  Checking {len(to_check)} unverified releases...")

    headers = {"User-Agent": "ValentinaReleaseRadar/1.0"}
    cutoff_year = datetime.now().year
    cutoff_month = datetime.now().month - 6
    if cutoff_month <= 0:
        cutoff_year -= 1
        cutoff_month += 12
    cutoff_date = f"{cutoff_year}-{cutoff_month:02d}-01"

    checked = 0
    restocks = 0

    for r in to_check:
        artist = r.get("artist", "").strip()
        title = r.get("title", "").strip()
        if not artist or not title or artist.lower() == "various":
            r["discogs_year_checked"] = True
            continue

        query = f"{artist} {title}"
        try:
            resp = req.get(
                "https://api.discogs.com/database/search",
                params={"q": query, "type": "release", "token": discogs_token,
                        "per_page": 3},
                headers=headers, timeout=15
            )
            time.sleep(1.0)  # Discogs rate limit

            if resp.status_code == 200:
                results = resp.json().get("results", [])
                if results:
                    first = results[0]
                    year = first.get("year")
                    if year and isinstance(year, (int, str)):
                        year = int(year) if str(year).isdigit() else 0
                        if year > 0:
                            r["discogs_year"] = year
                            release_approx = f"{year}-01-01"
                            if release_approx < cutoff_date:
                                r["is_restock"] = True
                                restocks += 1

                    # Bonus: Discogs Search liefert auch styles+genre. Mitnehmen
                    # statt extra Phase 7c-Call zu brauchen.
                    discogs_styles = first.get("style", []) or []
                    discogs_genres = first.get("genre", []) or []
                    if discogs_styles or discogs_genres:
                        existing = {s.lower() for s in (r.get("styles") or [])}
                        merged = list(r.get("styles") or [])
                        for s in discogs_styles:
                            if s.lower() not in existing:
                                merged.append(s)
                                existing.add(s.lower())
                        if merged:
                            r["styles"] = merged
                        # Re-classify falls vorher Other/Electronic war
                        if r.get("genre", "") in ("Other", "Electronic", ""):
                            from sources.genre_map import classify_genre
                            new_genre = classify_genre(merged or [], discogs_genres)
                            if new_genre not in ("Other", "Electronic", ""):
                                r["genre"] = new_genre
                        r["style_enriched"] = True

            r["discogs_year_checked"] = True
            checked += 1

            if checked % 25 == 0:
                print(f"    ... {checked}/{len(to_check)} checked")

        except Exception:
            r["discogs_year_checked"] = True
            continue

    print(f"  ✓ Checked {checked} releases, {restocks} marked as restocks")
    return releases


def enrich_styles_via_discogs(releases, discogs_token, max_enrich=100):
    """Enrich releases that have no style tags via Discogs search.

    Phase 7c: For releases with empty styles or genre "Other"/"Electronic",
    look up the release on Discogs and copy style tags from there, then
    re-classify the genre.

    Args:
        releases: list of release dicts
        discogs_token: Discogs API token
        max_enrich: max releases to enrich per run (default 100, ~2 min)

    Returns:
        modified releases list
    """
    import requests as req

    # Find candidates: no styles OR genre is Other/Electronic, not already enriched
    candidates = [
        r for r in releases
        if not r.get("style_enriched")
        and (
            not r.get("styles")
            or r.get("genre", "") in ("Other", "Electronic", "")
        )
    ]

    if not candidates:
        print(f"\n▶ Phase 7c: Style Enrichment via Discogs")
        print(f"  No candidates for style enrichment.")
        return releases

    # Prioritize by quality_score (higher = more relevant), then by source priority
    candidates.sort(
        key=lambda r: (r.get("quality_score", 0), SOURCE_PRIORITY.get(r.get("source", ""), 0)),
        reverse=True
    )

    to_enrich = candidates[:max_enrich]

    print(f"\n▶ Phase 7c: Style Enrichment via Discogs")
    print(f"  Candidates: {len(candidates)} releases without styles/genre")
    print(f"  Enriching:  {min(len(candidates), max_enrich)} (max {max_enrich}/run)")

    headers = {"User-Agent": "ValentinaReleaseRadar/1.0"}
    enriched = 0
    reclassified = 0

    for r in to_enrich:
        if _shutdown:
            break

        artist = r.get("artist", "").strip()
        title = r.get("title", "").strip()

        # Skip releases without enough info to search
        if not artist or not title or artist.lower() == "various":
            r["style_enriched"] = True
            continue

        query = f"{artist} {title}"
        try:
            resp = req.get(
                "https://api.discogs.com/database/search",
                params={
                    "q": query,
                    "type": "release",
                    "token": discogs_token,
                    "per_page": 3,
                },
                headers=headers,
                timeout=15,
            )
            time.sleep(1.0)  # Discogs rate limit: 1 req/sec

            if resp.status_code == 200:
                results = resp.json().get("results", [])
                if results:
                    discogs_styles = results[0].get("style", [])
                    if discogs_styles:
                        # Only fill empty styles, don't overwrite existing
                        if not r.get("styles"):
                            r["styles"] = discogs_styles
                        else:
                            # Merge: add new styles not already present
                            existing_lower = {s.lower() for s in r["styles"]}
                            for s in discogs_styles:
                                if s.lower() not in existing_lower:
                                    r["styles"].append(s)

                        # Re-classify genre from enriched styles
                        old_genre = r.get("genre", "Other")
                        new_genre = classify_genre(r["styles"])
                        if new_genre not in ("Other", "Electronic") and new_genre != old_genre:
                            r["genre"] = new_genre
                            reclassified += 1

                        enriched += 1

            r["style_enriched"] = True

            if (enriched + 1) % 25 == 0:
                print(f"    ... {enriched} enriched so far")

        except Exception:
            r["style_enriched"] = True
            continue

    print(f"  ✓ Enriched {enriched}/{len(to_enrich)} releases with Discogs styles")
    print(f"  ✓ Reclassified {reclassified} releases to specific genres")
    return releases


def merge_duplicates(releases):
    """Deduplicate releases across sources. Returns list of unique releases."""
    if not releases:
        return []

    # Sort by source priority (preferred sources first)
    releases.sort(key=lambda r: SOURCE_PRIORITY.get(r.get("source", ""), 0), reverse=True)

    unique = []
    for rel in releases:
        is_dup = False
        for existing in unique:
            if are_duplicates(rel, existing):
                # Merge: enrich existing with data from duplicate
                _merge_fields(existing, rel)
                is_dup = True
                break
        if not is_dup:
            unique.append(rel)

    return unique


def _merge_fields(primary, secondary):
    """Enrich primary release with missing data from secondary."""
    # Add source URLs
    if "source_urls" not in primary:
        primary["source_urls"] = {}
    primary["source_urls"][primary["source"]] = primary.get("source_url", "")
    primary["source_urls"][secondary["source"]] = secondary.get("source_url", "")

    # Fill missing fields from secondary
    for field in ["bpm", "catalog_number", "duration", "label", "discogs_url"]:
        if not primary.get(field) and secondary.get(field):
            primary[field] = secondary[field]

    # Merge styles
    if secondary.get("styles"):
        existing_styles = set(s.lower() for s in (primary.get("styles") or []))
        for s in secondary["styles"]:
            if s.lower() not in existing_styles:
                primary.setdefault("styles", []).append(s)

    # Keep discogs_url if from discogs
    if secondary.get("source") == "discogs" and secondary.get("discogs_url"):
        primary["discogs_url"] = secondary["discogs_url"]
    if secondary.get("source") == "discogs" and secondary.get("discogs_release_id"):
        primary["discogs_release_id"] = secondary["discogs_release_id"]


# ─── CHECKPOINT ────────────────────────────────────────

def load_checkpoint():
    path = Path(__file__).parent / CHECKPOINT_FILE
    if path.exists():
        try:
            with open(path) as f:
                return json.load(f)
        except (json.JSONDecodeError, IOError):
            pass
    return None


def save_checkpoint(data):
    path = Path(__file__).parent / CHECKPOINT_FILE
    tmp = path.with_suffix(".tmp")
    with open(tmp, "w") as f:
        json.dump(data, f, ensure_ascii=False)
    os.replace(tmp, path)


def clear_checkpoint():
    path = Path(__file__).parent / CHECKPOINT_FILE
    if path.exists():
        os.remove(path)


# ─── LOAD / SAVE RELEASES ────────────────────────────

def load_existing_releases():
    """Load existing releases.json."""
    path = Path(__file__).parent / OUTPUT_FILE
    if path.exists():
        try:
            with open(path) as f:
                data = json.load(f)
            return data if isinstance(data, list) else []
        except (json.JSONDecodeError, IOError):
            pass
    return []


def save_releases(releases):
    """Save releases.json atomically, sorted by date desc."""
    releases.sort(key=lambda r: r.get("date", ""), reverse=True)
    path = Path(__file__).parent / OUTPUT_FILE
    tmp = path.with_suffix(".tmp")
    with open(tmp, "w") as f:
        json.dump(releases, f, ensure_ascii=False, indent=2)
    os.replace(tmp, path)
    print(f"\n✓ Saved {len(releases)} releases to {OUTPUT_FILE}")


def load_network_artists(spotify_only=False):
    """Load artist list from network_data.json for per-artist fetching.

    When spotify_only=True, applies strict filtering:
    - Seed artists: always included
    - Depth 0 non-seeds: included if they have 'Electronic' in discogs_genres
    - Depth 1+: included only if 'Electronic' in discogs_genres
    - Artists with no discogs_genres: included only if is_seed
    This prevents non-electronic artists (Rock, Pop, etc.) from polluting Spotify results.
    """
    path = Path(__file__).parent / NETWORK_FILE
    if not path.exists():
        return []
    try:
        with open(path) as f:
            data = json.load(f)
        artists = []
        skipped = 0
        for key, info in data.get("artists", {}).items():
            name = info.get("name", "")
            if not name:
                continue

            if spotify_only:
                is_seed = info.get("is_seed", False)
                discogs_genres = info.get("discogs_genres", [])
                has_electronic = "Electronic" in discogs_genres

                # Seeds always pass
                if not is_seed:
                    # Non-seeds need 'Electronic' in their Discogs genres
                    if not has_electronic:
                        skipped += 1
                        continue

            artists.append({
                "name": name,
                "spotify_id": info.get("spotify_id"),
                "discogs_id": info.get("discogs_id"),
            })

        if spotify_only and skipped:
            print(f"  Spotify filter: {skipped} non-electronic artists removed, {len(artists)} remain")
        return artists
    except (json.JSONDecodeError, IOError):
        return []


# ─── MAIN ORCHESTRATOR ────────────────────────────────

def run(args):
    global _shutdown

    cutoff = datetime.now() - timedelta(days=args.months * 30)
    sources = [s.strip().lower() for s in args.sources.split(",")]
    all_new_releases = []

    print(f"═══ Valentina Multi-Source Release Fetcher ═══")
    print(f"  Sources: {', '.join(sources)}")
    print(f"  Window:  {args.months} months (since {cutoff.strftime('%Y-%m-%d')})")
    print(f"  Mode:    {'browse only' if args.browse_only else 'browse + per-artist'}")
    print()

    # ─── Phase 1: Beatport Genre Browse ───────────────
    if "beatport" in sources and not _shutdown:
        print("▶ Phase 1: Beatport Genre Browse")
        bp = BeatportFetcher(rate_limit=2.0)
        bp_releases = bp.fetch_all_genres(
            cutoff, max_pages=args.limit or 3
        )

        # Apply label filter: only keep releases from reference labels / network artists
        bp_filtered, bp_removed = filter_beatport_releases(bp_releases)
        all_new_releases.extend(bp_filtered)
        print()

        # Checkpoint after Beatport
        save_checkpoint({
            "phase": "beatport_done",
            "releases_count": len(all_new_releases),
            "timestamp": datetime.now().isoformat(),
        })

    # ─── Phase 2: Bandcamp Label Browse ───────────────
    if "bandcamp" in sources and not _shutdown:
        print("▶ Phase 2: Bandcamp Label Browse")
        bc = BandcampFetcher(rate_limit=2.0)
        bc_releases = bc.fetch_all_labels(
            cutoff, max_labels=args.limit
        )
        all_new_releases.extend(bc_releases)
        print()

        save_checkpoint({
            "phase": "bandcamp_done",
            "releases_count": len(all_new_releases),
            "timestamp": datetime.now().isoformat(),
        })

    # ─── Phase 3: Discogs Label + Artist Fetch ────────
    if "discogs" in sources and not _shutdown:
        print("▶ Phase 3: Discogs Label + Artist Fetch")
        dc = DiscogsFetcher()
        if dc.available:
            # Load network data
            network_path = Path(__file__).parent / NETWORK_FILE
            if network_path.exists():
                with open(network_path) as f:
                    network_data = json.load(f)
                # Load reference lists
                ref_labels_list = list(_get_reference_labels())
                ref_artists_path = Path(__file__).parent / REFERENCE_ARTISTS_FILE
                ref_artists_list = []
                if ref_artists_path.exists():
                    ref_artists_list = [l.strip() for l in ref_artists_path.read_text().splitlines()
                                        if l.strip() and not l.startswith("#")]
                dc_releases = dc.fetch_for_network(
                    network_data, ref_labels_list, ref_artists_list,
                    cutoff, max_labels=args.limit or 50,
                    max_artists=args.limit or 30
                )
                all_new_releases.extend(dc_releases)
            else:
                print("  ⚠ No network_data.json found")
        print()

        save_checkpoint({
            "phase": "discogs_done",
            "releases_count": len(all_new_releases),
            "timestamp": datetime.now().isoformat(),
        })

    # ─── Phase 4: Spotify Per-Artist ──────────────────
    if "spotify" in sources and not args.browse_only and not _shutdown:
        print("▶ Phase 4: Spotify Per-Artist Fetch")
        sp = SpotifyFetcher()
        if sp.available:
            artists = load_network_artists(spotify_only=True)
            if artists:
                # Limit to first N artists for testing
                if args.limit:
                    artists = artists[:args.limit * 10]
                print(f"  Checking {len(artists)} artists...")
                sp_releases = sp.fetch_for_artists(artists, cutoff)
                all_new_releases.extend(sp_releases)
            else:
                print("  ⚠ No artists found in network_data.json")
        print()

    # ─── Phase 5: Curated Shop Scrapers ────────────────
    if "hardwax" in sources and not _shutdown:
        print("▶ Phase 5a: Hardwax Scraper")
        try:
            HardwaxFetcher = _import_hardwax()
            hw = HardwaxFetcher(rate_limit=2.0)
            hw_releases = hw.fetch_all(cutoff, max_pages=args.limit or 3)
            all_new_releases.extend(hw_releases)
            print(f"  ✓ Hardwax: {len(hw_releases)} releases")
        except ImportError as e:
            print(f"  ⚠ Hardwax skipped (missing dependency: {e})")
        except Exception as e:
            print(f"  ⚠ Hardwax error: {e}")
        print()

    if "boomkat" in sources and not _shutdown:
        print("▶ Phase 5b: Boomkat Scraper")
        try:
            BoomkatFetcher = _import_boomkat()
            bk = BoomkatFetcher(rate_limit=2.0)
            bk_releases = bk.fetch_all(cutoff, max_pages=args.limit or 3)
            all_new_releases.extend(bk_releases)
            print(f"  ✓ Boomkat: {len(bk_releases)} releases")
        except ImportError as e:
            print(f"  ⚠ Boomkat skipped (missing dependency: {e})")
        except Exception as e:
            print(f"  ⚠ Boomkat error: {e}")
        print()

    if "juno" in sources and not _shutdown:
        print("▶ Phase 5c: Juno Scraper")
        try:
            JunoFetcher = _import_juno()
            jn = JunoFetcher(rate_limit=2.0)
            jn_releases = jn.fetch_all(cutoff, max_pages=args.limit or 3)
            all_new_releases.extend(jn_releases)
            print(f"  ✓ Juno: {len(jn_releases)} releases")
        except ImportError as e:
            print(f"  ⚠ Juno skipped (missing dependency: {e})")
        except Exception as e:
            print(f"  ⚠ Juno error: {e}")
        print()

    if "clone" in sources and not _shutdown:
        print("▶ Phase 5d: Clone.nl RSS")
        try:
            CloneFetcher = _import_clone()
            cl = CloneFetcher(rate_limit=2.0)
            cl_releases = cl.fetch_all(cutoff)
            all_new_releases.extend(cl_releases)
            print(f"  ✓ Clone: {len(cl_releases)} releases")
        except ImportError as e:
            print(f"  ⚠ Clone skipped (missing dependency: {e})")
        except Exception as e:
            print(f"  ⚠ Clone error: {e}")
        print()

    if "rushhour" in sources and not _shutdown:
        print("▶ Phase 5e: Rush Hour RSS")
        try:
            RushHourFetcher = _import_rushhour()
            rh = RushHourFetcher(rate_limit=2.0)
            rh_releases = rh.fetch_all(cutoff)
            all_new_releases.extend(rh_releases)
            print(f"  ✓ Rush Hour: {len(rh_releases)} releases")
        except ImportError as e:
            print(f"  ⚠ Rush Hour skipped (missing dependency: {e})")
        except Exception as e:
            print(f"  ⚠ Rush Hour error: {e}")
        print()

    if "deejay" in sources and not _shutdown:
        print("▶ Phase 5f: Deejay.de Scraper")
        try:
            DeejayFetcher = _import_deejay()
            dj = DeejayFetcher(rate_limit=2.0)
            dj_releases = dj.fetch_all(cutoff, max_pages=args.limit or 2)
            all_new_releases.extend(dj_releases)
            print(f"  ✓ Deejay.de: {len(dj_releases)} releases")
        except ImportError as e:
            print(f"  ⚠ Deejay.de skipped (missing dependency: {e})")
        except Exception as e:
            print(f"  ⚠ Deejay.de error: {e}")
        print()

    if "phonica" in sources and not _shutdown:
        print("▶ Phase 5g: Phonica Records RSS")
        try:
            PhonicaFetcher = _import_phonica()
            pc = PhonicaFetcher(rate_limit=2.0)
            pc_releases = pc.fetch_all(cutoff)
            all_new_releases.extend(pc_releases)
            print(f"  ✓ Phonica: {len(pc_releases)} releases")
        except ImportError as e:
            print(f"  ⚠ Phonica skipped (missing dependency: {e})")
        except Exception as e:
            print(f"  ⚠ Phonica error: {e}")
        print()

    if "redeye" in sources and not _shutdown:
        print("▶ Phase 5h: Redeye Records Scraper")
        try:
            RedeyeFetcher = _import_redeye()
            ry = RedeyeFetcher(rate_limit=10.0)
            ry_releases = ry.fetch_all(cutoff, max_pages=args.limit or 1)
            all_new_releases.extend(ry_releases)
            print(f"  ✓ Redeye: {len(ry_releases)} releases")
        except ImportError as e:
            print(f"  ⚠ Redeye skipped (missing dependency: {e})")
        except Exception as e:
            print(f"  ⚠ Redeye error: {e}")
        print()

    # ─── Phase 5i-n: New Curated Shop Scrapers ────────
    if "traxsource" in sources and not _shutdown:
        print("▶ Phase 5i: Traxsource Scraper")
        try:
            TraxsourceFetcher = _import_traxsource()
            tx = TraxsourceFetcher(rate_limit=10.0)
            tx_releases = tx.fetch_all(cutoff, max_pages=args.limit or 2)
            all_new_releases.extend(tx_releases)
            print(f"  ✓ Traxsource: {len(tx_releases)} releases")
        except ImportError as e:
            print(f"  ⚠ Traxsource skipped (missing dependency: {e})")
        except Exception as e:
            print(f"  ⚠ Traxsource error: {e}")
        print()

    if "decks" in sources and not _shutdown:
        print("▶ Phase 5j: Decks.de Scraper")
        try:
            DecksFetcher = _import_decks()
            dk = DecksFetcher(rate_limit=2.0)
            dk_releases = dk.fetch_all(cutoff, max_pages=args.limit or 2)
            all_new_releases.extend(dk_releases)
            print(f"  ✓ Decks.de: {len(dk_releases)} releases")
        except ImportError as e:
            print(f"  ⚠ Decks.de skipped (missing dependency: {e})")
        except Exception as e:
            print(f"  ⚠ Decks.de error: {e}")
        print()

    if "piccadilly" in sources and not _shutdown:
        print("▶ Phase 5k: Piccadilly Records Scraper")
        try:
            PiccadillyFetcher = _import_piccadilly()
            pc = PiccadillyFetcher(rate_limit=2.0)
            pc_releases = pc.fetch_all(cutoff)
            all_new_releases.extend(pc_releases)
            print(f"  ✓ Piccadilly: {len(pc_releases)} releases")
        except ImportError as e:
            print(f"  ⚠ Piccadilly skipped (missing dependency: {e})")
        except Exception as e:
            print(f"  ⚠ Piccadilly error: {e}")
        print()

    if "honestjons" in sources and not _shutdown:
        print("▶ Phase 5l: Honest Jon's Scraper")
        try:
            HonestJonsFetcher = _import_honestjons()
            hj = HonestJonsFetcher(rate_limit=2.0)
            hj_releases = hj.fetch_all(cutoff, max_pages=args.limit or 2)
            all_new_releases.extend(hj_releases)
            print(f"  ✓ Honest Jon's: {len(hj_releases)} releases")
        except ImportError as e:
            print(f"  ⚠ Honest Jon's skipped (missing dependency: {e})")
        except Exception as e:
            print(f"  ⚠ Honest Jon's error: {e}")
        print()

    if "norman" in sources and not _shutdown:
        print("▶ Phase 5m: Norman Records Scraper")
        try:
            NormanFetcher = _import_norman()
            nm = NormanFetcher(rate_limit=3.0)
            nm_releases = nm.fetch_all(cutoff, max_pages=args.limit or 4)
            all_new_releases.extend(nm_releases)
            print(f"  ✓ Norman: {len(nm_releases)} releases")
        except ImportError as e:
            print(f"  ⚠ Norman skipped (missing dependency: {e})")
        except Exception as e:
            print(f"  ⚠ Norman error: {e}")
        print()

    if "bandcamp_daily" in sources and not _shutdown:
        print("▶ Phase 5n: Bandcamp Daily Scraper")
        try:
            BandcampDailyFetcher = _import_bandcamp_daily()
            bd = BandcampDailyFetcher(rate_limit=3.0)
            bd_releases = bd.fetch_all(cutoff)
            all_new_releases.extend(bd_releases)
            print(f"  ✓ Bandcamp Daily: {len(bd_releases)} releases")
        except ImportError as e:
            print(f"  ⚠ Bandcamp Daily skipped (missing dependency: {e})")
        except Exception as e:
            print(f"  ⚠ Bandcamp Daily error: {e}")
        print()

    # ─── Phase 6: Deduplicate ─────────────────────────
    if not _shutdown:
        print(f"▶ Phase 6: Deduplication")
        print(f"  Before: {len(all_new_releases)} releases")
        unique_new = merge_duplicates(all_new_releases)
        print(f"  After:  {len(unique_new)} unique releases")
        print()

    # ─── Phase 7: Merge with existing ─────────────────
    if not _shutdown:
        print(f"▶ Phase 7: Merge with existing releases.json")
        existing = load_existing_releases()

        # Tag existing releases with source/source_urls if missing
        for r in existing:
            if "source" not in r:
                r["source"] = "discogs"
            if "source_url" not in r:
                r["source_url"] = r.get("discogs_url", "")
            if "source_urls" not in r:
                r["source_urls"] = {r["source"]: r.get("source_url", "")}

        print(f"  Existing: {len(existing)} releases")
        print(f"  New:      {len(unique_new)} releases")

        # Merge: add new releases, deduplicate against existing
        combined = existing + unique_new
        final = merge_duplicates(combined)

        # Cap future dates: releases with dates > today+14 get capped to today
        today_str = datetime.now().strftime("%Y-%m-%d")
        max_date = (datetime.now() + timedelta(days=14)).strftime("%Y-%m-%d")
        capped = 0
        for r in final:
            if r.get("date", "") > max_date:
                r["date"] = today_str
                capped += 1
        if capped:
            print(f"  ⚠ Capped {capped} releases with future dates to {today_str}")

        print(f"  Final:    {len(final)} releases (net +{len(final) - len(existing)})")

        # Discogs year verification for unverified releases
        discogs_token = os.environ.get("DISCOGS_TOKEN", "")
        if discogs_token:
            final = verify_release_dates(final, discogs_token)
        else:
            print("  ⚠ DISCOGS_TOKEN not set, skipping date verification")

        # Style enrichment via Discogs (Phase 7c)
        if discogs_token and not getattr(args, 'no_enrich_styles', False):
            final = enrich_styles_via_discogs(final, discogs_token,
                                              max_enrich=getattr(args, 'enrich_limit', 200))
        elif not discogs_token:
            print("  ⚠ DISCOGS_TOKEN not set, skipping style enrichment")
        else:
            print("  ⏭ Style enrichment skipped (--no-enrich-styles)")

        # Phase 7d: Skip non-electronic Releases (Hardcore/Pop Rock/Punk/Trap...)
        # Greift erst NACH Enrichment, damit Borderline-Cases korrekt klassifiziert sind.
        try:
            from sources.genre_map import should_skip_release
            before = len(final)
            final = [r for r in final
                     if not should_skip_release(r.get("styles") or [],
                                                [r.get("genre", "")] if r.get("genre") else None)]
            skipped = before - len(final)
            print(f"\n▶ Phase 7d: Skip non-electronic releases")
            print(f"  Removed {skipped} releases (Hardcore/Punk/Pop Rock/Trap/etc.)")
        except ImportError:
            pass

        # Quality scoring
        try:
            from quality_score import score_all_releases, print_score_summary
            print(f"\n▶ Phase 8: Quality Scoring")
            final = score_all_releases(final, str(Path(__file__).parent))
            print_score_summary(final)
        except ImportError:
            print("  ⚠ quality_score.py not found, skipping scoring")

        save_releases(final)

    # Cleanup
    clear_checkpoint()
    print(f"\n═══ Done! ═══")


def main():
    parser = argparse.ArgumentParser(description="Valentina Multi-Source Release Fetcher")
    parser.add_argument("--sources", default="bandcamp,spotify,discogs,hardwax,boomkat,juno,clone,rushhour,deejay,phonica,redeye,traxsource,decks,piccadilly,honestjons,norman,bandcamp_daily",
                        help="Comma-separated sources (default: all active)")
    parser.add_argument("--months", type=int, default=6,
                        help="Look back N months (default: 6)")
    parser.add_argument("--browse-only", action="store_true",
                        help="Only do genre/label browse, skip per-artist fetch")
    parser.add_argument("--limit", type=int, default=None,
                        help="Limit pages/labels/artists (for testing)")
    parser.add_argument("--resume", action="store_true",
                        help="Resume from checkpoint")
    parser.add_argument("--no-enrich-styles", action="store_true",
                        help="Skip Phase 7c Discogs style enrichment")
    parser.add_argument("--enrich-limit", type=int, default=200,
                        help="Max releases to enrich per run (default: 200)")
    args = parser.parse_args()
    run(args)


if __name__ == "__main__":
    main()
