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
from sources.genre_map import BEATPORT_GENRE_MAP

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


def are_duplicates(r1, r2):
    """Check if two releases are the same across sources."""
    # Same source + same source_id = obvious duplicate
    if r1.get("source") == r2.get("source"):
        return r1["id"] == r2["id"]

    # Strategy 1: Catalog number + label match
    cat1 = (r1.get("catalog_number") or "").strip()
    cat2 = (r2.get("catalog_number") or "").strip()
    if cat1 and cat2 and cat1.lower() == cat2.lower():
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

    # Check date proximity (within 30 days)
    try:
        d1 = datetime.strptime(r1["date"], "%Y-%m-%d")
        d2 = datetime.strptime(r2["date"], "%Y-%m-%d")
        if abs((d1 - d2).days) > 30:
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
SOURCE_PRIORITY = {"beatport": 4, "discogs": 3, "bandcamp": 2, "spotify": 1}


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


def load_network_artists():
    """Load artist list from network_data.json for per-artist fetching."""
    path = Path(__file__).parent / NETWORK_FILE
    if not path.exists():
        return []
    try:
        with open(path) as f:
            data = json.load(f)
        artists = []
        for key, info in data.get("artists", {}).items():
            name = info.get("name", "")
            if name:
                artists.append({
                    "name": name,
                    "spotify_id": info.get("spotify_id"),
                    "discogs_id": info.get("discogs_id"),
                })
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

    # ─── Phase 3: Spotify Per-Artist ──────────────────
    if "spotify" in sources and not args.browse_only and not _shutdown:
        print("▶ Phase 3: Spotify Per-Artist Fetch")
        sp = SpotifyFetcher()
        if sp.available:
            artists = load_network_artists()
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

    # ─── Phase 4: Deduplicate ─────────────────────────
    if not _shutdown:
        print(f"▶ Phase 4: Deduplication")
        print(f"  Before: {len(all_new_releases)} releases")
        unique_new = merge_duplicates(all_new_releases)
        print(f"  After:  {len(unique_new)} unique releases")
        print()

    # ─── Phase 5: Merge with existing ─────────────────
    if not _shutdown:
        print(f"▶ Phase 5: Merge with existing releases.json")
        existing = load_existing_releases()

        # Tag existing Discogs releases with source if missing
        for r in existing:
            if "source" not in r:
                r["source"] = "discogs"
            if "source_url" not in r:
                r["source_url"] = r.get("discogs_url", "")

        print(f"  Existing: {len(existing)} releases")
        print(f"  New:      {len(unique_new)} releases")

        # Merge: add new releases, deduplicate against existing
        combined = existing + unique_new
        final = merge_duplicates(combined)
        print(f"  Final:    {len(final)} releases (net +{len(final) - len(existing)})")

        save_releases(final)

    # Cleanup
    clear_checkpoint()
    print(f"\n═══ Done! ═══")


def main():
    parser = argparse.ArgumentParser(description="Valentina Multi-Source Release Fetcher")
    parser.add_argument("--sources", default="bandcamp,spotify",
                        help="Comma-separated sources: beatport,bandcamp,spotify,discogs (default: bandcamp,spotify)")
    parser.add_argument("--months", type=int, default=6,
                        help="Look back N months (default: 6)")
    parser.add_argument("--browse-only", action="store_true",
                        help="Only do genre/label browse, skip per-artist fetch")
    parser.add_argument("--limit", type=int, default=None,
                        help="Limit pages/labels/artists (for testing)")
    parser.add_argument("--resume", action="store_true",
                        help="Resume from checkpoint")
    args = parser.parse_args()
    run(args)


if __name__ == "__main__":
    main()
