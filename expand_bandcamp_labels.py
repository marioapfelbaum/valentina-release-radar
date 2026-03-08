"""
Auto-expand Bandcamp labels from network_data.json.

Strategy:
1. Find labels in the network connected to seed artists (>= 2 connections)
2. Check if they have a Bandcamp presence (search or known mappings)
3. Add validated labels to reference_labels.txt

Usage:
    python3 expand_bandcamp_labels.py [--dry-run] [--min-connections 2]
"""

import json
import os
import re
import subprocess
import sys
import time
import argparse


def load_network(path="network_data.json"):
    """Load network data and extract label-artist connections."""
    with open(path) as f:
        data = json.load(f)

    artists = data.get("artists", {})
    labels = data.get("labels", {})

    # Build label → seed artist connections
    label_seeds = {}  # label_id → set of seed artist names
    label_all = {}    # label_id → set of all artist names

    for aid, artist in artists.items():
        name = artist.get("name", "")
        is_seed = artist.get("is_seed", False) or artist.get("depth", 99) == 0
        for lid in artist.get("label_ids", []):
            label_all.setdefault(lid, set()).add(name)
            if is_seed:
                label_seeds.setdefault(lid, set()).add(name)

    return labels, label_seeds, label_all


def load_existing_labels(path="reference_labels.txt"):
    """Load existing reference labels."""
    labels = set()
    if os.path.exists(path):
        with open(path) as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith("#"):
                    labels.add(line.lower())
    return labels


def load_blacklist(path="label_blacklist.txt"):
    """Load blacklisted labels."""
    blacklist = set()
    if os.path.exists(path):
        with open(path) as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith("#"):
                    blacklist.add(line.lower())
    return blacklist


def load_bandcamp_cache(path="bandcamp_labels.json"):
    """Load cached Bandcamp label mappings."""
    if os.path.exists(path):
        with open(path) as f:
            return json.load(f)
    return {}


def check_bandcamp_presence(label_name, cache):
    """Check if a label likely has Bandcamp presence.

    Uses cached data first, then tries a simple URL check.
    Returns (has_presence, slug_or_none)
    """
    # Check cache first
    label_lower = label_name.lower()
    for cached_name, cached_data in cache.items():
        if cached_name.lower() == label_lower:
            return True, cached_data.get("slug", cached_name)

    # Try to construct a Bandcamp URL from the label name
    # Common pattern: labelname.bandcamp.com
    slug = re.sub(r'[^a-z0-9]', '', label_name.lower())
    if not slug:
        return False, None

    # Try curl to check if the Bandcamp page exists
    url = f"https://{slug}.bandcamp.com"
    try:
        result = subprocess.run(
            ["curl", "-s", "-o", "/dev/null", "-w", "%{http_code}",
             "-L", "--max-time", "5", url],
            capture_output=True, text=True, timeout=10
        )
        status = result.stdout.strip()
        if status == "200":
            return True, slug
    except (subprocess.TimeoutExpired, Exception):
        pass

    # Try with hyphens instead of removing spaces
    slug_hyphen = re.sub(r'[^a-z0-9]+', '', label_name.lower().replace(' ', ''))
    if slug_hyphen != slug:
        url = f"https://{slug_hyphen}.bandcamp.com"
        try:
            result = subprocess.run(
                ["curl", "-s", "-o", "/dev/null", "-w", "%{http_code}",
                 "-L", "--max-time", "5", url],
                capture_output=True, text=True, timeout=10
            )
            status = result.stdout.strip()
            if status == "200":
                return True, slug_hyphen
        except (subprocess.TimeoutExpired, Exception):
            pass

    return False, None


def find_expansion_candidates(labels, label_seeds, label_all,
                               existing, blacklist, min_connections=2):
    """Find labels that should be added to reference_labels.txt."""
    candidates = []

    for lid, seed_artists in label_seeds.items():
        if len(seed_artists) < min_connections:
            continue

        label_info = labels.get(lid, {})
        label_name = label_info.get("name", "") if isinstance(label_info, dict) else str(label_info)

        if not label_name:
            continue

        # Skip if already in reference or blacklist
        if label_name.lower() in existing:
            continue
        if label_name.lower() in blacklist:
            continue

        # Skip obvious non-labels and media/compilations
        skip_patterns = [
            "not on label", "self-released", "self released",
            "unsigned", "independent", "private", "test",
            "unknown", "none", "n/a",
            # Media outlets, not labels
            "resident advisor", "fact magazine", "groove", "mixmag",
            "xlr8r", "tsugi", "spex", "little white earbuds",
            "dublab", "sgustok", "clubstar", "brawlcast",
            "kitchen spasm", "onlyhousemusic",
            # Major labels / distributors / compilations
            "ministry of sound", "zyx music", "kontor records",
            "virgin", "dmc", "promo only", "wagram",
            "541", "ith records", "defected",
            # Media / podcast / DJ mix series
            "electronic beats", "dj magazine", "bleep (2)",
            "cd pool", "white light mixes", "polystar",
            "bigcitybeats", "hed kandi", "cr2 records",
            "time (2)", "6-ryl sessions", "micromix",
            "more music and media", "n.e.w.s.", "field records (3)",
            # Generic / catch-all
            "[no label]", "no label", "fabric (2)", "trax (9)",
            "boomkat",  # shop, not label
        ]
        if any(p in label_name.lower() for p in skip_patterns):
            continue

        total_artists = len(label_all.get(lid, set()))

        candidates.append({
            "label_id": lid,
            "name": label_name,
            "seed_connections": len(seed_artists),
            "seed_artists": sorted(seed_artists),
            "total_artists": total_artists,
        })

    # Sort by seed connections (desc), then total artists (desc)
    candidates.sort(key=lambda c: (-c["seed_connections"], -c["total_artists"]))

    return candidates


def main():
    parser = argparse.ArgumentParser(description="Expand Bandcamp labels from network data")
    parser.add_argument("--dry-run", action="store_true",
                        help="Show candidates without modifying files")
    parser.add_argument("--min-connections", type=int, default=2,
                        help="Minimum seed-artist connections (default: 2)")
    parser.add_argument("--check-bandcamp", action="store_true",
                        help="Actually check Bandcamp URLs (slow, ~5s per label)")
    parser.add_argument("--max-add", type=int, default=200,
                        help="Maximum labels to add (default: 200)")
    args = parser.parse_args()

    print("Loading network data...")
    labels, label_seeds, label_all = load_network()
    existing = load_existing_labels()
    blacklist = load_blacklist()
    bc_cache = load_bandcamp_cache()

    print(f"  Network: {len(labels)} labels, {len(label_seeds)} with seed connections")
    print(f"  Existing reference labels: {len(existing)}")
    print(f"  Blacklisted: {len(blacklist)}")

    candidates = find_expansion_candidates(
        labels, label_seeds, label_all,
        existing, blacklist,
        min_connections=args.min_connections
    )

    print(f"\nFound {len(candidates)} expansion candidates (min {args.min_connections} seed connections):")
    print(f"{'Label':<40s} {'Seeds':>5s} {'Total':>5s} Seed Artists")
    print("-" * 100)

    validated = []
    for c in candidates[:args.max_add]:
        artists_str = ", ".join(c["seed_artists"][:5])
        if len(c["seed_artists"]) > 5:
            artists_str += f" (+{len(c['seed_artists'])-5} more)"

        bc_status = ""
        if args.check_bandcamp:
            has_bc, slug = check_bandcamp_presence(c["name"], bc_cache)
            bc_status = f" [BC: {slug}]" if has_bc else " [no BC]"
            if has_bc:
                validated.append(c)
            time.sleep(0.5)  # Be gentle
        else:
            validated.append(c)

        print(f"  {c['name']:<38s} {c['seed_connections']:>5d} {c['total_artists']:>5d} {artists_str}{bc_status}")

    if args.dry_run:
        print(f"\n[DRY RUN] Would add {len(validated)} labels to reference_labels.txt")
        return

    if not validated:
        print("\nNo labels to add.")
        return

    # Add to reference_labels.txt
    print(f"\nAdding {len(validated)} labels to reference_labels.txt...")
    with open("reference_labels.txt", "a") as f:
        f.write(f"\n# Auto-expanded from network ({time.strftime('%Y-%m-%d')})\n")
        for c in validated:
            f.write(f"{c['name']}\n")

    print(f"Done! reference_labels.txt now has {len(existing) + len(validated)} labels.")
    print("Run 'python3 fetch_multi.py --sources bandcamp' to fetch from new labels.")


if __name__ == "__main__":
    main()
