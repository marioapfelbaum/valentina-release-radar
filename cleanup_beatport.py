#!/usr/bin/env python3
"""One-time cleanup: remove spam Beatport releases from releases.json.

Uses optimized matching:
  - Blacklist: remove releases from known distributors/spam labels
  - Whitelist: keep releases from reference labels
  - Network match: keep releases where artist is in the network (set-based, fast)
"""

import json
import re
import os
from pathlib import Path
from collections import Counter


def load_filter(filename):
    path = Path(__file__).parent / filename
    if not path.exists():
        return set()
    return {line.strip().lower() for line in path.read_text().splitlines()
            if line.strip() and not line.strip().startswith('#')}


blacklist = load_filter('label_blacklist.txt')
ref_labels = load_filter('reference_labels.txt')
ref_artists_raw = load_filter('reference_artists.txt')

# Load network artists
network_artists_raw = set()
try:
    with open(Path(__file__).parent / 'network_data.json') as f:
        data = json.load(f)
    for key, info in data.get('artists', {}).items():
        name = info.get('name', '')
        if name:
            network_artists_raw.add(name.lower().strip())
except Exception:
    pass
network_artists_raw.update(ref_artists_raw)

# Filter out short names (< 4 chars) to avoid false positives
network_artists = {a for a in network_artists_raw if len(a) >= 4}

print(f'Blacklist: {len(blacklist)} entries')
print(f'Reference labels: {len(ref_labels)} entries')
print(f'Network artists (4+ chars): {len(network_artists)} entries')


def is_label_blacklisted(label):
    label_lower = label.lower().strip()
    if label_lower in blacklist:
        return True
    for bl in blacklist:
        if bl in label_lower or label_lower in bl:
            return True
    return False


def is_reference_label(label):
    label_lower = label.lower().strip()
    if label_lower in ref_labels:
        return True
    for ref in ref_labels:
        if ref in label_lower or label_lower in ref:
            return True
    return False


def has_network_artist(artist_str):
    """Fast set-based matching for artist names.

    Splits the artist string into individual names and checks
    each against the network set. No regex needed.
    """
    if not artist_str:
        return False

    # Split into individual artist names using common separators
    parts = re.split(r'\s*,\s*|\s*&\s*|\s+feat\.?\s+|\s+ft\.?\s+|\s*/\s*', artist_str.lower().strip())

    for part in parts:
        part = part.strip()
        if not part:
            continue

        # Direct set lookup (fast!)
        if part in network_artists:
            return True

        # Also check without parenthetical suffixes like "(DE)", "(CA)"
        clean = re.sub(r'\s*\([^)]*\)\s*$', '', part).strip()
        if clean and clean in network_artists:
            return True

    return False


# Load releases
releases_path = Path(__file__).parent / 'releases.json'
with open(releases_path) as f:
    releases = json.load(f)

print(f'\nTotal releases before cleanup: {len(releases)}')

# Filter
kept = []
removed_blacklist = 0
removed_no_match = 0
kept_label = 0
kept_artist = 0

for r in releases:
    if r.get('source') != 'beatport':
        kept.append(r)
        continue

    label = r.get('label', '')
    artist = r.get('artist', '')

    # Step 1: Blacklist check
    if is_label_blacklisted(label):
        removed_blacklist += 1
        continue

    # Step 2: Reference label check
    if is_reference_label(label):
        kept.append(r)
        kept_label += 1
        continue

    # Step 3: Network artist check
    if has_network_artist(artist):
        # Extra filter: skip compilations with 8+ artists from non-reference labels
        artist_count = len(artist.split(','))
        if artist_count >= 8 and not is_reference_label(label):
            removed_no_match += 1
            continue
        kept.append(r)
        kept_artist += 1
        continue

    # Step 4: No match -> remove
    removed_no_match += 1

total_removed = removed_blacklist + removed_no_match
print(f'\nFilter results:')
print(f'  Kept:    {kept_label + kept_artist} ({kept_label} via label, {kept_artist} via artist)')
print(f'  Removed: {total_removed} ({removed_blacklist} blacklist, {removed_no_match} no match)')

# Show surviving Beatport releases
bp_kept = [r for r in kept if r.get('source') == 'beatport']
print(f'\nSurviving Beatport releases: {len(bp_kept)}')
for r in bp_kept[:40]:
    print(f'  {r["date"]} | {r["artist"]} -- {r["title"]} [{r.get("label","")}]')

if len(bp_kept) > 40:
    print(f'  ... and {len(bp_kept) - 40} more')

# Save
kept.sort(key=lambda r: r.get('date', ''), reverse=True)
tmp = releases_path.with_suffix('.tmp')
with open(tmp, 'w') as f:
    json.dump(kept, f, ensure_ascii=False, indent=2)
os.replace(tmp, releases_path)
print(f'\nSaved {len(kept)} releases to releases.json')

# Count by source
sources = Counter(r.get('source', '?') for r in kept)
print(f'\nReleases by source:')
for s, c in sources.most_common():
    print(f'  {s}: {c}')
