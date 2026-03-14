#!/usr/bin/env python3
"""
refocus_network.py — Valentina Netzwerk-Refokussierung
======================================================
Bewertet und beschneidet das Netzwerk auf minimal-relevante Artists.

Das Netzwerk hat 22k Artists, aber fast alle haben leere Genre-Arrays,
sodass der Blacklist-Filter in fetch_releases.py nie greift. Dieses Script
scored Labels und Artists nach ihrer Nähe zu Minimal Music und entfernt
irrelevante Einträge.

Usage:
  python refocus_network.py                    # Score + Prune (threshold=5.0)
  python refocus_network.py --dry-run          # Vorschau ohne Änderungen
  python refocus_network.py --threshold 3.0    # Niedrigerer Schwellenwert
  python refocus_network.py --stats-only       # Nur Score-Verteilung
  python refocus_network.py --verbose          # Zeigt jeden entfernten Artist
  python refocus_network.py --export-scores scores.json  # Alle Scores exportieren
"""

import json
import os
import sys
import argparse
from pathlib import Path
from collections import defaultdict, Counter
from datetime import datetime

# ─────────────────────────────────────────────────
# PATHS
# ─────────────────────────────────────────────────

BASE_DIR = Path(__file__).parent
NETWORK_FILE = BASE_DIR / "network_data.json"
BACKUP_FILE = BASE_DIR / "network_data.backup.json"
RELEASES_FILE = BASE_DIR / "releases.json"
SEED_FILE = BASE_DIR / "seed_data.json"
LABELS_FILE = BASE_DIR / "reference_labels.txt"
ARTISTS_FILE = BASE_DIR / "reference_artists.txt"
WHITELIST_FILE = BASE_DIR / "genre_whitelist.txt"

# ─────────────────────────────────────────────────
# KNOWN AGGREGATORS
# ─────────────────────────────────────────────────

AGGREGATOR_NAMES = {
    "resident advisor", "ra", "groove magazin", "groove magazine",
    "mixmag", "dj mag", "xlr8r", "electronic beats",
    "boiler room", "hör berlin", "hör", "cercle",
    "the lot radio", "nts", "nts radio", "rinse fm",
    "red bull music", "redbull", "fact magazine", "fact mag",
    "pitchfork", "bbc radio", "essential mix",
}


# ─────────────────────────────────────────────────
# DATA LOADING
# ─────────────────────────────────────────────────

def load_lines(path):
    """Load non-empty, stripped lines from a text file."""
    if not path.exists():
        return []
    return [l.strip() for l in path.read_text(encoding="utf-8").splitlines() if l.strip()]


def load_json(path):
    """Load JSON file, return None if missing."""
    if not path.exists():
        return None
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def load_all_data():
    """Load all data files needed for scoring."""
    network = load_json(NETWORK_FILE)
    if not network:
        print("  Fehler: network_data.json nicht gefunden!")
        sys.exit(1)

    seed_data = load_json(SEED_FILE) or {}
    releases = load_json(RELEASES_FILE) or []

    reference_labels = set(load_lines(LABELS_FILE))
    reference_artists = set(load_lines(ARTISTS_FILE))
    genre_whitelist = {g.lower() for g in load_lines(WHITELIST_FILE)}

    priority_labels = set(seed_data.get("priority_labels", []))
    known_associations = seed_data.get("known_associations", {})

    return (network, seed_data, releases,
            reference_labels, reference_artists, genre_whitelist,
            priority_labels, known_associations)


# ─────────────────────────────────────────────────
# HELPERS
# ─────────────────────────────────────────────────

def build_label_name_index(labels):
    """Map lowercase label name -> label key for fast lookup."""
    idx = {}
    for lkey, ldata in labels.items():
        name = ldata.get("name", "").strip().lower()
        if name:
            idx[name] = lkey
    return idx


def build_artist_name_index(artists):
    """Map lowercase artist name -> artist key for fast lookup."""
    idx = {}
    for akey, adata in artists.items():
        name = adata.get("name", "").strip().lower()
        if name:
            idx[name] = akey
    return idx


def build_edge_lookup(edges):
    """Build artist->labels and label->artists edge lookups."""
    artist_to_labels = defaultdict(set)
    label_to_artists = defaultdict(set)
    for e in edges:
        aid = e.get("artist_id", "")
        lid = e.get("label_id", "")
        if aid and lid:
            artist_to_labels[aid].add(lid)
            label_to_artists[lid].add(aid)
    return artist_to_labels, label_to_artists


def build_releases_index(releases, artist_name_index, genre_whitelist):
    """Index which artists have releases, and which have whitelisted genres."""
    artists_with_releases = set()
    artists_with_whitelisted = set()

    for rel in releases:
        # Match artist names from releases to network artist keys
        artist_str = rel.get("artist", "")
        genre = (rel.get("genre", "") or "").lower()
        styles = [s.lower() for s in rel.get("styles", [])]

        has_wl_genre = genre in genre_whitelist or any(s in genre_whitelist for s in styles)

        # Handle comma/ampersand-separated artists
        for part in artist_str.replace(" & ", ", ").replace(" feat. ", ", ").split(", "):
            name = part.strip().lower()
            if name in artist_name_index:
                akey = artist_name_index[name]
                artists_with_releases.add(akey)
                if has_wl_genre:
                    artists_with_whitelisted.add(akey)

    return artists_with_releases, artists_with_whitelisted


# ─────────────────────────────────────────────────
# PHASE 1: LABEL SCORING
# ─────────────────────────────────────────────────

def score_labels(labels, label_name_index, label_to_artists, artists,
                 reference_labels, priority_labels, known_associations,
                 seed_artist_keys):
    """Score each label based on curation signals. Returns {label_key: score}."""
    scores = {}

    # Pre-compute: which labels do seed artists appear on?
    seed_label_counts = Counter()
    for akey in seed_artist_keys:
        adata = artists.get(akey, {})
        for lid in adata.get("label_ids", []):
            seed_label_counts[lid] += 1

    # Pre-compute: lowercase reference & priority sets for matching
    ref_lower = {n.strip().lower() for n in reference_labels}
    prio_lower = {n.strip().lower() for n in priority_labels}

    for lkey, ldata in labels.items():
        score = 0.0
        name = ldata.get("name", "").strip()
        name_lower = name.lower()
        sources = ldata.get("sources", [])
        rc = ldata.get("release_count", 0) or 0
        n_artists = len(label_to_artists.get(lkey, set()))

        # In reference_labels.txt? (+20)
        if name_lower in ref_lower or name in reference_labels:
            score += 20

        # In seed_data priority_labels? (+15)
        if name_lower in prio_lower or name in priority_labels:
            score += 15

        # Has "reference_labels" in sources? (+10)
        if "reference_labels" in sources:
            score += 10

        # Release count buckets
        if 5 <= rc <= 50:
            score += 8
        elif 51 <= rc <= 200:
            score += 5
        elif 201 <= rc <= 500:
            score += 3
        elif 1 <= rc <= 4:
            score += 2
        elif rc > 500:
            score += 1

        # Seed artists on this label (+3 each, cap 30)
        seed_count = seed_label_counts.get(lkey, 0)
        score += min(seed_count * 3, 30)

        # Multi-source confirmation
        n_sources = len(sources)
        if n_sources >= 3:
            score += 4
        elif n_sources == 2:
            score += 2

        # Aggregator penalties
        if rc == 0 and n_artists >= 30:
            score -= 10
        if rc == 0 and n_artists >= 80:
            score -= 10  # Additional -10

        if name_lower in AGGREGATOR_NAMES:
            score -= 5

        scores[lkey] = score

    return scores


# ─────────────────────────────────────────────────
# PHASE 2: ARTIST SCORING
# ─────────────────────────────────────────────────

def score_artists(artists, label_scores, artist_to_labels, artist_name_index,
                  seed_artist_keys, reference_artists,
                  artists_with_releases, artists_with_whitelisted,
                  ref_label_keys):
    """Score each artist based on label connections + signals. Returns {artist_key: score}."""
    scores = {}

    ref_artists_lower = {n.strip().lower() for n in reference_artists}

    for akey, adata in artists.items():
        score = 0.0
        name = adata.get("name", "").strip()
        name_lower = name.lower()
        sources = adata.get("sources", [])

        # Seed artist: +100
        if adata.get("is_seed"):
            score += 100

        # In reference_artists.txt: +50
        if name_lower in ref_artists_lower or name in reference_artists:
            score += 50

        # Label-based scoring
        label_ids = set(adata.get("label_ids", [])) | artist_to_labels.get(akey, set())
        good_label_count = 0
        ref_label_count = 0

        for lid in label_ids:
            ls = label_scores.get(lid, 0)
            score += 0.5 * ls

            if ls >= 5:
                good_label_count += 1

            if lid in ref_label_keys:
                score += 10
                ref_label_count += 1

        # Label diversity bonus
        if good_label_count >= 3:
            score += 8
        elif good_label_count == 2:
            score += 4

        # Multi-source confirmation
        n_sources = len(sources)
        if n_sources >= 3:
            score += 5
        elif n_sources == 2:
            score += 2

        # Has whitelisted-genre releases: +15
        if akey in artists_with_whitelisted:
            score += 15

        # Has any releases: +5
        if akey in artists_with_releases:
            score += 5

        scores[akey] = score

    return scores


# ─────────────────────────────────────────────────
# PHASE 3: CROSS-POLLINATION
# ─────────────────────────────────────────────────

def cross_pollinate(labels, artists, label_scores, artist_scores,
                    label_to_artists, artist_to_labels,
                    seed_artist_keys, reference_artists,
                    artists_with_releases, artists_with_whitelisted,
                    ref_label_keys, reference_labels, priority_labels,
                    known_associations, label_name_index, artist_name_index):
    """One extra pass: boost labels with many good artists, then re-score artists."""

    # Boost label scores based on well-scored artists
    boosted_label_scores = dict(label_scores)
    for lkey in labels:
        connected_artists = label_to_artists.get(lkey, set())
        good_artists = sum(1 for a in connected_artists if artist_scores.get(a, 0) >= 10)
        if good_artists >= 5:
            boosted_label_scores[lkey] = label_scores.get(lkey, 0) + 8
        elif good_artists >= 3:
            boosted_label_scores[lkey] = label_scores.get(lkey, 0) + 4
        elif good_artists >= 1:
            boosted_label_scores[lkey] = label_scores.get(lkey, 0) + 2

    # Re-score artists with boosted label scores
    new_artist_scores = score_artists(
        artists, boosted_label_scores, artist_to_labels, artist_name_index,
        seed_artist_keys, reference_artists,
        artists_with_releases, artists_with_whitelisted,
        ref_label_keys
    )

    return boosted_label_scores, new_artist_scores


# ─────────────────────────────────────────────────
# PRUNING
# ─────────────────────────────────────────────────

def prune_network(network, artist_scores, label_scores,
                  seed_artist_keys, ref_artist_keys, ref_label_keys,
                  threshold, verbose=False):
    """Remove artists and labels below threshold. Returns stats dict."""
    artists = network["artists"]
    labels = network["labels"]
    edges = network["edges"]

    before_artists = len(artists)
    before_labels = len(labels)
    before_edges = len(edges)

    # Determine which artists to keep
    keep_artists = set()
    removed_artists = []

    for akey, adata in artists.items():
        score = artist_scores.get(akey, 0)
        is_protected = (
            akey in seed_artist_keys or
            akey in ref_artist_keys or
            adata.get("is_seed", False)
        )

        if is_protected or score >= threshold:
            keep_artists.add(akey)
        else:
            removed_artists.append((adata.get("name", "?"), score))
            if verbose:
                print(f"  - {adata.get('name', '?'):<40} score={score:.1f}")

    # Remove artists
    for akey in list(artists.keys()):
        if akey not in keep_artists:
            del artists[akey]

    # Remove orphan labels (no remaining artists, unless reference label)
    keep_labels = set()
    for lkey, ldata in labels.items():
        # Check if any remaining artist references this label
        has_artist = False
        for akey in keep_artists:
            adata = artists.get(akey, {})
            if lkey in adata.get("label_ids", []):
                has_artist = True
                break

        if has_artist or lkey in ref_label_keys:
            keep_labels.add(lkey)

    removed_labels = []
    for lkey in list(labels.keys()):
        if lkey not in keep_labels:
            removed_labels.append(labels[lkey].get("name", "?"))
            del labels[lkey]

    # Clean up edges
    new_edges = [
        e for e in edges
        if e.get("artist_id") in keep_artists and e.get("label_id") in keep_labels
    ]
    network["edges"] = new_edges

    # Update label artist_ids to only reference kept artists
    for lkey, ldata in labels.items():
        if "artist_ids" in ldata:
            ldata["artist_ids"] = [a for a in ldata["artist_ids"] if a in keep_artists]

    # Update artist label_ids to only reference kept labels
    for akey, adata in artists.items():
        if "label_ids" in adata:
            adata["label_ids"] = [l for l in adata["label_ids"] if l in keep_labels]

    # Update metadata
    network["metadata"]["artists_found"] = len(artists)
    network["metadata"]["labels_found"] = len(labels)
    network["metadata"]["last_updated"] = datetime.now().isoformat()
    network["metadata"]["refocused_at"] = datetime.now().isoformat()
    network["metadata"]["refocus_threshold"] = threshold

    after_artists = len(artists)
    after_labels = len(labels)
    after_edges = len(network["edges"])

    seeds_remaining = sum(1 for a in artists.values() if a.get("is_seed"))

    return {
        "before_artists": before_artists,
        "after_artists": after_artists,
        "removed_artists": before_artists - after_artists,
        "before_labels": before_labels,
        "after_labels": after_labels,
        "removed_labels": before_labels - after_labels,
        "before_edges": before_edges,
        "after_edges": after_edges,
        "seeds_remaining": seeds_remaining,
    }


# ─────────────────────────────────────────────────
# SAFETY: BACKUP + ATOMIC WRITE
# ─────────────────────────────────────────────────

def backup_network():
    """Copy network_data.json → network_data.backup.json."""
    import shutil
    if NETWORK_FILE.exists():
        shutil.copy2(NETWORK_FILE, BACKUP_FILE)
        return True
    return False


def save_network(network):
    """Atomic write: tmp file → os.replace."""
    tmp = NETWORK_FILE.with_suffix(".tmp")
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(network, f, ensure_ascii=False, indent=2)
    os.replace(tmp, NETWORK_FILE)


# ─────────────────────────────────────────────────
# OUTPUT
# ─────────────────────────────────────────────────

def print_header(title):
    print(f"\n{'='*60}")
    print(f"  {title}")
    print(f"{'='*60}")


def print_score_distribution(scores, entity_name="Entity"):
    """Print histogram of score distribution."""
    if not scores:
        print(f"  Keine {entity_name} zum Anzeigen.")
        return

    buckets = Counter()
    for s in scores.values():
        if s <= 0:
            buckets["  <= 0"] += 1
        elif s < 1:
            buckets["   0-1"] += 1
        elif s < 2:
            buckets["   1-2"] += 1
        elif s < 5:
            buckets["   2-5"] += 1
        elif s < 10:
            buckets["  5-10"] += 1
        elif s < 20:
            buckets[" 10-20"] += 1
        elif s < 50:
            buckets[" 20-50"] += 1
        elif s < 100:
            buckets["50-100"] += 1
        else:
            buckets["  100+"] += 1

    bucket_order = ["  <= 0", "   0-1", "   1-2", "   2-5", "  5-10",
                    " 10-20", " 20-50", "50-100", "  100+"]

    max_count = max(buckets.values()) if buckets else 1
    for b in bucket_order:
        count = buckets.get(b, 0)
        bar_len = int(40 * count / max_count) if max_count > 0 else 0
        bar = "█" * bar_len
        print(f"  {b}:  {count:>6}  {bar}")

    vals = list(scores.values())
    vals.sort()
    n = len(vals)
    print(f"\n  Total: {n:,}")
    print(f"  Min: {vals[0]:.1f}  Median: {vals[n//2]:.1f}  Max: {vals[-1]:.1f}")
    mean = sum(vals) / n
    print(f"  Mean: {mean:.1f}")


def print_top_labels(label_scores, labels, n=20):
    """Show top-scored labels."""
    ranked = sorted(label_scores.items(), key=lambda x: -x[1])[:n]
    for i, (lkey, score) in enumerate(ranked):
        name = labels.get(lkey, {}).get("name", "?")
        src = ", ".join(labels.get(lkey, {}).get("sources", []))
        print(f"  {i+1:>3}. {name:<35} {score:>6.1f}  [{src}]")


def print_top_artists(artist_scores, artists, n=20):
    """Show top-scored artists."""
    ranked = sorted(artist_scores.items(), key=lambda x: -x[1])[:n]
    for i, (akey, score) in enumerate(ranked):
        adata = artists.get(akey, {})
        name = adata.get("name", "?")
        seed = "🌱" if adata.get("is_seed") else "  "
        n_labels = len(adata.get("label_ids", []))
        print(f"  {seed}{i+1:>3}. {name:<35} {score:>6.1f}  ({n_labels} labels)")


def print_summary(stats, threshold):
    """Print before/after comparison table."""
    print_header("ERGEBNIS — Netzwerk-Refokussierung")

    ba = stats["before_artists"]
    aa = stats["after_artists"]
    ra = stats["removed_artists"]
    bl = stats["before_labels"]
    al = stats["after_labels"]
    rl = stats["removed_labels"]
    be = stats["before_edges"]
    ae = stats["after_edges"]
    re_ = be - ae
    seeds = stats["seeds_remaining"]

    pct_a = (ra * 100 // ba) if ba else 0
    pct_l = (rl * 100 // bl) if bl else 0
    pct_e = (re_ * 100 // be) if be else 0

    print(f"\n  Threshold: {threshold}")
    print(f"\n  {'':20} {'Vorher':>10} {'Nachher':>10} {'Entfernt':>14}")
    print(f"  {'─'*56}")
    print(f"  {'Artists:':<20} {ba:>10,} {aa:>10,} {ra:>8,} ({pct_a}%)")
    print(f"  {'Labels:':<20} {bl:>10,} {al:>10,} {rl:>8,} ({pct_l}%)")
    print(f"  {'Connections:':<20} {be:>10,} {ae:>10,} {re_:>8,} ({pct_e}%)")
    print(f"  {'─'*56}")
    print(f"  {'Seeds:':<20} {'':>10} {seeds:>10,} {'0':>8} (0%)")


# ─────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────

def run(args):
    """Main scoring and pruning pipeline."""

    # ── Load ──
    print_header("VALENTINA NETZWERK-REFOKUSSIERUNG")

    (network, seed_data, releases,
     reference_labels, reference_artists, genre_whitelist,
     priority_labels, known_associations) = load_all_data()

    artists = network.get("artists", {})
    labels = network.get("labels", {})
    edges = network.get("edges", [])

    print(f"\n  Geladen: {len(artists):,} Artists, {len(labels):,} Labels, {len(edges):,} Edges")
    print(f"  Reference Labels: {len(reference_labels)}")
    print(f"  Reference Artists: {len(reference_artists)}")
    print(f"  Priority Labels: {len(priority_labels)}")
    print(f"  Genre Whitelist: {len(genre_whitelist)}")
    print(f"  Releases: {len(releases):,}")

    # ── Build indexes ──
    label_name_index = build_label_name_index(labels)
    artist_name_index = build_artist_name_index(artists)
    artist_to_labels, label_to_artists = build_edge_lookup(edges)
    artists_with_releases, artists_with_whitelisted = build_releases_index(
        releases, artist_name_index, genre_whitelist
    )

    # Identify seed artists, reference artists, reference labels by key
    seed_artist_keys = {k for k, v in artists.items() if v.get("is_seed")}
    ref_artist_keys = set()
    for name in reference_artists:
        nl = name.strip().lower()
        if nl in artist_name_index:
            ref_artist_keys.add(artist_name_index[nl])

    ref_label_keys = set()
    for name in reference_labels:
        nl = name.strip().lower()
        if nl in label_name_index:
            ref_label_keys.add(label_name_index[nl])

    print(f"  Seeds im Netzwerk: {len(seed_artist_keys)}")
    print(f"  Ref Artists im Netzwerk: {len(ref_artist_keys)}")
    print(f"  Ref Labels im Netzwerk: {len(ref_label_keys)}")
    print(f"  Artists mit Releases: {len(artists_with_releases)}")
    print(f"  Artists mit WL-Genre: {len(artists_with_whitelisted)}")

    # ── Phase 1: Label Scoring ──
    print_header("PHASE 1 — Label Scoring")
    label_scores = score_labels(
        labels, label_name_index, label_to_artists, artists,
        reference_labels, priority_labels, known_associations,
        seed_artist_keys
    )
    print_score_distribution(label_scores, "Labels")

    print(f"\n  Top 15 Labels:")
    print_top_labels(label_scores, labels, n=15)

    # ── Phase 2: Artist Scoring ──
    print_header("PHASE 2 — Artist Scoring")
    artist_scores = score_artists(
        artists, label_scores, artist_to_labels, artist_name_index,
        seed_artist_keys, reference_artists,
        artists_with_releases, artists_with_whitelisted,
        ref_label_keys
    )
    print_score_distribution(artist_scores, "Artists")

    print(f"\n  Top 15 Artists:")
    print_top_artists(artist_scores, artists, n=15)

    # ── Phase 3: Cross-Pollination ──
    print_header("PHASE 3 — Cross-Pollination")
    label_scores, artist_scores = cross_pollinate(
        labels, artists, label_scores, artist_scores,
        label_to_artists, artist_to_labels,
        seed_artist_keys, reference_artists,
        artists_with_releases, artists_with_whitelisted,
        ref_label_keys, reference_labels, priority_labels,
        known_associations, label_name_index, artist_name_index
    )
    print(f"  Label Scores nach Cross-Pollination:")
    print_score_distribution(label_scores, "Labels")
    print(f"\n  Artist Scores nach Cross-Pollination:")
    print_score_distribution(artist_scores, "Artists")

    # ── Preview threshold impact ──
    above = sum(1 for s in artist_scores.values() if s >= args.threshold)
    protected = len(seed_artist_keys | ref_artist_keys)
    survive = above  # seeds/refs already counted if above threshold
    # Count protected artists below threshold
    protected_below = 0
    for akey in (seed_artist_keys | ref_artist_keys):
        if artist_scores.get(akey, 0) < args.threshold:
            protected_below += 1
            survive += 1

    print(f"\n  Threshold {args.threshold}: {above:,} Artists >= threshold, "
          f"{protected_below} protected below threshold")
    print(f"  Geschätzt {survive:,} Artists nach Pruning "
          f"(von {len(artists):,}, {len(artists) - survive:,} entfernt)")

    # ── Export Scores ──
    if args.export_scores:
        export = {
            "labels": {lk: {"name": labels[lk].get("name", "?"), "score": s}
                       for lk, s in sorted(label_scores.items(), key=lambda x: -x[1])},
            "artists": {ak: {"name": artists[ak].get("name", "?"), "score": s}
                        for ak, s in sorted(artist_scores.items(), key=lambda x: -x[1])},
        }
        with open(args.export_scores, "w", encoding="utf-8") as f:
            json.dump(export, f, ensure_ascii=False, indent=2)
        print(f"\n  Scores exportiert nach: {args.export_scores}")

    # ── Stats Only? ──
    if args.stats_only:
        print("\n  (--stats-only: Keine Änderungen)")
        return

    # ── Dry Run? ──
    if args.dry_run:
        print_header("DRY RUN — Keine Änderungen")
        # Simulate pruning on a copy
        import copy
        network_copy = copy.deepcopy(network)
        stats = prune_network(
            network_copy, artist_scores, label_scores,
            seed_artist_keys, ref_artist_keys, ref_label_keys,
            args.threshold, verbose=args.verbose
        )
        print_summary(stats, args.threshold)
        print("\n  (--dry-run: Nichts gespeichert)")
        return

    # ── Prune ──
    print_header("PRUNING")

    # Backup first
    if backup_network():
        print(f"  Backup erstellt: {BACKUP_FILE.name}")
    else:
        print("  Warnung: Backup fehlgeschlagen!")
        sys.exit(1)

    stats = prune_network(
        network, artist_scores, label_scores,
        seed_artist_keys, ref_artist_keys, ref_label_keys,
        args.threshold, verbose=args.verbose
    )

    # Atomic save
    save_network(network)
    print(f"  Gespeichert: {NETWORK_FILE.name}")

    print_summary(stats, args.threshold)


def main():
    parser = argparse.ArgumentParser(
        description="Valentina Netzwerk-Refokussierung — Scored und beschneidet das Netzwerk"
    )
    parser.add_argument("--threshold", type=float, default=5.0,
                        help="Mindest-Score zum Behalten (default: 5.0)")
    parser.add_argument("--dry-run", action="store_true",
                        help="Vorschau ohne Änderungen")
    parser.add_argument("--stats-only", action="store_true",
                        help="Nur Score-Verteilung anzeigen")
    parser.add_argument("--verbose", action="store_true",
                        help="Jeden entfernten Artist anzeigen")
    parser.add_argument("--export-scores", metavar="FILE",
                        help="Alle Scores als JSON exportieren")
    args = parser.parse_args()

    run(args)


if __name__ == "__main__":
    main()
