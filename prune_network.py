#!/usr/bin/env python3
"""
Pruned network_data.json: removes labels and orphan edges that don't contribute
to release search. Keeps:
- Labels with >= 2 distinct artist edges
- Labels in reference_labels.txt (whitelist, by name)
- Labels with at least 1 edge to a reference artist (reference_artists.txt)

Usage:
  python3 prune_network.py --dry-run   # show stats only
  python3 prune_network.py             # write pruned file
"""
import json
import argparse
import sys
from pathlib import Path
from collections import defaultdict

BASE_DIR = Path(__file__).parent
NETWORK_FILE = BASE_DIR / "network_data.json"
REF_LABELS_FILE = BASE_DIR / "reference_labels.txt"
REF_ARTISTS_FILE = BASE_DIR / "reference_artists.txt"


def load_lines(path):
    if not path.exists():
        return set()
    return {line.strip().lower() for line in path.read_text().splitlines()
            if line.strip() and not line.startswith("#")}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--input", default=str(NETWORK_FILE))
    ap.add_argument("--output", default=str(NETWORK_FILE))
    ap.add_argument("--min-artists", type=int, default=2,
                    help="Min distinct artists per label to keep (default 2)")
    args = ap.parse_args()

    print(f"Loading {args.input}...")
    with open(args.input) as f:
        data = json.load(f)

    artists = data["artists"]
    labels = data["labels"]
    edges = data["edges"]

    print(f"Before: {len(artists)} artists, {len(labels)} labels, {len(edges)} edges")

    ref_label_names = load_lines(REF_LABELS_FILE)
    ref_artist_names = load_lines(REF_ARTISTS_FILE)
    print(f"Whitelists: {len(ref_label_names)} ref labels, "
          f"{len(ref_artist_names)} ref artists")

    # Build artist_id -> name map (lowercase) for ref-artist lookup
    ref_artist_ids = {aid for aid, a in artists.items()
                      if a.get("name", "").lower() in ref_artist_names}
    print(f"Resolved {len(ref_artist_ids)} reference artists by name in network")

    # Per-label: count distinct artists, mark if has ref-artist edge
    label_artists = defaultdict(set)
    label_has_ref_artist = set()
    for e in edges:
        lid = e["label_id"]
        aid = e["artist_id"]
        label_artists[lid].add(aid)
        if aid in ref_artist_ids:
            label_has_ref_artist.add(lid)

    # Decide which labels to keep
    keep_labels = set()
    reasons = {"ref_label_name": 0, "ref_artist_edge": 0, "min_artists": 0}
    for lid, lbl in labels.items():
        name = lbl.get("name", "").lower()
        if name in ref_label_names:
            keep_labels.add(lid)
            reasons["ref_label_name"] += 1
            continue
        if lid in label_has_ref_artist:
            keep_labels.add(lid)
            reasons["ref_artist_edge"] += 1
            continue
        if len(label_artists[lid]) >= args.min_artists:
            keep_labels.add(lid)
            reasons["min_artists"] += 1

    print(f"\nKeeping {len(keep_labels)} labels:")
    print(f"  in reference_labels.txt: {reasons['ref_label_name']}")
    print(f"  has ref-artist edge:     {reasons['ref_artist_edge']}")
    print(f"  >= {args.min_artists} distinct artists:    {reasons['min_artists']}")

    # Filter edges
    new_edges = [e for e in edges if e["label_id"] in keep_labels]
    print(f"Edges: {len(edges)} -> {len(new_edges)}")

    # Filter labels
    new_labels = {lid: lbl for lid, lbl in labels.items() if lid in keep_labels}

    # Clean artists' label_ids to only point to kept labels
    artists_touched = 0
    for aid, a in artists.items():
        if "label_ids" in a:
            old_n = len(a["label_ids"])
            a["label_ids"] = [lid for lid in a["label_ids"] if lid in keep_labels]
            if len(a["label_ids"]) != old_n:
                artists_touched += 1
    print(f"Cleaned label_ids on {artists_touched} artists")

    # Clean labels' artist_ids: only artists actually still connected via edges
    label_to_artists_after = defaultdict(set)
    for e in new_edges:
        label_to_artists_after[e["label_id"]].add(e["artist_id"])
    for lid, lbl in new_labels.items():
        if "artist_ids" in lbl:
            lbl["artist_ids"] = sorted(label_to_artists_after.get(lid, set()))

    data["labels"] = new_labels
    data["edges"] = new_edges
    if "metadata" in data:
        data["metadata"]["pruned_at"] = __import__("datetime").datetime.utcnow().isoformat()
        data["metadata"]["labels_after_prune"] = len(new_labels)
        data["metadata"]["edges_after_prune"] = len(new_edges)

    if args.dry_run:
        print("\n[DRY RUN] No file written.")
        return

    print(f"\nWriting {args.output}...")
    with open(args.output, "w") as f:
        json.dump(data, f, separators=(",", ":"))

    new_size = Path(args.output).stat().st_size / (1024 * 1024)
    print(f"New size: {new_size:.1f} MB")


if __name__ == "__main__":
    main()
