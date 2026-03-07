#!/usr/bin/env python3
"""
NETWORK STATS v2.0 — Multi-Source Analysis
Analysiert das gecrawlte Netzwerk mit Quellen-Vergleich.

Usage:
  python stats.py                    # Vollständige Analyse
  python stats.py --top 30           # Top 30 statt Top 20
  python stats.py --recommend        # Nur Empfehlungen
  python stats.py --sources          # Quellen-Vergleich
  python stats.py --csv              # CSV Export
"""

import json
import sys
import argparse
import csv
from collections import defaultdict, Counter

def load_network(path="network_data.json"):
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)

def print_header(title):
    print(f"\n{'='*60}")
    print(f"  {title}")
    print(f"{'='*60}")

def analyze(data, top_n=20, show_recommend=False, show_sources=False, export_csv=False):
    artists = data.get("artists", {})
    labels = data.get("labels", {})
    edges = data.get("edges", [])
    meta = data.get("metadata", {})

    seeds = {k: v for k, v in artists.items() if v.get("is_seed")}
    discovered = {k: v for k, v in artists.items() if not v.get("is_seed")}

    # ── Overview ──
    print_header("📊 NETZWERK ÜBERSICHT")
    print(f"  Artists:     {len(artists):>6}  (Seeds: {len(seeds)}, Entdeckt: {len(discovered)})")
    print(f"  Labels:      {len(labels):>6}")
    print(f"  Connections: {len(edges):>6}")
    print(f"  Quellen:     {', '.join(meta.get('sources_used', ['?']))}")
    print(f"  Requests:    {meta.get('total_requests', '?')}")
    if meta.get("discogs_requests"):
        print(f"    Discogs:     {meta['discogs_requests']}")
    if meta.get("musicbrainz_requests"):
        print(f"    MusicBrainz: {meta['musicbrainz_requests']}")
    if meta.get("spotify_requests"):
        print(f"    Spotify:     {meta['spotify_requests']}")
    print(f"  Max Tiefe:   {meta.get('max_depth_reached', '?')}")

    # ── Quellen-Analyse ──
    if show_sources or not show_recommend:
        print_header("🔗 QUELLEN-VERGLEICH")

        # Artists per source
        source_artists = defaultdict(int)
        multi_source_artists = 0
        for a in artists.values():
            sources = a.get("sources", [])
            for s in sources:
                source_artists[s] += 1
            if len(sources) > 1:
                multi_source_artists += 1

        print(f"\n  Artists pro Quelle:")
        for s, count in sorted(source_artists.items(), key=lambda x: -x[1]):
            print(f"    {s:<15} {count:>5}")
        print(f"    {'Multi-Source':<15} {multi_source_artists:>5} ({multi_source_artists*100//max(len(artists),1)}%)")

        # Labels per source
        source_labels = defaultdict(int)
        multi_source_labels = 0
        for l in labels.values():
            sources = l.get("sources", [])
            for s in sources:
                source_labels[s] += 1
            if len(sources) > 1:
                multi_source_labels += 1

        print(f"\n  Labels pro Quelle:")
        for s, count in sorted(source_labels.items(), key=lambda x: -x[1]):
            print(f"    {s:<15} {count:>5}")
        print(f"    {'Multi-Source':<15} {multi_source_labels:>5} ({multi_source_labels*100//max(len(labels),1)}%)")

        # Unique per source (nur in einer Quelle)
        print(f"\n  Exklusive Funde (nur in einer Quelle):")
        for source in set(source_artists.keys()):
            exclusive = sum(1 for a in artists.values()
                          if a.get("sources") == [source])
            print(f"    {source:<15} {exclusive:>5} exklusive Artists")

        # Edges per source
        source_edges = defaultdict(int)
        for e in edges:
            for s in e.get("sources", ["discogs"]):
                source_edges[s] += 1
        print(f"\n  Connections pro Quelle:")
        for s, count in sorted(source_edges.items(), key=lambda x: -x[1]):
            print(f"    {s:<15} {count:>5}")

    # ── Top Labels ──
    if not show_recommend:
        print_header(f"🏷️  TOP {top_n} LABELS (nach Artist-Anzahl)")
        label_artist_count = []
        for lkey, ldata in labels.items():
            n_artists = len(ldata.get("artist_ids", []))
            label_artist_count.append((ldata["name"], n_artists,
                                       ldata.get("release_count", 0),
                                       ldata.get("sources", []),
                                       lkey))
        label_artist_count.sort(key=lambda x: -x[1])
        for i, (name, n_art, n_rel, sources, _) in enumerate(label_artist_count[:top_n]):
            src = "+".join(s[0].upper() for s in sources)  # D+M+S
            print(f"  {i+1:>3}. {name:<35} {n_art:>3} artists  {n_rel:>4} releases  [{src}]")

    # ── Top Artists ──
    if not show_recommend:
        print_header(f"🎵 TOP {top_n} ARTISTS (nach Label-Anzahl)")
        artist_label_count = []
        for akey, adata in artists.items():
            n_labels = len(adata.get("label_ids", []))
            artist_label_count.append((adata["name"], n_labels,
                                        adata.get("popularity"),
                                        adata.get("sources", []),
                                        adata.get("is_seed")))
        artist_label_count.sort(key=lambda x: -x[1])
        for i, (name, n_lab, pop, sources, is_seed) in enumerate(artist_label_count[:top_n]):
            src = "+".join(s[0].upper() for s in sources)
            seed = "🌱" if is_seed else "  "
            pop_str = f"pop:{pop}" if pop else ""
            print(f"  {seed}{i+1:>3}. {name:<30} {n_lab:>3} labels  [{src}] {pop_str}")

    # ── Genre Distribution ──
    if not show_recommend:
        print_header("🎶 GENRE VERTEILUNG")
        genre_count = Counter()
        for a in artists.values():
            for g in a.get("genres", []):
                genre_count[g.lower()] += 1
        for genre, count in genre_count.most_common(25):
            bar = "█" * min(count // 3, 40)
            print(f"  {genre:<25} {count:>4}  {bar}")

    # ── Depth Distribution ──
    if not show_recommend:
        print_header("📏 TIEFE-VERTEILUNG")
        depth_count = Counter()
        for a in artists.values():
            depth_count[a.get("depth", 0)] += 1
        for d in sorted(depth_count.keys()):
            bar = "█" * min(depth_count[d] // 2, 50)
            print(f"  Tiefe {d}: {depth_count[d]:>5} Artists  {bar}")

    # ── Spotify Popularity ──
    pop_artists = [(a["name"], a["popularity"], a.get("is_seed"))
                   for a in artists.values() if a.get("popularity")]
    if pop_artists and not show_recommend:
        print_header(f"🔥 SPOTIFY POPULARITY TOP {top_n}")
        pop_artists.sort(key=lambda x: -x[1])
        for i, (name, pop, is_seed) in enumerate(pop_artists[:top_n]):
            seed = "🌱" if is_seed else "  "
            bar = "█" * (pop // 2)
            print(f"  {seed}{i+1:>3}. {name:<30} {pop:>3}  {bar}")

    # ── EMPFEHLUNGEN ──
    print_header("💡 EMPFEHLUNGEN — Neue Artists auf deinen Labels")
    print("  (Artists die auf Labels deiner Seeds releasen, aber nicht in deiner Sammlung)\n")

    seed_keys = set(seeds.keys())
    seed_label_keys = set()
    for sk in seed_keys:
        seed_label_keys.update(artists[sk].get("label_ids", []))

    recommendations = []
    for akey, adata in discovered.items():
        shared_labels = set(adata.get("label_ids", [])) & seed_label_keys
        if shared_labels:
            label_names = [labels.get(lk, {}).get("name", "?") for lk in shared_labels]
            n_sources = len(adata.get("sources", []))
            pop = adata.get("popularity", 0) or 0
            # Score: mehr shared labels + mehr quellen + popularity
            score = len(shared_labels) * 10 + n_sources * 5 + pop / 10
            recommendations.append((adata["name"], label_names, score,
                                     adata.get("sources", []), pop))

    recommendations.sort(key=lambda x: -x[2])
    for i, (name, label_names, score, sources, pop) in enumerate(recommendations[:top_n * 2]):
        src = "+".join(s[0].upper() for s in sources)
        pop_str = f" (pop:{pop})" if pop else ""
        labels_str = ", ".join(label_names[:3])
        if len(label_names) > 3:
            labels_str += f" +{len(label_names)-3}"
        print(f"  {i+1:>3}. {name:<28} [{src}]{pop_str}")
        print(f"       → {labels_str}")

    if not recommendations:
        print("  Keine Empfehlungen — crawle mehr Tiefe!")

    # ── CSV Export ──
    if export_csv:
        print_header("📁 CSV EXPORT")

        with open("artists.csv", "w", newline="", encoding="utf-8") as f:
            w = csv.writer(f)
            w.writerow(["name", "depth", "is_seed", "genres", "sources",
                        "n_labels", "popularity", "discogs_id", "mbid", "spotify_id"])
            for a in sorted(artists.values(), key=lambda x: x.get("name", "")):
                w.writerow([
                    a.get("name"), a.get("depth"), a.get("is_seed"),
                    "|".join(a.get("genres", [])), "|".join(a.get("sources", [])),
                    len(a.get("label_ids", [])), a.get("popularity", ""),
                    a.get("discogs_id", ""), a.get("mbid", ""), a.get("spotify_id", "")
                ])
        print(f"  ✅ artists.csv ({len(artists)} Einträge)")

        with open("labels.csv", "w", newline="", encoding="utf-8") as f:
            w = csv.writer(f)
            w.writerow(["name", "depth", "genres", "sources", "n_artists",
                        "release_count", "discogs_id", "mbid"])
            for l in sorted(labels.values(), key=lambda x: x.get("name", "")):
                w.writerow([
                    l.get("name"), l.get("depth"),
                    "|".join(l.get("genres", [])), "|".join(l.get("sources", [])),
                    len(l.get("artist_ids", [])), l.get("release_count", 0),
                    l.get("discogs_id", ""), l.get("mbid", "")
                ])
        print(f"  ✅ labels.csv ({len(labels)} Einträge)")

        with open("connections.csv", "w", newline="", encoding="utf-8") as f:
            w = csv.writer(f)
            w.writerow(["artist", "label", "release_count", "years", "sources"])
            for e in edges:
                aname = artists.get(e["artist_id"], {}).get("name", "?")
                lname = labels.get(e["label_id"], {}).get("name", "?")
                w.writerow([
                    aname, lname, e.get("release_count", 0),
                    "|".join(str(y) for y in e.get("years", [])),
                    "|".join(e.get("sources", []))
                ])
        print(f"  ✅ connections.csv ({len(edges)} Einträge)")


def main():
    parser = argparse.ArgumentParser(description="📊 Network Stats v2.0")
    parser.add_argument("--input", default="network_data.json")
    parser.add_argument("--top", type=int, default=20)
    parser.add_argument("--recommend", action="store_true")
    parser.add_argument("--sources", action="store_true")
    parser.add_argument("--csv", action="store_true")
    args = parser.parse_args()

    data = load_network(args.input)
    analyze(data, args.top, args.recommend, args.sources, args.csv)


if __name__ == "__main__":
    main()
