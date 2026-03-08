"""
Quality scoring system for Valentina Release Radar.
Each release gets a score (0-100) based on label relevance, artist relevance,
genre match, source trust, and multi-source presence.
"""

import json
import os
from sources.genre_map import classify_genre

# Genres the user actively likes (higher weight)
PREFERRED_GENRES = {
    "Minimal House", "Minimal Techno", "Microhouse", "Rominimal",
    "Deep House", "Soulful House",
    "Downtempo", "Ambient",
    "Dub Techno",
    "Afro House",
    "Broken Beat", "Nu Jazz",
    "Jazz", "Jazz-Electronic",
    "Organic House",
    "Electro", "Detroit Techno",
    "Leftfield", "Experimental", "Electronica",
    "Trip Hop", "IDM",
    "Lo-Fi House", "Chicago House",
    "Acid",
}

# Genres the user tolerates but doesn't love
NEUTRAL_GENRES = {
    "House", "Techno", "Tech House", "Melodic House",
    "Disco", "Nu Disco", "Indie Dance",
    "Breaks", "Electro House",
    "Electronic",
}

# Genres the user dislikes
DISLIKED_GENRES = {
    "Mainstage", "Dance / Pop", "Psy Trance", "Hard Techno",
    "Peak Time Techno", "Trance", "Dubstep", "Bass House",
    "EBM", "Noise", "Synthwave", "Synth Pop",
    "Big Room",
}

# Source trust levels (higher = more trusted for quality)
SOURCE_TRUST = {
    "hardwax": 5,
    "boomkat": 4,
    "discogs": 4,
    "bandcamp": 3,
    "juno": 3,
    "spotify": 2,
    "beatport": 1,
}


def load_reference_data(project_dir="."):
    """Load reference labels, artists, and network data."""
    ref_labels = set()
    ref_artists = set()
    network_artists = {}
    network_labels = {}
    seed_artist_labels = {}

    # Reference labels
    labels_file = os.path.join(project_dir, "reference_labels.txt")
    if os.path.exists(labels_file):
        with open(labels_file) as f:
            ref_labels = {line.strip().lower() for line in f if line.strip() and not line.startswith("#")}

    # Reference artists
    artists_file = os.path.join(project_dir, "reference_artists.txt")
    if os.path.exists(artists_file):
        with open(artists_file) as f:
            ref_artists = {line.strip().lower() for line in f if line.strip() and not line.startswith("#")}

    # Network data
    network_file = os.path.join(project_dir, "network_data.json")
    if os.path.exists(network_file):
        with open(network_file) as f:
            data = json.load(f)
            network_artists = data.get("artists", {})
            network_labels = data.get("labels", {})

            # Build seed artist → label mapping
            for aid, artist in network_artists.items():
                if artist.get("is_seed") or artist.get("depth", 99) == 0:
                    for lid in artist.get("label_ids", []):
                        label_info = network_labels.get(lid, {})
                        label_name = label_info.get("name", "").lower() if isinstance(label_info, dict) else ""
                        if label_name:
                            seed_artist_labels.setdefault(label_name, set()).add(artist.get("name", ""))

    return {
        "ref_labels": ref_labels,
        "ref_artists": ref_artists,
        "network_artists": network_artists,
        "network_labels": network_labels,
        "seed_artist_labels": seed_artist_labels,
    }


def score_label_relevance(label, ref_data):
    """Score 0-30 based on label quality."""
    if not label:
        return 0

    label_lower = label.lower().strip()

    # In reference labels = high score
    if label_lower in ref_data["ref_labels"]:
        return 30

    # Connected to seed artists
    seed_connections = len(ref_data["seed_artist_labels"].get(label_lower, set()))
    if seed_connections >= 3:
        return 25
    if seed_connections >= 2:
        return 20
    if seed_connections >= 1:
        return 15

    # In network at all
    for lid, linfo in ref_data["network_labels"].items():
        lname = linfo.get("name", "").lower() if isinstance(linfo, dict) else ""
        if lname == label_lower:
            return 8

    return 0


def score_artist_relevance(artist, ref_data):
    """Score 0-30 based on artist connection to the network."""
    if not artist:
        return 0

    artist_lower = artist.lower().strip()

    # Direct reference artist
    if artist_lower in ref_data["ref_artists"]:
        return 30

    # In network — check depth
    for aid, ainfo in ref_data["network_artists"].items():
        aname = ainfo.get("name", "").lower()
        if aname == artist_lower:
            depth = ainfo.get("depth", 99)
            if ainfo.get("is_seed"):
                return 30
            if depth == 0:
                return 28
            if depth == 1:
                return 20
            if depth == 2:
                return 12
            return 6

    return 0


def score_genre_match(genre, styles=None):
    """Score 0-20 based on genre preference match."""
    # Use the classified genre
    classified = genre if genre else classify_genre(styles or [])

    if classified in PREFERRED_GENRES:
        return 20
    if classified in NEUTRAL_GENRES:
        return 10
    if classified in DISLIKED_GENRES:
        return 0

    # Check styles individually for partial matches
    if styles:
        for style in styles:
            style_genre = classify_genre([style])
            if style_genre in PREFERRED_GENRES:
                return 18
            if style_genre in NEUTRAL_GENRES:
                return 8

    # Unknown genre — mild positive (might be interesting)
    return 5


def score_source_trust(source):
    """Score 0-10 based on source trustworthiness for quality."""
    trust = SOURCE_TRUST.get(source, 1)
    return trust * 2  # Scale 1-5 → 2-10


def score_multi_source(release):
    """Score 0-10 based on presence on multiple sources."""
    source_urls = release.get("source_urls", {})
    n_sources = len(source_urls) if source_urls else 1
    if n_sources >= 3:
        return 10
    if n_sources >= 2:
        return 6
    return 0


def score_release(release, ref_data):
    """Calculate total quality score (0-100) for a release."""
    label_score = score_label_relevance(release.get("label", ""), ref_data)
    artist_score = score_artist_relevance(release.get("artist", ""), ref_data)
    genre_score = score_genre_match(
        release.get("genre", ""),
        release.get("styles", [])
    )
    source_score = score_source_trust(release.get("source", ""))
    multi_score = score_multi_source(release)

    total = label_score + artist_score + genre_score + source_score + multi_score

    return {
        "total": min(total, 100),
        "label": label_score,
        "artist": artist_score,
        "genre": genre_score,
        "source": source_score,
        "multi": multi_score,
    }


def score_all_releases(releases, project_dir="."):
    """Score all releases and add quality_score field."""
    ref_data = load_reference_data(project_dir)

    for release in releases:
        scores = score_release(release, ref_data)
        release["quality_score"] = scores["total"]
        release["_score_breakdown"] = scores

    # Sort by quality score descending
    releases.sort(key=lambda r: r.get("quality_score", 0), reverse=True)

    return releases


def print_score_summary(releases):
    """Print distribution of quality scores."""
    if not releases:
        print("No releases to score.")
        return

    scored = [r for r in releases if "quality_score" in r]
    if not scored:
        print("No scored releases.")
        return

    total = len(scored)
    tiers = {
        "Excellent (80-100)": len([r for r in scored if r["quality_score"] >= 80]),
        "Good (60-79)": len([r for r in scored if 60 <= r["quality_score"] < 80]),
        "Decent (40-59)": len([r for r in scored if 40 <= r["quality_score"] < 60]),
        "Low (20-39)": len([r for r in scored if 20 <= r["quality_score"] < 40]),
        "Noise (0-19)": len([r for r in scored if r["quality_score"] < 20]),
    }

    print(f"\n{'='*50}")
    print(f"Quality Score Distribution ({total} releases)")
    print(f"{'='*50}")
    for tier, count in tiers.items():
        pct = (count / total * 100) if total else 0
        bar = "█" * int(pct / 2)
        print(f"  {tier:20s} {count:4d} ({pct:5.1f}%) {bar}")

    avg = sum(r["quality_score"] for r in scored) / total
    print(f"\n  Average score: {avg:.1f}")
    print(f"  Top 10:")
    for r in scored[:10]:
        print(f"    {r['quality_score']:3d} | {r.get('artist','?'):30s} | {r.get('title','?'):30s} | {r.get('label','?')}")


if __name__ == "__main__":
    import sys
    project_dir = sys.argv[1] if len(sys.argv) > 1 else "."
    releases_file = os.path.join(project_dir, "releases.json")

    if not os.path.exists(releases_file):
        print(f"No releases.json found in {project_dir}")
        sys.exit(1)

    with open(releases_file) as f:
        releases = json.load(f)

    print(f"Scoring {len(releases)} releases...")
    scored = score_all_releases(releases, project_dir)
    print_score_summary(scored)

    # Save scored releases
    with open(releases_file, "w") as f:
        json.dump(scored, f, indent=2, ensure_ascii=False)
    print(f"\nSaved scored releases to {releases_file}")
