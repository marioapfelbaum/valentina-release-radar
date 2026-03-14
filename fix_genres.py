#!/usr/bin/env python3
"""
fix_genres.py — Beatport-basierte Genre-Korrektur für Valentina Release Radar
==============================================================================
Liest releases.json, korrigiert zu generische Genres (House, Techno, Electronic,
Other) via Beatport-Suche, und schreibt die Datei zurück.

Beatport hat die beste Genre-Taxonomie für elektronische Musik — die Genres werden
von den Labels selbst zugewiesen und sind viel spezifischer als Discogs.

Usage:
  python fix_genres.py                # Korrigiert generische Genres
  python fix_genres.py --all          # Prüft ALLE Tracks, nicht nur generische
  python fix_genres.py --dry-run      # Zeigt Änderungen ohne zu speichern
"""

import json
import os
import re
import sys
import time
import argparse
from pathlib import Path

try:
    import requests
except ImportError:
    print("pip install requests")
    sys.exit(1)

RELEASES_FILE = Path(__file__).parent / "releases.json"

UA = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"

# Genres die zu unspezifisch sind und via Beatport korrigiert werden sollen
GENERIC_GENRES = {"House", "Techno", "Electronic", "Other", ""}

# Beatport genre_name → unser Genre-Label
BEATPORT_MAP = {
    # House
    "house": "House",
    "deep house": "Deep House",
    "tech house": "Tech House",
    "progressive house": "Progressive House",
    "future house": "Future House",
    "electro house": "Electro House",
    "funky house": "Funky House",
    "jackin house": "Jackin House",
    "afro house": "Afro House",
    "organic house / downtempo": "Organic House",
    "organic house": "Organic House",
    "bass house": "Bass House",
    "melodic house & techno": "Melodic House",
    "melodic house": "Melodic House",
    "indie dance": "Indie Dance",
    "nu disco / disco": "Nu Disco",
    "nu disco": "Nu Disco",
    "disco": "Disco",
    "mainstage": "Mainstage",
    "dance / pop": "Dance / Pop",
    "dance": "Dance / Pop",
    # Techno
    "techno (peak time / driving)": "Peak Time Techno",
    "peak time / driving": "Peak Time Techno",
    "techno (raw / deep / hypnotic)": "Hypnotic Techno",
    "raw / deep / hypnotic": "Hypnotic Techno",
    "hard techno": "Hard Techno",
    "techno": "Techno",
    # Minimal
    "minimal / deep tech": "Minimal House",
    "minimal": "Minimal House",
    "microhouse": "Microhouse",
    # Trance
    "trance (main floor)": "Trance",
    "trance (raw / deep / hypnotic)": "Psy Trance",
    "trance": "Trance",
    "psy-trance": "Psy Trance",
    # Breaks / Bass
    "breaks": "Breaks",
    "uk garage / bassline": "UK Garage",
    "drum & bass": "Drum & Bass",
    "dubstep": "Dubstep",
    # Other
    "ambient": "Ambient",
    "electronica": "Electronica",
    "downtempo": "Downtempo",
    "electro (classic / detroit / modern)": "Electro",
    "electro": "Electro",
    "dj tools": "DJ Tools",
}

# Erweiterter GENRE_MAP für Discogs-Styles (Fallback wenn Beatport nichts liefert)
EXPANDED_STYLE_MAP = {
    # Original
    "minimal": "Minimal House",
    "minimal house": "Minimal House",
    "minimal techno": "Minimal Techno",
    "micro house": "Microhouse",
    "microhouse": "Microhouse",
    "deep house": "Deep House",
    "dub techno": "Dub Techno",
    "dub": "Dub Techno",
    "tech house": "Tech House",
    "ambient": "Ambient",
    "electro": "Electro",
    "acid house": "Acid",
    "acid": "Acid",
    "downtempo": "Downtempo",
    "experimental": "Experimental",
    "detroit techno": "Detroit Techno",
    "romanian minimal": "Rominimal",
    "breaks": "Breaks",
    "idm": "IDM",
    "disco": "Disco",
    "italo-disco": "Italo Disco",
    # Neue Subgenres
    "progressive house": "Progressive House",
    "future house": "Future House",
    "electro house": "Electro House",
    "big room": "Mainstage",
    "mainstage": "Mainstage",
    "edm": "Mainstage",
    "melodic techno": "Melodic House",
    "melodic house": "Melodic House",
    "hard techno": "Hard Techno",
    "industrial techno": "Hard Techno",
    "peak time techno": "Peak Time Techno",
    "hypnotic": "Hypnotic Techno",
    "raw techno": "Hypnotic Techno",
    "afro house": "Afro House",
    "organic house": "Organic House",
    "bass house": "Bass House",
    "funky house": "Funky House",
    "jackin house": "Jackin House",
    "nu disco": "Nu Disco",
    "nu-disco": "Nu Disco",
    "indie dance": "Indie Dance",
    "trance": "Trance",
    "progressive trance": "Trance",
    "psy-trance": "Psy Trance",
    "psytrance": "Psy Trance",
    "goa trance": "Psy Trance",
    "drum and bass": "Drum & Bass",
    "drum & bass": "Drum & Bass",
    "jungle": "Drum & Bass",
    "dubstep": "Dubstep",
    "uk garage": "UK Garage",
    "bassline": "UK Garage",
    "garage": "UK Garage",
    "euro house": "Euro House",
    "tribal house": "Tribal House",
    "soulful house": "Soulful House",
    "chicago house": "Chicago House",
    "latin house": "Latin House",
    "dance-pop": "Dance / Pop",
    "synth-pop": "Synth Pop",
    "synthwave": "Synthwave",
    "new wave": "New Wave",
    "ebm": "EBM",
    "dark ambient": "Dark Ambient",
    "drone": "Drone",
    "noise": "Noise",
    "leftfield": "Leftfield",
    "abstract": "Leftfield",
    "glitch": "Glitch",
    "electronica": "Electronica",
    "trip hop": "Trip Hop",
    "lo-fi house": "Lo-Fi House",
}


def fetch_beatport_genre(artist, title):
    """Sucht auf Beatport nach Artist+Title und extrahiert das Genre."""
    query = f"{artist} {title}"
    url = f"https://www.beatport.com/search?q={requests.utils.quote(query)}"

    try:
        r = requests.get(url, headers={"User-Agent": UA}, timeout=10, allow_redirects=True)
        if r.status_code >= 400:
            return None

        body = r.text

        # Beatport liefert Genre-Info als JSON im HTML:
        # "genre":[{"genre_id":96,"genre_name":"Mainstage"}]
        # Wir suchen das erste Track-Result mit genre_name
        matches = re.findall(r'"genre_name"\s*:\s*"([^"]+)"', body)
        if not matches:
            return None

        # Erstes Genre-Match nehmen (vom ersten Track-Ergebnis)
        raw = matches[0].strip()
        key = raw.lower()

        # Durch BEATPORT_MAP mappen
        if key in BEATPORT_MAP:
            return BEATPORT_MAP[key]

        # Teilmatch versuchen
        for bk, bv in BEATPORT_MAP.items():
            if bk in key or key in bk:
                return bv

        # Unbekanntes Beatport-Genre direkt zurückgeben (besser als "House")
        return raw

    except Exception as e:
        print(f"    ⚠ Beatport-Fehler: {e}")
        return None


def reclassify_from_styles(styles):
    """Versucht aus der Discogs styles-Liste ein spezifischeres Genre zu finden."""
    if not styles:
        return None

    for style in styles:
        key = style.lower().strip()
        if key in EXPANDED_STYLE_MAP:
            return EXPANDED_STYLE_MAP[key]

    # Kombinationen probieren
    combined = " ".join(s.lower() for s in styles)
    for key, val in EXPANDED_STYLE_MAP.items():
        if key in combined:
            return val

    return None


def fix_genres(dry_run=False, check_all=False):
    """Hauptfunktion: Genres in releases.json korrigieren."""
    if not RELEASES_FILE.exists():
        print(f"✗ {RELEASES_FILE} nicht gefunden")
        sys.exit(1)

    with open(RELEASES_FILE) as f:
        releases = json.load(f)

    print(f"📀 {len(releases)} Releases geladen")

    # --- PHASE 1: Erweiterte Style-Klassifikation (sofort, kein Netzwerk) ---
    print(f"\n{'='*50}")
    print(f"PHASE 1: Style-Reklassifikation (offline)")
    print(f"{'='*50}")

    style_changed = 0
    for r in releases:
        old = r.get("genre", "")
        if old not in GENERIC_GENRES and not check_all:
            continue
        new = reclassify_from_styles(r.get("styles", []))
        if new and new != old:
            print(f"  ↻ {r['artist'][:25]:25s} | {old:12s} → {new} (styles: {r.get('styles',[])})")
            r["genre"] = new
            style_changed += 1

    print(f"\n  Phase 1: {style_changed} Genres via Styles korrigiert")

    # --- PHASE 2: Beatport-Suche für verbleibende generische Genres ---
    still_generic = [r for r in releases if r.get("genre", "") in GENERIC_GENRES]
    print(f"\n{'='*50}")
    print(f"PHASE 2: Beatport-Suche ({len(still_generic)} verbleibende)")
    print(f"{'='*50}")

    beatport_changed = 0
    beatport_hits = 0
    errors = 0

    for i, r in enumerate(still_generic):
        old_genre = r.get("genre", "")
        artist = r.get("artist", "")
        title = r.get("title", "")

        sys.stdout.write(f"\r  [{i+1}/{len(still_generic)}] {artist[:30]} — {title[:30]}...          ")
        sys.stdout.flush()

        bp_genre = fetch_beatport_genre(artist, title)
        if bp_genre:
            beatport_hits += 1
            if bp_genre != old_genre:
                print(f"\n  ✓ {artist[:25]:25s} | {old_genre:12s} → {bp_genre}")
                r["genre"] = bp_genre
                beatport_changed += 1
        else:
            errors += 1

        # Rate Limit
        time.sleep(0.3)

    print(f"\n\n  Phase 2: {beatport_changed} Genres via Beatport korrigiert ({beatport_hits} Hits, {errors} nicht gefunden)")

    # --- SUMMARY ---
    total_changed = style_changed + beatport_changed
    print(f"\n{'='*50}")
    print(f"📊 Gesamt: {total_changed} Genres korrigiert")
    print(f"  Style-Map:  {style_changed}")
    print(f"  Beatport:   {beatport_changed}")

    # Genre-Verteilung danach
    genres = {}
    for r in releases:
        g = r.get("genre", "")
        genres[g] = genres.get(g, 0) + 1
    print(f"\nGenre-Verteilung:")
    for g, c in sorted(genres.items(), key=lambda x: -x[1])[:20]:
        print(f"  {c:4d}  {g}")

    if total_changed > 0 and not dry_run:
        tmp = RELEASES_FILE.with_suffix(".tmp")
        with open(tmp, "w") as f:
            json.dump(releases, f, ensure_ascii=False, indent=2)
        os.replace(tmp, RELEASES_FILE)
        print(f"\n✓ {RELEASES_FILE} gespeichert ({total_changed} Genres korrigiert)")
    elif dry_run:
        print(f"\n⚠ DRY RUN — nichts gespeichert")
    else:
        print(f"\n✓ Keine Änderungen nötig")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Beatport-basierte Genre-Korrektur")
    parser.add_argument("--all", action="store_true", help="Alle Tracks prüfen, nicht nur generische")
    parser.add_argument("--dry-run", action="store_true", help="Änderungen nur anzeigen, nicht speichern")
    args = parser.parse_args()

    fix_genres(dry_run=args.dry_run, check_all=args.all)
