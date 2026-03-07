"""
Shared genre classification for all sources.
Extracted from fetch_releases.py so Beatport, Bandcamp, Spotify
all use the same mapping.
"""

# Discogs/general style → UI genre
GENRE_MAP = {
    # Minimal / Micro
    "minimal": "Minimal House",
    "minimal house": "Minimal House",
    "minimal techno": "Minimal Techno",
    "micro house": "Microhouse",
    "microhouse": "Microhouse",
    "romanian minimal": "Rominimal",
    # House variants
    "deep house": "Deep House",
    "tech house": "Tech House",
    "progressive house": "Progressive House",
    "future house": "Future House",
    "electro house": "Electro House",
    "afro house": "Afro House",
    "organic house": "Organic House",
    "bass house": "Bass House",
    "funky house": "Funky House",
    "jackin house": "Jackin House",
    "soulful house": "Soulful House",
    "chicago house": "Chicago House",
    "tribal house": "Tribal House",
    "lo-fi house": "Lo-Fi House",
    "house": "House",
    # Techno variants
    "dub techno": "Dub Techno",
    "melodic techno": "Melodic House",
    "melodic house": "Melodic House",
    "hard techno": "Hard Techno",
    "industrial techno": "Hard Techno",
    "peak time techno": "Peak Time Techno",
    "detroit techno": "Detroit Techno",
    "hypnotic": "Hypnotic Techno",
    "techno": "Techno",
    # Mainstage / EDM
    "big room": "Mainstage",
    "mainstage": "Mainstage",
    "edm": "Mainstage",
    "dance-pop": "Dance / Pop",
    # Trance
    "trance": "Trance",
    "progressive trance": "Trance",
    "psy-trance": "Psy Trance",
    "psytrance": "Psy Trance",
    "goa trance": "Psy Trance",
    # Bass / Breaks
    "breaks": "Breaks",
    "drum and bass": "Drum & Bass",
    "drum & bass": "Drum & Bass",
    "jungle": "Drum & Bass",
    "dubstep": "Dubstep",
    "uk garage": "UK Garage",
    "bassline": "UK Garage",
    # Dub / Ambient / Down
    "dub": "Dub Techno",
    "ambient": "Ambient",
    "dark ambient": "Dark Ambient",
    "downtempo": "Downtempo",
    # Electro / Acid
    "electro": "Electro",
    "acid house": "Acid",
    "acid": "Acid",
    # Disco
    "disco": "Disco",
    "nu disco": "Nu Disco",
    "nu-disco": "Nu Disco",
    "italo-disco": "Italo Disco",
    "indie dance": "Indie Dance",
    # Other electronic
    "experimental": "Experimental",
    "idm": "IDM",
    "leftfield": "Leftfield",
    "glitch": "Glitch",
    "electronica": "Electronica",
    "synth-pop": "Synth Pop",
    "synthwave": "Synthwave",
    "ebm": "EBM",
    "noise": "Noise",
    "trip hop": "Trip Hop",
    # Beatport-specific labels
    "minimal / deep tech": "Minimal House",
    "deep tech": "Minimal House",
    "minimal deep tech": "Minimal House",
    "afro house / afro tech": "Afro House",
    "melodic house & techno": "Melodic House",
    "organic house / downtempo": "Downtempo",
    "peak time / driving": "Peak Time Techno",
    "raw / deep / hypnotic": "Hypnotic Techno",
}

# Beatport genre ID → (slug, default UI genre)
# Verified slugs as of Feb 2026
BEATPORT_GENRE_MAP = {
    14: ("minimal-deep-tech", "Minimal House"),
    5:  ("house", "House"),
    6:  ("techno-raw-deep-hypnotic", "Techno"),
    11: ("techno-peak-time-driving", "Peak Time Techno"),
    1:  ("afro-house-afro-tech", "Afro House"),
    90: ("melodic-house-techno", "Melodic House"),
    12: ("electronica", "Electronica"),
    2:  ("deep-house", "Deep House"),
    16: ("organic-house-downtempo", "Downtempo"),
    13: ("tech-house", "Tech House"),
    3:  ("electro", "Electro"),
    15: ("nu-disco-disco", "Nu Disco"),
    9:  ("breaks", "Breaks"),
    18: ("acid", "Acid"),
}


def classify_genre(styles, genres=None):
    """Map style/genre tags to our UI genre categories.
    Works with Discogs styles, Beatport genres, Bandcamp tags, Spotify genres.
    """
    all_tags = [s.lower().strip() for s in (styles or [])]
    if genres:
        all_tags += [g.lower().strip() for g in genres]

    for tag in all_tags:
        # Exact match first
        if tag in GENRE_MAP:
            return GENRE_MAP[tag]

    for tag in all_tags:
        # Partial match
        for key, val in GENRE_MAP.items():
            if key in tag:
                return val

    if any("electronic" in t for t in all_tags):
        return "Electronic"
    return "Other"
