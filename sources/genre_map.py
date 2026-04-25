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
    "micro house": "Minimal House",
    "microhouse": "Minimal House",
    "romanian minimal": "Rominimal",
    # House variants
    "deep house": "Deep House",
    "tech house": "Tech House",
    "progressive house": "Progressive House",
    "future house": "Future House",
    "electro house": "Electro House",
    "afro house": "Afro House",
    "afro / latin / brazilian": "Afro House",
    "afro latin brazilian": "Afro House",
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
    "techhouse": "Tech House",
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
    "drum n bass": "Drum & Bass",
    "jungle": "Drum & Bass",
    "dubstep": "Dubstep",
    "uk garage": "UK Garage",
    "bassline": "UK Garage",
    # Dub / Ambient / Down
    "dub": "Dub",
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
    # Jazz / Soul / Broken Beat
    "broken beat": "Broken Beat",
    "brokenbeat": "Broken Beat",
    "jazz-funk": "Jazz-Funk",
    "jazz funk": "Jazz-Funk",
    "jazz house": "Jazz House",
    "future jazz": "Future Jazz",
    "nu jazz": "Nu Jazz",
    "nu-jazz": "Nu Jazz",
    "soul": "Soul",
    "neo soul": "Neo Soul",
    "neo-soul": "Neo Soul",
    "garage house": "Garage House",
    "speed garage": "UK Garage",
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


_IGNORE_TAGS = {"label catalog", "merchandise", "compilation", "reissue",
                 "repress", "pop rock", "alternative rock", "art rock",
                 "chanson", "soundtrack", "holiday", "interview"}

# Tags die ein Release als unerwuenscht markieren — wenn KEIN Electronic-Tag
# im Release vorhanden ist, wird es ausgefiltert. Reduziert die "Other"-Kategorie
# (Hardcore-Spam, Pop Rock, etc.). Drum n Bass + verwandte Hard-Genres explizit
# raus weil User keine harten/kommerziellen Genres will.
_HARD_SKIP_TAGS = {
    # Klar non-electronic
    "pop rock", "alternative rock", "art rock", "indie rock", "rock",
    "punk", "hardcore punk", "metal", "heavy metal", "death metal",
    "country", "folk", "bluegrass", "blues", "classical", "opera",
    "chanson", "soundtrack", "musical", "comedy", "spoken word",
    "religious", "gospel", "christian", "interview", "holiday",
    "bollywood", "k-pop", "j-pop", "schlager",
    # Hard/kommerzielle Electronic-Genres die User nicht will
    "hardcore", "happy hardcore", "hardstyle", "gabber", "speedcore",
    "trap", "future bass", "brostep", "complextro",
    # Drum n Bass: laut User-Memory genre_blindheit_analyse als "Discogs-Muell"
    # markiert, daher rausfiltern
    "drum and bass", "drum & bass", "drum n bass", "jungle", "dnb",
    "neurofunk", "drumstep",
}

# Electronic-Indikator-Tags: wenn eines davon im Release ist, wird es NICHT geskipped
# (auch wenn ein Hard-Skip-Tag dabei ist). Schuetzt Borderline-Cases.
_ELECTRONIC_INDICATORS = {
    "house", "techno", "minimal", "deep house", "tech house", "ambient",
    "downtempo", "electro", "acid", "dub techno", "detroit techno",
    "chicago house", "soulful house", "afro house", "broken beat",
    "jazz house", "nu jazz", "future jazz", "leftfield", "experimental",
    "idm", "trip hop", "garage house", "uk garage", "deep tech",
    "rominimal", "microhouse", "micro house", "dub", "balearic", "italo",
    "nu disco", "synth-pop", "synthwave", "electronica", "lo-fi house",
    "tribal house", "funky house", "jackin house", "ebm", "neo-soul",
    "jazz-funk", "jazz funk",
}


def _tag_components(tag):
    """Split multi-component tags like 'Folk / Roots' or 'Indie Rock, Pop'
    into individual components to check against skip/indicator sets.
    Returns the original tag plus all components."""
    tag = tag.lower().strip()
    parts = [tag]
    for sep in [" / ", " - ", ",", "/", "&"]:
        new_parts = []
        for p in parts:
            new_parts.extend(s.strip() for s in p.split(sep) if s.strip())
        parts = new_parts
    return parts


def _matches_set(tags, target_set):
    for tag in tags:
        for comp in _tag_components(tag):
            if comp in target_set:
                return True
    return False


def should_skip_release(styles, genres=None):
    """Returns True if this release should be filtered out entirely.

    Logic: a release is skipped when any of its tag-components is in
    _HARD_SKIP_TAGS AND none is an electronic-indicator. Protects releases
    tagged with both (e.g. 'rock' + 'deep house') — those get kept.
    """
    all_tags = [s for s in (styles or []) if s]
    if genres:
        all_tags += [g for g in genres if g]
    if not all_tags:
        return False
    if not _matches_set(all_tags, _HARD_SKIP_TAGS):
        return False
    return not _matches_set(all_tags, _ELECTRONIC_INDICATORS)

# Tag-level specificity overrides: when a tag is more generic than its
# mapped UI genre, override here.  E.g. "minimal" alone is vague (3)
# even though it maps to "Minimal House" (genre-level 5).
_TAG_SPECIFICITY = {
    "minimal": 3,
    "house": 1,
    "techno": 1,
}

# Genre specificity: higher = more specific, preferred over generic matches
_GENRE_SPECIFICITY = {
    "Other": 0, "Electronic": 0,
    "House": 1, "Techno": 1,
    "Electro": 2, "Ambient": 2, "Disco": 2, "Trance": 2,
    "Electronica": 2,
    "Deep House": 5, "Tech House": 5, "Detroit Techno": 5,
    "Dub Techno": 5, "Chicago House": 5, "Soulful House": 5,
    "Afro House": 5, "Funky House": 5, "Minimal House": 5,
    "Minimal Techno": 5, "Rominimal": 5,
    "Broken Beat": 5, "Jazz-Funk": 5, "Jazz House": 5,
    "UK Garage": 5, "Garage House": 5, "Acid": 5,
    "Downtempo": 5, "Trip Hop": 5, "Nu Jazz": 5,
    "Experimental": 3, "Leftfield": 3, "IDM": 3,
}


def classify_genre(styles, genres=None):
    """Map style/genre tags to our UI genre categories.
    Prefers the most specific genre match over generic ones.
    Works with Discogs styles, Beatport genres, Bandcamp tags, Spotify genres.
    """
    all_tags = [s.lower().strip() for s in (styles or [])]
    if genres:
        all_tags += [g.lower().strip() for g in genres]

    # Filter noise tags
    all_tags = [t for t in all_tags if t not in _IGNORE_TAGS]

    # Collect ALL exact matches, pick the most specific one.
    # Use tag-level specificity when available (e.g. "minimal" is vague
    # even though it maps to the specific genre "Minimal House").
    matches = []
    for tag in all_tags:
        if tag in GENRE_MAP:
            genre = GENRE_MAP[tag]
            if tag in _TAG_SPECIFICITY:
                specificity = _TAG_SPECIFICITY[tag]
            else:
                specificity = _GENRE_SPECIFICITY.get(genre, 5)
            matches.append((specificity, genre))

    if matches:
        matches.sort(key=lambda x: x[0], reverse=True)
        return matches[0][1]

    # Partial match fallback — prefer longer (= more specific) keys
    # Use word boundary logic: key must match as a whole word within tag
    for tag in all_tags:
        partial = []
        for key, val in GENRE_MAP.items():
            if key in tag and (len(key) >= len(tag) - 2 or
                               tag.startswith(key + " ") or
                               tag.endswith(" " + key) or
                               " " + key + " " in tag or
                               tag == key):
                if key in _TAG_SPECIFICITY:
                    specificity = _TAG_SPECIFICITY[key]
                else:
                    specificity = _GENRE_SPECIFICITY.get(val, 5)
                partial.append((specificity, val))
        if partial:
            partial.sort(key=lambda x: x[0], reverse=True)
            return partial[0][1]

    if any("electronic" in t for t in all_tags):
        return "Electronic"
    return "Other"
