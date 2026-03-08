"""
Discogs source fetcher.
Uses Discogs API to fetch new releases from labels and artists in the network.
Rate limit: 1 request per second (authenticated).
"""

import json
import os
import re
import sys
import time
from datetime import datetime, timedelta
from pathlib import Path

try:
    import requests
except ImportError:
    print("pip install requests")
    sys.exit(1)

from .base import BaseSourceFetcher
from .genre_map import classify_genre

# Load .env
_env_path = Path(__file__).parent.parent / ".env"
if _env_path.exists():
    for line in _env_path.read_text().splitlines():
        line = line.strip()
        if line and not line.startswith("#") and "=" in line:
            k, v = line.split("=", 1)
            os.environ.setdefault(k.strip(), v.strip().strip('"').strip("'"))


class DiscogsFetcher(BaseSourceFetcher):
    name = "discogs"

    BASE = "https://api.discogs.com"
    USER_AGENT = "ValentinaReleaseRadar/1.0"

    def __init__(self, token=None, rate_limit=1.0):
        super().__init__(rate_limit=rate_limit)
        self.token = token or os.getenv("DISCOGS_TOKEN", "")
        self.available = bool(self.token)
        self._session = requests.Session()
        self._session.headers.update({
            "User-Agent": self.USER_AGENT,
            "Authorization": f"Discogs token={self.token}",
            "Accept": "application/json",
        })
        if not self.available:
            print("  ⚠ Discogs: no DISCOGS_TOKEN configured")

    def _get(self, endpoint, params=None):
        """Make authenticated GET request to Discogs API.

        Handles rate limiting (429) and common errors.
        Returns parsed JSON or None on failure.
        """
        if not self.available:
            return None

        self._throttle()
        url = f"{self.BASE}/{endpoint}" if not endpoint.startswith("http") else endpoint
        try:
            resp = self._session.get(url, params=params, timeout=15)

            if resp.status_code == 429:
                # Discogs rate limit hit — wait and retry once
                wait = int(resp.headers.get("Retry-After", 5))
                if wait > 60:
                    print(f"    ⚠ Discogs rate limit too long ({wait}s), skipping")
                    return None
                print(f"    ⏳ Discogs rate limit, waiting {wait}s...")
                time.sleep(wait)
                return self._get(endpoint, params)

            if resp.status_code == 404:
                return None
            if resp.status_code == 401:
                print("    ⚠ Discogs: invalid token")
                self.available = False
                return None

            resp.raise_for_status()
            return resp.json()

        except requests.RequestException as e:
            print(f"    ⚠ Discogs error: {e}")
            return None

    def _parse_date(self, raw_date):
        """Parse Discogs date strings into YYYY-MM-DD format.

        Discogs uses various formats:
        - "2026-03-01"
        - "2026-03"
        - "2026"
        - "01 Mar 2026"
        - ""
        """
        if not raw_date:
            return ""

        raw_date = raw_date.strip()

        # Already ISO format
        if re.match(r'^\d{4}-\d{2}-\d{2}$', raw_date):
            return raw_date

        # Year-month only
        if re.match(r'^\d{4}-\d{2}$', raw_date):
            return f"{raw_date}-01"

        # Year only
        if re.match(r'^\d{4}$', raw_date):
            return f"{raw_date}-01-01"

        # Try common date formats
        for fmt in ["%d %b %Y", "%B %d, %Y", "%b %d, %Y", "%Y-%m-%dT%H:%M:%S"]:
            try:
                dt = datetime.strptime(raw_date, fmt)
                return dt.strftime("%Y-%m-%d")
            except ValueError:
                continue

        return ""

    def _is_within_cutoff(self, date_str, cutoff_str):
        """Check if a date string is on or after the cutoff date."""
        if not date_str or len(date_str) < 4:
            return False
        return date_str >= cutoff_str

    def _detect_reissue(self, title, formats=None):
        """Detect if a release is a reissue based on title and format info."""
        title_lower = title.lower()
        if any(kw in title_lower for kw in ["reissue", "repress", "remastered", "re-issue"]):
            return True
        if formats:
            for fmt in formats:
                descriptions = fmt.get("descriptions", [])
                if any(d.lower() in ("reissue", "repress") for d in descriptions):
                    return True
        return False

    def _extract_format_type(self, formats):
        """Extract format type (LP, EP, Single) from Discogs format data."""
        if not formats:
            return ""
        for fmt in formats:
            name = fmt.get("name", "")
            descriptions = fmt.get("descriptions", [])
            qty = fmt.get("qty", "1")

            # Check descriptions for EP, LP, Single, Compilation
            desc_lower = [d.lower() for d in descriptions]
            if "ep" in desc_lower:
                return "EP"
            if "lp" in desc_lower or "album" in desc_lower:
                return "LP"
            if "single" in desc_lower:
                return "Single"
            if "compilation" in desc_lower:
                return "Compilation"

            # Infer from format name
            if name == "Vinyl":
                try:
                    if int(qty) >= 2:
                        return "LP"
                except (ValueError, TypeError):
                    pass
                return ""  # Could be EP or LP
            elif name == "CD":
                return "LP"
            elif name == "File" or name == "Digital Media":
                return ""  # Can't tell from format alone

        return ""

    def fetch_label_releases(self, label_id, label_name, cutoff_date=None,
                             max_pages=5):
        """Fetch releases from a Discogs label, filtered to recent ones.

        Args:
            label_id: Discogs label ID (numeric)
            label_name: Label name for display
            cutoff_date: datetime, defaults to 90 days ago
            max_pages: Maximum pages to fetch (50 results per page)

        Returns:
            List of unified release dicts
        """
        if not self.available:
            return []

        if cutoff_date is None:
            cutoff_date = datetime.now() - timedelta(days=90)
        cutoff_str = cutoff_date.strftime("%Y-%m-%d")

        releases = []
        page = 1

        while page <= max_pages:
            data = self._get(f"labels/{label_id}/releases", {
                "page": page,
                "per_page": 50,
                "sort": "year",
                "sort_order": "desc",
            })
            if not data or "releases" not in data:
                break

            found_old = False
            for item in data["releases"]:
                rel = self._normalize_label_release(item, label_name, cutoff_str)
                if rel:
                    if self._is_within_cutoff(rel["date"], cutoff_str):
                        releases.append(rel)
                    else:
                        # Sorted desc by year, so older releases mean we can stop
                        found_old = True

            # Check pagination
            pagination = data.get("pagination", {})
            total_pages = pagination.get("pages", 1)
            if found_old or page >= total_pages:
                break
            page += 1

        return releases

    def _normalize_label_release(self, item, label_name, cutoff_str):
        """Normalize a release from a label's release list.

        Label release list items have limited data — we get the basics
        and optionally fetch full details for recent releases.
        """
        discogs_id = item.get("id")
        if not discogs_id:
            return None

        title = (item.get("title") or "").strip()
        artist = (item.get("artist") or "").strip()
        if not title:
            return None

        # Clean up artist name (Discogs appends (n) for disambiguation)
        artist = re.sub(r'\s*\(\d+\)\s*$', '', artist)

        # Year from the listing
        year = item.get("year")
        if year and year > 0:
            date = f"{year}-01-01"
        else:
            date = ""

        # The label release list is sparse — fetch full release for metadata
        # Only for releases that could be within cutoff (by year)
        if date and not self._is_within_cutoff(date[:4] + "-01-01", cutoff_str[:4] + "-01-01"):
            return None

        # Fetch full release details for accurate date and metadata
        full = self._get(f"releases/{discogs_id}")
        if not full:
            # Fall back to sparse data
            return self.make_release(
                source="discogs",
                source_id=str(discogs_id),
                title=title,
                artist=artist,
                label=label_name,
                genre=classify_genre([], item.get("genres", [])),
                date=date,
                source_url=f"https://www.discogs.com/release/{discogs_id}",
                discogs_url=f"https://www.discogs.com/release/{discogs_id}",
                reissue=self._detect_reissue(title),
            )

        return self._normalize_full_release(full, label_name)

    def _normalize_full_release(self, release, fallback_label=""):
        """Normalize a full Discogs release object."""
        discogs_id = release.get("id")
        if not discogs_id:
            return None

        title = (release.get("title") or "").strip()
        if not title:
            return None

        # Artists — join multiple artists
        artists_data = release.get("artists", [])
        if artists_data:
            parts = []
            for a in artists_data:
                name = (a.get("name") or "").strip()
                name = re.sub(r'\s*\(\d+\)\s*$', '', name)  # Remove disambiguation
                if name:
                    join = (a.get("join") or "").strip()
                    parts.append(name)
                    if join and join != ",":
                        parts.append(join)
            artist = " ".join(parts)
        else:
            artist = ""

        if not artist:
            return None

        # Date
        date = self._parse_date(release.get("released", ""))
        if not date:
            year = release.get("year")
            if year and year > 0:
                date = f"{year}-01-01"

        # Label
        labels_data = release.get("labels", [])
        if labels_data:
            label = (labels_data[0].get("name") or "").strip()
            label = re.sub(r'\s*\(\d+\)\s*$', '', label)
        else:
            label = fallback_label

        # Catalog number
        catno = ""
        if labels_data:
            catno = (labels_data[0].get("catno") or "").strip()
            if catno.lower() == "none":
                catno = ""

        # Genres and styles
        genres = release.get("genres", [])
        styles = release.get("styles", [])
        genre = classify_genre(styles, genres)

        # Format
        formats = release.get("formats", [])
        format_type = self._extract_format_type(formats)

        # Reissue detection
        reissue = self._detect_reissue(title, formats)

        # URL
        uri = release.get("uri", "")
        discogs_url = f"https://www.discogs.com{uri}" if uri and not uri.startswith("http") else uri
        if not discogs_url:
            discogs_url = f"https://www.discogs.com/release/{discogs_id}"

        return self.make_release(
            source="discogs",
            source_id=str(discogs_id),
            title=title,
            artist=artist,
            label=label,
            genre=genre,
            date=date or "",
            source_url=discogs_url,
            discogs_url=discogs_url,
            styles=styles,
            catalog_number=catno,
            format_type=format_type,
            reissue=reissue,
        )

    def fetch_artist_releases(self, artist_id, artist_name, cutoff_date=None,
                              max_pages=3):
        """Fetch releases for a specific artist from Discogs.

        Args:
            artist_id: Discogs artist ID (numeric)
            artist_name: Artist name for display
            cutoff_date: datetime, defaults to 90 days ago
            max_pages: Maximum pages to fetch

        Returns:
            List of unified release dicts
        """
        if not self.available:
            return []

        if cutoff_date is None:
            cutoff_date = datetime.now() - timedelta(days=90)
        cutoff_str = cutoff_date.strftime("%Y-%m-%d")

        releases = []
        page = 1

        while page <= max_pages:
            data = self._get(f"artists/{artist_id}/releases", {
                "page": page,
                "per_page": 50,
                "sort": "year",
                "sort_order": "desc",
            })
            if not data or "releases" not in data:
                break

            found_old = False
            for item in data["releases"]:
                # Filter: only main releases and co-releases, skip appearances
                role = (item.get("role") or "").lower()
                if role and role not in ("main", ""):
                    continue

                rel_id = item.get("id")
                title = (item.get("title") or "").strip()
                year = item.get("year", 0)

                if not rel_id or not title:
                    continue

                # Quick year check before expensive full-release fetch
                if year and year > 0:
                    year_date = f"{year}-01-01"
                    if not self._is_within_cutoff(year_date, cutoff_str[:4] + "-01-01"):
                        found_old = True
                        continue

                # Fetch full release for detailed metadata
                full = self._get(f"releases/{rel_id}")
                if full:
                    rel = self._normalize_full_release(full, "")
                    if rel and self._is_within_cutoff(rel["date"], cutoff_str):
                        releases.append(rel)
                    elif rel and rel["date"] and rel["date"] < cutoff_str:
                        found_old = True

            # Check pagination
            pagination = data.get("pagination", {})
            total_pages = pagination.get("pages", 1)
            if found_old or page >= total_pages:
                break
            page += 1

        return releases

    def fetch_by_genre(self, genre_id, cutoff_date, max_pages=1):
        """Not directly applicable for Discogs. Use fetch_for_network instead."""
        return []

    def fetch_by_artist(self, artist_name, cutoff_date):
        """Fetch releases for an artist by name.

        Searches Discogs for the artist, then fetches their releases.
        """
        if not self.available:
            return []

        # Search for artist
        data = self._get("database/search", {
            "q": artist_name,
            "type": "artist",
            "per_page": 5,
        })
        if not data or "results" not in data:
            return []

        # Find best match
        name_lower = artist_name.lower().strip()
        artist_id = None
        for result in data["results"]:
            result_name = (result.get("title") or "").strip()
            result_name_clean = re.sub(r'\s*\(\d+\)\s*$', '', result_name)
            if result_name_clean.lower() == name_lower:
                artist_id = result.get("id")
                break

        if not artist_id and data["results"]:
            artist_id = data["results"][0].get("id")

        if not artist_id:
            return []

        return self.fetch_artist_releases(artist_id, artist_name, cutoff_date)

    def fetch_for_network(self, network_data, reference_labels=None,
                          reference_artists=None, cutoff_date=None,
                          max_labels=50, max_artists=30):
        """Main entry point: fetch releases from the network.

        Strategy:
        1. Find top labels from network (connected to >= 2 seed artists)
        2. Fetch new releases from those labels
        3. Fetch releases from reference_artists by their discogs_id

        Args:
            network_data: Parsed network_data.json dict
            reference_labels: List of label names (from reference_labels.txt)
            reference_artists: List of artist names (from reference_artists.txt)
            cutoff_date: datetime, defaults to 90 days ago
            max_labels: Max number of labels to fetch from
            max_artists: Max number of artists to fetch from

        Returns:
            List of unified release dicts
        """
        if not self.available:
            print("  ⚠ Discogs: no token configured, skipping")
            return []

        if cutoff_date is None:
            cutoff_date = datetime.now() - timedelta(days=90)

        all_releases = {}  # dedup by id
        artists_data = network_data.get("artists", {})
        labels_data = network_data.get("labels", {})

        # --- Step 1: Find top labels connected to seed artists ---
        print("  ▸ Discogs: finding top labels from network...")
        seed_artist_keys = {k for k, v in artists_data.items()
                           if v.get("depth", 99) == 0}

        # Count how many seed artists each label is connected to
        label_seed_count = {}
        for artist_key in seed_artist_keys:
            artist = artists_data[artist_key]
            for label_id in artist.get("label_ids", []):
                label_seed_count[label_id] = label_seed_count.get(label_id, 0) + 1

        # Labels connected to >= 2 seed artists, sorted by connection count
        top_labels = sorted(
            [(lid, count) for lid, count in label_seed_count.items() if count >= 2],
            key=lambda x: -x[1]
        )[:max_labels]

        print(f"    Found {len(top_labels)} labels connected to 2+ seed artists")

        # Fetch releases from top labels
        for label_key, seed_count in top_labels:
            label_info = labels_data.get(label_key, {})
            label_name = label_info.get("name", label_key)
            # Extract numeric ID from key like "d_12345"
            discogs_id = label_info.get("discogs_id")
            if not discogs_id:
                match = re.match(r'd_(\d+)', label_key)
                if match:
                    discogs_id = int(match.group(1))
            if not discogs_id:
                continue

            print(f"  ▸ Discogs label: {label_name} (id={discogs_id}, "
                  f"{seed_count} seed artists)")
            releases = self.fetch_label_releases(
                discogs_id, label_name, cutoff_date, max_pages=3
            )
            for rel in releases:
                all_releases[rel["id"]] = rel
            if releases:
                print(f"    → {len(releases)} new releases")

        # --- Step 2: Fetch from reference artists with discogs_id ---
        if reference_artists:
            print(f"  ▸ Discogs: checking reference artists...")
            artist_count = 0
            for artist_name in reference_artists:
                if artist_count >= max_artists:
                    break

                # Find artist in network by name
                discogs_id = None
                for akey, aval in artists_data.items():
                    if aval.get("name", "").lower() == artist_name.lower().strip():
                        discogs_id = aval.get("discogs_id")
                        break

                if not discogs_id:
                    continue

                releases = self.fetch_artist_releases(
                    discogs_id, artist_name, cutoff_date, max_pages=2
                )
                for rel in releases:
                    all_releases[rel["id"]] = rel
                if releases:
                    print(f"    {artist_name}: {len(releases)} new releases")
                artist_count += 1

                if (artist_count) % 10 == 0:
                    print(f"    Discogs: {artist_count}/{min(len(reference_artists), max_artists)} "
                          f"artists checked, {len(all_releases)} releases total")

        result = list(all_releases.values())
        print(f"  ✓ Discogs total: {len(result)} unique releases")
        return result


if __name__ == "__main__":
    cutoff = datetime.now() - timedelta(days=90)
    fetcher = DiscogsFetcher()
    if fetcher.available:
        # Test with a known label (Perlon = 1043)
        print("Testing label fetch (Perlon)...")
        releases = fetcher.fetch_label_releases(1043, "Perlon", cutoff)
        print(f"\nPerlon: {len(releases)} releases in last 90 days")
        for r in releases[:5]:
            print(f"  {r['date']} | {r['artist']} - {r['title']} [{r['label']}] "
                  f"({r['format']}) {r['genre']}")

        # Test with a known artist
        print("\nTesting artist fetch (Ricardo Villalobos)...")
        releases = fetcher.fetch_by_artist("Ricardo Villalobos", cutoff)
        print(f"\nRicardo Villalobos: {len(releases)} releases")
        for r in releases[:5]:
            print(f"  {r['date']} | {r['artist']} - {r['title']} [{r['label']}] "
                  f"({r['format']}) {r['genre']}")
    else:
        print("Discogs not configured (DISCOGS_TOKEN empty)")
