"""
Beatport source fetcher.
Scrapes new releases from Beatport genre pages.
Beatport embeds full release data as JSON in server-rendered HTML.
"""

import json
import re
import sys
import time
from datetime import datetime, timedelta

try:
    import requests
except ImportError:
    print("pip install requests")
    sys.exit(1)

from .base import BaseSourceFetcher
from .genre_map import BEATPORT_GENRE_MAP, classify_genre


class BeatportFetcher(BaseSourceFetcher):
    name = "beatport"

    BASE_URL = "https://www.beatport.com"
    HEADERS = {
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                      "AppleWebKit/537.36 (KHTML, like Gecko) "
                      "Chrome/122.0.0.0 Safari/537.36",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9",
    }

    def __init__(self, genre_ids=None, rate_limit=2.0):
        """
        Args:
            genre_ids: List of Beatport genre IDs to scrape. Default: all from BEATPORT_GENRE_MAP.
            rate_limit: Seconds between requests. Default 2.0.
        """
        super().__init__(rate_limit=rate_limit)
        self.genre_ids = genre_ids or list(BEATPORT_GENRE_MAP.keys())
        self._session = requests.Session()
        self._session.headers.update(self.HEADERS)

    def fetch_by_genre(self, genre_id, cutoff_date, max_pages=3):
        """Fetch new releases from a Beatport genre page.

        Args:
            genre_id: Beatport genre numeric ID (e.g. 14 for Minimal/Deep Tech)
            cutoff_date: datetime — only include releases on or after this date
            max_pages: Max pages to scrape (each page has up to 100 releases)

        Returns:
            List of unified release dicts
        """
        if genre_id not in BEATPORT_GENRE_MAP:
            print(f"  ⚠ Unknown Beatport genre ID: {genre_id}")
            return []

        slug, default_genre = BEATPORT_GENRE_MAP[genre_id]
        releases = []

        for page in range(1, max_pages + 1):
            url = f"{self.BASE_URL}/genre/{slug}/{genre_id}/releases"
            params = {"page": page, "per_page": 100}

            self._throttle()
            try:
                resp = self._session.get(url, params=params, timeout=30)
                resp.raise_for_status()
            except requests.RequestException as e:
                print(f"  ✗ Beatport request failed (genre={slug}, page={page}): {e}")
                break

            page_releases = self._parse_releases(resp.text, default_genre)

            if not page_releases:
                break

            # Filter by cutoff date
            cutoff_str = cutoff_date.strftime("%Y-%m-%d")
            for rel in page_releases:
                if rel["date"] >= cutoff_str:
                    releases.append(rel)

            # If oldest release on this page is before cutoff, no need for more pages
            oldest = min(r["date"] for r in page_releases)
            if oldest < cutoff_str:
                break

            print(f"    Page {page}: {len(page_releases)} releases (oldest: {oldest})")

        return releases

    def fetch_all_genres(self, cutoff_date, max_pages=3, progress_cb=None):
        """Fetch from all configured genres.

        Args:
            cutoff_date: datetime
            max_pages: Pages per genre
            progress_cb: Optional callback(genre_name, count)

        Returns:
            List of unified release dicts (deduplicated within Beatport)
        """
        all_releases = {}
        for genre_id in self.genre_ids:
            slug, genre_name = BEATPORT_GENRE_MAP[genre_id]
            print(f"  ▸ Beatport: {genre_name} ({slug}/{genre_id})")

            genre_releases = self.fetch_by_genre(genre_id, cutoff_date, max_pages)

            for rel in genre_releases:
                # Deduplicate by Beatport ID (same release can appear in multiple genres)
                all_releases[rel["id"]] = rel

            if progress_cb:
                progress_cb(genre_name, len(genre_releases))

        result = list(all_releases.values())
        print(f"  ✓ Beatport total: {len(result)} unique releases")
        return result

    def fetch_by_artist(self, artist_name, cutoff_date):
        """Search Beatport for a specific artist's releases.

        Args:
            artist_name: Artist name to search
            cutoff_date: datetime

        Returns:
            List of unified release dicts
        """
        self._throttle()
        url = f"{self.BASE_URL}/search/releases"
        params = {"q": artist_name, "per_page": 50}

        try:
            resp = self._session.get(url, params=params, timeout=30)
            resp.raise_for_status()
        except requests.RequestException as e:
            print(f"  ✗ Beatport artist search failed ({artist_name}): {e}")
            return []

        releases = self._parse_releases(resp.text, "Other")
        cutoff_str = cutoff_date.strftime("%Y-%m-%d")

        # Filter by artist name (fuzzy) and cutoff date
        artist_lower = artist_name.lower().strip()
        result = []
        for rel in releases:
            if rel["date"] < cutoff_str:
                continue
            # Check if the searched artist is in the release's artist field
            if artist_lower in rel["artist"].lower():
                result.append(rel)

        return result

    def _parse_releases(self, html, default_genre):
        """Extract release data from Beatport HTML.

        Beatport embeds JSON in the page via React server-side rendering.
        We look for __NEXT_DATA__ script tag or dehydrated state.
        """
        releases = []

        # Strategy 1: Look for __NEXT_DATA__ JSON
        next_data = self._extract_next_data(html)
        if next_data:
            releases = self._parse_next_data(next_data, default_genre)
            if releases:
                return releases

        # Strategy 2: Look for dehydrated React Query state
        dehydrated = self._extract_dehydrated_state(html)
        if dehydrated:
            releases = self._parse_dehydrated(dehydrated, default_genre)
            if releases:
                return releases

        # Strategy 3: Regex fallback for structured data in script tags
        releases = self._parse_script_json(html, default_genre)

        return releases

    def _extract_next_data(self, html):
        """Extract __NEXT_DATA__ JSON from script tag."""
        pattern = r'<script\s+id="__NEXT_DATA__"\s+type="application/json">(.*?)</script>'
        match = re.search(pattern, html, re.DOTALL)
        if match:
            try:
                return json.loads(match.group(1))
            except json.JSONDecodeError:
                pass
        return None

    def _extract_dehydrated_state(self, html):
        """Extract dehydrated state from React Query."""
        # Look for large JSON objects containing release data
        patterns = [
            r'"dehydratedState"\s*:\s*(\{.*?"queries".*?\})\s*[,}]',
            r'window\.__REACT_QUERY_STATE__\s*=\s*(\{.*?\});',
        ]
        for pattern in patterns:
            match = re.search(pattern, html, re.DOTALL)
            if match:
                try:
                    return json.loads(match.group(1))
                except json.JSONDecodeError:
                    continue
        return None

    def _parse_script_json(self, html, default_genre):
        """Fallback: find release JSON objects in any script tag."""
        releases = []
        # Look for JSON arrays or objects containing release-like data
        pattern = r'"publish_date"\s*:\s*"(\d{4}-\d{2}-\d{2})".*?"name"\s*:\s*"([^"]+)"'
        # More robust: find JSON blobs in script tags
        script_pattern = r'<script[^>]*>(.*?)</script>'
        for match in re.finditer(script_pattern, html, re.DOTALL):
            content = match.group(1)
            if '"publish_date"' not in content:
                continue
            # Try to find release objects
            release_objs = self._find_release_objects(content)
            for obj in release_objs:
                rel = self._normalize_beatport_release(obj, default_genre)
                if rel:
                    releases.append(rel)
        return releases

    def _find_release_objects(self, text):
        """Find JSON objects that look like Beatport releases."""
        objects = []
        # Find JSON objects with publish_date field
        depth = 0
        start = None
        for i, c in enumerate(text):
            if c == '{':
                if depth == 0:
                    start = i
                depth += 1
            elif c == '}':
                depth -= 1
                if depth == 0 and start is not None:
                    candidate = text[start:i+1]
                    if '"publish_date"' in candidate and '"name"' in candidate and len(candidate) < 5000:
                        try:
                            obj = json.loads(candidate)
                            if "publish_date" in obj and "name" in obj:
                                objects.append(obj)
                        except json.JSONDecodeError:
                            pass
                    start = None
        return objects

    def _parse_next_data(self, data, default_genre):
        """Parse releases from __NEXT_DATA__ structure."""
        releases = []
        # Navigate Next.js data structure
        try:
            page_props = data.get("props", {}).get("pageProps", {})
            # Try various paths where releases might live
            for key in ["releases", "results", "data"]:
                if key in page_props:
                    items = page_props[key]
                    if isinstance(items, dict) and "results" in items:
                        items = items["results"]
                    if isinstance(items, list):
                        for item in items:
                            rel = self._normalize_beatport_release(item, default_genre)
                            if rel:
                                releases.append(rel)
        except (AttributeError, TypeError):
            pass

        # Also try dehydrated state within __NEXT_DATA__
        if not releases:
            dehydrated = data.get("props", {}).get("pageProps", {}).get("dehydratedState", {})
            if dehydrated:
                releases = self._parse_dehydrated(dehydrated, default_genre)

        return releases

    def _parse_dehydrated(self, dehydrated, default_genre):
        """Parse releases from dehydrated React Query state."""
        releases = []
        queries = dehydrated.get("queries", [])
        for query in queries:
            state = query.get("state", {})
            data = state.get("data", {})

            # Data might be the results directly or wrapped
            items = None
            if isinstance(data, list):
                items = data
            elif isinstance(data, dict):
                items = data.get("results", data.get("releases", data.get("items", [])))
                # Sometimes it's data.data
                if not items and "data" in data:
                    inner = data["data"]
                    if isinstance(inner, list):
                        items = inner
                    elif isinstance(inner, dict):
                        items = inner.get("results", inner.get("releases", []))

            if not items or not isinstance(items, list):
                continue

            for item in items:
                rel = self._normalize_beatport_release(item, default_genre)
                if rel:
                    releases.append(rel)

        return releases

    def _normalize_beatport_release(self, raw, default_genre):
        """Convert a Beatport release object to unified schema."""
        if not isinstance(raw, dict):
            return None

        bp_id = raw.get("id")
        name = raw.get("name", "").strip()
        if not bp_id or not name:
            return None

        # Artists
        artists = raw.get("artists", [])
        if isinstance(artists, list) and artists:
            artist_names = [a.get("name", "") for a in artists if isinstance(a, dict)]
            artist = ", ".join(n for n in artist_names if n)
        else:
            artist = ""

        if not artist:
            return None

        # Label
        label_info = raw.get("label", {})
        label = label_info.get("name", "") if isinstance(label_info, dict) else ""

        # Date
        date = raw.get("publish_date", raw.get("new_release_date", ""))
        if not date:
            return None

        # BPM
        bpm = None
        bpm_range = raw.get("bpm_range", {})
        if isinstance(bpm_range, dict) and bpm_range:
            bpm_min = bpm_range.get("min")
            bpm_max = bpm_range.get("max")
            if bpm_min and bpm_max:
                bpm = round((bpm_min + bpm_max) / 2)
            elif bpm_min:
                bpm = bpm_min

        # Genre from Beatport's own genre data
        genre = default_genre
        genre_info = raw.get("genre", {})
        if isinstance(genre_info, dict) and genre_info.get("name"):
            genre = classify_genre([genre_info["name"]]) or default_genre
        sub_genre = raw.get("sub_genre", {})
        if isinstance(sub_genre, dict) and sub_genre.get("name"):
            sub_classified = classify_genre([sub_genre["name"]])
            if sub_classified and sub_classified != "Other":
                genre = sub_classified

        # Catalog number
        catalog = raw.get("catalog_number", "")

        # Track count for format inference
        track_count = raw.get("track_count", 0)
        if track_count == 1:
            fmt = "Single"
        elif track_count and track_count <= 6:
            fmt = "EP"
        elif track_count and track_count > 6:
            fmt = "LP"
        else:
            fmt = ""

        # Slug for URL
        slug = raw.get("slug", "")
        source_url = f"{self.BASE_URL}/release/{slug}/{bp_id}" if slug else ""

        return self.make_release(
            source="beatport",
            source_id=bp_id,
            title=name,
            artist=artist,
            label=label,
            genre=genre,
            date=date,
            source_url=source_url,
            bpm=bpm,
            catalog_number=catalog,
            format_type=fmt,
        )


# Quick standalone test
if __name__ == "__main__":
    from datetime import timedelta
    cutoff = datetime.now() - timedelta(days=30)
    fetcher = BeatportFetcher(genre_ids=[14])  # Minimal/Deep Tech only
    releases = fetcher.fetch_all_genres(cutoff, max_pages=1)
    print(f"\nFound {len(releases)} releases")
    for r in releases[:5]:
        print(f"  {r['date']} | {r['artist']} — {r['title']} [{r['label']}] ({r['genre']})")
