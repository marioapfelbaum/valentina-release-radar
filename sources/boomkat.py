"""
Boomkat source fetcher.
Fetches new releases from boomkat.com via RSS feed.

Boomkat provides RSS feeds at:
  - /new-releases.rss          All new releases
  - /pre-orders.rss            Pre-orders
  - /new-releases.rss?q[format]=Vinyl   Vinyl only

RSS item structure:
  <item>
    <title>Artist - Title</title>
    <pubDate>Today / Yesterday / 14 Mar 2026</pubDate>
    <link>https://boomkat.com/products/slug</link>
    <guid>3994946</guid>
    <description>HTML with genre, label, format info</description>
  </item>

The description HTML contains:
  - Genre: <a href="/t/genre/electronic">ELECTRONIC</a>
  - Label: <a href="/labels/mute">Mute</a>
  - Formats: WAV, CD, LP, etc.
  - Editorial description text
"""

import re
import subprocess
import sys
import time
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta
from pathlib import Path

try:
    from bs4 import BeautifulSoup
except ImportError:
    BeautifulSoup = None

try:
    import requests
except ImportError:
    print("pip install requests")
    sys.exit(1)

from .base import BaseSourceFetcher
from .genre_map import classify_genre


# Boomkat genres we care about (electronic music focus)
RELEVANT_GENRES = {
    "electronic", "ambient", "experimental", "house", "techno",
    "dub", "electro", "disco", "downtempo", "minimal",
    "bass-music", "leftfield", "soundtrack", "drone",
    "industrial", "noise", "jazz", "soul", "funk",
}

# Genres to skip (not electronic music)
SKIP_GENRES = {
    "rock", "pop", "hip-hop", "metal", "country", "classical",
    "reggae", "punk", "r-and-b", "folk",
}

RSS_URL = "https://boomkat.com/new-releases.rss"


class BoomkatFetcher(BaseSourceFetcher):
    """Fetches new releases from boomkat.com via RSS feed."""

    name = "boomkat"

    def __init__(self, genres=None, rate_limit=2.0):
        super().__init__(rate_limit=rate_limit)
        self._seen_ids = set()

    # ── HTTP helpers ──────────────────────────────────────

    def _curl_get(self, url, timeout=25):
        """Fetch URL via curl."""
        try:
            result = subprocess.run(
                [
                    "curl", "-s", "-L",
                    "--max-time", str(timeout),
                    "-H", "User-Agent: Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                          "AppleWebKit/537.36 (KHTML, like Gecko) "
                          "Chrome/121.0.0.0 Safari/537.36",
                    "-H", "Accept: application/rss+xml,application/xml,text/xml,*/*",
                    url,
                ],
                capture_output=True, text=True, timeout=timeout + 10
            )
            return result.stdout
        except (subprocess.TimeoutExpired, FileNotFoundError):
            return ""

    def _fetch_rss(self, url, timeout=25):
        """Fetch RSS feed, trying requests first, falling back to curl."""
        self._throttle()

        # Try requests first
        try:
            headers = {
                "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                              "AppleWebKit/537.36",
                "Accept": "application/rss+xml,application/xml,text/xml,*/*",
            }
            resp = requests.get(url, headers=headers, timeout=timeout)
            if resp.status_code == 200 and "<?xml" in resp.text[:100]:
                return resp.text
        except requests.RequestException:
            pass

        # Fallback to curl
        return self._curl_get(url, timeout)

    # ── RSS Parsing ───────────────────────────────────────

    def _parse_rss(self, xml_text, cutoff_date):
        """Parse Boomkat RSS feed XML into release dicts."""
        if not xml_text or "<?xml" not in xml_text[:100]:
            return []

        releases = []
        cutoff_str = cutoff_date.strftime("%Y-%m-%d")

        try:
            root = ET.fromstring(xml_text)
        except ET.ParseError:
            print("    ✗ Boomkat: RSS parse error")
            return []

        channel = root.find("channel")
        if channel is None:
            return []

        for item in channel.findall("item"):
            rel = self._parse_rss_item(item, cutoff_str)
            if rel and rel["id"] not in self._seen_ids:
                self._seen_ids.add(rel["id"])
                releases.append(rel)

        return releases

    def _parse_rss_item(self, item, cutoff_str):
        """Parse a single RSS <item> element."""
        raw_title = (item.findtext("title") or "").strip()
        if not raw_title:
            return None

        # Parse "Artist - Title" format
        artist, title = self._split_artist_title(raw_title)
        if not title:
            return None

        # URL and ID
        link = (item.findtext("link") or "").strip()
        guid = (item.findtext("guid") or "").strip()
        source_id = guid or link

        if not source_id:
            return None

        # Date
        pub_date = (item.findtext("pubDate") or "").strip()
        date = self._parse_pub_date(pub_date)
        if date < cutoff_str:
            return None

        # Parse description HTML for label, genre, format
        description = (item.findtext("description") or "").strip()
        label, genres, format_type = self._parse_description(description)

        # Filter: skip non-electronic genres
        if genres:
            genre_slugs = {self._genre_to_slug(g) for g in genres}
            if genre_slugs & SKIP_GENRES and not (genre_slugs & RELEVANT_GENRES):
                return None

        # Classify genre
        genre = classify_genre(genres) if genres else "Electronic"

        return self.make_release(
            source="boomkat",
            source_id=f"bk:{source_id}",
            title=title,
            artist=artist,
            label=label,
            genre=genre,
            date=date,
            source_url=link,
            format_type=format_type,
            styles=genres,
        )

    def _parse_description(self, description_html):
        """Extract label, genres, and format from RSS description HTML."""
        label = ""
        genres = []
        format_type = ""

        if not description_html:
            return label, genres, format_type

        # Use BeautifulSoup if available, otherwise regex
        if BeautifulSoup:
            soup = BeautifulSoup(description_html, "html.parser")

            # Genre from links like <a href="/t/genre/electronic">ELECTRONIC</a>
            for a in soup.find_all("a", href=True):
                href = a.get("href", "")
                if "/t/genre/" in href:
                    genre_text = a.get_text(strip=True)
                    if genre_text:
                        genres.append(genre_text.title())

            # Label from <a href="/labels/...">Label Name</a>
            for a in soup.find_all("a", href=True):
                href = a.get("href", "")
                if "/labels/" in href:
                    label = a.get_text(strip=True)
                    break

            # Format from text like "Formats: WAV, CD, LP, FLAC, MP3"
            text = soup.get_text()
            fmt_match = re.search(r'Formats?:\s*(.+?)(?:\n|$)', text)
            if fmt_match:
                format_type = self._detect_format(fmt_match.group(1))
        else:
            # Regex fallback
            genre_matches = re.findall(
                r'/t/genre/[^"]+">([^<]+)</a>', description_html
            )
            genres = [g.strip().title() for g in genre_matches if g.strip()]

            label_match = re.search(
                r'/labels/[^"]+">([^<]+)</a>', description_html
            )
            if label_match:
                label = label_match.group(1).strip()

            fmt_match = re.search(r'Formats?:\s*(.+?)(?:<|$)', description_html)
            if fmt_match:
                format_type = self._detect_format(fmt_match.group(1))

        return label, genres, format_type

    # ── Public API ────────────────────────────────────────

    def fetch_new_releases(self, cutoff_date=None, max_pages=3):
        """Fetch new releases from Boomkat RSS feed.

        Args:
            cutoff_date: Only include releases on or after this date.
            max_pages: Ignored (RSS has no pagination).

        Returns:
            List of unified release dicts.
        """
        if cutoff_date is None:
            cutoff_date = datetime.now() - timedelta(days=90)

        print("  ▸ Boomkat: Fetching RSS feed...")
        xml = self._fetch_rss(RSS_URL)

        if not xml:
            print("    ✗ Boomkat: RSS feed unavailable")
            return []

        releases = self._parse_rss(xml, cutoff_date)
        print(f"    → {len(releases)} releases from RSS")
        return releases

    def fetch_by_genre(self, genre_slug, cutoff_date=None, max_pages=3):
        """Boomkat RSS doesn't support per-genre feeds reliably.
        All releases come from the main feed and are filtered by genre.
        """
        return []

    def fetch_by_artist(self, artist_name, cutoff_date):
        """Search not available via RSS. Returns empty."""
        return []

    def fetch_all(self, cutoff_date=None, max_pages=2):
        """Main entry point: fetch from Boomkat RSS feed.

        Args:
            cutoff_date: datetime. Defaults to 90 days ago.
            max_pages: Ignored.

        Returns:
            List of unified release dicts.
        """
        if cutoff_date is None:
            cutoff_date = datetime.now() - timedelta(days=90)

        self._seen_ids.clear()
        releases = self.fetch_new_releases(cutoff_date)
        print(f"  ✓ Boomkat total: {len(releases)} unique releases")
        return releases

    # ── Utility methods ───────────────────────────────────

    @staticmethod
    def _split_artist_title(raw_title):
        """Split 'Artist - Title' into (artist, title)."""
        if " - " in raw_title:
            artist, title = raw_title.split(" - ", 1)
            return artist.strip(), title.strip()
        return "", raw_title.strip()

    @staticmethod
    def _parse_pub_date(pub_date):
        """Parse Boomkat RSS pubDate to YYYY-MM-DD.

        Boomkat uses non-standard dates:
          "Today", "Yesterday", "14 Mar 2026", etc.
        """
        if not pub_date:
            return datetime.now().strftime("%Y-%m-%d")

        pub_lower = pub_date.strip().lower()

        if pub_lower == "today":
            return datetime.now().strftime("%Y-%m-%d")
        if pub_lower == "yesterday":
            return (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")

        # Try common date formats
        for fmt in ["%d %b %Y", "%d %B %Y", "%a, %d %b %Y %H:%M:%S %z",
                    "%a, %d %b %Y %H:%M:%S %Z", "%Y-%m-%d"]:
            try:
                dt = datetime.strptime(pub_date.strip(), fmt)
                return dt.strftime("%Y-%m-%d")
            except ValueError:
                continue

        # Try to find a date pattern
        m = re.search(r"(\d{1,2})\s+(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\w*\s+(\d{4})", pub_date, re.I)
        if m:
            try:
                dt = datetime.strptime(f"{m.group(1)} {m.group(2)} {m.group(3)}", "%d %b %Y")
                return dt.strftime("%Y-%m-%d")
            except ValueError:
                pass

        return datetime.now().strftime("%Y-%m-%d")

    @staticmethod
    def _detect_format(text):
        """Detect release format from format string."""
        if not text:
            return ""
        t = text.lower()
        if "lp" in t or "album" in t:
            return "LP"
        if "ep" in t:
            return "EP"
        if '12"' in t or "12 inch" in t or "single" in t:
            return "Single"
        if "vinyl" in t:
            return "Vinyl"
        if "cd" in t:
            return "CD"
        if "cassette" in t or "tape" in t:
            return "Cassette"
        return ""

    @staticmethod
    def _genre_to_slug(genre_name):
        """Convert genre name to slug for filtering."""
        return re.sub(r'[^a-z0-9]+', '-', genre_name.lower()).strip('-')


# Quick standalone test
if __name__ == "__main__":
    cutoff = datetime.now() - timedelta(days=30)
    fetcher = BoomkatFetcher()
    releases = fetcher.fetch_all(cutoff)
    print(f"\nFound {len(releases)} releases")
    for r in releases[:15]:
        print(
            f"  {r['date']} | {r['artist']} - {r['title']} "
            f"[{r['label']}] ({r['genre']})"
        )
