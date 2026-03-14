"""
Clone.nl source fetcher.
Fetches new releases from clone.nl via RSS feeds.

Clone provides RSS feeds at:
  - /rss/new                All new additions
  - /rss/tag/House          House genre
  - /rss/tag/Techno         Techno genre
  - /rss/tag/Ambient        Ambient genre
  - /rss/tag/Electro        Electro genre

RSS item structure:
  <item>
    <title><![CDATA[ARTIST - Title (format) - CatalogNumber]]></title>
    <link>https://clone.nl/item12345.html</link>
    <guid>https://clone.nl/item12345.html</guid>
    <description>HTML with image</description>
    <content:encoded>Editorial description</content:encoded>
    <dc:creator>ARTIST NAME</dc:creator>
    <pubDate>2026-05-11 00:00:00</pubDate>
  </item>
"""

import re
import subprocess
import sys
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta
from pathlib import Path

try:
    import requests
except ImportError:
    print("pip install requests")
    sys.exit(1)

from .base import BaseSourceFetcher
from .genre_map import classify_genre

BASE_URL = "https://clone.nl"

# RSS feeds to fetch — main feed + genre-specific
RSS_FEEDS = [
    (f"{BASE_URL}/rss/new", "all"),
    (f"{BASE_URL}/rss/tag/House", "House"),
    (f"{BASE_URL}/rss/tag/Techno", "Techno"),
    (f"{BASE_URL}/rss/tag/Ambient", "Ambient"),
    (f"{BASE_URL}/rss/tag/Electro", "Electro"),
]

# Namespaces used in Clone RSS
NS = {
    "dc": "http://purl.org/dc/elements/1.1/",
    "content": "http://purl.org/rss/1.0/modules/content/",
}


class CloneFetcher(BaseSourceFetcher):
    """Fetches new releases from clone.nl via RSS feeds."""

    name = "clone"

    def __init__(self, rate_limit=2.0):
        super().__init__(rate_limit=rate_limit)
        self._seen_ids = set()

    def _fetch_rss(self, url, timeout=25):
        """Fetch RSS feed, trying requests first, falling back to curl."""
        self._throttle()
        try:
            headers = {
                "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                              "AppleWebKit/537.36",
                "Accept": "application/rss+xml,application/xml,text/xml,*/*",
            }
            resp = requests.get(url, headers=headers, timeout=timeout)
            if resp.status_code == 200 and ("<?xml" in resp.text[:200] or "<rss" in resp.text[:200]):
                return resp.text
        except requests.RequestException:
            pass

        # Fallback to curl
        try:
            result = subprocess.run(
                ["curl", "-s", "-L", "--max-time", str(timeout),
                 "-H", "User-Agent: Mozilla/5.0",
                 "-H", "Accept: application/rss+xml,application/xml,text/xml,*/*",
                 url],
                capture_output=True, text=True, timeout=timeout + 10
            )
            return result.stdout
        except (subprocess.TimeoutExpired, FileNotFoundError):
            return ""

    @staticmethod
    def _fix_xml(xml_text):
        """Fix common XML issues in Clone's RSS (unescaped & in artist names)."""
        # Replace bare & that aren't part of entities (&amp; &lt; &gt; &quot; &apos; &#NNN;)
        return re.sub(r'&(?!amp;|lt;|gt;|quot;|apos;|#\d+;|#x[0-9a-fA-F]+;)', '&amp;', xml_text)

    def _parse_rss(self, xml_text, cutoff_date, genre_hint=""):
        """Parse Clone RSS feed XML into release dicts."""
        if not xml_text:
            return []

        releases = []
        cutoff_str = cutoff_date.strftime("%Y-%m-%d")

        try:
            root = ET.fromstring(self._fix_xml(xml_text))
        except ET.ParseError:
            print("    ✗ Clone: RSS parse error")
            return []

        channel = root.find("channel")
        if channel is None:
            return []

        for item in channel.findall("item"):
            rel = self._parse_item(item, cutoff_str, genre_hint)
            if rel and rel["id"] not in self._seen_ids:
                self._seen_ids.add(rel["id"])
                releases.append(rel)

        return releases

    def _parse_item(self, item, cutoff_str, genre_hint):
        """Parse a single RSS <item> element."""
        raw_title = (item.findtext("title") or "").strip()
        if not raw_title:
            return None

        # Parse title: "ARTIST - Title (format) - CatalogNumber"
        artist, title, format_type, catalog = self._parse_title(raw_title)

        # Artist fallback from dc:creator
        if not artist:
            artist = (item.findtext("dc:creator", namespaces=NS)
                      or item.findtext("{http://purl.org/dc/elements/1.1/}creator")
                      or "").strip()
        if not title:
            return None

        # URL and ID
        link = (item.findtext("link") or "").strip()
        guid = (item.findtext("guid") or "").strip()
        source_id = guid or link
        if not source_id:
            return None

        # Date: "2026-05-11 00:00:00"
        pub_date = (item.findtext("pubDate") or "").strip()
        date = self._parse_date(pub_date)
        if date < cutoff_str:
            return None

        # Genre from feed hint or default
        genre = genre_hint if genre_hint and genre_hint != "all" else "Electronic"

        return self.make_release(
            source="clone",
            source_id=f"cl:{source_id}",
            title=title,
            artist=artist,
            label="",  # Clone RSS doesn't include label info
            genre=genre,
            date=date,
            source_url=link,
            format_type=format_type,
            catalog_number=catalog,
        )

    @staticmethod
    def _parse_title(raw_title):
        """Parse Clone title format: 'ARTIST - Title (format) - CatalogNumber'

        Returns (artist, title, format_type, catalog_number)
        """
        artist = ""
        title = raw_title
        format_type = ""
        catalog = ""

        # Extract format in parentheses: (EP), (12inch), (2x12inch), (LP)
        fmt_match = re.search(r'\((\d*x?\d*(?:inch|LP|EP|CD|cassette))\)', raw_title, re.I)
        if fmt_match:
            raw_fmt = fmt_match.group(1).lower()
            if "lp" in raw_fmt or "2x12" in raw_fmt:
                format_type = "LP"
            elif "ep" in raw_fmt:
                format_type = "EP"
            elif "12" in raw_fmt:
                format_type = "Single"
            elif "cd" in raw_fmt:
                format_type = "CD"
            elif "cassette" in raw_fmt:
                format_type = "Cassette"

        # Split on " - " separators
        parts = raw_title.split(" - ")
        if len(parts) >= 3:
            # ARTIST - Title (format) - CatalogNumber
            artist = parts[0].strip()
            # Middle part(s) = title (may contain format in parens)
            middle = " - ".join(parts[1:-1])
            title = re.sub(r'\s*\(\d*x?\d*(?:inch|LP|EP|CD|cassette)\)\s*', ' ', middle, flags=re.I).strip()
            catalog = parts[-1].strip()
        elif len(parts) == 2:
            artist = parts[0].strip()
            title = re.sub(r'\s*\(\d*x?\d*(?:inch|LP|EP|CD|cassette)\)\s*', ' ', parts[1], flags=re.I).strip()

        return artist, title, format_type, catalog

    @staticmethod
    def _parse_date(date_str):
        """Parse Clone date format: '2026-05-11 00:00:00' or ISO."""
        if not date_str:
            return datetime.now().strftime("%Y-%m-%d")

        for fmt in ["%Y-%m-%d %H:%M:%S", "%Y-%m-%d",
                    "%a, %d %b %Y %H:%M:%S %z", "%a, %d %b %Y %H:%M:%S %Z"]:
            try:
                dt = datetime.strptime(date_str.strip(), fmt)
                return dt.strftime("%Y-%m-%d")
            except ValueError:
                continue

        # Try to extract date pattern
        m = re.search(r"(\d{4})-(\d{2})-(\d{2})", date_str)
        if m:
            return f"{m.group(1)}-{m.group(2)}-{m.group(3)}"

        return datetime.now().strftime("%Y-%m-%d")

    # ── Public API ────────────────────────────────────────

    def fetch_new_releases(self, cutoff_date=None, max_pages=3):
        """Fetch new releases from Clone RSS feeds.

        Args:
            cutoff_date: Only include releases on or after this date.
            max_pages: Ignored (RSS has no pagination).

        Returns:
            List of unified release dicts.
        """
        if cutoff_date is None:
            cutoff_date = datetime.now() - timedelta(days=90)

        all_releases = []

        for url, genre_hint in RSS_FEEDS:
            label = genre_hint if genre_hint != "all" else "new"
            print(f"  ▸ Clone: Fetching /{label} feed...")
            xml = self._fetch_rss(url)
            if not xml:
                print(f"    ✗ Clone: {label} feed unavailable")
                continue
            releases = self._parse_rss(xml, cutoff_date, genre_hint)
            all_releases.extend(releases)
            print(f"    → {len(releases)} releases")

        return all_releases

    def fetch_by_genre(self, genre_slug, cutoff_date=None, max_pages=3):
        """Fetch from a specific genre feed."""
        if cutoff_date is None:
            cutoff_date = datetime.now() - timedelta(days=90)

        url = f"{BASE_URL}/rss/tag/{genre_slug}"
        xml = self._fetch_rss(url)
        if not xml:
            return []
        return self._parse_rss(xml, cutoff_date, genre_slug)

    def fetch_by_artist(self, artist_name, cutoff_date):
        """Not available via RSS."""
        return []

    def fetch_all(self, cutoff_date=None, max_pages=3):
        """Main entry point: fetch from all Clone RSS feeds.

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
        print(f"  ✓ Clone total: {len(releases)} unique releases")
        return releases


if __name__ == "__main__":
    cutoff = datetime.now() - timedelta(days=30)
    fetcher = CloneFetcher()
    releases = fetcher.fetch_all(cutoff)
    print(f"\nFound {len(releases)} releases")
    for r in releases[:15]:
        print(
            f"  {r['date']} | {r['artist']} - {r['title']} "
            f"({r['genre']}) [{r.get('catalog_number', '')}]"
        )
