"""
Rush Hour source fetcher.
Fetches new releases from rushhour.nl via RSS feed.

Rush Hour RSS at https://www.rushhour.nl/rss.xml

RSS item structure:
  <item>
    <title>RELEASE TITLE</title>
    <link>https://www.rushhour.nl/record/vinyl/slug</link>
    <description>HTML with artist, title, body, price</description>
    <pubDate>Thu, 26 Feb 2026 15:39:08 +0000</pubDate>
    <dc:creator>Staff Name</dc:creator>
    <guid>319748 at https://www.rushhour.nl</guid>
  </item>

The description HTML contains:
  - Artist: <div class="field field-name-field-artist">Artist:&nbsp;NAME</div>
  - Title: <div class="field field-name-title"><h2>TITLE</h2></div>
  - Body: <div class="field field-name-body"><p>Description</p></div>
  - Price: <div class="field field-name-commerce-price">€ 14,50</div>
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

RSS_URL = "https://www.rushhour.nl/rss.xml"


class RushHourFetcher(BaseSourceFetcher):
    """Fetches new releases from rushhour.nl via RSS feed."""

    name = "rushhour"

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

    def _parse_rss(self, xml_text, cutoff_date):
        """Parse Rush Hour RSS feed XML into release dicts."""
        if not xml_text:
            return []

        releases = []
        cutoff_str = cutoff_date.strftime("%Y-%m-%d")

        try:
            root = ET.fromstring(xml_text)
        except ET.ParseError:
            print("    ✗ Rush Hour: RSS parse error")
            return []

        channel = root.find("channel")
        if channel is None:
            return []

        for item in channel.findall("item"):
            rel = self._parse_item(item, cutoff_str)
            if rel and rel["id"] not in self._seen_ids:
                self._seen_ids.add(rel["id"])
                releases.append(rel)

        return releases

    def _parse_item(self, item, cutoff_str):
        """Parse a single RSS <item> element."""
        title = (item.findtext("title") or "").strip()
        link = (item.findtext("link") or "").strip()
        guid = (item.findtext("guid") or "").strip()
        description = (item.findtext("description") or "").strip()

        if not title or not link:
            return None

        source_id = guid or link

        # Date
        pub_date = (item.findtext("pubDate") or "").strip()
        date = self._parse_date(pub_date)
        if date < cutoff_str:
            return None

        # Parse description HTML for artist and body
        artist, body = self._parse_description(description)

        if not artist:
            return None

        return self.make_release(
            source="rushhour",
            source_id=f"rh:{source_id}",
            title=title,
            artist=artist,
            label="",  # Rush Hour RSS doesn't include label info per item
            genre="Electronic",
            date=date,
            source_url=link,
        )

    @staticmethod
    def _parse_description(html):
        """Extract artist and body text from Rush Hour description HTML.

        The HTML uses encoded entities, so we need to handle both
        raw HTML and entity-encoded HTML.
        """
        artist = ""
        body = ""

        if not html:
            return artist, body

        # Decode HTML entities if needed
        import html as html_module
        decoded = html_module.unescape(html)

        # Extract artist from field-name-field-artist div
        artist_match = re.search(
            r'field-name-field-artist[^>]*>.*?Artist:\s*(?:&nbsp;)?\s*(.+?)</div>',
            decoded, re.DOTALL | re.I
        )
        if artist_match:
            artist = re.sub(r'<[^>]+>', '', artist_match.group(1)).strip()

        # Fallback: try simpler pattern
        if not artist:
            artist_match = re.search(r'Artist:\s*(?:&nbsp;)?\s*([^<]+)', decoded, re.I)
            if artist_match:
                artist = artist_match.group(1).strip()

        # Extract body text
        body_match = re.search(
            r'field-name-body[^>]*>(.*?)</div>',
            decoded, re.DOTALL
        )
        if body_match:
            body = re.sub(r'<[^>]+>', '', body_match.group(1)).strip()

        return artist, body

    @staticmethod
    def _parse_date(date_str):
        """Parse Rush Hour date format: RFC 2822."""
        if not date_str:
            return datetime.now().strftime("%Y-%m-%d")

        for fmt in ["%a, %d %b %Y %H:%M:%S %z", "%a, %d %b %Y %H:%M:%S %Z",
                    "%d %b %Y %H:%M:%S %z", "%Y-%m-%d"]:
            try:
                dt = datetime.strptime(date_str.strip(), fmt)
                return dt.strftime("%Y-%m-%d")
            except ValueError:
                continue

        # Try to find a date pattern
        m = re.search(
            r"(\d{1,2})\s+(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\w*\s+(\d{4})",
            date_str, re.I
        )
        if m:
            try:
                dt = datetime.strptime(
                    f"{m.group(1)} {m.group(2)} {m.group(3)}", "%d %b %Y"
                )
                return dt.strftime("%Y-%m-%d")
            except ValueError:
                pass

        return datetime.now().strftime("%Y-%m-%d")

    # ── Public API ────────────────────────────────────────

    def fetch_new_releases(self, cutoff_date=None, max_pages=3):
        """Fetch new releases from Rush Hour RSS feed.

        Args:
            cutoff_date: Only include releases on or after this date.
            max_pages: Ignored (RSS has no pagination).

        Returns:
            List of unified release dicts.
        """
        if cutoff_date is None:
            cutoff_date = datetime.now() - timedelta(days=90)

        print("  ▸ Rush Hour: Fetching RSS feed...")
        xml = self._fetch_rss(RSS_URL)

        if not xml:
            print("    ✗ Rush Hour: RSS feed unavailable")
            return []

        releases = self._parse_rss(xml, cutoff_date)
        print(f"    → {len(releases)} releases from RSS")
        return releases

    def fetch_by_genre(self, genre_slug, cutoff_date=None, max_pages=3):
        """Not available via RSS."""
        return []

    def fetch_by_artist(self, artist_name, cutoff_date):
        """Not available via RSS."""
        return []

    def fetch_all(self, cutoff_date=None, max_pages=2):
        """Main entry point: fetch from Rush Hour RSS feed.

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
        print(f"  ✓ Rush Hour total: {len(releases)} unique releases")
        return releases


if __name__ == "__main__":
    cutoff = datetime.now() - timedelta(days=60)
    fetcher = RushHourFetcher()
    releases = fetcher.fetch_all(cutoff)
    print(f"\nFound {len(releases)} releases")
    for r in releases[:15]:
        print(
            f"  {r['date']} | {r['artist']} - {r['title']} "
            f"({r['genre']})"
        )
