"""
Phonica Records source fetcher.
Fetches new releases from phonicarecords.com via RSS feed.

Phonica is a curated London record shop specializing in electronic music.

RSS feed at: /rss_feed/just_arrived
Direct access is blocked by Sucuri WAF, so we use rss2json.com proxy
(same approach as Boomkat).

RSS item structure (via proxy):
  - title: "Artist - Release Title"
  - link: "https://www.phonicarecords.com/product/view/{id}"
  - description: HTML table with thumbnail, artist, label, description
  - pubDate: NOT included (null)
  - thumbnail: cover image URL
"""

import html as html_module
import re
import sys
from datetime import datetime, timedelta

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


RSS_FEED_URL = "https://www.phonicarecords.com/rss_feed/just_arrived"
RSS_PROXY_URL = (
    "https://api.rss2json.com/v1/api.json"
    "?rss_url=https%3A%2F%2Fwww.phonicarecords.com%2Frss_feed%2Fjust_arrived"
)


class PhonicaFetcher(BaseSourceFetcher):
    """Fetches new releases from phonicarecords.com via RSS proxy."""

    name = "phonica"

    def __init__(self, rate_limit=2.0):
        super().__init__(rate_limit=rate_limit)
        self._seen_ids = set()

    def _fetch_via_proxy(self, cutoff_date, timeout=25):
        """Fetch Phonica releases via rss2json.com proxy."""
        self._throttle()
        try:
            resp = requests.get(RSS_PROXY_URL, timeout=timeout)
            if resp.status_code != 200:
                print(f"    ✗ Proxy returned {resp.status_code}")
                return []
            data = resp.json()
            if data.get("status") != "ok" or not data.get("items"):
                print("    ✗ Proxy returned no items")
                return []

            releases = []
            for item in data["items"]:
                rel = self._parse_proxy_item(item)
                if rel and rel["id"] not in self._seen_ids:
                    self._seen_ids.add(rel["id"])
                    releases.append(rel)

            return releases
        except Exception as e:
            print(f"    ✗ Proxy error: {e}")
            return []

    def _parse_proxy_item(self, item):
        """Parse a single item from the rss2json proxy response."""
        raw_title = html_module.unescape((item.get("title") or "").strip())
        if not raw_title:
            return None

        # Parse "Artist - Title" format
        artist, title = self._split_artist_title(raw_title)
        if not title:
            return None

        # URL and ID
        link = (item.get("link") or "").strip()
        guid = (item.get("guid") or link).strip()
        source_id = guid or link
        if not source_id:
            return None

        # Parse description for label
        description = item.get("description") or ""
        label = self._extract_label(description)

        # No pubDate available — use today
        date = datetime.now().strftime("%Y-%m-%d")

        return self.make_release(
            source="phonica",
            source_id=f"ph:{source_id}",
            title=title,
            artist=artist,
            label=label,
            genre="Electronic",
            date=date,
            source_url=link,
        )

    @staticmethod
    def _split_artist_title(raw_title):
        """Split 'Artist - Title' into (artist, title)."""
        if " - " in raw_title:
            artist, title = raw_title.split(" - ", 1)
            return artist.strip(), title.strip()
        return "", raw_title.strip()

    @staticmethod
    def _extract_label(description_html):
        """Extract label name from RSS description HTML.

        The description is an HTML table with rows containing:
        - Thumbnail image
        - Release title in <strong>
        - Artist name (uppercase)
        - Label name (uppercase)
        - Description paragraph

        The label is typically the text between <br> tags after the artist.
        """
        if not description_html:
            return ""

        decoded = html_module.unescape(description_html)

        if BeautifulSoup:
            soup = BeautifulSoup(decoded, "html.parser")
            text = soup.get_text(separator="\n")
            lines = [l.strip() for l in text.split("\n") if l.strip()]

            # Label is typically the 3rd non-empty line after filtering
            # Pattern: title, artist, label, description
            # Look for short uppercase lines that look like label names
            for i, line in enumerate(lines):
                # Skip the title (usually in <strong>)
                if i == 0:
                    continue
                # Labels are usually ALL CAPS and short
                if (line == line.upper() and 2 < len(line) < 60
                        and not line.startswith("FORMAT")
                        and not line.startswith("RELEASE")):
                    # First uppercase line after title = artist, second = label
                    # Check if there's another uppercase line after
                    remaining_upper = [
                        l for l in lines[i+1:]
                        if l == l.upper() and 2 < len(l) < 60
                        and not l.startswith("FORMAT")
                    ]
                    if remaining_upper:
                        return remaining_upper[0].title()
                    return line.title()
        else:
            # Regex fallback: find text between <br> tags
            parts = re.split(r'<br\s*/?>|\n', decoded)
            uppercase_parts = [
                p.strip() for p in parts
                if p.strip() and p.strip() == p.strip().upper()
                and 2 < len(p.strip()) < 60
            ]
            if len(uppercase_parts) >= 2:
                return uppercase_parts[1].title()

        return ""

    # ── Public API ────────────────────────────────────────

    def fetch_new_releases(self, cutoff_date=None, max_pages=3):
        """Fetch new releases from Phonica via RSS proxy.

        Args:
            cutoff_date: Only include releases on or after this date.
            max_pages: Ignored (RSS has no pagination).

        Returns:
            List of unified release dicts.
        """
        if cutoff_date is None:
            cutoff_date = datetime.now() - timedelta(days=90)

        print("  ▸ Phonica: Fetching via RSS proxy...")
        releases = self._fetch_via_proxy(cutoff_date)
        print(f"    → {len(releases)} releases")
        return releases

    def fetch_by_genre(self, genre_slug, cutoff_date=None, max_pages=3):
        """Not available via RSS."""
        return []

    def fetch_by_artist(self, artist_name, cutoff_date):
        """Not available via RSS."""
        return []

    def fetch_all(self, cutoff_date=None, max_pages=2):
        """Main entry point: fetch from Phonica RSS feed.

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
        print(f"  ✓ Phonica total: {len(releases)} unique releases")
        return releases


if __name__ == "__main__":
    cutoff = datetime.now() - timedelta(days=30)
    fetcher = PhonicaFetcher()
    releases = fetcher.fetch_all(cutoff)
    print(f"\nFound {len(releases)} releases")
    for r in releases[:15]:
        print(
            f"  {r['date']} | {r['artist']} - {r['title']} "
            f"[{r['label']}] ({r['genre']})"
        )
