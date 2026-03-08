"""
Boomkat source fetcher.
Scrapes new releases from boomkat.com.
Uses curl fallback to bypass Cloudflare TLS fingerprinting.
Parses HTML with BeautifulSoup.

Boomkat HTML structure (as of 2026):
- Product listings live at /products with optional genre filters
- Each product is in a <li> with class "product" or a <div class="product-card">
- Artist, title, label, format, and genre info are in nested elements
- Pagination via ?page=N parameter
"""

import json
import re
import subprocess
import sys
import time
from datetime import datetime, timedelta
from pathlib import Path
from urllib.parse import urlencode, quote_plus

try:
    from bs4 import BeautifulSoup
except ImportError:
    print("pip install beautifulsoup4")
    sys.exit(1)

try:
    import requests
except ImportError:
    print("pip install requests")
    sys.exit(1)

from .base import BaseSourceFetcher
from .genre_map import classify_genre


# Boomkat genre/category slugs relevant to electronic music
BOOMKAT_GENRES = {
    "electronic": "Electronic",
    "ambient": "Ambient",
    "experimental": "Experimental",
    "house": "House",
    "techno": "Techno",
    "dub": "Dub Techno",
    "electro": "Electro",
    "disco-boogie": "Disco",
    "downtempo": "Downtempo",
    "minimal": "Minimal House",
    "bass-music": "Breaks",
    "leftfield": "Leftfield",
    "soundtrack": "Other",
}

# Default genres to scrape (focused on the user's taste profile)
DEFAULT_GENRES = [
    "electronic",
    "ambient",
    "experimental",
    "house",
    "techno",
    "downtempo",
    "minimal",
    "electro",
    "disco-boogie",
]


class BoomkatFetcher(BaseSourceFetcher):
    """Scrapes new releases from boomkat.com."""

    name = "boomkat"

    BASE_URL = "https://boomkat.com"
    HEADERS = {
        "User-Agent": (
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/121.0.0.0 Safari/537.36"
        ),
        "Accept": (
            "text/html,application/xhtml+xml,application/xml;"
            "q=0.9,image/avif,image/webp,*/*;q=0.8"
        ),
        "Accept-Language": "en-US,en;q=0.9",
    }

    def __init__(self, genres=None, rate_limit=2.0):
        """
        Args:
            genres: List of Boomkat genre slugs to scrape.
                    Defaults to DEFAULT_GENRES.
            rate_limit: Seconds between requests. Default 2.0.
        """
        super().__init__(rate_limit=rate_limit)
        self._genres = genres or list(DEFAULT_GENRES)
        self._session = requests.Session()
        self._session.headers.update(self.HEADERS)
        self._seen_ids = set()  # For deduplication

    # ── HTTP helpers ──────────────────────────────────────

    def _curl_get(self, url, timeout=25):
        """Fetch URL via curl (bypasses Cloudflare TLS fingerprinting)."""
        try:
            result = subprocess.run(
                [
                    "curl", "-s", "-L",
                    "--max-time", str(timeout),
                    "--http2",
                    "-H", f"User-Agent: {self.HEADERS['User-Agent']}",
                    "-H", f"Accept: {self.HEADERS['Accept']}",
                    "-H", f"Accept-Language: {self.HEADERS['Accept-Language']}",
                    "-H", "Accept-Encoding: gzip, deflate, br",
                    "-H", "Sec-Fetch-Dest: document",
                    "-H", "Sec-Fetch-Mode: navigate",
                    "-H", "Sec-Fetch-Site: none",
                    "-H", "Sec-Fetch-User: ?1",
                    "-H", "Upgrade-Insecure-Requests: 1",
                    "--compressed",
                    url,
                ],
                capture_output=True, text=True, timeout=timeout + 10
            )
            return result.stdout
        except (subprocess.TimeoutExpired, FileNotFoundError):
            return ""

    def _fetch_page(self, url, timeout=25):
        """Fetch a page, trying requests first, falling back to curl."""
        # Try requests first
        try:
            resp = self._session.get(url, timeout=timeout)
            resp.raise_for_status()
            html = resp.text
            # Check for Cloudflare challenge
            if "Just a moment" in html or "challenge-platform" in html:
                raise requests.RequestException("Cloudflare challenge")
            return html
        except requests.RequestException:
            pass

        # Fallback to curl
        return self._curl_get(url, timeout=timeout)

    # ── Parsing ───────────────────────────────────────────

    def _parse_products_page(self, html, default_genre="Electronic"):
        """Parse product listings from a Boomkat HTML page.

        Boomkat uses several possible HTML structures:
        1. Product cards with data attributes (JSON-LD or data-product)
        2. Structured product list items
        3. JSON-LD structured data in <script> tags

        Returns list of release dicts.
        """
        if not html or len(html) < 500:
            return []

        releases = []
        soup = BeautifulSoup(html, "html.parser")

        # Strategy 1: JSON-LD structured data (most reliable)
        releases = self._parse_json_ld(soup, default_genre)
        if releases:
            return releases

        # Strategy 2: Product cards with data attributes
        releases = self._parse_product_cards(soup, default_genre)
        if releases:
            return releases

        # Strategy 3: Product list items (older layout)
        releases = self._parse_product_list(soup, default_genre)
        if releases:
            return releases

        # Strategy 4: Generic link/text extraction fallback
        releases = self._parse_product_links(soup, default_genre)

        return releases

    def _parse_json_ld(self, soup, default_genre):
        """Extract releases from JSON-LD structured data."""
        releases = []
        for script in soup.find_all("script", type="application/ld+json"):
            try:
                data = json.loads(script.string or "")
            except (json.JSONDecodeError, TypeError):
                continue

            items = []
            if isinstance(data, list):
                items = data
            elif isinstance(data, dict):
                if data.get("@type") in ("Product", "MusicAlbum", "MusicRecording"):
                    items = [data]
                elif "itemListElement" in data:
                    items = [
                        el.get("item", el)
                        for el in data["itemListElement"]
                        if isinstance(el, dict)
                    ]

            for item in items:
                rel = self._normalize_json_ld_item(item, default_genre)
                if rel:
                    releases.append(rel)

        return releases

    def _normalize_json_ld_item(self, item, default_genre):
        """Normalize a JSON-LD product/album item."""
        if not isinstance(item, dict):
            return None

        name = (item.get("name") or "").strip()
        if not name:
            return None

        # Artist: can be in "brand", "byArtist", or "artist"
        artist = ""
        for key in ("byArtist", "artist", "brand"):
            val = item.get(key)
            if isinstance(val, dict):
                artist = (val.get("name") or "").strip()
            elif isinstance(val, str):
                artist = val.strip()
            elif isinstance(val, list):
                artist = ", ".join(
                    (v.get("name", "") if isinstance(v, dict) else str(v)).strip()
                    for v in val
                )
            if artist:
                break

        # Parse "Artist - Title" pattern from name if no separate artist
        if not artist and " - " in name:
            artist, name = name.split(" - ", 1)
            artist = artist.strip()
            name = name.strip()

        if not artist:
            return None

        # Label
        label = ""
        label_val = item.get("recordLabel") or item.get("publisher") or ""
        if isinstance(label_val, dict):
            label = (label_val.get("name") or "").strip()
        elif isinstance(label_val, str):
            label = label_val.strip()

        # URL
        url = (item.get("url") or item.get("@id") or "").strip()
        if url and not url.startswith("http"):
            url = f"{self.BASE_URL}{url}"

        # Date
        date = (
            item.get("datePublished")
            or item.get("releaseDate")
            or item.get("dateCreated")
            or ""
        )
        if not date:
            date = datetime.now().strftime("%Y-%m-%d")
        else:
            date = self._parse_date(date)

        # Genre
        genre_tags = []
        genre_val = item.get("genre")
        if isinstance(genre_val, list):
            genre_tags = genre_val
        elif isinstance(genre_val, str):
            genre_tags = [genre_val]
        genre = classify_genre(genre_tags) if genre_tags else default_genre

        # Format
        fmt = ""
        fmt_val = item.get("musicReleaseFormat") or item.get("additionalType") or ""
        if isinstance(fmt_val, str):
            fl = fmt_val.lower()
            if "single" in fl:
                fmt = "Single"
            elif "ep" in fl:
                fmt = "EP"
            elif "album" in fl or "lp" in fl:
                fmt = "LP"

        source_id = url or f"{artist}-{name}"

        return self.make_release(
            source="boomkat",
            source_id=f"bk:{source_id}",
            title=name,
            artist=artist,
            label=label,
            genre=genre,
            date=date,
            source_url=url,
            format_type=fmt,
        )

    def _parse_product_cards(self, soup, default_genre):
        """Parse product card elements."""
        releases = []

        # Try various product card selectors used by Boomkat
        selectors = [
            "div.product-card",
            "li.product",
            "div[data-product]",
            "div.release-item",
            "article.product",
            "div.product",
            "li[data-product-id]",
        ]

        cards = []
        for selector in selectors:
            cards = soup.select(selector)
            if cards:
                break

        for card in cards:
            rel = self._parse_single_card(card, default_genre)
            if rel:
                releases.append(rel)

        return releases

    def _parse_single_card(self, card, default_genre):
        """Parse a single product card element."""
        # Extract artist
        artist = ""
        for sel in [".product-card__artist", ".artist", "h3 a", ".product-name a"]:
            el = card.select_one(sel)
            if el:
                artist = el.get_text(strip=True)
                break

        # Extract title
        title = ""
        for sel in [".product-card__title", ".title", "h4 a", ".release-title"]:
            el = card.select_one(sel)
            if el:
                title = el.get_text(strip=True)
                break

        if not title and not artist:
            return None

        # Extract label
        label = ""
        for sel in [".product-card__label", ".label", ".product-label"]:
            el = card.select_one(sel)
            if el:
                label = el.get_text(strip=True)
                break

        # Extract URL
        url = ""
        link = card.select_one("a[href*='/products/']") or card.select_one("a")
        if link and link.get("href"):
            url = link["href"]
            if not url.startswith("http"):
                url = f"{self.BASE_URL}{url}"

        # Extract format
        fmt = ""
        for sel in [".product-card__format", ".format", ".media-format"]:
            el = card.select_one(sel)
            if el:
                fmt_text = el.get_text(strip=True).lower()
                if "lp" in fmt_text or "album" in fmt_text:
                    fmt = "LP"
                elif "ep" in fmt_text:
                    fmt = "EP"
                elif "single" in fmt_text or "12\"" in fmt_text or '12"' in fmt_text:
                    fmt = "Single"
                elif "vinyl" in fmt_text:
                    fmt = "Vinyl"
                elif "cd" in fmt_text:
                    fmt = "CD"
                break

        # Extract genre tags
        genre_tags = []
        for sel in [".product-card__genre", ".genre", ".tags a", ".tag"]:
            els = card.select(sel)
            if els:
                genre_tags = [el.get_text(strip=True) for el in els]
                break

        genre = classify_genre(genre_tags) if genre_tags else default_genre

        # Try data attributes for additional info
        data_product = card.get("data-product")
        if data_product:
            try:
                pdata = json.loads(data_product)
                artist = artist or pdata.get("artist", "")
                title = title or pdata.get("title", pdata.get("name", ""))
                label = label or pdata.get("label", "")
            except (json.JSONDecodeError, TypeError):
                pass

        # Parse "Artist - Title" if combined
        if not artist and title and " - " in title:
            artist, title = title.split(" - ", 1)
            artist = artist.strip()
            title = title.strip()

        if not artist or not title:
            return None

        source_id = url or f"{artist}-{title}"
        date = datetime.now().strftime("%Y-%m-%d")

        return self.make_release(
            source="boomkat",
            source_id=f"bk:{source_id}",
            title=title,
            artist=artist,
            label=label,
            genre=genre,
            date=date,
            source_url=url,
            format_type=fmt,
        )

    def _parse_product_list(self, soup, default_genre):
        """Parse a table/list-based product layout."""
        releases = []

        # Boomkat sometimes uses table rows for product listings
        rows = soup.select("tr.product-row") or soup.select("tbody tr")
        for row in rows:
            tds = row.find_all("td")
            if len(tds) < 2:
                continue

            # Try to extract artist/title from link text
            link = row.select_one("a[href*='/products/']")
            if not link:
                continue

            text = link.get_text(strip=True)
            url = link.get("href", "")
            if not url.startswith("http"):
                url = f"{self.BASE_URL}{url}"

            artist, title = "", text
            if " - " in text:
                artist, title = text.split(" - ", 1)

            if not artist or not title:
                continue

            releases.append(self.make_release(
                source="boomkat",
                source_id=f"bk:{url}",
                title=title.strip(),
                artist=artist.strip(),
                label="",
                genre=default_genre,
                date=datetime.now().strftime("%Y-%m-%d"),
                source_url=url,
            ))

        return releases

    def _parse_product_links(self, soup, default_genre):
        """Fallback: extract product info from any links to /products/."""
        releases = []

        links = soup.select("a[href*='/products/']")
        seen_urls = set()

        for link in links:
            url = link.get("href", "")
            if not url.startswith("http"):
                url = f"{self.BASE_URL}{url}"

            if url in seen_urls:
                continue
            seen_urls.add(url)

            text = link.get_text(strip=True)
            if not text or len(text) < 3:
                continue

            artist, title = "", text
            if " - " in text:
                artist, title = text.split(" - ", 1)
            elif text:
                # Try to get artist from a parent or sibling element
                parent = link.parent
                if parent:
                    prev = link.find_previous_sibling()
                    if prev:
                        artist = prev.get_text(strip=True)
                title = text

            if not title:
                continue

            releases.append(self.make_release(
                source="boomkat",
                source_id=f"bk:{url}",
                title=title.strip(),
                artist=artist.strip() if artist else "Unknown",
                label="",
                genre=default_genre,
                date=datetime.now().strftime("%Y-%m-%d"),
                source_url=url,
            ))

        return releases

    def _has_next_page(self, soup):
        """Check if there's a next page link."""
        # Look for pagination elements
        next_link = (
            soup.select_one("a.next_page")
            or soup.select_one("a[rel='next']")
            or soup.select_one("li.next a")
            or soup.select_one("a:contains('Next')")
            or soup.select_one(".pagination a.next")
        )
        return next_link is not None

    # ── Date parsing ──────────────────────────────────────

    @staticmethod
    def _parse_date(date_str):
        """Parse various date formats to YYYY-MM-DD."""
        if not date_str:
            return datetime.now().strftime("%Y-%m-%d")

        date_str = date_str.strip()

        # Already in YYYY-MM-DD
        if re.match(r"^\d{4}-\d{2}-\d{2}$", date_str):
            return date_str

        # Try common formats
        for fmt in [
            "%Y-%m-%dT%H:%M:%S",
            "%Y-%m-%dT%H:%M:%SZ",
            "%Y-%m-%dT%H:%M:%S%z",
            "%d %b %Y",
            "%d %B %Y",
            "%b %d, %Y",
            "%B %d, %Y",
            "%d/%m/%Y",
            "%m/%d/%Y",
        ]:
            try:
                dt = datetime.strptime(date_str[:len(date_str)], fmt)
                return dt.strftime("%Y-%m-%d")
            except ValueError:
                continue

        # Extract YYYY-MM-DD from longer string
        m = re.search(r"(\d{4}-\d{2}-\d{2})", date_str)
        if m:
            return m.group(1)

        return datetime.now().strftime("%Y-%m-%d")

    # ── Public API ────────────────────────────────────────

    def fetch_new_releases(self, cutoff_date=None, max_pages=3):
        """Scrape the main new releases page on Boomkat.

        Args:
            cutoff_date: datetime -- only include releases on or after this date.
                         If None, defaults to 90 days ago.
            max_pages: Max pages to scrape.

        Returns:
            List of unified release dicts.
        """
        if cutoff_date is None:
            cutoff_date = datetime.now() - timedelta(days=90)

        cutoff_str = cutoff_date.strftime("%Y-%m-%d")
        all_releases = []

        for page in range(1, max_pages + 1):
            url = f"{self.BASE_URL}/products?page={page}&per_page=100"
            print(f"  ▸ Boomkat: new releases page {page}")

            self._throttle()
            html = self._fetch_page(url)

            if not html or "Just a moment" in html:
                print(f"    ✗ Cloudflare challenge or empty response")
                break

            soup = BeautifulSoup(html, "html.parser")
            releases = self._parse_products_page(html)

            if not releases:
                print(f"    → 0 releases (no more results)")
                break

            # Filter by cutoff date
            filtered = [r for r in releases if r["date"] >= cutoff_str]
            new_releases = self._deduplicate(filtered)
            all_releases.extend(new_releases)

            print(f"    → {len(new_releases)} releases")

            # Check if we should continue
            if not self._has_next_page(soup):
                break

            # If the oldest release is before cutoff, stop
            if releases:
                oldest = min(r["date"] for r in releases)
                if oldest < cutoff_str:
                    break

        return all_releases

    def fetch_by_genre(self, genre_slug, cutoff_date=None, max_pages=3):
        """Scrape releases filtered by genre on Boomkat.

        Args:
            genre_slug: Boomkat genre slug (e.g. 'electronic', 'ambient')
            cutoff_date: datetime
            max_pages: Max pages per genre

        Returns:
            List of unified release dicts.
        """
        if cutoff_date is None:
            cutoff_date = datetime.now() - timedelta(days=90)

        cutoff_str = cutoff_date.strftime("%Y-%m-%d")
        default_genre = BOOMKAT_GENRES.get(genre_slug, "Electronic")
        all_releases = []

        for page in range(1, max_pages + 1):
            url = (
                f"{self.BASE_URL}/products"
                f"?q[genre][]={genre_slug}&page={page}&per_page=100"
            )
            print(f"  ▸ Boomkat: {genre_slug} page {page}")

            self._throttle()
            html = self._fetch_page(url)

            if not html or "Just a moment" in html:
                print(f"    ✗ Cloudflare challenge or empty response")
                break

            soup = BeautifulSoup(html, "html.parser")
            releases = self._parse_products_page(html, default_genre=default_genre)

            if not releases:
                print(f"    → 0 releases")
                break

            filtered = [r for r in releases if r["date"] >= cutoff_str]
            new_releases = self._deduplicate(filtered)
            all_releases.extend(new_releases)

            print(f"    → {len(new_releases)} releases")

            if not self._has_next_page(soup):
                break

            if releases:
                oldest = min(r["date"] for r in releases)
                if oldest < cutoff_str:
                    break

        return all_releases

    def fetch_by_artist(self, artist_name, cutoff_date):
        """Search Boomkat for a specific artist's releases.

        Args:
            artist_name: Artist name to search.
            cutoff_date: datetime

        Returns:
            List of unified release dicts.
        """
        cutoff_str = cutoff_date.strftime("%Y-%m-%d")
        url = f"{self.BASE_URL}/products?q[keywords]={quote_plus(artist_name)}"

        self._throttle()
        html = self._fetch_page(url)

        if not html or "Just a moment" in html:
            return []

        releases = self._parse_products_page(html)
        artist_lower = artist_name.lower()

        return [
            r for r in releases
            if r["date"] >= cutoff_str
            and artist_lower in r["artist"].lower()
        ]

    def fetch_all(self, cutoff_date=None, max_pages=2):
        """Main entry point: fetch from all configured genres + new releases.

        Args:
            cutoff_date: datetime. Defaults to 90 days ago.
            max_pages: Pages per genre.

        Returns:
            List of unified release dicts (deduplicated).
        """
        if cutoff_date is None:
            cutoff_date = datetime.now() - timedelta(days=90)

        all_releases = {}

        # First, fetch general new releases
        print("  ── Boomkat: New Releases ──")
        new_releases = self.fetch_new_releases(cutoff_date, max_pages=max_pages)
        for r in new_releases:
            all_releases[r["id"]] = r

        # Then fetch by each genre
        for genre_slug in self._genres:
            print(f"  ── Boomkat: Genre {genre_slug} ──")
            genre_releases = self.fetch_by_genre(
                genre_slug, cutoff_date, max_pages=max_pages
            )
            for r in genre_releases:
                all_releases[r["id"]] = r

        result = list(all_releases.values())
        print(f"  ✓ Boomkat total: {len(result)} unique releases")
        return result

    # ── Helpers ────────────────────────────────────────────

    def _deduplicate(self, releases):
        """Remove duplicates seen in this session."""
        unique = []
        for r in releases:
            if r["id"] not in self._seen_ids:
                self._seen_ids.add(r["id"])
                unique.append(r)
        return unique


# Quick standalone test
if __name__ == "__main__":
    cutoff = datetime.now() - timedelta(days=90)
    fetcher = BoomkatFetcher(genres=["electronic", "ambient"])
    releases = fetcher.fetch_all(cutoff, max_pages=1)
    print(f"\nFound {len(releases)} releases")
    for r in releases[:10]:
        print(
            f"  {r['date']} | {r['artist']} - {r['title']} "
            f"[{r['label']}] ({r['genre']})"
        )
