"""
Juno source fetcher.
Scrapes new releases from juno.co.uk with genre filtering.
Uses curl fallback to bypass Cloudflare TLS fingerprinting.
Parses HTML with BeautifulSoup.

Juno HTML structure (as of 2026):
- Genre pages at /all/{genre-slug}/ or /{genre-slug}/
- Product listings in <div class="product-list"> or similar containers
- Each product has artist, title, label, cat#, format, BPM, genre
- Pagination via ?page=N or /page-N/
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


# Juno genre slugs -> default genre classification
# Focused on genres matching the user's taste profile
JUNO_GENRES = {
    "deep-house": "Deep House",
    "minimal-tech-house": "Minimal House",
    "downtempo-balearic": "Downtempo",
    "broken-beat-nu-jazz": "Leftfield",
    "dub-techno": "Dub Techno",
    "ambient-electronic": "Ambient",
    "techno": "Techno",
    "house": "House",
    "electro": "Electro",
    "disco-nu-disco": "Nu Disco",
    "acid-house": "Acid",
    "detroit": "Detroit Techno",
    "leftfield": "Leftfield",
    "experimental-electronic": "Experimental",
    "drum-and-bass": "Drum & Bass",
    "breaks-electro": "Breaks",
}

# Default genres to scrape (matching the user's taste profile)
DEFAULT_GENRES = [
    "deep-house",
    "minimal-tech-house",
    "downtempo-balearic",
    "broken-beat-nu-jazz",
    "dub-techno",
    "ambient-electronic",
]


class JunoFetcher(BaseSourceFetcher):
    """Scrapes new releases from juno.co.uk."""

    name = "juno"

    BASE_URL = "https://www.juno.co.uk"
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
            genres: List of Juno genre slugs to scrape.
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
        """Fetch a page, trying requests first, falling back to curl.

        Returns empty string if Cloudflare challenge is detected.
        Juno is behind Cloudflare as of March 2026 — simple HTTP
        requests won't work without a headless browser.
        """
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
        html = self._curl_get(url, timeout=timeout)

        # Check curl result for Cloudflare too
        if html and ("Just a moment" in html or "challenge-platform" in html):
            return ""

        return html

    # ── Parsing ───────────────────────────────────────────

    def _parse_products_page(self, html, default_genre="Electronic"):
        """Parse product listings from a Juno HTML page.

        Juno uses several possible structures:
        1. JSON-LD structured data
        2. Product divs with structured class names
        3. Table-based product listings

        Returns list of release dicts.
        """
        if not html or len(html) < 500:
            return []

        releases = []
        soup = BeautifulSoup(html, "html.parser")

        # Strategy 1: JSON-LD structured data
        releases = self._parse_json_ld(soup, default_genre)
        if releases:
            return releases

        # Strategy 2: Product container divs
        releases = self._parse_product_containers(soup, default_genre)
        if releases:
            return releases

        # Strategy 3: Product grid/list items
        releases = self._parse_product_grid(soup, default_genre)
        if releases:
            return releases

        # Strategy 4: Table-based layout
        releases = self._parse_product_table(soup, default_genre)
        if releases:
            return releases

        # Strategy 5: Fallback link-based extraction
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
                if data.get("@type") in (
                    "Product", "MusicAlbum", "MusicRecording", "MusicRelease"
                ):
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
        """Normalize a JSON-LD product item from Juno."""
        if not isinstance(item, dict):
            return None

        name = (item.get("name") or "").strip()
        if not name:
            return None

        # Artist
        artist = ""
        for key in ("byArtist", "artist", "brand", "manufacturer"):
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

        # Parse "Artist - Title" from combined name
        if not artist and " - " in name:
            artist, name = name.split(" - ", 1)
            artist = artist.strip()
            name = name.strip()

        if not artist:
            return None

        # Label
        label = ""
        for key in ("recordLabel", "publisher", "brand"):
            val = item.get(key)
            if isinstance(val, dict):
                label = (val.get("name") or "").strip()
            elif isinstance(val, str):
                label = val.strip()
            if label:
                break

        # URL
        url = (item.get("url") or item.get("@id") or "").strip()
        if url and not url.startswith("http"):
            url = f"{self.BASE_URL}{url}"

        # Date
        date = (
            item.get("datePublished")
            or item.get("releaseDate")
            or ""
        )
        date = self._parse_date(date) if date else datetime.now().strftime("%Y-%m-%d")

        # Genre
        genre_tags = []
        genre_val = item.get("genre")
        if isinstance(genre_val, list):
            genre_tags = genre_val
        elif isinstance(genre_val, str):
            genre_tags = [genre_val]
        genre = classify_genre(genre_tags) if genre_tags else default_genre

        # Catalog number
        catalog = (item.get("catalogNumber") or item.get("sku") or "").strip()

        # Format
        fmt = self._detect_format(item.get("musicReleaseFormat", ""))

        source_id = url or f"{artist}-{name}"

        return self.make_release(
            source="juno",
            source_id=f"juno:{source_id}",
            title=name,
            artist=artist,
            label=label,
            genre=genre,
            date=date,
            source_url=url,
            catalog_number=catalog or None,
            format_type=fmt,
        )

    def _parse_product_containers(self, soup, default_genre):
        """Parse product container elements (Juno's primary layout)."""
        releases = []

        # Try various selectors Juno may use
        selectors = [
            "div.dv-item",
            "div.product-item",
            "div.product",
            "div[data-product-id]",
            "li.product",
            "article.product",
            "div.release",
            "div.product_info",
            "div.row.product",
        ]

        items = []
        for selector in selectors:
            items = soup.select(selector)
            if items:
                break

        for item in items:
            rel = self._parse_single_product(item, default_genre)
            if rel:
                releases.append(rel)

        return releases

    def _parse_single_product(self, item, default_genre):
        """Parse a single product container element."""
        # ── Artist ──
        artist = ""
        artist_selectors = [
            ".product-artist a",
            ".artist a",
            ".juno-artist a",
            "a.text-artist",
            ".product_info_artist a",
            "h3.product-artist",
            ".product-title .artist",
            "a[href*='/artists/']",
        ]
        for sel in artist_selectors:
            el = item.select_one(sel)
            if el:
                artist = el.get_text(strip=True)
                break

        # ── Title ──
        title = ""
        title_selectors = [
            ".product-title a",
            ".title a",
            ".juno-title a",
            "a.text-title",
            ".product_info_title a",
            "h4.product-title",
            ".product-title .title",
            "a[href*='/products/']",
        ]
        for sel in title_selectors:
            el = item.select_one(sel)
            if el:
                title = el.get_text(strip=True)
                break

        if not title and not artist:
            return None

        # ── Label ──
        label = ""
        label_selectors = [
            ".product-label a",
            ".label a",
            ".juno-label a",
            "a.text-label",
            ".product_info_label a",
            "a[href*='/labels/']",
        ]
        for sel in label_selectors:
            el = item.select_one(sel)
            if el:
                label = el.get_text(strip=True)
                break

        # ── Catalog Number ──
        catalog = ""
        cat_selectors = [
            ".product-cat",
            ".cat-number",
            ".catalogue",
            ".product_info_cat",
            ".catno",
        ]
        for sel in cat_selectors:
            el = item.select_one(sel)
            if el:
                catalog = el.get_text(strip=True)
                break

        # ── URL ──
        url = ""
        link = (
            item.select_one("a[href*='/products/']")
            or item.select_one("a.product-title")
            or item.select_one("h4 a")
            or item.select_one("a")
        )
        if link and link.get("href"):
            url = link["href"]
            if not url.startswith("http"):
                url = f"{self.BASE_URL}{url}"

        # ── Format ──
        fmt = ""
        format_selectors = [
            ".product-format",
            ".format",
            ".media-type",
            ".product_info_format",
        ]
        for sel in format_selectors:
            el = item.select_one(sel)
            if el:
                fmt = self._detect_format(el.get_text(strip=True))
                break

        # ── BPM ──
        bpm = None
        bpm_selectors = [".bpm", ".product-bpm", ".track-bpm"]
        for sel in bpm_selectors:
            el = item.select_one(sel)
            if el:
                bpm_text = el.get_text(strip=True)
                bpm_match = re.search(r"(\d{2,3})", bpm_text)
                if bpm_match:
                    bpm_val = int(bpm_match.group(1))
                    if 60 <= bpm_val <= 200:
                        bpm = bpm_val
                break

        # ── Genre tags ──
        genre_tags = []
        genre_selectors = [
            ".product-genre a",
            ".genre a",
            ".tags a",
            ".product_info_genre a",
        ]
        for sel in genre_selectors:
            els = item.select(sel)
            if els:
                genre_tags = [el.get_text(strip=True) for el in els]
                break

        genre = classify_genre(genre_tags) if genre_tags else default_genre

        # ── Date ──
        date = ""
        date_selectors = [".product-date", ".release-date", ".date"]
        for sel in date_selectors:
            el = item.select_one(sel)
            if el:
                date = self._parse_date(el.get_text(strip=True))
                break
        if not date:
            date = datetime.now().strftime("%Y-%m-%d")

        # Try data attributes
        for attr in ("data-product", "data-item"):
            raw = item.get(attr)
            if raw:
                try:
                    pdata = json.loads(raw)
                    artist = artist or pdata.get("artist", "")
                    title = title or pdata.get("title", pdata.get("name", ""))
                    label = label or pdata.get("label", "")
                    catalog = catalog or pdata.get("catno", "")
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

        return self.make_release(
            source="juno",
            source_id=f"juno:{source_id}",
            title=title,
            artist=artist,
            label=label,
            genre=genre,
            date=date,
            source_url=url,
            bpm=bpm,
            catalog_number=catalog or None,
            format_type=fmt,
        )

    def _parse_product_grid(self, soup, default_genre):
        """Parse grid-based product layout."""
        releases = []

        # Juno sometimes renders a grid of product cards
        cards = (
            soup.select("div.product-card")
            or soup.select("div.grid-item")
            or soup.select("div.col-product")
        )

        for card in cards:
            # Extract basic info from card structure
            link = card.select_one("a[href*='/products/']") or card.select_one("a")
            if not link:
                continue

            text = link.get_text(strip=True)
            url = link.get("href", "")
            if not url.startswith("http"):
                url = f"{self.BASE_URL}{url}"

            artist, title = "", text
            if " - " in text:
                artist, title = text.split(" - ", 1)

            if not title:
                continue

            releases.append(self.make_release(
                source="juno",
                source_id=f"juno:{url}",
                title=title.strip(),
                artist=artist.strip() if artist else "Unknown",
                label="",
                genre=default_genre,
                date=datetime.now().strftime("%Y-%m-%d"),
                source_url=url,
            ))

        return releases

    def _parse_product_table(self, soup, default_genre):
        """Parse table-based product layout."""
        releases = []

        tables = soup.select("table.product-list") or soup.select("table")
        for table in tables:
            rows = table.select("tr")
            for row in rows:
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

                # Try to find other cells
                tds = row.find_all("td")
                label = ""
                for td in tds:
                    label_link = td.select_one("a[href*='/labels/']")
                    if label_link:
                        label = label_link.get_text(strip=True)
                        break

                if not title:
                    continue

                releases.append(self.make_release(
                    source="juno",
                    source_id=f"juno:{url}",
                    title=title.strip(),
                    artist=artist.strip() if artist else "Unknown",
                    label=label,
                    genre=default_genre,
                    date=datetime.now().strftime("%Y-%m-%d"),
                    source_url=url,
                ))

        return releases

    def _parse_product_links(self, soup, default_genre):
        """Fallback: extract product info from links to /products/."""
        releases = []
        seen_urls = set()

        links = soup.select("a[href*='/products/']")
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

            if not title:
                continue

            releases.append(self.make_release(
                source="juno",
                source_id=f"juno:{url}",
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
        next_link = (
            soup.select_one("a.next")
            or soup.select_one("a[rel='next']")
            or soup.select_one("li.next a")
            or soup.select_one(".pagination a.next")
            or soup.select_one("a[title='Next']")
        )
        return next_link is not None

    # ── Date parsing ──────────────────────────────────────

    @staticmethod
    def _parse_date(date_str):
        """Parse various date formats to YYYY-MM-DD."""
        if not date_str:
            return datetime.now().strftime("%Y-%m-%d")

        date_str = date_str.strip()

        # Already YYYY-MM-DD
        if re.match(r"^\d{4}-\d{2}-\d{2}$", date_str):
            return date_str

        # Common formats
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
            "%d-%m-%Y",
        ]:
            try:
                dt = datetime.strptime(date_str, fmt)
                return dt.strftime("%Y-%m-%d")
            except ValueError:
                continue

        # Extract YYYY-MM-DD
        m = re.search(r"(\d{4}-\d{2}-\d{2})", date_str)
        if m:
            return m.group(1)

        return datetime.now().strftime("%Y-%m-%d")

    # ── Format detection ──────────────────────────────────

    @staticmethod
    def _detect_format(text):
        """Detect release format from descriptive text."""
        if not text:
            return ""
        t = text.lower()
        if "lp" in t or "album" in t:
            return "LP"
        if "ep" in t:
            return "EP"
        if "single" in t or '12"' in t or "12\"" in t or "12 inch" in t:
            return "Single"
        if "vinyl" in t:
            return "Vinyl"
        if "cd" in t:
            return "CD"
        if "digital" in t:
            return "Digital"
        return ""

    # ── Public API ────────────────────────────────────────

    def fetch_new_releases(self, cutoff_date=None, max_pages=3):
        """Scrape the general new releases page.

        Args:
            cutoff_date: datetime -- only include releases on or after this date.
            max_pages: Max pages to scrape.

        Returns:
            List of unified release dicts.
        """
        if cutoff_date is None:
            cutoff_date = datetime.now() - timedelta(days=90)

        cutoff_str = cutoff_date.strftime("%Y-%m-%d")
        all_releases = []

        for page in range(1, max_pages + 1):
            url = f"{self.BASE_URL}/new-releases/?page={page}"
            print(f"  ▸ Juno: new releases page {page}")

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

    def fetch_by_genre(self, genre_slug, cutoff_date=None, max_pages=3):
        """Scrape releases from a specific genre page on Juno.

        Args:
            genre_slug: Juno genre slug (e.g. 'deep-house', 'minimal-tech-house')
            cutoff_date: datetime
            max_pages: Max pages per genre

        Returns:
            List of unified release dicts.
        """
        if cutoff_date is None:
            cutoff_date = datetime.now() - timedelta(days=90)

        cutoff_str = cutoff_date.strftime("%Y-%m-%d")
        default_genre = JUNO_GENRES.get(genre_slug, "Electronic")
        all_releases = []

        for page in range(1, max_pages + 1):
            # Juno genre URLs: /all/{genre-slug}/ or /{genre-slug}/
            url = f"{self.BASE_URL}/all/{genre_slug}/?page={page}"
            print(f"  ▸ Juno: {genre_slug} page {page}")

            self._throttle()
            html = self._fetch_page(url)

            if not html or "Just a moment" in html:
                print(f"    ✗ Cloudflare challenge or empty response")
                break

            soup = BeautifulSoup(html, "html.parser")
            releases = self._parse_products_page(html, default_genre=default_genre)

            if not releases:
                # Try alternate URL pattern
                alt_url = f"{self.BASE_URL}/{genre_slug}/?page={page}"
                self._throttle()
                html = self._fetch_page(alt_url)
                if html and "Just a moment" not in html:
                    soup = BeautifulSoup(html, "html.parser")
                    releases = self._parse_products_page(
                        html, default_genre=default_genre
                    )

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
        """Search Juno for a specific artist's releases.

        Args:
            artist_name: Artist name to search.
            cutoff_date: datetime

        Returns:
            List of unified release dicts.
        """
        cutoff_str = cutoff_date.strftime("%Y-%m-%d")
        url = f"{self.BASE_URL}/search/?q[all][]={quote_plus(artist_name)}"

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
        """Main entry point: fetch from all configured genres.

        Note: As of March 2026, Juno is behind Cloudflare challenge pages.
        Simple HTTP requests (requests + curl) can't bypass this.
        The fetcher will gracefully return 0 releases until a headless
        browser solution (e.g. cloudscraper, Playwright) is implemented.

        Args:
            cutoff_date: datetime. Defaults to 90 days ago.
            max_pages: Pages per genre.

        Returns:
            List of unified release dicts (deduplicated).
        """
        if cutoff_date is None:
            cutoff_date = datetime.now() - timedelta(days=90)

        all_releases = {}

        # Quick test: check if Juno is accessible
        print("  ── Juno: Checking accessibility ──")
        test_html = self._fetch_page(f"{self.BASE_URL}/", timeout=10)
        if not test_html:
            print("    ⚠ Juno: blocked by Cloudflare challenge — skipping")
            print("    (requires headless browser to bypass, not yet implemented)")
            return []

        # Fetch general new releases first
        print("  ── Juno: New Releases ──")
        new_releases = self.fetch_new_releases(cutoff_date, max_pages=max_pages)
        for r in new_releases:
            all_releases[r["id"]] = r

        # Then fetch by each configured genre
        for genre_slug in self._genres:
            print(f"  ── Juno: Genre {genre_slug} ──")
            genre_releases = self.fetch_by_genre(
                genre_slug, cutoff_date, max_pages=max_pages
            )
            for r in genre_releases:
                all_releases[r["id"]] = r

        result = list(all_releases.values())
        print(f"  ✓ Juno total: {len(result)} unique releases")
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
    fetcher = JunoFetcher(genres=["deep-house", "minimal-tech-house"])
    releases = fetcher.fetch_all(cutoff, max_pages=1)
    print(f"\nFound {len(releases)} releases")
    for r in releases[:10]:
        print(
            f"  {r['date']} | {r['artist']} - {r['title']} "
            f"[{r['label']}] ({r['genre']}) "
            f"{'BPM:' + str(r['bpm']) if r.get('bpm') else ''}"
        )
