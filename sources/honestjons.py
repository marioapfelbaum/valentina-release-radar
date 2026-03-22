"""
Honest Jon's source fetcher.

Scrapes new releases from honestjons.com, London's legendary record shop
(est. 1974, Portobello Road). Strong curation for House, Broken Beat,
Jazz-Electronic, Deep House, and Detroit-influenced music.

URL structure:
  - /shop/category/Dance/New_fresh_back_in   New arrivals
  - /shop/category/Dance/Chicago_Detroit_House
  - /shop/category/Dance/Jazzy_Bruk
  - /shop/category/Dance/Moodymann_Theo_Parrish
  - /shop/category/Dance/House
  - Pagination: /2, /3 appended to URL

HTML structure (inside div.item):
  - Artist:  h2 a
  - Title:   h3 a
  - Label:   h4 a
  - Format:  button text (LP, EP, 12", 7", CD, etc.)
  - No release dates in HTML
  - No Cloudflare protection
"""

import re
import subprocess
import sys
import time
from datetime import datetime, timedelta

try:
    from bs4 import BeautifulSoup
except ImportError:
    print("pip install beautifulsoup4")
    sys.exit(1)

try:
    import requests
except ImportError:
    requests = None

from .base import BaseSourceFetcher
from .genre_map import classify_genre


BASE_URL = "https://www.honestjons.com"

# Category paths mapped to default genre hints
HONESTJONS_CATEGORIES = {
    "Dance/New_fresh_back_in": "House",
    "Dance/Chicago_Detroit_House": "Deep House",
    "Dance/Jazzy_Bruk": "Broken Beat",
    "Dance/Moodymann_Theo_Parrish": "Deep House",
    "Dance/House": "House",
}

# Format keywords to detect from button text
FORMAT_PATTERNS = {
    "LP": "LP",
    "2LP": "2xLP",
    "2xLP": "2xLP",
    "3LP": "3xLP",
    "EP": "EP",
    '12"': '12"',
    '12"': '12"',
    "12 inch": '12"',
    '10"': '10"',
    '10"': '10"',
    '7"': '7"',
    '7"': '7"',
    "CD": "CD",
    "2CD": "2xCD",
    "cassette": "Cassette",
    "tape": "Cassette",
}


class HonestJonsFetcher(BaseSourceFetcher):
    """Scrapes new releases from honestjons.com."""

    name = "honestjons"

    def __init__(self, rate_limit=2.0):
        super().__init__(rate_limit=rate_limit)
        self._seen_ids = set()

    # ── HTTP helpers ────────────────────────────────────────────────

    def _curl_get(self, url, timeout=20):
        """Fetch URL via curl as fallback."""
        try:
            result = subprocess.run(
                ["curl", "-s", "-L", "--max-time", str(timeout),
                 "-H", "User-Agent: Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
                 "-H", "Accept: text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                 "-H", "Accept-Language: en-US,en;q=0.5",
                 url],
                capture_output=True, text=True, timeout=timeout + 5
            )
            return result.stdout
        except (subprocess.TimeoutExpired, FileNotFoundError):
            return ""

    def _fetch_page(self, url, timeout=20):
        """Fetch URL, trying requests first, falling back to curl."""
        self._throttle()

        if requests:
            try:
                headers = {
                    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                                  "AppleWebKit/537.36 (KHTML, like Gecko) "
                                  "Chrome/120.0.0.0 Safari/537.36",
                    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                    "Accept-Language": "en-US,en;q=0.5",
                }
                resp = requests.get(url, headers=headers, timeout=timeout)
                if resp.status_code == 200:
                    return resp.text
                print(f"    ▸ HTTP {resp.status_code} for {url}")
            except Exception as e:
                print(f"    ▸ requests failed: {e}, trying curl...")

        # Fallback to curl
        return self._curl_get(url, timeout)

    # ── Parsing ────────────────────────────────────────────────────

    def _parse_products(self, html, default_genre="House"):
        """Parse product items from an Honest Jon's category page.

        Args:
            html: Raw HTML string.
            default_genre: Genre hint from the category URL context.

        Returns:
            List of release dicts.
        """
        if not html:
            return []

        soup = BeautifulSoup(html, "html.parser")
        releases = []

        for item_div in soup.find_all("div", class_="item"):
            rel = self._parse_item(item_div, default_genre)
            if rel and rel["id"] not in self._seen_ids:
                self._seen_ids.add(rel["id"])
                releases.append(rel)

        return releases

    def _parse_item(self, item_div, default_genre):
        """Parse a single div.item element into a release dict.

        Expected structure:
          <div class="item">
            <h2><a href="/shop/...">Artist Name</a></h2>
            <h3><a href="/shop/...">Release Title</a></h3>
            <h4><a href="/shop/...">Label Name</a></h4>
            <button>Add 12" to basket</button>
          </div>
        """
        # Artist from h2 > a
        artist = ""
        h2 = item_div.find("h2")
        if h2:
            a = h2.find("a")
            if a:
                artist = a.get_text(strip=True)

        # Title from h3 > a
        title = ""
        title_href = ""
        h3 = item_div.find("h3")
        if h3:
            a = h3.find("a")
            if a:
                title = a.get_text(strip=True)
                title_href = a.get("href", "")

        if not title:
            return None

        # Label from h4 > a
        label = ""
        h4 = item_div.find("h4")
        if h4:
            a = h4.find("a")
            if a:
                label = a.get_text(strip=True)

        # Format from button text
        format_type = self._detect_format(item_div)

        # Source URL from title href
        source_url = ""
        if title_href:
            if title_href.startswith("http"):
                source_url = title_href
            else:
                source_url = BASE_URL + title_href

        # Product ID from href slug
        product_id = self._extract_product_id(title_href)
        if not product_id:
            # Fallback: hash artist + title
            product_id = f"{artist}-{title}".lower().replace(" ", "-")[:60]

        # Genre from category context
        styles = [default_genre.lower()]
        genre = classify_genre(styles)

        # Check for reissue hints in title
        reissue = self._is_reissue(title)

        # No release dates on Honest Jon's — use today
        date = datetime.now().strftime("%Y-%m-%d")

        return self.make_release(
            source="honestjons",
            source_id=f"hj:{product_id}",
            title=title,
            artist=artist,
            label=label,
            genre=genre,
            date=date,
            source_url=source_url,
            format_type=format_type,
            styles=styles,
            reissue=reissue,
            date_verified=False,
        )

    def _detect_format(self, item_div):
        """Detect release format from button text inside a product item.

        Button text is typically like "Add 12\" to basket" or "Add LP to basket".
        """
        button = item_div.find("button")
        if not button:
            return ""

        btn_text = button.get_text(strip=True).lower()

        for keyword, fmt in FORMAT_PATTERNS.items():
            if keyword.lower() in btn_text:
                return fmt

        return ""

    @staticmethod
    def _extract_product_id(href):
        """Extract a usable product ID from the item href.

        Examples:
          /shop/product/artist-name/title-name  -> "artist-name/title-name"
          /shop/12345/some-release              -> "12345"
        """
        if not href:
            return ""

        # Try numeric ID first
        match = re.search(r'/(\d{3,})(?:/|$)', href)
        if match:
            return match.group(1)

        # Use the last path segments as slug
        parts = [p for p in href.strip("/").split("/") if p]
        if len(parts) >= 2:
            # Skip "shop" prefix, take the rest
            slug_parts = [p for p in parts if p != "shop"]
            if slug_parts:
                return "/".join(slug_parts[-2:])

        return href.strip("/").replace("/", "-")[:60] if href else ""

    @staticmethod
    def _is_reissue(title):
        """Check if the title hints at a reissue."""
        return bool(re.search(
            r'\breissue\b|\brepress\b|\bre-issue\b|\bre-press\b|\bremastered\b',
            title, re.IGNORECASE
        ))

    # ── Paginated fetching ─────────────────────────────────────────

    def _fetch_category(self, category, max_pages=2):
        """Fetch releases from one Honest Jon's category with pagination.

        Args:
            category: Category path (e.g. "Dance/New_fresh_back_in").
            max_pages: Maximum pages to fetch per category.

        Returns:
            List of release dicts.
        """
        default_genre = HONESTJONS_CATEGORIES.get(category, "House")
        all_releases = []

        for page in range(1, max_pages + 1):
            if page == 1:
                url = f"{BASE_URL}/shop/category/{category}"
            else:
                url = f"{BASE_URL}/shop/category/{category}/{page}"

            html = self._fetch_page(url)
            releases = self._parse_products(html, default_genre)

            if not releases:
                break

            all_releases.extend(releases)
            print(f"    page {page}: {len(releases)} releases")

        return all_releases

    # ── Public API ──────────────────────────────────────────────────

    def fetch_by_genre(self, genre_id, cutoff_date=None, max_pages=2):
        """Fetch releases from a specific Honest Jon's category.

        Args:
            genre_id: Category path key (e.g. "Dance/Jazzy_Bruk").
                     Must be a key in HONESTJONS_CATEGORIES.
            cutoff_date: Ignored (no dates available from HTML).
            max_pages: Maximum pages to fetch.

        Returns:
            List of unified release dicts.
        """
        if genre_id not in HONESTJONS_CATEGORIES:
            print(f"  ▸ Honest Jon's: Unknown category '{genre_id}', "
                  f"valid: {', '.join(HONESTJONS_CATEGORIES.keys())}")
            return []

        print(f"  ▸ Honest Jon's: Fetching {genre_id}...")
        releases = self._fetch_category(genre_id, max_pages)
        print(f"    → {len(releases)} releases from {genre_id}")
        return releases

    def fetch_by_artist(self, artist_name, cutoff_date=None):
        """Artist search not supported on Honest Jon's.

        Returns:
            Empty list.
        """
        return []

    def fetch_all(self, cutoff_date=None, genres=None, max_pages=2):
        """Main entry point: fetch new releases from honestjons.com.

        Scrapes all configured Dance categories. Deduplicates across
        categories (same release may appear in multiple).

        Args:
            cutoff_date: Ignored (no dates in HTML).
            genres: Optional list of category keys to fetch.
                   Defaults to all HONESTJONS_CATEGORIES.
            max_pages: Maximum pages per category.

        Returns:
            List of unified release dicts (deduplicated).
        """
        self._seen_ids.clear()
        all_releases = []

        categories = genres or list(HONESTJONS_CATEGORIES.keys())

        for category in categories:
            if category not in HONESTJONS_CATEGORIES:
                continue

            print(f"  ▸ Honest Jon's: Fetching {category}...")
            releases = self._fetch_category(category, max_pages)
            all_releases.extend(releases)
            print(f"    → {len(releases)} releases from {category}")

        print(f"  ✓ Honest Jon's total: {len(all_releases)} releases "
              f"({len(self._seen_ids)} unique)")
        return all_releases


if __name__ == "__main__":
    fetcher = HonestJonsFetcher()

    print("=== Fetching all Honest Jon's categories ===")
    releases = fetcher.fetch_all(max_pages=2)
    print(f"\nFound {len(releases)} releases total")
    for r in releases[:20]:
        print(f"  {r['artist']} - {r['title']} [{r['label']}] "
              f"({r['genre']}) {r.get('format', '')}")

    print("\n=== Fetching Jazzy_Bruk only ===")
    fetcher._seen_ids.clear()
    bruk = fetcher.fetch_by_genre("Dance/Jazzy_Bruk", max_pages=1)
    print(f"\nFound {len(bruk)} Broken Beat releases")
    for r in bruk[:10]:
        print(f"  {r['artist']} - {r['title']} [{r['label']}] ({r['genre']})")
