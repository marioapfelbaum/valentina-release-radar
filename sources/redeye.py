"""
Redeye Records source fetcher.
Scrapes new releases from redeyerecords.co.uk, a Bristol-based
electronic music distributor/shop.

Uses HTML scraping with requests + BeautifulSoup.
No Cloudflare protection, but robots.txt requests 10s crawl delay.

URL structure:
  - /{genre}/new-releases        New releases per genre
  - /{genre}/weekly-chart        Weekly chart per genre
  - /record-label-releases/{id}  Per-label releases

Genre slugs:
  - house-disco
  - techno-electro
  - experimental
  - balearic-and-downtempo
  - funk-hip-hop-soul

HTML product structure:
  <div id="{productId}" class="releaseGrid grid">
    <div class="left">
      <img src="/imagery/{productId}-2.jpg" />
      <div class="type">Exp. 27 Mar / Out Of Stock!</div>
    </div>
    <div class="right">
      <div class="listing">
        <p class="artist">Artist - Title</p>
        <p class="tracks">A1 – Track 1...</p>
        <p class="label">{catNo}<br><a href='/record-label-releases/...'>Label</a></p>
      </div>
      <a class="link" href="/vinyl/{itemId}-{catNo}-{slug}/">View Full Info</a>
    </div>
  </div>
"""

import re
import subprocess
import sys
from datetime import datetime, timedelta

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

BASE_URL = "https://www.redeyerecords.co.uk"

# Genre slugs -> default genre classification
REDEYE_GENRES = {
    "house-disco": "House",
    "techno-electro": "Techno",
    "experimental": "Experimental",
    "balearic-and-downtempo": "Downtempo",
    "funk-hip-hop-soul": "Funky House",
}

# Default genres to scrape (matching the user's taste profile)
DEFAULT_GENRES = [
    "house-disco",
    "techno-electro",
    "experimental",
    "balearic-and-downtempo",
]


class RedeyeFetcher(BaseSourceFetcher):
    """Scrapes new releases from redeyerecords.co.uk."""

    name = "redeye"

    def __init__(self, genres=None, rate_limit=10.0):
        """Rate limit set to 10s to respect robots.txt Crawl-delay: 10."""
        super().__init__(rate_limit=rate_limit)
        self._genres = genres or list(DEFAULT_GENRES)
        self._seen_ids = set()

    # ── HTTP helpers ──────────────────────────────────────

    def _fetch_page(self, url, timeout=30):
        """Fetch an HTML page from redeyerecords.co.uk."""
        self._throttle()
        try:
            headers = {
                "User-Agent": (
                    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/121.0.0.0 Safari/537.36"
                ),
                "Accept": "text/html,application/xhtml+xml,*/*",
                "Accept-Language": "en-US,en;q=0.9",
            }
            resp = requests.get(url, headers=headers, timeout=timeout)
            if resp.status_code == 200 and len(resp.text) > 500:
                return resp.text
        except requests.RequestException:
            pass

        # Fallback to curl
        try:
            result = subprocess.run(
                ["curl", "-s", "-L", "--max-time", str(timeout),
                 "-H", "User-Agent: Mozilla/5.0",
                 "-H", "Accept: text/html,*/*",
                 url],
                capture_output=True, text=True, timeout=timeout + 10
            )
            if result.stdout and len(result.stdout) > 500:
                return result.stdout
        except (subprocess.TimeoutExpired, FileNotFoundError):
            pass

        return ""

    # ── Parsing ───────────────────────────────────────────

    def _parse_products(self, html, default_genre="Electronic"):
        """Parse product listings from Redeye HTML page."""
        if not html:
            return []

        soup = BeautifulSoup(html, "html.parser")
        releases = []

        # Each product is a div.releaseGrid
        grids = soup.find_all("div", class_="releaseGrid")
        if not grids:
            # Fallback: try finding divs with numeric IDs
            grids = soup.find_all("div", id=re.compile(r'^\d+$'))

        for grid in grids:
            rel = self._parse_grid(grid, default_genre)
            if rel and rel["id"] not in self._seen_ids:
                self._seen_ids.add(rel["id"])
                releases.append(rel)

        return releases

    def _parse_grid(self, grid, default_genre):
        """Parse a single releaseGrid div."""
        # Product ID from div id
        product_id = grid.get("id", "")
        if not product_id or not product_id.isdigit():
            return None

        # Check if pre-order
        is_preorder = "preorder" in (grid.get("class") or [])

        # Artist + Title from p.artist
        artist_p = grid.find("p", class_="artist")
        if not artist_p:
            return None

        raw_text = artist_p.get_text(strip=True)
        artist, title = self._split_artist_title(raw_text)
        if not title:
            return None

        # Label from p.label > a
        label = ""
        catalog = ""
        label_p = grid.find("p", class_="label")
        if label_p:
            label_link = label_p.find("a")
            if label_link:
                label = label_link.get_text(strip=True)

            # Catalog number is text before <br>
            label_text = label_p.get_text(separator="\n").strip()
            lines = [l.strip() for l in label_text.split("\n") if l.strip()]
            if lines and lines[0] != label:
                catalog = lines[0]

        # Product URL
        source_url = ""
        link_el = grid.find("a", class_="link")
        if link_el and link_el.get("href"):
            href = link_el["href"]
            source_url = f"{BASE_URL}{href}" if href.startswith("/") else href
        else:
            # Try buy link
            buy_el = grid.find("a", class_="buy")
            if buy_el and buy_el.get("href"):
                href = buy_el["href"]
                source_url = f"{BASE_URL}{href}" if href.startswith("/") else href

        # Date: try to extract from "Exp. DD Mon" for pre-orders
        date = datetime.now().strftime("%Y-%m-%d")
        type_div = grid.find("div", class_="type")
        if type_div:
            type_text = type_div.get_text(strip=True)
            exp_date = self._parse_expected_date(type_text)
            if exp_date:
                date = exp_date

        return self.make_release(
            source="redeye",
            source_id=f"re:{product_id}",
            title=title,
            artist=artist or "Various",
            label=label,
            genre=default_genre,
            date=date,
            source_url=source_url,
            catalog_number=catalog or None,
        )

    @staticmethod
    def _split_artist_title(raw_text):
        """Split 'Artist - Title' into (artist, title)."""
        if " - " in raw_text:
            artist, title = raw_text.split(" - ", 1)
            return artist.strip(), title.strip()
        return "", raw_text.strip()

    @staticmethod
    def _parse_expected_date(text):
        """Parse expected date from type div: 'Exp. 27 Mar'."""
        if not text:
            return ""

        m = re.search(
            r'Exp\.?\s*(\d{1,2})\s+(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)',
            text, re.I
        )
        if m:
            try:
                day = int(m.group(1))
                month_str = m.group(2)
                year = datetime.now().year
                dt = datetime.strptime(f"{day} {month_str} {year}", "%d %b %Y")
                # If date is in the past by more than 6 months, it's next year
                if dt < datetime.now() - timedelta(days=180):
                    dt = dt.replace(year=year + 1)
                return dt.strftime("%Y-%m-%d")
            except ValueError:
                pass

        return ""

    # ── Public API ────────────────────────────────────────

    def fetch_by_genre(self, genre_slug, cutoff_date=None, max_pages=1):
        """Fetch new releases from a specific genre on Redeye.

        Only fetches page 1 to respect the 10s crawl delay.

        Args:
            genre_slug: Redeye genre slug.
            cutoff_date: datetime
            max_pages: Max pages (default 1 due to crawl delay).

        Returns:
            List of unified release dicts.
        """
        if cutoff_date is None:
            cutoff_date = datetime.now() - timedelta(days=90)

        default_genre = REDEYE_GENRES.get(genre_slug, "Electronic")
        all_releases = []

        for page in range(1, max_pages + 1):
            url = f"{BASE_URL}/{genre_slug}/new-releases"
            if page > 1:
                url += f"?page={page}"

            print(f"  ▸ Redeye: {genre_slug} page {page}")
            html = self._fetch_page(url)

            if not html:
                print(f"    ✗ No response")
                break

            releases = self._parse_products(html, default_genre)
            if not releases:
                print(f"    → 0 releases")
                break

            all_releases.extend(releases)
            print(f"    → {len(releases)} releases")

        return all_releases

    def fetch_by_artist(self, artist_name, cutoff_date):
        """Not available."""
        return []

    def fetch_all(self, cutoff_date=None, max_pages=1):
        """Main entry point: fetch from all configured genres.

        Uses only page 1 per genre to respect 10s crawl delay.
        4 genres × 50 items/page = ~200 releases per run.

        Args:
            cutoff_date: datetime. Defaults to 90 days ago.
            max_pages: Pages per genre (default 1).

        Returns:
            List of unified release dicts (deduplicated).
        """
        if cutoff_date is None:
            cutoff_date = datetime.now() - timedelta(days=90)

        self._seen_ids.clear()
        all_releases = []

        for genre in self._genres:
            print(f"  ── Redeye: {genre} ──")
            releases = self.fetch_by_genre(genre, cutoff_date, max_pages)
            all_releases.extend(releases)

        print(f"  ✓ Redeye total: {len(all_releases)} unique releases")
        return all_releases


if __name__ == "__main__":
    cutoff = datetime.now() - timedelta(days=60)
    fetcher = RedeyeFetcher()
    releases = fetcher.fetch_all(cutoff, max_pages=1)
    print(f"\nFound {len(releases)} releases")
    for r in releases[:15]:
        print(
            f"  {r['date']} | {r['artist']} - {r['title']} "
            f"[{r['label']}] ({r['genre']}) [{r.get('catalog_number', '')}]"
        )
