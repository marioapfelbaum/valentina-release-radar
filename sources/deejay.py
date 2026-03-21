"""
Deejay.de source fetcher.
Scrapes new releases from deejay.de, a major German electronic music vinyl shop.

Uses HTML scraping with requests + BeautifulSoup (no Cloudflare protection).

URL structure:
  - /m_All/sm_News              All genres, new releases
  - /m_House/sm_News            House new releases
  - /m_Techno/sm_News           Techno new releases
  - /m_Beats/sm_News            Beats (Downtempo/Broken/Electro)
  - /m_House/sm_News/nheit_7    Only last 7 days
  - /m_House/sm_News/sort_voe   Sort by release date
  - /m_House/sm_News/page_2     Pagination

HTML product structure:
  <article id="a{PRODUCT_ID}" class="clearfix product">
    <h2 class="artist"><a>Artist Name</a></h2>
    <h3 class="title"><a>Release Title</a></h3>
    <strong>CatalogNumber</strong>
    <a href="/labels/...">Label Name</a>
    <span class="date">DD.MM.YYYY</span>
    <span class="medium">...</span>
    <a href="/Artist_Title_CatNo_Vinyl__ID">Product link</a>
  </article>
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

BASE_URL = "https://www.deejay.de"

# Genre slugs -> default genre classification
# Focused on genres matching the user's taste profile
DEEJAY_GENRES = {
    "House": "House",
    "Techno": "Techno",
    "Beats": "Downtempo",
    "Electro": "Electro",
    "Exclusive": "Electronic",
}

# Default genres to scrape
DEFAULT_GENRES = ["House", "Techno", "Beats", "Electro"]


class DeejayFetcher(BaseSourceFetcher):
    """Scrapes new releases from deejay.de."""

    name = "deejay"

    def __init__(self, genres=None, rate_limit=2.0):
        super().__init__(rate_limit=rate_limit)
        self._genres = genres or list(DEFAULT_GENRES)
        self._seen_ids = set()
        self._session = requests.Session()
        self._session.headers.update({
            "User-Agent": (
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/121.0.0.0 Safari/537.36"
            ),
            "Accept": (
                "text/html,application/xhtml+xml,application/xml;"
                "q=0.9,image/avif,image/webp,*/*;q=0.8"
            ),
            "Accept-Language": "en-US,en;q=0.9,de;q=0.8",
        })

    # ── HTTP helpers ──────────────────────────────────────

    def _fetch_page(self, url, timeout=25):
        """Fetch an HTML page from deejay.de."""
        self._throttle()
        try:
            resp = self._session.get(url, timeout=timeout)
            if resp.status_code == 200 and len(resp.text) > 500:
                return resp.text
        except requests.RequestException:
            pass

        # Fallback to curl
        try:
            result = subprocess.run(
                ["curl", "-s", "-L", "--max-time", str(timeout),
                 "-H", "User-Agent: Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7)",
                 "-H", "Accept: text/html,*/*",
                 "-H", "Accept-Language: en-US,en;q=0.9,de;q=0.8",
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
        """Parse product listings from deejay.de HTML page."""
        if not html:
            return []

        soup = BeautifulSoup(html, "html.parser")
        releases = []

        # Each product is an <article> with class "product"
        articles = soup.find_all("article", class_="product")
        if not articles:
            # Fallback: try finding articles by id pattern
            articles = soup.find_all("article", id=re.compile(r'^a\d+'))

        for article in articles:
            rel = self._parse_article(article, default_genre)
            if rel and rel["id"] not in self._seen_ids:
                self._seen_ids.add(rel["id"])
                releases.append(rel)

        return releases

    def _parse_article(self, article, default_genre):
        """Parse a single product <article> element."""
        # Product ID from article id="a{ID}"
        article_id = article.get("id", "")
        product_id = article_id.lstrip("a") if article_id.startswith("a") else ""
        if not product_id:
            return None

        # Artist from h2.artist > a
        artist = ""
        artist_el = article.find("h2", class_="artist")
        if artist_el:
            artist_link = artist_el.find("a")
            if artist_link:
                artist = artist_link.get_text(strip=True)
            else:
                artist = artist_el.get_text(strip=True)

        # Title from h3.title > a
        title = ""
        title_el = article.find("h3", class_="title")
        if title_el:
            title_link = title_el.find("a")
            if title_link:
                title = title_link.get_text(strip=True)
            else:
                title = title_el.get_text(strip=True)

        if not title:
            return None

        # Label — look for label link or text near catalog number
        label = ""
        catalog = ""

        # Catalog number is often in <strong>
        strong = article.find("strong")
        if strong:
            catalog = strong.get_text(strip=True)

        # Label: find links that are not artist/title links
        for a in article.find_all("a"):
            href = a.get("href", "")
            text = a.get_text(strip=True)
            # Label links typically have /labels/ or are near the catalog
            if "/labels/" in href or "/label/" in href:
                label = text
                break

        # If no label link found, try to find label text near catalog
        if not label and catalog:
            # Label name often follows the catalog number as a sibling text/link
            if strong:
                next_sibling = strong.next_sibling
                while next_sibling:
                    if hasattr(next_sibling, 'name') and next_sibling.name == 'a':
                        label = next_sibling.get_text(strip=True)
                        break
                    next_sibling = getattr(next_sibling, 'next_sibling', None)

        # Date from span.date (format: DD.MM.YYYY or "Release unknown")
        date = ""
        date_span = article.find("span", class_="date")
        if date_span:
            date_text = date_span.get_text(strip=True)
            date = self._parse_date(date_text)

        if not date:
            date = datetime.now().strftime("%Y-%m-%d")

        # Product URL
        source_url = ""
        # Find the main product link (links to /Artist_Title_...__ID)
        product_link = article.find("a", href=re.compile(rf'__{product_id}$'))
        if product_link:
            href = product_link.get("href", "")
            source_url = f"{BASE_URL}{href}" if href.startswith("/") else href
        elif title_el:
            title_link = title_el.find("a")
            if title_link and title_link.get("href"):
                href = title_link["href"]
                source_url = f"{BASE_URL}{href}" if href.startswith("/") else href

        # Format from span.medium
        format_type = ""
        medium_span = article.find("span", class_="medium")
        if medium_span:
            format_type = self._detect_format(medium_span.get_text(strip=True))

        # Genre classification
        genre = default_genre

        return self.make_release(
            source="deejay",
            source_id=f"dj:{product_id}",
            title=title,
            artist=artist or "Various",
            label=label,
            genre=genre,
            date=date,
            source_url=source_url,
            catalog_number=catalog or None,
            format_type=format_type,
        )

    # ── Date parsing ──────────────────────────────────────

    @staticmethod
    def _parse_date(date_text):
        """Parse deejay.de date format: DD.MM.YYYY or 'Release unknown'."""
        if not date_text or "unknown" in date_text.lower():
            return datetime.now().strftime("%Y-%m-%d")

        # DD.MM.YYYY format
        m = re.match(r'(\d{1,2})\.(\d{1,2})\.(\d{4})', date_text.strip())
        if m:
            try:
                dt = datetime(int(m.group(3)), int(m.group(2)), int(m.group(1)))
                return dt.strftime("%Y-%m-%d")
            except ValueError:
                pass

        # Try other common formats
        for fmt in ["%Y-%m-%d", "%d/%m/%Y", "%d-%m-%Y"]:
            try:
                dt = datetime.strptime(date_text.strip(), fmt)
                return dt.strftime("%Y-%m-%d")
            except ValueError:
                continue

        return datetime.now().strftime("%Y-%m-%d")

    @staticmethod
    def _detect_format(text):
        """Detect release format from text."""
        if not text:
            return ""
        t = text.lower()
        if "lp" in t or "album" in t:
            return "LP"
        if "ep" in t:
            return "EP"
        if '12"' in t or "12 inch" in t or "12inch" in t:
            return "Single"
        if '7"' in t or "7 inch" in t:
            return "Single"
        if "vinyl" in t:
            return "Vinyl"
        if "cd" in t:
            return "CD"
        if "excl" in t:
            return "Vinyl"
        return "Vinyl"

    # ── Public API ────────────────────────────────────────

    def fetch_by_genre(self, genre_slug, cutoff_date=None, max_pages=3):
        """Fetch new releases from a specific genre on deejay.de.

        Args:
            genre_slug: Genre name (House, Techno, Beats, Exclusive).
            cutoff_date: datetime
            max_pages: Max pages per genre (40 products/page).

        Returns:
            List of unified release dicts.
        """
        if cutoff_date is None:
            cutoff_date = datetime.now() - timedelta(days=90)

        cutoff_str = cutoff_date.strftime("%Y-%m-%d")
        default_genre = DEEJAY_GENRES.get(genre_slug, "Electronic")
        all_releases = []

        for page in range(1, max_pages + 1):
            # Build URL: /m_{Genre}/sm_News/nheit_14/sort_voe/page_{N}
            url_parts = [f"{BASE_URL}/m_{genre_slug}/sm_News/nheit_14/sort_voe"]
            if page > 1:
                url_parts.append(f"/page_{page}")
            url = "".join(url_parts)

            print(f"  ▸ Deejay.de: {genre_slug} page {page}")
            html = self._fetch_page(url)

            if not html:
                print(f"    ✗ No response")
                break

            releases = self._parse_products(html, default_genre)
            if not releases:
                print(f"    → 0 releases (no more results)")
                break

            # Filter by cutoff date
            filtered = [r for r in releases if r["date"] >= cutoff_str]
            all_releases.extend(filtered)
            print(f"    → {len(filtered)} releases")

            # If oldest release is before cutoff, stop paginating
            if releases:
                oldest = min(r["date"] for r in releases)
                if oldest < cutoff_str:
                    break

        return all_releases

    def fetch_by_artist(self, artist_name, cutoff_date):
        """Not available."""
        return []

    def fetch_all(self, cutoff_date=None, max_pages=4):
        """Main entry point: fetch from all configured genres.

        Args:
            cutoff_date: datetime. Defaults to 90 days ago.
            max_pages: Pages per genre (40 products/page).

        Returns:
            List of unified release dicts (deduplicated).
        """
        if cutoff_date is None:
            cutoff_date = datetime.now() - timedelta(days=90)

        self._seen_ids.clear()
        all_releases = []

        for genre in self._genres:
            print(f"  ── Deejay.de: {genre} ──")
            releases = self.fetch_by_genre(genre, cutoff_date, max_pages)
            all_releases.extend(releases)

        print(f"  ✓ Deejay.de total: {len(all_releases)} unique releases")
        return all_releases


if __name__ == "__main__":
    cutoff = datetime.now() - timedelta(days=30)
    fetcher = DeejayFetcher()
    releases = fetcher.fetch_all(cutoff, max_pages=1)
    print(f"\nFound {len(releases)} releases")
    for r in releases[:15]:
        print(
            f"  {r['date']} | {r['artist']} - {r['title']} "
            f"[{r['label']}] ({r['genre']}) [{r.get('catalog_number', '')}]"
        )
