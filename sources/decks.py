"""
Decks.de source fetcher.
Scrapes new releases from decks.de, a major German electronic music vinyl shop.

Uses HTML scraping with requests + BeautifulSoup (no Cloudflare protection).
IMPORTANT: Encoding is iso-8859-1 (latin-1), NOT utf-8.

URL structure:
  - decks.de/decks/workfloor/lists/list_db.php?wo=hon&nowstyle=zz&now_Date=lw
  - Sections: hon (House), ten (Techno), sfn (Hip Hop/Soul/Funk), jcn (Jazz/World)
  - now_Date=lw restricts to last 7 days
  - Pagination: &aktuell=0,1,2...

HTML product structure:
  <div class="oneLine" data-code="PRODUCT_ID">
    <div class="LArtist"><a>Artist Name</a></div>
    <div class="LTitel">Release Title</div>
    <div class="LLabel">Label Name</div>
    <div class="LStyle"><a>Genre1</a> <a>Genre2</a> ...</div>
    <span class="RelFeature">DD.MM.YY</span>
    <div class="LLabelcat">Catalog Number</div>
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

BASE_URL = "https://www.decks.de"
LIST_URL = BASE_URL + "/decks/workfloor/lists/list_db.php"

# Section codes -> default genre classification
DECKS_SECTIONS = {
    "hon": "House",
    "ten": "Techno",
    "sfn": "Funky House",
    "jcn": "Downtempo",
}

# Default sections to scrape
DEFAULT_SECTIONS = ["hon", "ten", "sfn", "jcn"]


class DecksFetcher(BaseSourceFetcher):
    """Scrapes new releases from decks.de."""

    name = "decks"

    def __init__(self, sections=None, rate_limit=2.0):
        super().__init__(rate_limit=rate_limit)
        self._sections = sections or list(DEFAULT_SECTIONS)
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
        """Fetch an HTML page from decks.de (iso-8859-1 encoding)."""
        self._throttle()
        try:
            resp = self._session.get(url, timeout=timeout)
            resp.encoding = "iso-8859-1"
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
                capture_output=True, timeout=timeout + 10
            )
            if result.stdout and len(result.stdout) > 500:
                # curl returns bytes; decode as iso-8859-1
                return result.stdout.decode("iso-8859-1", errors="replace")
        except (subprocess.TimeoutExpired, FileNotFoundError):
            pass

        return ""

    # ── Parsing ───────────────────────────────────────────

    def _parse_products(self, html, default_genre="Electronic"):
        """Parse product listings from decks.de HTML page."""
        if not html:
            return []

        soup = BeautifulSoup(html, "html.parser")
        releases = []

        # Each product is a div.oneLine with data-code attribute
        items = soup.find_all("div", class_="oneLine", attrs={"data-code": True})

        for item in items:
            rel = self._parse_item(item, default_genre)
            if rel and rel["id"] not in self._seen_ids:
                self._seen_ids.add(rel["id"])
                releases.append(rel)

        return releases

    def _parse_item(self, item, default_genre):
        """Parse a single product div.oneLine element."""
        # Product ID from data-code attribute
        product_id = item.get("data-code", "").strip()
        if not product_id:
            return None

        # Artist from div.LArtist > a
        artist = ""
        artist_div = item.find("div", class_="LArtist")
        if artist_div:
            artist_link = artist_div.find("a")
            if artist_link:
                artist = artist_link.get_text(strip=True)
            else:
                artist = artist_div.get_text(strip=True)

        # Title from div.LTitel
        title = ""
        title_div = item.find("div", class_="LTitel")
        if title_div:
            # Title might be in a link or plain text
            title_link = title_div.find("a")
            if title_link:
                title = title_link.get_text(strip=True)
            else:
                title = title_div.get_text(strip=True)

        if not title:
            return None

        # Label from div.LLabel
        label = ""
        label_div = item.find("div", class_="LLabel")
        if label_div:
            label_link = label_div.find("a")
            if label_link:
                label = label_link.get_text(strip=True)
            else:
                label = label_div.get_text(strip=True)

        # Catalog number from div.LLabelcat
        catalog = ""
        cat_div = item.find("div", class_="LLabelcat")
        if cat_div:
            catalog = cat_div.get_text(strip=True)

        # Styles from div.LStyle — multiple <a> tags with genre text
        styles = []
        style_div = item.find("div", class_="LStyle")
        if style_div:
            for a in style_div.find_all("a"):
                tag = a.get_text(strip=True)
                if tag:
                    styles.append(tag)

        # Genre classification from style tags
        if styles:
            genre = classify_genre(styles)
        else:
            genre = default_genre

        # Date from span.RelFeature (format: DD.MM.YY)
        date = ""
        date_span = item.find("span", class_="RelFeature")
        if date_span:
            date_text = date_span.get_text(strip=True)
            date = self._parse_date(date_text)

        if not date:
            date = datetime.now().strftime("%Y-%m-%d")

        # Source URL — construct from artist or title links
        source_url = ""
        for div in [title_div, artist_div]:
            if div:
                link = div.find("a", href=True)
                if link:
                    href = link.get("href", "")
                    if href:
                        source_url = f"{BASE_URL}{href}" if href.startswith("/") else href
                        break

        return self.make_release(
            source="decks",
            source_id=f"decks:{product_id}",
            title=title,
            artist=artist or "Various",
            label=label,
            genre=genre,
            date=date,
            date_verified=True,
            source_url=source_url,
            catalog_number=catalog or None,
            styles=styles,
        )

    # ── Date parsing ──────────────────────────────────────

    @staticmethod
    def _parse_date(date_text):
        """Parse decks.de date format: DD.MM.YY (e.g. '15.03.26' -> '2026-03-15')."""
        if not date_text:
            return ""

        # DD.MM.YY format (2-digit year)
        m = re.match(r'(\d{1,2})\.(\d{1,2})\.(\d{2})\b', date_text.strip())
        if m:
            try:
                day = int(m.group(1))
                month = int(m.group(2))
                year_short = int(m.group(3))
                # 2-digit year: 00-99 -> 2000-2099
                year = 2000 + year_short
                dt = datetime(year, month, day)
                return dt.strftime("%Y-%m-%d")
            except ValueError:
                pass

        # DD.MM.YYYY format (4-digit year fallback)
        m = re.match(r'(\d{1,2})\.(\d{1,2})\.(\d{4})', date_text.strip())
        if m:
            try:
                dt = datetime(int(m.group(3)), int(m.group(2)), int(m.group(1)))
                return dt.strftime("%Y-%m-%d")
            except ValueError:
                pass

        # Other common formats
        for fmt in ["%Y-%m-%d", "%d/%m/%Y", "%d-%m-%Y"]:
            try:
                dt = datetime.strptime(date_text.strip(), fmt)
                return dt.strftime("%Y-%m-%d")
            except ValueError:
                continue

        return ""

    # ── Public API ────────────────────────────────────────

    def fetch_by_genre(self, section, cutoff_date=None, max_pages=3):
        """Fetch new releases from a specific section on decks.de.

        Args:
            section: Section code (hon, ten, sfn, jcn).
            cutoff_date: datetime
            max_pages: Max pages per section (100 products/page).

        Returns:
            List of unified release dicts.
        """
        if cutoff_date is None:
            cutoff_date = datetime.now() - timedelta(days=90)

        cutoff_str = cutoff_date.strftime("%Y-%m-%d")
        default_genre = DECKS_SECTIONS.get(section, "Electronic")
        section_name = DECKS_SECTIONS.get(section, section)
        all_releases = []

        for page in range(max_pages):
            # Build URL with section and last-week filter
            url = f"{LIST_URL}?wo={section}&nowstyle=zz&now_Date=lw"
            if page > 0:
                url += f"&aktuell={page}"

            print(f"  \u25b8 Decks.de: {section_name} page {page + 1}")
            html = self._fetch_page(url)

            if not html:
                print(f"    \u2717 No response")
                break

            releases = self._parse_products(html, default_genre)
            if not releases:
                print(f"    \u2192 0 releases (no more results)")
                break

            # Filter by cutoff date
            filtered = [r for r in releases if r["date"] >= cutoff_str]
            all_releases.extend(filtered)
            print(f"    \u2192 {len(filtered)} releases")

            # If oldest release is before cutoff, stop paginating
            if releases:
                oldest = min(r["date"] for r in releases)
                if oldest < cutoff_str:
                    break

        return all_releases

    def fetch_by_artist(self, artist_name, cutoff_date):
        """Not available."""
        return []

    def fetch_all(self, cutoff_date=None, max_pages=3):
        """Main entry point: fetch from all configured sections.

        Args:
            cutoff_date: datetime. Defaults to 90 days ago.
            max_pages: Pages per section (100 products/page).

        Returns:
            List of unified release dicts (deduplicated).
        """
        if cutoff_date is None:
            cutoff_date = datetime.now() - timedelta(days=90)

        self._seen_ids.clear()
        all_releases = []

        for section in self._sections:
            section_name = DECKS_SECTIONS.get(section, section)
            print(f"  \u2500\u2500 Decks.de: {section_name} \u2500\u2500")
            releases = self.fetch_by_genre(section, cutoff_date, max_pages)
            all_releases.extend(releases)

        print(f"  \u2713 Decks.de total: {len(all_releases)} unique releases")
        return all_releases


if __name__ == "__main__":
    cutoff = datetime.now() - timedelta(days=30)
    fetcher = DecksFetcher()
    releases = fetcher.fetch_all(cutoff, max_pages=1)
    print(f"\nFound {len(releases)} releases")
    for r in releases[:15]:
        print(
            f"  {r['date']} | {r['artist']} - {r['title']} "
            f"[{r['label']}] ({r['genre']}) {r.get('styles', [])} "
            f"[{r.get('catalog_number', '')}]"
        )
