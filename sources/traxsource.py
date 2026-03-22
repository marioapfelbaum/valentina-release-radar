"""
Traxsource source fetcher.
Scrapes new releases from traxsource.com, a major electronic music download store.

Uses cloudscraper (Cloudflare bypass) + BeautifulSoup for HTML parsing.
Curl fallback if cloudscraper fails.

URL structure:
  - /genre/{id}/{slug}/top?cn=new&ob=r_date&so=desc    New releases by genre
  - ?page={n}                                            Pagination (100 tracks/page)

HTML product structure:
  <div class="trk-row" data-trid="{TRACK_ID}">
    <div class="trk-cell title"><a class="com-title">Title</a></div>
    <div class="trk-cell artists"><a class="com-artists">Artist</a></div>
    <div class="trk-cell label"><a class="com-label">Label</a></div>
    <div class="trk-cell r-date">YYYY-MM-DD</div>
    <div class="trk-cell genre"><a class="genre-lnk">Genre Tag</a></div>
  </div>

Crawl-delay: 10 (robots.txt)
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
    import cloudscraper
except ImportError:
    cloudscraper = None
    print("Warning: cloudscraper not installed, will use curl fallback")

try:
    import requests
except ImportError:
    print("pip install requests")
    sys.exit(1)

from .base import BaseSourceFetcher
from .genre_map import classify_genre

BASE_URL = "https://www.traxsource.com"

# Genre ID -> (slug, default_genre)
TRAXSOURCE_GENRES = {
    13: ("deep-house", "Deep House"),
    24: ("soulful-house", "Soulful House"),
    27: ("afro-house", "Afro House"),
    16: ("minimal-deep-tech", "Minimal House"),
    2: ("broken-beat-nu-jazz", "Broken Beat"),
    15: ("jackin-house", "Jackin House"),
    23: ("afro-latin-brazilian", "Afro House"),
}

DEFAULT_GENRE_IDS = list(TRAXSOURCE_GENRES.keys())


class TraxsourceFetcher(BaseSourceFetcher):
    """Scrapes new releases from traxsource.com."""

    name = "traxsource"

    def __init__(self, genre_ids=None, rate_limit=10.0):
        super().__init__(rate_limit=rate_limit)
        self._genre_ids = genre_ids or list(DEFAULT_GENRE_IDS)
        self._seen_ids = set()
        self._scraper = None
        if cloudscraper:
            try:
                self._scraper = cloudscraper.create_scraper(
                    browser={"browser": "chrome", "platform": "darwin", "mobile": False}
                )
                self._scraper.headers.update({
                    "Accept": (
                        "text/html,application/xhtml+xml,application/xml;"
                        "q=0.9,image/avif,image/webp,*/*;q=0.8"
                    ),
                    "Accept-Language": "en-US,en;q=0.9",
                })
            except Exception:
                self._scraper = None

    # ── HTTP helpers ──────────────────────────────────────

    def _fetch_page(self, url, timeout=30):
        """Fetch an HTML page from traxsource.com via cloudscraper or curl."""
        self._throttle()

        # Primary: cloudscraper (bypasses Cloudflare)
        if self._scraper:
            try:
                resp = self._scraper.get(url, timeout=timeout)
                if resp.status_code == 200 and len(resp.text) > 500:
                    return resp.text
            except Exception:
                pass

        # Fallback: plain requests
        try:
            resp = requests.get(
                url, timeout=timeout,
                headers={
                    "User-Agent": (
                        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                        "AppleWebKit/537.36 (KHTML, like Gecko) "
                        "Chrome/121.0.0.0 Safari/537.36"
                    ),
                    "Accept": "text/html,*/*",
                    "Accept-Language": "en-US,en;q=0.9",
                },
            )
            if resp.status_code == 200 and len(resp.text) > 500:
                return resp.text
        except requests.RequestException:
            pass

        # Last resort: curl
        try:
            result = subprocess.run(
                ["curl", "-s", "-L", "--max-time", str(timeout),
                 "-H", "User-Agent: Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7)",
                 "-H", "Accept: text/html,*/*",
                 "-H", "Accept-Language: en-US,en;q=0.9",
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
        """Parse track/release listings from Traxsource HTML page."""
        if not html:
            return []

        soup = BeautifulSoup(html, "html.parser")
        releases = []

        # Each track row is a div.trk-row with data-trid attribute
        rows = soup.find_all("div", class_="trk-row", attrs={"data-trid": True})
        if not rows:
            # Fallback: try any element with data-trid
            rows = soup.find_all(attrs={"data-trid": True})

        for row in rows:
            rel = self._parse_track_row(row, default_genre)
            if rel and rel["id"] not in self._seen_ids:
                self._seen_ids.add(rel["id"])
                releases.append(rel)

        return releases

    def _parse_track_row(self, row, default_genre):
        """Parse a single track row element."""
        track_id = row.get("data-trid", "")
        if not track_id:
            return None

        # Title from .trk-cell.title a.com-title
        title = ""
        title_cell = row.find(class_="title")
        if title_cell:
            title_link = title_cell.find("a", class_="com-title")
            if title_link:
                title = title_link.get_text(strip=True)
            else:
                # Fallback: any link in title cell
                any_link = title_cell.find("a")
                if any_link:
                    title = any_link.get_text(strip=True)

        if not title:
            return None

        # Artist from .trk-cell.artists a.com-artists
        artist = ""
        artist_cell = row.find(class_="artists")
        if artist_cell:
            artist_links = artist_cell.find_all("a", class_="com-artists")
            if artist_links:
                artist = ", ".join(a.get_text(strip=True) for a in artist_links)
            else:
                # Fallback: any link in artists cell
                any_links = artist_cell.find_all("a")
                if any_links:
                    artist = ", ".join(a.get_text(strip=True) for a in any_links)
                else:
                    artist = artist_cell.get_text(strip=True)

        # Label from .trk-cell.label a.com-label
        label = ""
        label_cell = row.find(class_="label")
        if label_cell:
            label_link = label_cell.find("a", class_="com-label")
            if label_link:
                label = label_link.get_text(strip=True)
            else:
                any_link = label_cell.find("a")
                if any_link:
                    label = any_link.get_text(strip=True)

        # Release date from .trk-cell.r-date (YYYY-MM-DD)
        date = ""
        date_cell = row.find(class_="r-date")
        if date_cell:
            date_text = date_cell.get_text(strip=True)
            date = self._parse_date(date_text)

        if not date:
            date = datetime.now().strftime("%Y-%m-%d")

        # Genre/style from .trk-cell.genre a.genre-lnk
        styles = []
        genre_cell = row.find(class_="genre")
        if genre_cell:
            genre_links = genre_cell.find_all("a", class_="genre-lnk")
            if genre_links:
                styles = [g.get_text(strip=True) for g in genre_links]
            else:
                any_links = genre_cell.find_all("a")
                if any_links:
                    styles = [g.get_text(strip=True) for g in any_links]

        # Classify genre from styles, fall back to default
        genre = default_genre
        if styles:
            classified = classify_genre(styles)
            if classified and classified != "Other":
                genre = classified

        # Source URL — try to find a link to the track page
        source_url = ""
        title_cell = row.find(class_="title")
        if title_cell:
            title_link = title_cell.find("a", href=True)
            if title_link:
                href = title_link.get("href", "")
                if href:
                    source_url = f"{BASE_URL}{href}" if href.startswith("/") else href

        return self.make_release(
            source="traxsource",
            source_id=f"tx:{track_id}",
            title=title,
            artist=artist or "Various",
            label=label,
            genre=genre,
            date=date,
            source_url=source_url,
            styles=styles,
            date_verified=True,
        )

    # ── Date parsing ──────────────────────────────────────

    @staticmethod
    def _parse_date(date_text):
        """Parse Traxsource date format: YYYY-MM-DD."""
        if not date_text:
            return ""

        text = date_text.strip()

        # YYYY-MM-DD (primary format)
        m = re.match(r'(\d{4})-(\d{2})-(\d{2})', text)
        if m:
            try:
                datetime(int(m.group(1)), int(m.group(2)), int(m.group(3)))
                return text[:10]
            except ValueError:
                pass

        # MM/DD/YYYY fallback
        m = re.match(r'(\d{1,2})/(\d{1,2})/(\d{4})', text)
        if m:
            try:
                dt = datetime(int(m.group(3)), int(m.group(1)), int(m.group(2)))
                return dt.strftime("%Y-%m-%d")
            except ValueError:
                pass

        # DD.MM.YYYY fallback
        m = re.match(r'(\d{1,2})\.(\d{1,2})\.(\d{4})', text)
        if m:
            try:
                dt = datetime(int(m.group(3)), int(m.group(2)), int(m.group(1)))
                return dt.strftime("%Y-%m-%d")
            except ValueError:
                pass

        return ""

    # ── Public API ────────────────────────────────────────

    def fetch_by_genre(self, genre_id, cutoff_date=None, max_pages=2):
        """Fetch new releases from a specific genre on Traxsource.

        Args:
            genre_id: Traxsource genre ID (e.g., 13 for Deep House).
            cutoff_date: datetime. Defaults to 90 days ago.
            max_pages: Max pages per genre (100 tracks/page).

        Returns:
            List of unified release dicts.
        """
        if cutoff_date is None:
            cutoff_date = datetime.now() - timedelta(days=90)

        cutoff_str = cutoff_date.strftime("%Y-%m-%d")

        if genre_id not in TRAXSOURCE_GENRES:
            print(f"  ✗ Unknown Traxsource genre ID: {genre_id}")
            return []

        slug, default_genre = TRAXSOURCE_GENRES[genre_id]
        all_releases = []

        for page in range(1, max_pages + 1):
            url = (
                f"{BASE_URL}/genre/{genre_id}/{slug}/top"
                f"?cn=new&ob=r_date&so=desc"
            )
            if page > 1:
                url += f"&page={page}"

            print(f"  ▸ Traxsource: {slug} page {page}")
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
        """Not available for Traxsource."""
        return []

    def fetch_all(self, cutoff_date=None, max_pages=2):
        """Main entry point: fetch from all configured genres.

        Args:
            cutoff_date: datetime. Defaults to 90 days ago.
            max_pages: Pages per genre (100 tracks/page).

        Returns:
            List of unified release dicts (deduplicated).
        """
        if cutoff_date is None:
            cutoff_date = datetime.now() - timedelta(days=90)

        self._seen_ids.clear()
        all_releases = []

        for genre_id in self._genre_ids:
            slug, _ = TRAXSOURCE_GENRES.get(genre_id, ("unknown", "Electronic"))
            print(f"  ── Traxsource: {slug} ──")
            releases = self.fetch_by_genre(genre_id, cutoff_date, max_pages)
            all_releases.extend(releases)

        print(f"  ✓ Traxsource total: {len(all_releases)} unique releases")
        return all_releases


if __name__ == "__main__":
    cutoff = datetime.now() - timedelta(days=30)
    fetcher = TraxsourceFetcher()
    releases = fetcher.fetch_all(cutoff, max_pages=1)
    print(f"\nFound {len(releases)} releases")
    for r in releases[:15]:
        print(
            f"  {r['date']} | {r['artist']} - {r['title']} "
            f"[{r['label']}] ({r['genre']}) {r.get('styles', [])}"
        )
