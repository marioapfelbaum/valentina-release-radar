"""
Norman Records source fetcher.
Scrapes new releases from normanrecords.com, a curated UK record shop
strong on Experimental, Ambient, Electronic, and Indie.

Uses cloudscraper to bypass Cloudflare (no RSS available).
Parses HTML with BeautifulSoup.

URL structure:
  - /new                   New releases, page 1
  - /new/2                 Page 2
  - /new/3                 Page 3
  - /new/4                 Page 4

HTML product structure:
  <div class="list_item">
    <h3><a href="/records/{slug}">Artist - Title</a></h3>
    <strong>Label</strong>: <a href="/labels/{id}">Label Name</a>
    <strong>Genres</strong>: <a href="/genres/{id}">Genre Tag</a>, ...
  </div>

Genre IDs: 1=Experimental, 3=Ambient/Downtempo, 6=Drone/Kosmische,
           8=Electronic, 10=Funk/Jazz/Soul, 12=House/Dance, 23=Techno/Dub Techno
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

try:
    import cloudscraper
except ImportError:
    cloudscraper = None

from .base import BaseSourceFetcher
from .genre_map import classify_genre

BASE_URL = "https://www.normanrecords.com"

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


class NormanFetcher(BaseSourceFetcher):
    """Scrapes new releases from normanrecords.com."""

    name = "norman"

    def __init__(self, rate_limit=3.0):
        """
        Args:
            rate_limit: Seconds between requests. Default 3.0 (Cloudflare).
        """
        super().__init__(rate_limit=rate_limit)
        self._seen_ids = set()
        if cloudscraper:
            self._session = cloudscraper.create_scraper()
        else:
            self._session = requests.Session()
        self._session.headers.update(HEADERS)

    # ── HTTP helpers ──────────────────────────────────────

    def _curl_get(self, url, timeout=25):
        """Fetch URL via curl (fallback for Cloudflare bypass)."""
        try:
            result = subprocess.run(
                [
                    "curl", "-s", "-L",
                    "--max-time", str(timeout),
                    "--http2",
                    "-H", f"User-Agent: {HEADERS['User-Agent']}",
                    "-H", f"Accept: {HEADERS['Accept']}",
                    "-H", f"Accept-Language: {HEADERS['Accept-Language']}",
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
        """Fetch a page via cloudscraper, with curl fallback.

        Returns empty string on Cloudflare challenge or failure.
        """
        # Try cloudscraper / requests session first
        try:
            resp = self._session.get(url, timeout=timeout)
            html = resp.text
            if "Just a moment" in html[:2000] and "challenge-platform" in html[:5000]:
                if not cloudscraper:
                    raise requests.RequestException("Cloudflare challenge")
            if len(html) > 1000:
                return html
        except requests.RequestException:
            pass

        # Fallback to curl
        html = self._curl_get(url, timeout=timeout)
        if html and "Just a moment" in html[:2000] and "challenge-platform" in html[:5000]:
            return ""

        return html

    # ── Parsing ───────────────────────────────────────────

    def _parse_products(self, html):
        """Parse product listings from a Norman Records HTML page.

        Returns list of release dicts.
        """
        if not html or len(html) < 500:
            return []

        soup = BeautifulSoup(html, "html.parser")
        releases = []

        items = soup.find_all("div", class_="list_item")
        for item in items:
            rel = self._parse_item(item)
            if rel and rel["id"] not in self._seen_ids:
                self._seen_ids.add(rel["id"])
                releases.append(rel)

        return releases

    def _parse_item(self, item):
        """Parse a single div.list_item element.

        Extracts:
          - Artist + Title from h3 > a (split on " - ")
          - Label from <strong>Label</strong> ... <a>
          - Genre tags from <strong>Genres</strong> ... <a href="/genres/...">
          - Product URL from h3 > a[href]
          - Pre-order date from "Ships on release (YYYY-MM-DD)" text
        """
        # ── Artist + Title from h3 ──
        h3 = item.find("h3")
        if not h3:
            return None

        link = h3.find("a")
        if not link:
            return None

        # Artist is text before <a>, title is inside <a>
        title = link.get_text(strip=True)
        if not title:
            return None

        # Get artist from h3 text before the link
        h3_text = h3.get_text(strip=True)
        artist = ""
        if title in h3_text:
            prefix = h3_text[:h3_text.index(title)].strip().rstrip("-").strip()
            if prefix:
                artist = prefix

        if not title:
            return None

        # ── Product URL + ID ──
        href = link.get("href", "")
        source_url = f"{BASE_URL}{href}" if href and not href.startswith("http") else href

        # Extract product ID from URL path (e.g. /records/123456-some-slug)
        product_id = ""
        id_match = re.search(r'/records/(\d+)', href)
        if id_match:
            product_id = id_match.group(1)
        else:
            # Fallback: use the full href as ID
            product_id = href.strip("/").replace("/", "-") if href else f"{artist}-{title}"

        # ── Label ──
        label = ""
        for strong in item.find_all("strong"):
            strong_text = strong.get_text(strip=True).lower()
            if "label" in strong_text:
                # The label name is in the next <a> sibling
                next_el = strong.next_sibling
                while next_el:
                    if hasattr(next_el, 'name') and next_el.name == 'a':
                        label = next_el.get_text(strip=True)
                        break
                    next_el = getattr(next_el, 'next_sibling', None)
                break

        # ── Genre tags ── find all /genres/ links directly
        styles = []
        for genre_link in item.find_all("a", href=lambda h: h and "/genres/" in h):
            tag_text = genre_link.get_text(strip=True)
            if tag_text:
                # Norman uses "Ambient / Downtempo / Dreampop" in one tag
                for part in tag_text.split("/"):
                    part = part.strip()
                    if part:
                        styles.append(part)

        genre = classify_genre(styles) if styles else "Electronic"

        # ── Date: check for pre-order pattern ──
        date = datetime.now().strftime("%Y-%m-%d")
        date_verified = False
        item_text = item.get_text()

        # Pre-orders: "Ships on release (YYYY-MM-DD)" or similar
        preorder_match = re.search(r'Ships on release\s*\((\d{4}-\d{2}-\d{2})\)', item_text)
        if preorder_match:
            date = preorder_match.group(1)
            date_verified = True
        else:
            # Also try "Release date: YYYY-MM-DD" or "Due: DD/MM/YYYY"
            date_match = re.search(r'(?:Release date|Due)[:\s]*(\d{4}-\d{2}-\d{2})', item_text)
            if date_match:
                date = date_match.group(1)
                date_verified = True
            else:
                due_match = re.search(r'Due[:\s]*(\d{1,2})/(\d{1,2})/(\d{4})', item_text)
                if due_match:
                    try:
                        dt = datetime(
                            int(due_match.group(3)),
                            int(due_match.group(2)),
                            int(due_match.group(1))
                        )
                        date = dt.strftime("%Y-%m-%d")
                        date_verified = True
                    except ValueError:
                        pass

        # ── Format detection ──
        format_type = self._detect_format(item_text)

        return self.make_release(
            source="norman",
            source_id=f"norman:{product_id}",
            title=title,
            artist=artist or "Various",
            label=label,
            genre=genre,
            date=date,
            date_verified=date_verified,
            source_url=source_url,
            styles=styles,
            format_type=format_type,
        )

    # ── Format detection ─────────────────────────────────

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
        if '12"' in t or "12 inch" in t:
            return "Single"
        if '7"' in t or "7 inch" in t:
            return "Single"
        if "vinyl" in t:
            return "Vinyl"
        if "cd" in t:
            return "CD"
        if "cassette" in t or "tape" in t:
            return "Cassette"
        if "digital" in t:
            return "Digital"
        return ""

    # ── Public API ────────────────────────────────────────

    def fetch_by_genre(self, genre_id, cutoff_date=None, max_pages=3):
        """Not available — Norman has no per-genre browse pages.

        Use fetch_all() instead (genre tags are parsed from each item).
        """
        return []

    def fetch_by_artist(self, artist_name, cutoff_date):
        """Not available."""
        return []

    def fetch_all(self, cutoff_date=None, max_pages=4):
        """Main entry point: scrape /new pages.

        Args:
            cutoff_date: datetime. Defaults to 90 days ago.
            max_pages: Max pages to scrape (1-4, ~50 items/page).

        Returns:
            List of unified release dicts (deduplicated).
        """
        if cutoff_date is None:
            cutoff_date = datetime.now() - timedelta(days=90)

        self._seen_ids.clear()
        all_releases = []

        # Quick accessibility check
        print("  ── Norman Records: Checking accessibility ──")
        if not cloudscraper:
            print("    ⚠ cloudscraper not installed (pip install cloudscraper)")
            print("    Falling back to curl (may be blocked by Cloudflare)")

        for page in range(1, max_pages + 1):
            if page == 1:
                url = f"{BASE_URL}/new"
            else:
                url = f"{BASE_URL}/new/{page}"

            print(f"  ▸ Norman Records: page {page}")
            self._throttle()
            html = self._fetch_page(url)

            if not html:
                print(f"    ✗ No response")
                break

            if "Just a moment" in html[:2000]:
                print(f"    ✗ Cloudflare challenge — skipping")
                break

            releases = self._parse_products(html)
            if not releases:
                print(f"    → 0 releases (no more results)")
                break

            all_releases.extend(releases)
            print(f"    → {len(releases)} releases")

        print(f"  ✓ Norman Records total: {len(all_releases)} unique releases")
        return all_releases


if __name__ == "__main__":
    cutoff = datetime.now() - timedelta(days=90)
    fetcher = NormanFetcher()
    releases = fetcher.fetch_all(cutoff, max_pages=2)
    print(f"\nFound {len(releases)} releases")
    for r in releases[:15]:
        print(
            f"  {r['date']} | {r['artist']} - {r['title']} "
            f"[{r['label']}] ({r['genre']}) "
            f"styles={r.get('styles', [])}"
        )
