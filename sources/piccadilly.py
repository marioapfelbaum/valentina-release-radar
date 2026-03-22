"""
Piccadilly Records source fetcher.
Scrapes new releases from piccadillyrecords.com, a curated Manchester record shop.

Uses HTML scraping with requests + BeautifulSoup (no Cloudflare protection).

URL structure:
  - /counter/catalogue.php?genre={g}&weekno={w}
  - genre: 6=House/Techno/Electronica, 8=Funk/Soul/Jazz/Broken Beat,
           24=Balearic/Downbeat, 22=Disco/Italo/Cosmic, 9=Bass/UK Garage
  - weekno: 0=current week, -1=last week

HTML product structure:
  <article class="stock-item" data-id="PRODUCT_ID">
    <h3 class="stock-item-artist">Artist Name</h3>
    <h4 class="stock-item-title">
      <a class="stock-item-title_link">Release Title</a>
      <a>Label Name</a>
    </h4>
    <dl>
      <dt>Cat Number</dt><dd>CAT001</dd>
      <dt>Release date</dt><dd>20 Mar '26</dd>
    </dl>
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
    requests = None

from .base import BaseSourceFetcher
from .genre_map import classify_genre

BASE_URL = "https://www.piccadillyrecords.com"

# Genre ID -> (display name, default genre for classify_genre)
PICCADILLY_GENRES = {
    6: ("House/Techno/Electronica", "House"),
    8: ("Funk/Soul/Jazz/Broken Beat", "Broken Beat"),
    24: ("Balearic/Downbeat", "Downtempo"),
    22: ("Disco/Italo/Cosmic", "Disco"),
    9: ("Bass/UK Garage", "UK Garage"),
}

DEFAULT_GENRE_IDS = list(PICCADILLY_GENRES.keys())


class PiccadillyFetcher(BaseSourceFetcher):
    """Scrapes new releases from piccadillyrecords.com."""

    name = "piccadilly"

    def __init__(self, rate_limit=2.0):
        super().__init__(rate_limit=rate_limit)
        self._seen_ids = set()

    # ── HTTP helpers ──────────────────────────────────────

    def _curl_get(self, url, timeout=20):
        """Fetch URL via curl (fallback)."""
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

    def _fetch_page(self, url, timeout=25):
        """Fetch an HTML page, trying requests first, falling back to curl."""
        self._throttle()

        if requests:
            try:
                headers = {
                    "User-Agent": (
                        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                        "AppleWebKit/537.36 (KHTML, like Gecko) "
                        "Chrome/120.0.0.0 Safari/537.36"
                    ),
                    "Accept": (
                        "text/html,application/xhtml+xml,application/xml;"
                        "q=0.9,image/avif,image/webp,*/*;q=0.8"
                    ),
                    "Accept-Language": "en-US,en;q=0.5",
                }
                resp = requests.get(url, headers=headers, timeout=timeout)
                if resp.status_code == 200 and len(resp.text) > 500:
                    return resp.text
            except Exception:
                pass

        # Fallback to curl
        return self._curl_get(url, timeout)

    # ── Parsing ───────────────────────────────────────────

    def _parse_products(self, html, default_genre="Electronic"):
        """Parse product listings from Piccadilly HTML page."""
        if not html:
            return []

        soup = BeautifulSoup(html, "html.parser")
        releases = []

        articles = soup.find_all("article", class_="stock-item")
        if not articles:
            # Fallback: try data-id attribute
            articles = soup.find_all("article", attrs={"data-id": True})

        for article in articles:
            rel = self._parse_article(article, default_genre)
            if rel and rel["id"] not in self._seen_ids:
                self._seen_ids.add(rel["id"])
                releases.append(rel)

        return releases

    def _parse_article(self, article, default_genre):
        """Parse a single product <article> element."""
        # Product ID from data-id attribute
        product_id = article.get("data-id", "")
        if not product_id:
            return None

        # Artist from h3.stock-item-artist
        artist = ""
        artist_el = article.find("h3", class_="stock-item-artist")
        if artist_el:
            artist = artist_el.get_text(strip=True)

        # Title and Label from h4 elements
        # Piccadilly has two h4.stock-item-title:
        #   1st: plain title text (no link)
        #   2nd: has additional class stock-item-title_link, contains <a> with label
        title = ""
        label = ""
        h4s = article.find_all("h4", class_="stock-item-title")
        for h4 in h4s:
            classes = h4.get("class", [])
            if "stock-item-title_link" in classes:
                # This h4 has the label link
                label_link = h4.find("a")
                if label_link:
                    label = label_link.get_text(strip=True)
            else:
                # This h4 has the plain title
                title = h4.get_text(strip=True)

        if not title:
            return None

        # Parse dt/dd pairs for catalog number and release date
        catalog = ""
        date = ""
        for dt in article.find_all("dt"):
            dt_text = dt.get_text(strip=True).lower()
            dd = dt.find_next_sibling("dd")
            if not dd:
                continue
            dd_text = dd.get_text(strip=True)

            if "cat number" in dt_text or "cat no" in dt_text:
                catalog = dd_text
            elif "release date" in dt_text:
                date = self._parse_date(dd_text)

        if not date:
            date = datetime.now().strftime("%Y-%m-%d")

        # Source URL — look for "more info" link or product link
        source_url = ""
        more_info = article.find("a", class_="stock-item-image-more-info_new")
        if more_info and more_info.get("href"):
            href = more_info["href"]
            source_url = href if href.startswith("http") else f"{BASE_URL}{href}"
        if not source_url:
            # Fallback: first link with product ID in URL
            for a_tag in article.find_all("a", href=True):
                href = a_tag["href"]
                if product_id in str(href):
                    source_url = href if href.startswith("http") else f"{BASE_URL}{href}"
                    break

        # Genre classification — use the default genre from the page category
        styles = [default_genre.lower()] if default_genre else []
        genre = classify_genre(styles) if styles else default_genre

        return self.make_release(
            source="piccadilly",
            source_id=f"pic:{product_id}",
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
        """Parse Piccadilly date format: '20 Mar '26' -> '2026-03-20'."""
        if not date_text:
            return ""

        date_text = date_text.strip()

        # Primary format: "20 Mar '26"
        m = re.match(r"(\d{1,2})\s+(\w{3})\s+'(\d{2})", date_text)
        if m:
            try:
                dt = datetime.strptime(
                    f"{m.group(1)} {m.group(2)} {m.group(3)}", "%d %b %y"
                )
                return dt.strftime("%Y-%m-%d")
            except ValueError:
                pass

        # Fallback: "20 Mar 2026"
        m = re.match(r"(\d{1,2})\s+(\w{3,})\s+(\d{4})", date_text)
        if m:
            try:
                dt = datetime.strptime(date_text, "%d %b %Y")
                return dt.strftime("%Y-%m-%d")
            except ValueError:
                pass

        # Fallback: ISO format
        m = re.match(r"\d{4}-\d{2}-\d{2}", date_text)
        if m:
            return m.group(0)

        return ""

    # ── Public API ────────────────────────────────────────

    def fetch_by_genre(self, genre_id, cutoff_date=None, weeks=None):
        """Fetch new releases from a specific genre on Piccadilly Records.

        Args:
            genre_id: Numeric genre ID (6, 8, 9, 22, 24).
            cutoff_date: datetime. Defaults to 30 days ago.
            weeks: List of week numbers to fetch (default: [0, -1]).

        Returns:
            List of unified release dicts.
        """
        if cutoff_date is None:
            cutoff_date = datetime.now() - timedelta(days=30)

        if weeks is None:
            weeks = [0, -1]

        if genre_id not in PICCADILLY_GENRES:
            print(f"  ▸ Piccadilly: Unknown genre ID {genre_id}, "
                  f"valid IDs: {list(PICCADILLY_GENRES.keys())}")
            return []

        genre_name, default_genre = PICCADILLY_GENRES[genre_id]
        cutoff_str = cutoff_date.strftime("%Y-%m-%d")
        all_releases = []

        for week in weeks:
            url = (f"{BASE_URL}/counter/catalogue.php"
                   f"?genre={genre_id}&weekno={week}")

            week_label = "current" if week == 0 else f"week {week}"
            print(f"  ▸ Piccadilly: {genre_name} ({week_label})")
            html = self._fetch_page(url)

            if not html:
                print(f"    ✗ No response")
                continue

            releases = self._parse_products(html, default_genre)
            if not releases:
                print(f"    → 0 releases")
                continue

            # Filter by cutoff date
            filtered = [r for r in releases if r["date"] >= cutoff_str]
            all_releases.extend(filtered)
            print(f"    → {len(filtered)} releases")

        return all_releases

    def fetch_by_artist(self, artist_name, cutoff_date=None):
        """Not available for Piccadilly Records."""
        return []

    def fetch_all(self, cutoff_date=None, genre_ids=None, weeks=None):
        """Main entry point: fetch from all configured genres.

        Args:
            cutoff_date: datetime. Defaults to 30 days ago.
            genre_ids: List of genre IDs to fetch. Defaults to all.
            weeks: List of week numbers (default: [0, -1]).

        Returns:
            List of unified release dicts (deduplicated).
        """
        if cutoff_date is None:
            cutoff_date = datetime.now() - timedelta(days=30)

        if genre_ids is None:
            genre_ids = list(DEFAULT_GENRE_IDS)

        if weeks is None:
            weeks = [0, -1]

        self._seen_ids.clear()
        all_releases = []

        for genre_id in genre_ids:
            genre_name = PICCADILLY_GENRES.get(genre_id, ("Unknown",))[0]
            print(f"  ── Piccadilly: {genre_name} ──")
            releases = self.fetch_by_genre(genre_id, cutoff_date, weeks)
            all_releases.extend(releases)

        print(f"  ✓ Piccadilly total: {len(all_releases)} unique releases")
        return all_releases


if __name__ == "__main__":
    cutoff = datetime.now() - timedelta(days=30)
    fetcher = PiccadillyFetcher()
    releases = fetcher.fetch_all(cutoff)
    print(f"\nFound {len(releases)} releases")
    for r in releases[:15]:
        print(
            f"  {r['date']} | {r['artist']} - {r['title']} "
            f"[{r['label']}] ({r['genre']}) [{r.get('catalog_number', '')}]"
        )
