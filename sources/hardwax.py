"""
Hardwax.com source fetcher.

Scrapes new releases from hardwax.com, Berlin's legendary record shop.
Uses the JSON feed for recent releases and HTML scraping for genre pages.
Falls back to curl for HTTP requests (like bandcamp.py) in case requests is blocked.

Hardwax URL structure:
  - /this-week/         New arrivals this week
  - /last-week/         Last week's arrivals
  - /house/             Genre tag pages
  - /techno/
  - /ambient/
  - /electro/
  - /electronica/
  - /bass/
  - /detroit/
  - /detroit-house/
  - /feeds/news.json    JSON feed of latest releases (structured data)
  - /<id>/<artist-slug>/<title-slug>/   Individual release pages

Release HTML structure (inside <article class="co cq px">):
  - Record ID:    <div id="record-XXXXX">
  - Artist:       <a class="rn"> inside <h2 class="rm">
  - Title:        <span class="rp"> inside <h2 class="rm">
  - Label:        <a href="/label/..."> inside <div class="qv">
  - Format:       <span class="rf"> (e.g. 12", LP, Tape)
  - Price:        <span class="qq"> (e.g. "EUR 16")
  - Description:  <p class="qt"> (staff description with genre hints)
  - Section:      <div class="qx"> links to /section/...
  - URL:          <a href="/XXXXX/artist-slug/title-slug/">
  - Pagination:   ?page=2, ?page=3, etc.
"""

import json
import re
import subprocess
import sys
import time
from datetime import datetime, timedelta
from html import unescape
from pathlib import Path

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


# Hardwax genre tag URLs relevant to the user's taste profile
# (Minimal, Deep House, Downtempo, Soulful, Broken Beat, Jazz-Electronic)
HARDWAX_GENRE_TAGS = {
    "house":          "House",
    "techno":         "Techno",
    "electro":        "Electro",
    "electronica":    "Electronica",
    "ambient":        "Ambient",
    "detroit":        "Detroit Techno",
    "detroit-house":  "Deep House",
    "bass":           "Bass",
}

# Map Hardwax description keywords to genre_map styles for classify_genre()
HARDWAX_DESCRIPTION_KEYWORDS = {
    "minimal": "minimal",
    "deep house": "deep house",
    "dub techno": "dub techno",
    "dub": "dub",
    "techno": "techno",
    "house": "house",
    "ambient": "ambient",
    "electro": "electro",
    "downtempo": "downtempo",
    "acid": "acid",
    "breakbeat": "breaks",
    "broken beat": "breaks",
    "disco": "disco",
    "jazz": "electronica",
    "experimental": "experimental",
    "noise": "noise",
    "industrial": "industrial techno",
    "detroit": "detroit techno",
    "drum and bass": "drum and bass",
    "drum & bass": "drum and bass",
    "dubstep": "dubstep",
    "garage": "uk garage",
    "leftfield": "leftfield",
    "idm": "idm",
    "trip hop": "trip hop",
    "synth": "synth-pop",
    "lo-fi": "lo-fi house",
    "afro": "afro house",
    "tribal": "tribal house",
    "melodic": "melodic techno",
    "hypnotic": "hypnotic",
    "soulful": "soulful house",
    "funk": "funky house",
    "tech house": "tech house",
}

BASE_URL = "https://hardwax.com"
JSON_FEED_URL = "https://hardwax.com/feeds/news.json"


class HardwaxFetcher(BaseSourceFetcher):
    """Scrapes new releases from hardwax.com."""

    name = "hardwax"

    def __init__(self, rate_limit=2.0):
        super().__init__(rate_limit=rate_limit)
        self._seen_ids = set()  # For deduplication

    # ── HTTP helpers ────────────────────────────────────────────────

    def _curl_get(self, url, timeout=20):
        """Fetch URL via curl (bypasses TLS fingerprinting blocks)."""
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

    def _fetch_url(self, url, timeout=20):
        """Fetch URL, trying requests first, falling back to curl."""
        self._throttle()

        # Try requests first
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
            except Exception:
                pass

        # Fallback to curl
        return self._curl_get(url, timeout)

    def _fetch_json(self, url, timeout=20):
        """Fetch JSON from URL."""
        text = self._fetch_url(url, timeout)
        if not text:
            return None
        try:
            return json.loads(text)
        except (json.JSONDecodeError, ValueError):
            return None

    # ── JSON feed parsing ───────────────────────────────────────────

    def _fetch_from_json_feed(self, cutoff_date):
        """Fetch latest releases from the hardwax JSON feed.

        The JSON feed contains ~30-40 items from /this-week/ with
        structured data including title, date, description, and URL.
        """
        data = self._fetch_json(JSON_FEED_URL)
        if not data or "items" not in data:
            print("  ▸ Hardwax: JSON feed unavailable, falling back to HTML")
            return []

        releases = []
        cutoff_str = cutoff_date.strftime("%Y-%m-%d")

        for item in data["items"]:
            rel = self._parse_json_feed_item(item, cutoff_str)
            if rel and rel["id"] not in self._seen_ids:
                self._seen_ids.add(rel["id"])
                releases.append(rel)

        return releases

    def _parse_json_feed_item(self, item, cutoff_str):
        """Parse a single item from the hardwax JSON feed.

        Feed item structure:
            title: "Artist: Title"
            content_html: "<p><strong>Label CatNo</strong> (Format) - EUR Price<br>
                          <em>Description</em></p>..."
            date_published: "2026-03-06T09:00:00Z"
            url: "https://hardwax.com/XXXXX/artist-slug/title-slug/"
        """
        raw_title = item.get("title", "")
        if not raw_title:
            return None

        # Parse "Artist: Title" format
        artist, title = self._split_artist_title(raw_title)

        # Parse date
        date_str = item.get("date_published", "")
        date = self._parse_iso_date(date_str)
        if not date or date < cutoff_str:
            return None

        # Parse content_html for label, format, price, description
        content = item.get("content_html", "")
        label, format_type, price, description = self._parse_feed_content(content)

        # Extract record ID from URL
        url = item.get("url", "")
        record_id = self._extract_record_id(url)
        if not record_id:
            return None

        # Check for reissue
        reissue = self._is_reissue(description, title)

        # Classify genre from description
        styles = self._extract_styles_from_description(description)
        genre = classify_genre(styles)

        return self.make_release(
            source="hardwax",
            source_id=f"hw:{record_id}",
            title=title,
            artist=artist,
            label=label,
            genre=genre,
            date=date,
            source_url=url,
            format_type=format_type,
            styles=styles,
            reissue=reissue,
        )

    def _parse_feed_content(self, content_html):
        """Extract label, format, price, description from feed content_html.

        Example: <p><strong>Klockworks 041</strong> (12") - EUR 16<br>
                 <em>Delivering, hypnotic Techno tracks</em></p>
        """
        label = ""
        format_type = ""
        price = ""
        description = ""

        if not content_html:
            return label, format_type, price, description

        # Parse with BeautifulSoup
        soup = BeautifulSoup(content_html, "html.parser")

        # Label + catalog number is in <strong>
        strong = soup.find("strong")
        if strong:
            label_cat = strong.get_text(strip=True)
            # Label is everything before the catalog number
            # e.g. "Klockworks 041" -> "Klockworks"
            # e.g. "Dekmantel 115" -> "Dekmantel"
            # e.g. "Ilian Tape X 42" -> "Ilian Tape"
            label = self._extract_label_name(label_cat)

        # Format is in parentheses after <strong>
        first_p = soup.find("p")
        if first_p:
            text = first_p.get_text()
            fmt_match = re.search(r'\(([^)]+)\)', text)
            if fmt_match:
                format_type = unescape(fmt_match.group(1)).strip()

            # Price after the dash
            price_match = re.search(r'[€$]\s*([\d.,]+)', text)
            if price_match:
                price = f"€{price_match.group(1)}"

        # Description is in <em>
        em = soup.find("em")
        if em:
            description = em.get_text(strip=True)

        return label, format_type, price, description

    # ── HTML scraping ───────────────────────────────────────────────

    def _fetch_page_releases(self, url, cutoff_date):
        """Fetch and parse releases from a single hardwax HTML page.

        Returns list of release dicts.
        """
        html = self._fetch_url(url)
        if not html:
            return []

        return self._parse_html_releases(html, cutoff_date)

    def _parse_html_releases(self, html, cutoff_date):
        """Parse release articles from hardwax HTML page."""
        soup = BeautifulSoup(html, "html.parser")
        releases = []
        cutoff_str = cutoff_date.strftime("%Y-%m-%d")

        # Each release is an <article class="co cq px">
        for article in soup.find_all("article", class_="co"):
            rel = self._parse_article(article, cutoff_str)
            if rel and rel["id"] not in self._seen_ids:
                self._seen_ids.add(rel["id"])
                releases.append(rel)

        return releases

    def _parse_article(self, article, cutoff_str):
        """Parse a single release <article> element.

        Structure:
          <div id="record-XXXXX">
            <div class="qu">
              <div class="qv"><a href="/label/...">Label Name</a> <a href="/ID/...">CatNo</a></div>
              <div class="qx"><a href="/section/...">Section</a></div>
            </div>
            <h2 class="rm">
              <span class="ro"><a class="rn">Artist:</a></span>
              <span class="ro"><span class="rp">Title</span></span>
            </h2>
            <p class="qt">Description text</p>
          </div>
        """
        # Extract record ID
        record_div = article.find("div", id=lambda x: x and x.startswith("record-"))
        if not record_div:
            return None
        record_id = record_div["id"].replace("record-", "")

        # Artist and title from <h2 class="rm">
        h2 = article.find("h2", class_="rm")
        if not h2:
            return None

        artist_tag = h2.find("a", class_="rn")
        title_tag = h2.find("span", class_="rp")

        artist = ""
        if artist_tag:
            artist = artist_tag.get_text(strip=True).rstrip(":")
        title = ""
        if title_tag:
            title = title_tag.get_text(strip=True)

        if not title:
            return None

        # Label from <div class="qv">
        label = ""
        label_div = article.find("div", class_="qv")
        if label_div:
            label_link = label_div.find("a", href=lambda x: x and "/label/" in x)
            if label_link:
                label = label_link.get_text(strip=True)

        # Format from <span class="rf">
        format_type = ""
        format_span = article.find("span", class_="rf")
        if format_span:
            fmt = unescape(format_span.get_text(strip=True))
            if fmt and fmt != "—":
                format_type = fmt

        # Price from <span class="qq">
        price = ""
        price_span = article.find("span", class_="qq")
        if price_span:
            price = price_span.get_text(strip=True)

        # Description from <p class="qt">
        description = ""
        desc_p = article.find("p", class_="qt")
        if desc_p:
            description = desc_p.get_text(strip=True)

        # Section from <div class="qx">
        section = ""
        section_div = article.find("div", class_="qx")
        if section_div:
            section_link = section_div.find("a")
            if section_link:
                section = section_link.get_text(strip=True)

        # Release URL from the first <a> with a product link
        source_url = ""
        product_link = article.find("a", href=re.compile(rf"^/{record_id}/"))
        if product_link:
            source_url = BASE_URL + product_link["href"]

        # Check for reissue
        reissue = self._is_reissue(description, title)

        # Classify genre from description and section
        styles = self._extract_styles_from_description(description)
        if section:
            styles.append(section.lower())
        genre = classify_genre(styles)

        # We don't have exact dates from HTML pages, use today
        date = datetime.now().strftime("%Y-%m-%d")

        return self.make_release(
            source="hardwax",
            source_id=f"hw:{record_id}",
            title=title,
            artist=artist,
            label=label,
            genre=genre,
            date=date,
            source_url=source_url,
            format_type=format_type,
            styles=styles,
            reissue=reissue,
        )

    def _fetch_paginated(self, base_url, cutoff_date, max_pages=3):
        """Fetch multiple pages from a hardwax listing URL.

        Hardwax uses ?page=2, ?page=3, etc. for pagination.
        """
        all_releases = []

        for page in range(1, max_pages + 1):
            url = base_url if page == 1 else f"{base_url}?page={page}"
            releases = self._fetch_page_releases(url, cutoff_date)

            if not releases:
                break  # No more results

            all_releases.extend(releases)
            print(f"    page {page}: {len(releases)} releases")

        return all_releases

    # ── Public API ──────────────────────────────────────────────────

    def fetch_new_releases(self, cutoff_date=None, max_pages=2):
        """Fetch new releases from hardwax.com.

        First tries the JSON feed (structured data, best quality).
        Then scrapes /this-week/ and /last-week/ HTML pages for
        any releases not in the feed.

        Args:
            cutoff_date: Only include releases on or after this date.
                        Defaults to 30 days ago.
            max_pages: Maximum pages to fetch per section.

        Returns:
            List of unified release dicts.
        """
        if cutoff_date is None:
            cutoff_date = datetime.now() - timedelta(days=30)

        releases = []

        # 1. JSON feed (best structured data)
        print("  ▸ Hardwax: Fetching JSON feed...")
        feed_releases = self._fetch_from_json_feed(cutoff_date)
        releases.extend(feed_releases)
        print(f"    → {len(feed_releases)} releases from feed")

        # 2. This week HTML (may have items not in feed)
        print("  ▸ Hardwax: Fetching /this-week/...")
        tw_releases = self._fetch_paginated(
            f"{BASE_URL}/this-week/", cutoff_date, max_pages
        )
        releases.extend(tw_releases)
        print(f"    → {len(tw_releases)} new releases from this-week")

        # 3. Last week HTML
        print("  ▸ Hardwax: Fetching /last-week/...")
        lw_releases = self._fetch_paginated(
            f"{BASE_URL}/last-week/", cutoff_date, max_pages
        )
        releases.extend(lw_releases)
        print(f"    → {len(lw_releases)} new releases from last-week")

        return releases

    def fetch_by_genre(self, genre_id, cutoff_date=None, max_pages=3):
        """Fetch releases from a specific hardwax genre/tag page.

        Args:
            genre_id: Hardwax tag slug (e.g. "house", "techno", "ambient").
                     Must be a key in HARDWAX_GENRE_TAGS.
            cutoff_date: Only include releases on or after this date.
            max_pages: Maximum pages to fetch.

        Returns:
            List of unified release dicts.
        """
        if cutoff_date is None:
            cutoff_date = datetime.now() - timedelta(days=90)

        if genre_id not in HARDWAX_GENRE_TAGS:
            print(f"  ▸ Hardwax: Unknown genre tag '{genre_id}', "
                  f"valid tags: {', '.join(HARDWAX_GENRE_TAGS.keys())}")
            return []

        url = f"{BASE_URL}/{genre_id}/"
        print(f"  ▸ Hardwax: Fetching /{genre_id}/...")
        releases = self._fetch_paginated(url, cutoff_date, max_pages)

        # Add the hardwax genre tag as a style if not already present
        hw_genre = HARDWAX_GENRE_TAGS[genre_id].lower()
        for rel in releases:
            if hw_genre not in [s.lower() for s in rel.get("styles", [])]:
                rel["styles"].append(hw_genre)
            # Re-classify with the added style
            rel["genre"] = classify_genre(rel["styles"])

        print(f"    → {len(releases)} total from /{genre_id}/")
        return releases

    def fetch_by_artist(self, artist_name, cutoff_date=None):
        """Search hardwax for a specific artist's releases.

        Uses the hardwax search functionality.

        Args:
            artist_name: Artist name to search for.
            cutoff_date: Only include releases on or after this date.

        Returns:
            List of unified release dicts.
        """
        if cutoff_date is None:
            cutoff_date = datetime.now() - timedelta(days=180)

        from urllib.parse import quote_plus
        url = f"{BASE_URL}/?find={quote_plus(artist_name)}"
        releases = self._fetch_page_releases(url, cutoff_date)
        return releases

    def fetch_all(self, cutoff_date=None, genres=None, max_pages=2):
        """Main entry point: fetch from /this-week/, /last-week/, and genre pages.

        Args:
            cutoff_date: Only include releases on or after this date.
                        Defaults to 30 days ago.
            genres: List of genre tag slugs to fetch. Defaults to all
                   tags in HARDWAX_GENRE_TAGS.
            max_pages: Maximum pages per section.

        Returns:
            List of unified release dicts (deduplicated).
        """
        if cutoff_date is None:
            cutoff_date = datetime.now() - timedelta(days=30)

        if genres is None:
            genres = list(HARDWAX_GENRE_TAGS.keys())

        self._seen_ids.clear()
        all_releases = []

        # 1. New releases (feed + this-week + last-week)
        new_releases = self.fetch_new_releases(cutoff_date, max_pages)
        all_releases.extend(new_releases)

        # 2. Genre tag pages
        for genre in genres:
            genre_releases = self.fetch_by_genre(genre, cutoff_date, max_pages)
            all_releases.extend(genre_releases)

        print(f"  ✓ Hardwax total: {len(all_releases)} releases "
              f"({len(self._seen_ids)} unique)")
        return all_releases

    # ── Utility methods ─────────────────────────────────────────────

    @staticmethod
    def _split_artist_title(raw_title):
        """Split 'Artist: Title' into (artist, title).

        Handles cases like:
          "Shlomi Aber & Kashpitzky: Klockworks 41"
          "Re:ni & BiggaBush: Bass Is The Space"
          "Title Only"
        """
        # Split on ": " but not on ":" inside words like "Re:ni"
        # Look for ": " (colon followed by space) as separator
        match = re.match(r'^(.+?):\s+(.+)$', raw_title)
        if match:
            return match.group(1).strip(), match.group(2).strip()
        return "", raw_title.strip()

    @staticmethod
    def _parse_iso_date(date_str):
        """Parse ISO 8601 date string to YYYY-MM-DD."""
        if not date_str:
            return ""
        try:
            # Handle "2026-03-06T09:00:00Z"
            dt = datetime.fromisoformat(date_str.replace("Z", "+00:00"))
            return dt.strftime("%Y-%m-%d")
        except (ValueError, TypeError):
            return ""

    @staticmethod
    def _extract_record_id(url):
        """Extract numeric record ID from hardwax URL.

        Example: "https://hardwax.com/87971/shlomi-aber/klockworks-41/" -> "87971"
        """
        match = re.search(r'hardwax\.com/(\d+)/', url)
        if match:
            return match.group(1)
        # Also handle relative URLs
        match = re.search(r'^/(\d+)/', url)
        if match:
            return match.group(1)
        return ""

    @staticmethod
    def _extract_label_name(label_cat_text):
        """Extract label name from 'Label CatNo' string.

        Examples:
          "Klockworks 041"      -> "Klockworks"
          "Dekmantel 115"       -> "Dekmantel"
          "Ilian Tape X 42"    -> "Ilian Tape"
          "Black Acre 100"     -> "Black Acre"
          "Mystic Red Corporation 8891" -> "Mystic Red Corporation"
        """
        if not label_cat_text:
            return ""
        # Remove trailing catalog number (digits, possibly with letter prefix)
        # Patterns: "041", "115", "X 42", "GRED 114", "8891", "028 LP"
        # But preserve label names that might contain numbers
        cleaned = re.sub(
            r'\s+(?:[A-Z]+\s+)?\d[\dA-Z\-\.]*(?:\s+(?:LP|EP))?\s*$',
            '', label_cat_text
        ).strip()
        if cleaned:
            return cleaned
        return label_cat_text.strip()

    @staticmethod
    def _is_reissue(description, title):
        """Check if release is a reissue based on description or title."""
        text = f"{description} {title}".lower()
        return bool(re.search(r'\breissue\b|\brepress\b|\bre-issue\b|\bre-press\b', text))

    @staticmethod
    def _extract_styles_from_description(description):
        """Extract genre/style tags from hardwax staff descriptions.

        Hardwax descriptions are short, opinionated blurbs like:
          "Delivering, hypnotic, sound designed, tension building Techno tracks"
          "Fantastic, solitary, The Bug prod. Ambient / Noise arctic scapes"
          "Dreamy Techno / Tech House floaters"
        """
        if not description:
            return []

        desc_lower = description.lower()
        styles = []

        for keyword, style in HARDWAX_DESCRIPTION_KEYWORDS.items():
            if keyword in desc_lower:
                if style not in styles:
                    styles.append(style)

        return styles


if __name__ == "__main__":
    cutoff = datetime.now() - timedelta(days=30)
    fetcher = HardwaxFetcher()

    print("=== Fetching new releases ===")
    releases = fetcher.fetch_new_releases(cutoff)
    print(f"\nFound {len(releases)} new releases")
    for r in releases[:15]:
        print(f"  {r['date']} | {r['artist']} - {r['title']} "
              f"[{r['label']}] ({r['genre']}) {r.get('format', '')}")

    print("\n=== Fetching house genre ===")
    fetcher._seen_ids.clear()
    house = fetcher.fetch_by_genre("house", cutoff, max_pages=1)
    print(f"\nFound {len(house)} house releases")
    for r in house[:10]:
        print(f"  {r['artist']} - {r['title']} [{r['label']}] ({r['genre']})")
