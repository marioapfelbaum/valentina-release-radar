"""
Bandcamp Daily — Album picks from daily.bandcamp.com editorial articles.

Parses the RSS feed for article URLs, then fetches each article page
to extract album data from data-player-infos attributes.

Bandcamp blocks Python requests via TLS fingerprinting, so curl is
used as the primary fetch method with requests as fallback.
"""

import json
import re
import subprocess
import time
import xml.etree.ElementTree as ET
from datetime import datetime, timezone
from html import unescape

try:
    import requests
except ImportError:
    requests = None

try:
    from bs4 import BeautifulSoup
except ImportError:
    BeautifulSoup = None

from .base import BaseSourceFetcher
from .genre_map import classify_genre


RELEVANT_CATEGORIES = [
    "album of the day",
    "essential releases",
    "best electronic",
    "best ambient",
    "best jazz",
    "big ups",
    "best experimental",
    "best dance",
    "best club",
    "best house",
    "best techno",
]

MAX_ARTICLES = 15
RATE_LIMIT = 3.0


class BandcampDailyFetcher(BaseSourceFetcher):
    name = "bandcamp_daily"

    def __init__(self, rate_limit=3.0):
        super().__init__(rate_limit=rate_limit)
        self._seen_ids = set()

    # --- HTTP helpers ---

    def _curl_get(self, url, timeout=25):
        """Primary fetch via curl (bypasses Bandcamp TLS fingerprinting)."""
        try:
            result = subprocess.run(
                ["curl", "-s", "-L", "--max-time", str(timeout),
                 "-H", "User-Agent: Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
                 "-H", "Accept: text/html,application/xhtml+xml,*/*",
                 "-H", "Accept-Language: en-US,en;q=0.9",
                 url],
                capture_output=True, text=True, timeout=timeout + 10
            )
            return result.stdout
        except (subprocess.TimeoutExpired, FileNotFoundError):
            return ""

    def _requests_get(self, url, timeout=25):
        """Fallback fetch via requests (may fail on Bandcamp)."""
        if requests is None:
            return ""
        try:
            headers = {
                "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                              "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
                "Accept": "text/html,application/xhtml+xml,*/*",
                "Accept-Language": "en-US,en;q=0.9",
            }
            resp = requests.get(url, headers=headers, timeout=timeout)
            if resp.status_code == 200:
                return resp.text
        except Exception:
            pass
        return ""

    def _fetch_page(self, url, timeout=25):
        """Fetch a page: curl first, requests fallback."""
        html = self._curl_get(url, timeout=timeout)
        if html and len(html) > 200:
            return html
        html = self._requests_get(url, timeout=timeout)
        if html and len(html) > 200:
            return html
        return ""

    # --- RSS parsing ---

    def _fetch_rss(self):
        """Fetch and parse the Bandcamp Daily RSS feed. Returns list of article dicts."""
        url = "https://daily.bandcamp.com/feed"
        xml_text = self._fetch_page(url)
        if not xml_text:
            print("  [bandcamp_daily] Could not fetch RSS feed")
            return []

        articles = []
        try:
            root = ET.fromstring(xml_text)
        except ET.ParseError:
            print("  [bandcamp_daily] RSS XML parse error")
            return []

        channel = root.find("channel")
        if channel is None:
            return []

        for item in channel.findall("item"):
            title_el = item.find("title")
            link_el = item.find("link")
            pub_date_el = item.find("pubDate")
            # Categories can be in <category> tags
            categories = [cat.text.strip().lower() for cat in item.findall("category") if cat.text]

            title = title_el.text.strip() if title_el is not None and title_el.text else ""
            link = link_el.text.strip() if link_el is not None and link_el.text else ""
            pub_date = pub_date_el.text.strip() if pub_date_el is not None and pub_date_el.text else ""

            if not link:
                continue

            articles.append({
                "title": title,
                "link": link,
                "pub_date": pub_date,
                "categories": categories,
            })

        return articles

    def _is_relevant_article(self, article):
        """Check if an article matches our relevant categories."""
        title_lower = article["title"].lower()
        cats = article["categories"]

        for relevant in RELEVANT_CATEGORIES:
            if relevant in title_lower:
                return True
            for cat in cats:
                if relevant in cat:
                    return True
        return False

    def _parse_pub_date(self, pub_date_str):
        """Parse RSS pubDate to ISO date string."""
        if not pub_date_str:
            return ""
        # RSS dates like "Fri, 21 Mar 2026 12:00:00 +0000"
        for fmt in [
            "%a, %d %b %Y %H:%M:%S %z",
            "%a, %d %b %Y %H:%M:%S %Z",
        ]:
            try:
                dt = datetime.strptime(pub_date_str, fmt)
                return dt.strftime("%Y-%m-%d")
            except ValueError:
                continue
        # Try to extract date with regex
        m = re.search(r"(\d{1,2})\s+(\w{3})\s+(\d{4})", pub_date_str)
        if m:
            try:
                dt = datetime.strptime(f"{m.group(1)} {m.group(2)} {m.group(3)}", "%d %b %Y")
                return dt.strftime("%Y-%m-%d")
            except ValueError:
                pass
        return ""

    # --- Article page parsing ---

    def _extract_player_infos(self, html):
        """Extract album data from data-player-infos attribute in article HTML."""
        albums = []

        # Try regex first (works without BeautifulSoup)
        matches = re.findall(r'data-player-infos="([^"]*)"', html)
        if not matches:
            # Try single quotes
            matches = re.findall(r"data-player-infos='([^']*)'", html)

        for match in matches:
            try:
                decoded = unescape(match)
                data = json.loads(decoded)
                if isinstance(data, list):
                    albums.extend(data)
                elif isinstance(data, dict):
                    albums.append(data)
            except (json.JSONDecodeError, ValueError):
                continue

        # Fallback: BeautifulSoup
        if not albums and BeautifulSoup:
            try:
                soup = BeautifulSoup(html, "html.parser")
                for el in soup.find_all(attrs={"data-player-infos": True}):
                    raw = el.get("data-player-infos", "")
                    if raw:
                        try:
                            data = json.loads(raw)
                            if isinstance(data, list):
                                albums.extend(data)
                            elif isinstance(data, dict):
                                albums.append(data)
                        except (json.JSONDecodeError, ValueError):
                            continue
            except Exception:
                pass

        return albums

    def _extract_jsonld(self, html):
        """Extract genre/keywords from JSON-LD in article HTML."""
        genre = ""
        keywords = []
        article_section = ""

        pattern = re.findall(
            r'<script\s+type="application/ld\+json">(.*?)</script>',
            html, re.DOTALL
        )
        for block in pattern:
            try:
                data = json.loads(block)
                if isinstance(data, list):
                    data = data[0] if data else {}
                genre = data.get("genre", genre) or genre
                kw = data.get("keywords", [])
                if isinstance(kw, str):
                    keywords = [k.strip() for k in kw.split(",")]
                elif isinstance(kw, list):
                    keywords = kw
                article_section = data.get("articleSection", article_section) or article_section
            except (json.JSONDecodeError, ValueError):
                continue

        return genre, keywords, article_section

    # --- Release creation ---

    def _album_to_release(self, album, pub_date_str, article_title=""):
        """Convert a player-infos album dict to a release dict."""
        band_name = album.get("band_name", "").strip()
        title = album.get("title", "").strip()
        tralbum_url = album.get("tralbum_url", "").strip()
        band_id = album.get("band_id", "")
        art_id = album.get("art_id", "")

        if not band_name or not title:
            return None
        if not tralbum_url:
            return None

        source_id = f"bcd:{band_id}:{art_id}"
        if source_id in self._seen_ids:
            return None
        self._seen_ids.add(source_id)

        release_date = self._parse_pub_date(pub_date_str)

        # Build image URL from art_id
        image_url = ""
        if art_id:
            image_url = f"https://f4.bcbits.com/img/a{art_id}_10.jpg"

        # Classify genre from article title hints
        style_tags = []
        if article_title:
            style_tags.append(article_title)
        genre = classify_genre(style_tags) if style_tags else "Electronic"

        release = self.make_release(
            artist=band_name,
            title=title,
            label="",
            source="bandcamp_daily",
            source_id=source_id,
            source_url=tralbum_url,
            date=release_date or datetime.now().strftime("%Y-%m-%d"),
            genre=genre,
            styles=style_tags,
            date_verified=True,
        )
        return release

    # --- Public API ---

    def fetch_by_genre(self, genre=None, cutoff_date=None, max_pages=3):
        """Fetch all releases from Bandcamp Daily (genre param ignored)."""
        return self.fetch_all(cutoff_date)

    def fetch_by_artist(self, artist_name, cutoff_date=None):
        """Not applicable for editorial source."""
        return []

    def fetch_all(self, cutoff_date=None):
        """
        Main entry point:
        1. Fetch RSS feed
        2. Filter relevant articles
        3. Fetch each article page
        4. Extract albums from data-player-infos
        5. Return deduplicated releases
        """
        print(f"  {chr(9656)} Bandcamp Daily: fetching RSS feed...")
        articles = self._fetch_rss()
        if not articles:
            print("  [bandcamp_daily] No articles found in RSS feed")
            return []

        print(f"  {chr(9656)} Bandcamp Daily: {len(articles)} articles in feed")

        # Filter to relevant categories
        relevant = [a for a in articles if self._is_relevant_article(a)]
        print(f"  {chr(9656)} Bandcamp Daily: {len(relevant)} relevant articles")

        if not relevant:
            # If no category match, take all articles (editorial is curated anyway)
            relevant = articles
            print(f"  {chr(9656)} Bandcamp Daily: using all {len(relevant)} articles (no category filter match)")

        # Limit to MAX_ARTICLES
        relevant = relevant[:MAX_ARTICLES]

        releases = []
        for i, article in enumerate(relevant):
            url = article["link"]
            pub_date = article["pub_date"]
            article_title = article["title"]
            print(f"  {chr(9656)} Bandcamp Daily: [{i+1}/{len(relevant)}] {article_title[:60]}...")

            html = self._fetch_page(url)
            if not html:
                print(f"    [skip] could not fetch article page")
                time.sleep(RATE_LIMIT)
                continue

            # Extract albums from player infos
            albums = self._extract_player_infos(html)

            # Extract genre hints from JSON-LD
            jsonld_genre, keywords, article_section = self._extract_jsonld(html)
            style_tags = []
            if jsonld_genre:
                style_tags.append(jsonld_genre)
            if article_section:
                style_tags.append(article_section)
            style_tags.extend(keywords[:5])

            for album in albums:
                release = self._album_to_release(album, pub_date, article_title)
                if release:
                    # Enrich with JSON-LD style tags if available
                    if style_tags and release.get("style_tags", []) != style_tags:
                        combined = list(set(release.get("style_tags", []) + style_tags))
                        release["style_tags"] = combined
                        # Re-classify with better tags
                        genre = classify_genre(combined)
                        if genre and genre != "Other":
                            release["genre"] = genre
                    releases.append(release)

            if albums:
                print(f"    {chr(10003)} {len(albums)} albums extracted")
            else:
                print(f"    [skip] no album data found")

            time.sleep(RATE_LIMIT)

        print(f"  {chr(10003)} Bandcamp Daily: {len(releases)} releases total")
        return releases


if __name__ == "__main__":
    fetcher = BandcampDailyFetcher()
    results = fetcher.fetch_all()
    print(f"\n=== Bandcamp Daily: {len(results)} releases ===")
    for r in results[:10]:
        print(f"  {r.get('artist', '?')} - {r.get('title', '?')} [{r.get('genre', '?')}]")
        print(f"    {r.get('source_url', '')}")
