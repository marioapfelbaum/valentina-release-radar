"""
Bandcamp source fetcher.
Uses Bandcamp's mobile API to fetch label discographies.
Falls back to curl for HTML scraping when needed (Bandcamp blocks Python requests via TLS fingerprinting).
"""

import json
import re
import subprocess
import sys
import time
from datetime import datetime, timedelta
from pathlib import Path
from urllib.parse import quote_plus

try:
    import requests
except ImportError:
    print("pip install requests")
    sys.exit(1)

from .base import BaseSourceFetcher
from .genre_map import classify_genre


# Known label → Bandcamp subdomain mappings
# Only includes labels verified to exist on Bandcamp
KNOWN_LABEL_SLUGS = {
    "perlon": "perlon",
    "cocoon": "cocoonrecordings",
    "kompakt": "kompakt",
    "dial": "dialrec",
    "running back": "runningback",
    "pampa records": "pamparecords",
    "giegling": "giegling",
    "yoyaku": "yoyaku",
    "mule musiq": "mulemusiq",
    "live at robert johnson": "liveatrobertjohnson",
    "rawax": "rawax",
    "cadenza": "cadenzarecords",
    "desolat": "desolat",
    "ornaments": "ornaments",
    "piv records": "pivrecords",
    "cyclic records": "cyclicrecords",
    "studio !k7": "k7records",
    "mojuba records": "mojuba",
    "fuse london": "fuselondon",
    "visionquest": "visionquest",
    "circus company": "circuscompany",
    "sacre": "sacrerecords",
    "nervmusic": "nervmusic",
}

CACHE_FILE = "bandcamp_labels.json"
MOBILE_API = "https://bandcamp.com/api/mobile/25/band_details"


class BandcampFetcher(BaseSourceFetcher):
    name = "bandcamp"

    def __init__(self, labels_file=None, rate_limit=2.0):
        super().__init__(rate_limit=rate_limit)
        self._labels_file = labels_file or Path(__file__).parent.parent / "reference_labels.txt"
        self._label_slugs = dict(KNOWN_LABEL_SLUGS)
        self._band_ids = {}  # label_slug → band_id cache
        self._load_cached_data()

    def _load_cached_data(self):
        """Load cached label slugs and band IDs."""
        cache_path = Path(__file__).parent.parent / CACHE_FILE
        if cache_path.exists():
            try:
                with open(cache_path) as f:
                    cached = json.load(f)
                if "slugs" in cached:
                    self._label_slugs.update(cached["slugs"])
                if "band_ids" in cached:
                    self._band_ids.update(cached["band_ids"])
            except (json.JSONDecodeError, IOError):
                pass

    def _save_cached_data(self):
        """Save label slugs and band IDs."""
        cache_path = Path(__file__).parent.parent / CACHE_FILE
        try:
            with open(cache_path, "w") as f:
                json.dump({
                    "slugs": self._label_slugs,
                    "band_ids": self._band_ids,
                }, f, indent=2, ensure_ascii=False)
        except IOError:
            pass

    def _curl_get(self, url, timeout=20):
        """Fetch URL via curl (bypasses TLS fingerprinting blocks)."""
        try:
            result = subprocess.run(
                ["curl", "-s", "-L", "--max-time", str(timeout),
                 "-H", "User-Agent: Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36",
                 url],
                capture_output=True, text=True, timeout=timeout + 5
            )
            return result.stdout
        except (subprocess.TimeoutExpired, FileNotFoundError):
            return ""

    def _curl_post_json(self, url, data, timeout=15):
        """POST JSON via curl."""
        try:
            result = subprocess.run(
                ["curl", "-s", "--max-time", str(timeout),
                 "-H", "User-Agent: Mozilla/5.0",
                 "-H", "Content-Type: application/json",
                 "-X", "POST",
                 "-d", json.dumps(data),
                 url],
                capture_output=True, text=True, timeout=timeout + 5
            )
            return json.loads(result.stdout) if result.stdout else None
        except (subprocess.TimeoutExpired, json.JSONDecodeError, FileNotFoundError):
            return None

    def _get_band_id(self, slug):
        """Get Bandcamp band_id for a label subdomain."""
        if slug in self._band_ids:
            return self._band_ids[slug]

        self._throttle()
        html = self._curl_get(f"https://{slug}.bandcamp.com")
        if not html:
            return None

        # Extract band data from data-band attribute
        match = re.search(r'data-band="([^"]+)"', html)
        if match:
            try:
                decoded = match.group(1).replace("&quot;", '"').replace("&amp;", "&")
                band_data = json.loads(decoded)
                band_id = band_data.get("id")
                if band_id:
                    self._band_ids[slug] = band_id
                    self._save_cached_data()
                    return band_id
            except json.JSONDecodeError:
                pass

        # Fallback: look for band_id in page source
        match = re.search(r'"band_id"\s*:\s*(\d+)', html)
        if match:
            band_id = int(match.group(1))
            self._band_ids[slug] = band_id
            self._save_cached_data()
            return band_id

        return None

    def _get_label_slug(self, label_name):
        """Get Bandcamp subdomain for a label."""
        key = label_name.lower().strip()
        if key in self._label_slugs:
            return self._label_slugs[key]

        # Try to find via curl search
        self._throttle()
        html = self._curl_get(
            f"https://bandcamp.com/search?q={quote_plus(label_name)}&item_type=b"
        )
        if not html:
            return None

        # Find label subdomain in search results
        matches = re.findall(r'href="https://([a-z0-9\-]+)\.bandcamp\.com', html)
        skip = {"daily", "isrc", "bandcamp", "get", "www"}
        for m in matches:
            if m not in skip:
                self._label_slugs[key] = m
                self._save_cached_data()
                return m

        return None

    def fetch_by_genre(self, genre_id, cutoff_date, max_pages=1):
        """Not applicable for Bandcamp. Use fetch_all_labels instead."""
        return self.fetch_all_labels(cutoff_date)

    def fetch_all_labels(self, cutoff_date, max_labels=None):
        """Fetch discographies for all reference labels via Bandcamp Mobile API.

        Returns:
            List of unified release dicts
        """
        labels = self._load_reference_labels()
        if max_labels:
            labels = labels[:max_labels]

        all_releases = []
        skipped = 0

        for label_name in labels:
            slug = self._get_label_slug(label_name)
            if not slug:
                print(f"  ▸ Bandcamp: {label_name} — not found on Bandcamp")
                skipped += 1
                continue

            band_id = self._get_band_id(slug)
            if not band_id:
                print(f"  ▸ Bandcamp: {label_name} ({slug}) — no band_id found")
                skipped += 1
                continue

            print(f"  ▸ Bandcamp: {label_name} ({slug}, id={band_id})")
            releases = self._fetch_discography(band_id, label_name, slug, cutoff_date)
            all_releases.extend(releases)
            print(f"    → {len(releases)} releases")

        print(f"  ✓ Bandcamp total: {len(all_releases)} releases ({skipped} labels skipped)")
        return all_releases

    def _fetch_discography(self, band_id, label_name, slug, cutoff_date):
        """Fetch label discography via Bandcamp Mobile API."""
        self._throttle()
        data = self._curl_post_json(MOBILE_API, {"band_id": band_id})

        if not data or "discography" not in data:
            return []

        releases = []
        cutoff_str = cutoff_date.strftime("%Y-%m-%d")

        for item in data["discography"]:
            rel = self._normalize_mobile_release(item, label_name, slug)
            if rel and rel["date"] >= cutoff_str:
                releases.append(rel)

        return releases

    def _normalize_mobile_release(self, item, label_name, slug):
        """Normalize a Bandcamp Mobile API discography item."""
        title = (item.get("title") or "").strip()
        if not title:
            return None

        artist = (item.get("artist_name") or item.get("band_name") or "").strip()
        item_type = item.get("item_type", "album")
        item_id = item.get("item_id", "")

        # Parse date: "02 Feb 2026 00:00:00 GMT"
        date = ""
        raw_date = item.get("release_date") or ""
        if raw_date:
            for fmt in ["%d %b %Y %H:%M:%S %Z", "%d %b %Y", "%Y-%m-%d"]:
                try:
                    dt = datetime.strptime(raw_date.strip(), fmt)
                    date = dt.strftime("%Y-%m-%d")
                    break
                except ValueError:
                    continue

        if not date:
            return None

        # Construct URL (we don't have the slug from the API, so use the item type)
        # The actual album page slug isn't in the mobile API, but we can reconstruct
        title_slug = re.sub(r'[^a-z0-9]+', '-', title.lower()).strip('-')
        source_url = f"https://{slug}.bandcamp.com/{item_type}/{title_slug}"

        # Different band_id means this is a guest artist release on the label
        item_band_id = item.get("band_id")

        return self.make_release(
            source="bandcamp",
            source_id=f"bc:{item_id}",
            title=title,
            artist=artist,
            label=label_name,
            genre="Electronic",  # Bandcamp mobile API doesn't provide tags
            date=date,
            source_url=source_url,
            format_type=self._infer_format(item_type),
        )

    @staticmethod
    def _infer_format(item_type):
        if item_type == "track":
            return "Single"
        return ""  # Can't distinguish EP/LP without track count from mobile API

    def fetch_by_artist(self, artist_name, cutoff_date):
        """Search Bandcamp for an artist's releases via curl."""
        self._throttle()
        html = self._curl_get(
            f"https://bandcamp.com/search?q={quote_plus(artist_name)}&item_type=a"
        )
        if not html:
            return []

        return self._parse_search_results(html, artist_name, cutoff_date)

    def _parse_search_results(self, html, artist_name, cutoff_date):
        """Parse Bandcamp search results."""
        releases = []
        artist_lower = artist_name.lower()

        # Find result items with title and artist
        pattern = (
            r'class="heading"[^>]*>\s*<a[^>]*>([^<]+)</a>.*?'
            r'class="subhead"[^>]*>\s*by\s+([^<]+?)(?:\s*<|$).*?'
            r'class="itemurl"[^>]*>\s*<a[^>]*href="([^"]+)"'
        )
        for m in re.finditer(pattern, html, re.DOTALL):
            title = m.group(1).strip()
            by_artist = m.group(2).strip()
            url = m.group(3).strip()

            if artist_lower not in by_artist.lower():
                continue

            bc_id = url.rstrip("/").split("/")[-1]
            releases.append(self.make_release(
                source="bandcamp",
                source_id=f"search:{bc_id}",
                title=title,
                artist=by_artist,
                label="",
                genre="Electronic",
                date=datetime.now().strftime("%Y-%m-%d"),
                source_url=url,
            ))

        return releases

    def _load_reference_labels(self):
        """Load label names from reference_labels.txt, ignoring comments and blanks."""
        path = Path(self._labels_file)
        if not path.exists():
            print(f"  ⚠ Labels file not found: {path}")
            return []
        return [line.strip() for line in path.read_text().splitlines()
                if line.strip() and not line.strip().startswith("#")]


if __name__ == "__main__":
    cutoff = datetime.now() - timedelta(days=180)
    fetcher = BandcampFetcher()
    releases = fetcher.fetch_all_labels(cutoff, max_labels=3)
    print(f"\nFound {len(releases)} releases")
    for r in releases[:10]:
        print(f"  {r['date']} | {r['artist']} - {r['title']} [{r['label']}]")
