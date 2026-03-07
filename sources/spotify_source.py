"""
Spotify source fetcher.
Uses Spotify Web API to fetch artist albums/singles.
Reuses auth pattern from crawler.py SpotifyClient.
"""

import json
import os
import re
import sys
import time
from datetime import datetime, timedelta
from pathlib import Path

try:
    import requests
except ImportError:
    print("pip install requests")
    sys.exit(1)

from .base import BaseSourceFetcher
from .genre_map import classify_genre

# Load .env
_env_path = Path(__file__).parent.parent / ".env"
if _env_path.exists():
    for line in _env_path.read_text().splitlines():
        line = line.strip()
        if line and not line.startswith("#") and "=" in line:
            k, v = line.split("=", 1)
            os.environ.setdefault(k.strip(), v.strip().strip('"').strip("'"))


class SpotifyFetcher(BaseSourceFetcher):
    name = "spotify"

    AUTH_URL = "https://accounts.spotify.com/api/token"
    BASE = "https://api.spotify.com/v1"

    def __init__(self, client_id=None, client_secret=None, rate_limit=0.15):
        super().__init__(rate_limit=rate_limit)
        self.client_id = client_id or os.getenv("SPOTIFY_CLIENT_ID", "")
        self.client_secret = client_secret or os.getenv("SPOTIFY_CLIENT_SECRET", "")
        self.available = bool(self.client_id and self.client_secret)
        self._session = requests.Session()
        self._token = None
        self._token_expires = 0

        if self.available:
            self._authenticate()

    def _authenticate(self):
        """Get Spotify access token via Client Credentials flow."""
        try:
            resp = requests.post(self.AUTH_URL, data={
                "grant_type": "client_credentials",
                "client_id": self.client_id,
                "client_secret": self.client_secret,
            }, timeout=10)
            resp.raise_for_status()
            data = resp.json()
            self._token = data["access_token"]
            self._token_expires = time.time() + data.get("expires_in", 3600) - 60
            self._session.headers["Authorization"] = f"Bearer {self._token}"
            print("  ✓ Spotify authenticated")
        except Exception as e:
            print(f"  ✗ Spotify auth failed: {e}")
            self.available = False

    def _get(self, endpoint, params=None):
        """Make authenticated GET request to Spotify API."""
        if not self.available:
            return None
        if time.time() > self._token_expires:
            self._authenticate()
            if not self.available:
                return None

        self._throttle()
        url = f"{self.BASE}/{endpoint}" if not endpoint.startswith("http") else endpoint
        try:
            resp = self._session.get(url, params=params, timeout=15)
            if resp.status_code == 429:
                wait = int(resp.headers.get("Retry-After", 5))
                if wait > 60:
                    print(f"    ⚠ Spotify rate limit too long ({wait}s), skipping")
                    return None
                print(f"    ⏳ Spotify rate limit, waiting {wait}s...")
                time.sleep(wait)
                return self._get(endpoint, params)
            if resp.status_code in (404, 400):
                return None
            resp.raise_for_status()
            return resp.json()
        except requests.RequestException as e:
            print(f"    ⚠ Spotify error: {e}")
            return None

    def fetch_by_genre(self, genre_id, cutoff_date, max_pages=1):
        """Spotify doesn't have genre-based release browsing in the same way.
        Use fetch_new_releases for general new releases.
        """
        return self.fetch_new_releases(cutoff_date, limit=10)

    def fetch_new_releases(self, cutoff_date, limit=10):
        """Fetch Spotify's new releases (general, not genre-specific)."""
        if not self.available:
            return []

        data = self._get("browse/new-releases", {"limit": limit, "country": "DE"})
        if not data or "albums" not in data:
            return []

        releases = []
        cutoff_str = cutoff_date.strftime("%Y-%m-%d")

        for album in data["albums"].get("items", []):
            rel = self._normalize_album(album)
            if rel and rel["date"] >= cutoff_str:
                releases.append(rel)

        return releases

    def fetch_by_artist(self, artist_name, cutoff_date, spotify_id=None):
        """Fetch releases for a specific artist.

        Args:
            artist_name: Artist name
            cutoff_date: datetime
            spotify_id: Optional Spotify artist ID (avoids search)

        Returns:
            List of unified release dicts
        """
        if not self.available:
            return []

        # Find artist ID
        if not spotify_id:
            spotify_id = self._search_artist_id(artist_name)
        if not spotify_id:
            return []

        # Get albums and singles
        releases = []
        cutoff_str = cutoff_date.strftime("%Y-%m-%d")

        page_size = 10  # Spotify API max for artist albums endpoint

        for group in ["album", "single"]:
            offset = 0
            while True:
                data = self._get(f"artists/{spotify_id}/albums", {
                    "include_groups": group,
                    "limit": page_size,
                    "offset": offset,
                    "market": "DE",
                })
                if not data or "items" not in data:
                    break

                past_cutoff = False
                for album in data["items"]:
                    rel = self._normalize_album(album, artist_name)
                    if rel:
                        if rel["date"] >= cutoff_str:
                            releases.append(rel)
                        elif rel["date"] < cutoff_str:
                            # Albums are sorted newest first, so we can stop
                            past_cutoff = True
                            break

                # Check if there are more pages
                if past_cutoff or not data.get("next") or len(data["items"]) < page_size:
                    break
                offset += page_size

        return releases

    def fetch_for_artists(self, artists, cutoff_date, progress_cb=None):
        """Fetch releases for a list of artists.

        Args:
            artists: List of dicts with 'name' and optional 'spotify_id'
            cutoff_date: datetime
            progress_cb: Optional callback(artist_name, count)

        Returns:
            List of unified release dicts
        """
        if not self.available:
            print("  ⚠ Spotify: no credentials configured, skipping")
            return []

        all_releases = {}
        for i, artist in enumerate(artists):
            name = artist.get("name", "")
            sp_id = artist.get("spotify_id")
            if not name:
                continue

            releases = self.fetch_by_artist(name, cutoff_date, spotify_id=sp_id)
            for rel in releases:
                all_releases[rel["id"]] = rel

            if progress_cb:
                progress_cb(name, len(releases))

            if (i + 1) % 50 == 0:
                print(f"    Spotify: {i+1}/{len(artists)} artists checked, {len(all_releases)} releases")

        result = list(all_releases.values())
        print(f"  ✓ Spotify total: {len(result)} unique releases")
        return result

    def _search_artist_id(self, artist_name):
        """Search Spotify for an artist, return their ID."""
        data = self._get("search", {
            "q": f'artist:"{artist_name}"',
            "type": "artist",
            "limit": 5,
        })
        if not data or "artists" not in data:
            return None

        items = data["artists"].get("items", [])
        if not items:
            return None

        # Exact match preferred
        name_lower = artist_name.lower().strip()
        for item in items:
            if item.get("name", "").lower().strip() == name_lower:
                return item["id"]

        # Fall back to first result
        return items[0]["id"]

    def get_artist_genres(self, spotify_id):
        """Get genres for an artist (useful for genre classification)."""
        if not self.available or not spotify_id:
            return []
        data = self._get(f"artists/{spotify_id}")
        if not data:
            return []
        return data.get("genres", [])

    def _normalize_album(self, album, fallback_artist=None):
        """Convert Spotify album object to unified release schema."""
        if not isinstance(album, dict):
            return None

        sp_id = album.get("id", "")
        name = album.get("name", "").strip()
        if not sp_id or not name:
            return None

        # Artists
        artists = album.get("artists", [])
        if artists:
            artist = ", ".join(a.get("name", "") for a in artists if a.get("name"))
        else:
            artist = fallback_artist or ""

        if not artist:
            return None

        # Date - Spotify provides various precision levels
        date = album.get("release_date", "")
        precision = album.get("release_date_precision", "day")
        if precision == "year" and len(date) == 4:
            date = f"{date}-01-01"
        elif precision == "month" and len(date) == 7:
            date = f"{date}-01"

        if not date:
            return None

        # Label (only in full album objects, not in simplified)
        label = album.get("label", "")

        # Format from album_type
        album_type = album.get("album_type", "")
        if album_type == "single":
            fmt = "Single"
        elif album_type == "album":
            total = album.get("total_tracks", 0)
            fmt = "EP" if total and total <= 6 else "LP"
        elif album_type == "compilation":
            fmt = "Compilation"
        else:
            fmt = ""

        # URL
        ext_urls = album.get("external_urls", {})
        source_url = ext_urls.get("spotify", "")

        # Genre - Spotify doesn't tag albums with genres, only artists
        # We'll classify based on artist genres if we have them
        genre = "Electronic"

        return self.make_release(
            source="spotify",
            source_id=sp_id,
            title=name,
            artist=artist,
            label=label,
            genre=genre,
            date=date,
            source_url=source_url,
            format_type=fmt,
        )


if __name__ == "__main__":
    cutoff = datetime.now() - timedelta(days=90)
    fetcher = SpotifyFetcher()
    if fetcher.available:
        # Test with a known artist
        releases = fetcher.fetch_by_artist("Ricardo Villalobos", cutoff)
        print(f"\nRicardo Villalobos: {len(releases)} releases")
        for r in releases[:5]:
            print(f"  {r['date']} | {r['artist']} - {r['title']} [{r['label']}] ({r['format']})")
    else:
        print("Spotify not configured (SPOTIFY_CLIENT_ID / SPOTIFY_CLIENT_SECRET empty)")
