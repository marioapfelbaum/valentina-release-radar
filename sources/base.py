"""Base class for all source fetchers."""

import hashlib
import time
from abc import ABC, abstractmethod
from datetime import datetime


class BaseSourceFetcher(ABC):
    """Abstract base for Beatport, Bandcamp, Spotify, Discogs fetchers."""

    name = "base"  # Override in subclasses

    def __init__(self, rate_limit=1.0):
        self._last_request = 0
        self._rate_limit = rate_limit
        self._request_count = 0

    def _throttle(self):
        """Respect rate limits between requests."""
        elapsed = time.time() - self._last_request
        if elapsed < self._rate_limit:
            time.sleep(self._rate_limit - elapsed)
        self._last_request = time.time()
        self._request_count += 1

    @abstractmethod
    def fetch_by_genre(self, genre_id, cutoff_date, max_pages=3):
        """Fetch new releases by genre/category browse.
        Returns list of release dicts in unified schema.
        """
        pass

    @abstractmethod
    def fetch_by_artist(self, artist_name, cutoff_date):
        """Fetch releases for a specific artist.
        Returns list of release dicts in unified schema.
        """
        pass

    @staticmethod
    def make_id(source, source_id):
        """Generate stable 8-char ID from source + source_id."""
        raw = f"{source}:{source_id}"
        return hashlib.md5(raw.encode()).hexdigest()[:8]

    @staticmethod
    def make_release(*, source, source_id, title, artist, label,
                     genre, date, source_url="", album=None,
                     duration="", styles=None, bpm=None,
                     catalog_number=None, format_type=None,
                     reissue=False, discogs_url=""):
        """Create a unified release dict."""
        return {
            "id": BaseSourceFetcher.make_id(source, source_id),
            "title": title,
            "artist": artist,
            "album": album or title,
            "label": label,
            "genre": genre,
            "duration": duration,
            "date": date,
            "re": reissue,
            "styles": styles or [],
            "source": source,
            "source_url": source_url,
            "discogs_url": discogs_url,
            "bpm": bpm,
            "catalog_number": catalog_number,
            "format": format_type or "",
        }
