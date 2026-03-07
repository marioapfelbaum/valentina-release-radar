#!/usr/bin/env python3
"""
LABEL CRAWLER v2.0 — Multi-Source Edition
==========================================
Rekursive Künstler ↔ Label Entdeckung über 3 Quellen:
  1. Discogs    — Vinyl/Physical, beste Label-Zuordnungen
  2. MusicBrainz — Open Database, Digital-Releases
  3. Spotify     — Related Artists, Popularity, Genre-Tags

Setup:
  pip install requests --break-system-packages
  
  Required:  Discogs Token (kostenlos: discogs.com/settings/developers)
  Optional:  Spotify Client ID + Secret (developer.spotify.com)
  MusicBrainz braucht keinen Key

Usage:
  python crawler.py                              # Discogs + MusicBrainz
  python crawler.py --spotify                    # + Spotify Related Artists
  python crawler.py --max-depth 4                # Tiefe begrenzen
  python crawler.py --resume                     # Fortsetzen nach Abbruch
"""

import json
import os
import sys
import time
import argparse
import re
from pathlib import Path
from datetime import datetime
from collections import defaultdict

try:
    import requests
except ImportError:
    print("❌ requests nicht installiert:")
    print("   pip install requests --break-system-packages")
    sys.exit(1)


# ─────────────────────────────────────────────────
# CONFIG
# ─────────────────────────────────────────────────

DEFAULT_MAX_DEPTH = 10
MIN_RELEVANCE_SCORE = 2
SAVE_INTERVAL = 10

RELEVANT_GENRES = {
    "electronic", "house", "deep house", "minimal", "tech house",
    "techno", "ambient", "experimental", "disco", "acid house",
    "microhouse", "dub techno", "progressive house", "downtempo",
    "electronica", "leftfield", "italo disco", "nu-disco",
    "detroit techno", "minimal techno", "afro house", "dub",
    "electro", "breaks", "drum and bass", "garage", "uk garage",
    "trip hop", "idm", "industrial"
}

SKIP_LABELS = {
    "not on label", "white label", "self-released", "unknown",
    "sony", "universal", "warner", "bmg", "emi", "columbia",
    "atlantic", "island", "polydor", "virgin records", "capitol",
    "rca", "epic", "elektra", "geffen", "interscope",
    "parlophone", "def jam", "republic", "hollywood"
}

SPOTIFY_GENRE_KEYWORDS = {
    "house", "techno", "electronic", "deep", "minimal", "ambient",
    "electro", "disco", "dub", "acid", "garage", "detroit",
    "downtempo", "idm", "experimental", "microhouse", "leftfield",
    "breaks", "bass", "dance", "club", "rave", "trance",
    "electronica", "synth", "wave"
}


# ─────────────────────────────────────────────────
# API CLIENTS
# ─────────────────────────────────────────────────

class DiscogsClient:
    """Discogs API — Rate Limit: 60 req/min mit Token"""

    BASE = "https://api.discogs.com"

    def __init__(self, token: str):
        self.session = requests.Session()
        self.session.headers.update({
            "Authorization": f"Discogs token={token}",
            "User-Agent": "ValentinaLabelCrawler/2.0 +https://valentina.studio"
        })
        self.request_count = 0
        self.last_request = 0

    def _get(self, url, params=None):
        elapsed = time.time() - self.last_request
        if elapsed < 1.1:
            time.sleep(1.1 - elapsed)
        self.last_request = time.time()
        self.request_count += 1
        try:
            resp = self.session.get(url, params=params, timeout=15)
            if resp.status_code == 429:
                wait = int(resp.headers.get("Retry-After", 60))
                print(f"    ⏳ Discogs Rate Limit, warte {wait}s...")
                time.sleep(wait)
                return self._get(url, params)
            if resp.status_code == 404:
                return None
            resp.raise_for_status()
            return resp.json()
        except requests.exceptions.RequestException as e:
            print(f"    ⚠️ Discogs Error: {e}")
            return None

    def search_artist(self, name):
        data = self._get(f"{self.BASE}/database/search",
                         {"q": name, "type": "artist", "per_page": 5})
        if not data or not data.get("results"):
            return None
        for r in data["results"]:
            if r.get("title", "").lower().strip() == name.lower().strip():
                return r
        return data["results"][0]

    def search_label(self, name):
        data = self._get(f"{self.BASE}/database/search",
                         {"q": name, "type": "label", "per_page": 5})
        if not data or not data.get("results"):
            return None
        for r in data["results"]:
            if r.get("title", "").lower().strip() == name.lower().strip():
                return r
        return data["results"][0]

    def get_artist_releases(self, artist_id, page=1):
        return self._get(f"{self.BASE}/artists/{artist_id}/releases",
                         {"page": page, "per_page": 100, "sort": "year", "sort_order": "desc"})

    def get_label_releases(self, label_id, page=1):
        return self._get(f"{self.BASE}/labels/{label_id}/releases",
                         {"page": page, "per_page": 100})

    def get_label_info(self, label_id):
        return self._get(f"{self.BASE}/labels/{label_id}")


class MusicBrainzClient:
    """MusicBrainz API — Rate Limit: 1 req/sec, kein Key nötig"""

    BASE = "https://musicbrainz.org/ws/2"

    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "ValentinaLabelCrawler/2.0 (mario@valentina.studio)",
            "Accept": "application/json"
        })
        self.request_count = 0
        self.last_request = 0

    def _get(self, url, params=None):
        elapsed = time.time() - self.last_request
        if elapsed < 1.1:
            time.sleep(1.1 - elapsed)
        self.last_request = time.time()
        self.request_count += 1
        if params is None:
            params = {}
        params["fmt"] = "json"
        try:
            resp = self.session.get(url, params=params, timeout=15)
            if resp.status_code == 503:
                print(f"    ⏳ MusicBrainz Rate Limit, warte 5s...")
                time.sleep(5)
                return self._get(url, params)
            if resp.status_code == 404:
                return None
            resp.raise_for_status()
            return resp.json()
        except requests.exceptions.RequestException as e:
            print(f"    ⚠️ MusicBrainz Error: {e}")
            return None

    def search_artist(self, name):
        data = self._get(f"{self.BASE}/artist", {"query": f'artist:"{name}"', "limit": 5})
        if not data or not data.get("artists"):
            return None
        for a in data["artists"]:
            if a.get("name", "").lower().strip() == name.lower().strip():
                return a
        for a in data["artists"]:
            if int(a.get("score", 0)) > 90:
                return a
        return data["artists"][0] if data["artists"] else None

    def get_artist_releases(self, mbid, offset=0):
        return self._get(f"{self.BASE}/release",
                         {"artist": mbid, "inc": "labels", "limit": 100, "offset": offset})

    def search_label(self, name):
        data = self._get(f"{self.BASE}/label", {"query": f'label:"{name}"', "limit": 5})
        if not data or not data.get("labels"):
            return None
        for l in data["labels"]:
            if l.get("name", "").lower().strip() == name.lower().strip():
                return l
        return data["labels"][0] if data["labels"] else None

    def get_label_releases(self, mbid, offset=0):
        return self._get(f"{self.BASE}/release",
                         {"label": mbid, "inc": "artist-credits", "limit": 100, "offset": offset})


class SpotifyClient:
    """Spotify Web API — Token über Client Credentials"""

    AUTH_URL = "https://accounts.spotify.com/api/token"
    BASE = "https://api.spotify.com/v1"

    def __init__(self, client_id, client_secret):
        self.client_id = client_id
        self.client_secret = client_secret
        self.session = requests.Session()
        self.token = None
        self.token_expires = 0
        self.request_count = 0
        self.last_request = 0
        self._authenticate()

    def _authenticate(self):
        try:
            resp = requests.post(self.AUTH_URL, data={
                "grant_type": "client_credentials",
                "client_id": self.client_id,
                "client_secret": self.client_secret
            })
            resp.raise_for_status()
            data = resp.json()
            self.token = data["access_token"]
            self.token_expires = time.time() + data.get("expires_in", 3600) - 60
            self.session.headers.update({"Authorization": f"Bearer {self.token}"})
        except Exception as e:
            print(f"  ❌ Spotify Auth fehlgeschlagen: {e}")
            self.token = None

    def _get(self, url, params=None):
        if not self.token:
            return None
        if time.time() > self.token_expires:
            self._authenticate()
        elapsed = time.time() - self.last_request
        if elapsed < 0.1:
            time.sleep(0.1 - elapsed)
        self.last_request = time.time()
        self.request_count += 1
        try:
            resp = self.session.get(url, params=params, timeout=15)
            if resp.status_code == 429:
                wait = int(resp.headers.get("Retry-After", 5))
                print(f"    ⏳ Spotify Rate Limit, warte {wait}s...")
                time.sleep(wait)
                return self._get(url, params)
            if resp.status_code in (404, 400):
                return None
            resp.raise_for_status()
            return resp.json()
        except requests.exceptions.RequestException as e:
            print(f"    ⚠️ Spotify Error: {e}")
            return None

    def search_artist(self, name):
        data = self._get(f"{self.BASE}/search",
                         {"q": f'artist:"{name}"', "type": "artist", "limit": 5})
        if not data or not data.get("artists", {}).get("items"):
            return None
        items = data["artists"]["items"]
        for a in items:
            if a.get("name", "").lower().strip() == name.lower().strip():
                return a
        return items[0] if items else None

    def get_related_artists(self, spotify_id):
        data = self._get(f"{self.BASE}/artists/{spotify_id}/related-artists")
        if not data or not data.get("artists"):
            return []
        return data["artists"]

    def get_artist_albums(self, spotify_id, offset=0):
        return self._get(f"{self.BASE}/artists/{spotify_id}/albums",
                         {"include_groups": "album,single", "limit": 50, "offset": offset})

    def is_electronic(self, artist_data):
        genres = set(g.lower() for g in artist_data.get("genres", []))
        return bool(genres & SPOTIFY_GENRE_KEYWORDS)


# ─────────────────────────────────────────────────
# NETWORK DATABASE
# ─────────────────────────────────────────────────

class NetworkDB:
    def __init__(self, path="network_data.json"):
        self.path = path
        self.artists = {}
        self.labels = {}
        self.edges = []
        self.crawled_artists = set()
        self.crawled_labels = set()
        self.spotify_related_crawled = set()
        self.stats = {
            "total_requests": 0,
            "discogs_requests": 0,
            "musicbrainz_requests": 0,
            "spotify_requests": 0,
            "artists_found": 0,
            "labels_found": 0,
            "max_depth_reached": 0,
            "started_at": None,
            "last_updated": None,
            "sources_used": []
        }

    def save(self):
        self.stats["last_updated"] = datetime.now().isoformat()
        self.stats["artists_found"] = len(self.artists)
        self.stats["labels_found"] = len(self.labels)
        data = {
            "metadata": self.stats,
            "artists": self.artists,
            "labels": self.labels,
            "edges": self.edges,
            "crawled_artists": list(self.crawled_artists),
            "crawled_labels": list(self.crawled_labels),
            "spotify_related_crawled": list(self.spotify_related_crawled)
        }
        tmp = self.path + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
        os.replace(tmp, self.path)
        print(f"  💾 Saved: {len(self.artists)} Artists, {len(self.labels)} Labels, {len(self.edges)} Edges")

    def load(self):
        if not os.path.exists(self.path):
            return False
        try:
            with open(self.path, "r", encoding="utf-8") as f:
                data = json.load(f)
            self.artists = data.get("artists", {})
            self.labels = data.get("labels", {})
            self.edges = data.get("edges", [])
            self.crawled_artists = set(data.get("crawled_artists", []))
            self.crawled_labels = set(data.get("crawled_labels", []))
            self.spotify_related_crawled = set(data.get("spotify_related_crawled", []))
            self.stats = data.get("metadata", self.stats)
            print(f"  📂 Loaded: {len(self.artists)} Artists, {len(self.labels)} Labels")
            return True
        except (json.JSONDecodeError, KeyError):
            return False

    def add_artist(self, uid, name, depth, source="discogs",
                   genres=None, url=None, discogs_id=None,
                   mbid=None, spotify_id=None, popularity=None):
        if uid not in self.artists:
            self.artists[uid] = {
                "name": name,
                "depth": depth,
                "genres": genres or [],
                "url": url or "",
                "discogs_id": discogs_id,
                "mbid": mbid,
                "spotify_id": spotify_id,
                "popularity": popularity,
                "sources": [source],
                "label_ids": [],
                "is_seed": depth == 0
            }
        else:
            ex = self.artists[uid]
            if source not in ex.get("sources", []):
                ex.setdefault("sources", []).append(source)
            if discogs_id and not ex.get("discogs_id"):
                ex["discogs_id"] = discogs_id
            if mbid and not ex.get("mbid"):
                ex["mbid"] = mbid
            if spotify_id and not ex.get("spotify_id"):
                ex["spotify_id"] = spotify_id
            if popularity and (not ex.get("popularity") or popularity > ex["popularity"]):
                ex["popularity"] = popularity
            if genres:
                ex["genres"] = list(set(ex.get("genres", []) + genres))
        return uid

    def add_label(self, uid, name, depth, source="discogs",
                  genres=None, url=None, discogs_id=None,
                  mbid=None, release_count=0, profile=None):
        if uid not in self.labels:
            self.labels[uid] = {
                "name": name,
                "depth": depth,
                "genres": genres or [],
                "url": url or "",
                "discogs_id": discogs_id,
                "mbid": mbid,
                "release_count": release_count,
                "profile": (profile or "")[:500],
                "sources": [source],
                "artist_ids": [],
                "is_seed": False
            }
        else:
            ex = self.labels[uid]
            if source not in ex.get("sources", []):
                ex.setdefault("sources", []).append(source)
            if discogs_id and not ex.get("discogs_id"):
                ex["discogs_id"] = discogs_id
            if mbid and not ex.get("mbid"):
                ex["mbid"] = mbid
            if release_count > ex.get("release_count", 0):
                ex["release_count"] = release_count
            if genres:
                ex["genres"] = list(set(ex.get("genres", []) + genres))
        return uid

    def add_edge(self, artist_id, label_id, release_count=1,
                 years=None, source="discogs"):
        for edge in self.edges:
            if edge["artist_id"] == artist_id and edge["label_id"] == label_id:
                edge["release_count"] = max(edge["release_count"], release_count)
                if years:
                    edge["years"] = sorted(set(edge.get("years", []) + years))
                if source not in edge.get("sources", []):
                    edge.setdefault("sources", []).append(source)
                return
        self.edges.append({
            "artist_id": artist_id,
            "label_id": label_id,
            "release_count": release_count,
            "years": years or [],
            "sources": [source]
        })
        if artist_id in self.artists and label_id not in self.artists[artist_id].get("label_ids", []):
            self.artists[artist_id].setdefault("label_ids", []).append(label_id)
        if label_id in self.labels and artist_id not in self.labels[label_id].get("artist_ids", []):
            self.labels[label_id].setdefault("artist_ids", []).append(artist_id)

    def find_artist_by_name(self, name):
        name_lower = name.lower().strip()
        for uid, data in self.artists.items():
            if data.get("name", "").lower().strip() == name_lower:
                return uid
        return None


# ─────────────────────────────────────────────────
# HELPERS
# ─────────────────────────────────────────────────

def is_relevant_label(name):
    if not name:
        return False
    name_lower = name.lower().strip()
    for skip in SKIP_LABELS:
        if skip in name_lower:
            return False
    return len(name_lower) > 1


def compute_label_relevance(label_data, db):
    score = 0
    rc = label_data.get("release_count", 0)
    if 5 <= rc <= 100:
        score += 5
    elif 100 < rc <= 500:
        score += 3
    elif rc > 500:
        score += 1
    elif rc < 5:
        score += 1

    genres = set(g.lower() for g in label_data.get("genres", []))
    score += len(genres & RELEVANT_GENRES) * 2

    seed_count = sum(1 for a in label_data.get("artist_ids", [])
                     if db.artists.get(a, {}).get("is_seed"))
    score += seed_count * 3

    # Bonus wenn mehrere Quellen das Label bestätigen
    score += (len(label_data.get("sources", [])) - 1) * 2
    return score


def clean_artist_name(name):
    clean = re.split(r'\s*(?:feat\.|featuring|ft\.|&|,|/|vs\.?)\s*', name, flags=re.IGNORECASE)[0]
    return clean.strip()


def make_label_key(name, discogs_id=None, mbid=None):
    if discogs_id:
        return f"d_{discogs_id}"
    if mbid:
        return f"mb_{mbid}"
    return f"n_{name.lower().strip().replace(' ', '_')[:50]}"


def make_artist_key(name, discogs_id=None, mbid=None, spotify_id=None):
    if discogs_id:
        return f"d_{discogs_id}"
    if mbid:
        return f"mb_{mbid}"
    if spotify_id:
        return f"sp_{spotify_id}"
    return f"n_{name.lower().strip().replace(' ', '_')[:50]}"


# ─────────────────────────────────────────────────
# CRAWLER ENGINE
# ─────────────────────────────────────────────────

class MultiSourceCrawler:
    def __init__(self, db, discogs=None, musicbrainz=None, spotify=None,
                 max_depth=DEFAULT_MAX_DEPTH, time_budget=0):
        self.db = db
        self.discogs = discogs
        self.mb = musicbrainz
        self.spotify = spotify
        self.max_depth = max_depth
        self.time_budget = time_budget * 60 if time_budget else 0  # Minuten → Sekunden
        self.sources = []
        if discogs:
            self.sources.append("discogs")
        if musicbrainz:
            self.sources.append("musicbrainz")
        if spotify:
            self.sources.append("spotify")

    # ── DISCOGS ──────────────────────────────────

    def discogs_artist_labels(self, artist_key, discogs_id, artist_name, depth):
        """Discogs: Labels eines Künstlers finden"""
        labels_found = defaultdict(lambda: {"name": "", "count": 0, "years": []})
        page = 1
        while page <= 3:
            data = self.discogs.get_artist_releases(discogs_id, page)
            if not data or not data.get("releases"):
                break
            for rel in data["releases"]:
                label_name = rel.get("label", "")
                if not label_name or not is_relevant_label(label_name):
                    continue
                labels_found[label_name]["name"] = label_name
                labels_found[label_name]["count"] += 1
                year = rel.get("year")
                if year and year > 0:
                    labels_found[label_name]["years"].append(year)
            pagination = data.get("pagination", {})
            if page >= pagination.get("pages", 1):
                break
            page += 1

        resolved = []
        for label_name, info in labels_found.items():
            lr = self.discogs.search_label(label_name)
            if lr:
                lid = lr["id"]
                lkey = make_label_key(label_name, discogs_id=lid)
                self.db.add_label(lkey, label_name, depth, source="discogs",
                                  genres=lr.get("genre", []), discogs_id=lid,
                                  release_count=info["count"])
                self.db.add_edge(artist_key, lkey, info["count"],
                                 sorted(set(info["years"])), source="discogs")
                resolved.append((lkey, lid, label_name, info["count"]))
        return resolved

    def discogs_label_artists(self, label_key, discogs_id, label_name, depth):
        """Discogs: Künstler eines Labels finden"""
        label_info = self.discogs.get_label_info(discogs_id)
        if label_info and label_key in self.db.labels:
            self.db.labels[label_key]["genres"] = (
                label_info.get("genres", []) + label_info.get("styles", [])
            )
            self.db.labels[label_key]["release_count"] = label_info.get("releases_count", 0)
            self.db.labels[label_key]["profile"] = (label_info.get("profile", "") or "")[:500]

        artists_found = {}
        page = 1
        while page <= 5:
            data = self.discogs.get_label_releases(discogs_id, page)
            if not data or not data.get("releases"):
                break
            for rel in data["releases"]:
                aname = rel.get("artist", "")
                if not aname or aname.lower() in ("various", "various artists", "unknown"):
                    continue
                clean = clean_artist_name(aname)
                if clean and len(clean) > 1:
                    if clean not in artists_found:
                        artists_found[clean] = {"count": 0}
                    artists_found[clean]["count"] += 1
            pagination = data.get("pagination", {})
            if page >= pagination.get("pages", 1):
                break
            page += 1

        resolved = []
        sorted_artists = sorted(artists_found.items(), key=lambda x: -x[1]["count"])
        for aname, info in sorted_artists[:30]:
            if info["count"] < 1:
                continue
            # Erst prüfen ob schon in DB
            existing = self.db.find_artist_by_name(aname)
            if existing:
                self.db.add_edge(existing, label_key, info["count"], source="discogs")
                resolved.append((existing, None, aname))
                continue
            sr = self.discogs.search_artist(aname)
            if sr:
                aid = sr["id"]
                akey = make_artist_key(aname, discogs_id=aid)
                self.db.add_artist(akey, aname, depth, source="discogs",
                                   genres=sr.get("genre", []), discogs_id=aid)
                self.db.add_edge(akey, label_key, info["count"], source="discogs")
                resolved.append((akey, aid, aname))
        return resolved

    # ── MUSICBRAINZ ──────────────────────────────

    def mb_artist_labels(self, artist_key, artist_name, depth):
        """MusicBrainz: Labels eines Künstlers finden"""
        mb_artist = self.mb.search_artist(artist_name)
        if not mb_artist:
            return []

        mbid = mb_artist["id"]
        if artist_key in self.db.artists:
            self.db.artists[artist_key]["mbid"] = mbid

        labels_found = defaultdict(lambda: {"name": "", "mbid": "", "count": 0, "years": []})
        offset = 0
        while offset < 300:
            data = self.mb.get_artist_releases(mbid, offset)
            if not data or not data.get("releases"):
                break
            for rel in data["releases"]:
                for li in rel.get("label-info", []):
                    label = li.get("label")
                    if not label or not is_relevant_label(label.get("name", "")):
                        continue
                    lname = label["name"]
                    lmbid = label["id"]
                    labels_found[lname]["name"] = lname
                    labels_found[lname]["mbid"] = lmbid
                    labels_found[lname]["count"] += 1
                    date = rel.get("date", "")
                    if date and len(date) >= 4:
                        try:
                            labels_found[lname]["years"].append(int(date[:4]))
                        except ValueError:
                            pass
            if len(data["releases"]) < 100:
                break
            offset += 100

        resolved = []
        for lname, info in labels_found.items():
            # Erst schauen ob Label unter anderem Key schon existiert
            existing = None
            for k, v in self.db.labels.items():
                if v.get("name", "").lower() == lname.lower():
                    existing = k
                    break
            if existing:
                lkey = existing
                if "musicbrainz" not in self.db.labels[lkey].get("sources", []):
                    self.db.labels[lkey].setdefault("sources", []).append("musicbrainz")
                if not self.db.labels[lkey].get("mbid"):
                    self.db.labels[lkey]["mbid"] = info["mbid"]
            else:
                lkey = make_label_key(lname, mbid=info["mbid"])
                self.db.add_label(lkey, lname, depth, source="musicbrainz",
                                  mbid=info["mbid"], release_count=info["count"])
            self.db.add_edge(artist_key, lkey, info["count"],
                             sorted(set(info["years"])), source="musicbrainz")
            resolved.append((lkey, info["mbid"], lname, info["count"]))
        return resolved

    def mb_label_artists(self, label_key, label_name, depth):
        """MusicBrainz: Künstler eines Labels finden"""
        mb_label = self.mb.search_label(label_name)
        if not mb_label:
            return []

        mbid = mb_label["id"]
        if label_key in self.db.labels:
            self.db.labels[label_key]["mbid"] = mbid

        artists_found = {}
        offset = 0
        while offset < 500:
            data = self.mb.get_label_releases(mbid, offset)
            if not data or not data.get("releases"):
                break
            for rel in data["releases"]:
                for ac in rel.get("artist-credit", []):
                    artist = ac.get("artist")
                    if not artist:
                        continue
                    aname = artist.get("name", "")
                    if not aname or aname.lower() in ("various artists", "[unknown]"):
                        continue
                    clean = clean_artist_name(aname)
                    if clean and len(clean) > 1:
                        if clean not in artists_found:
                            artists_found[clean] = {"mbid": artist["id"], "count": 0}
                        artists_found[clean]["count"] += 1
            if len(data["releases"]) < 100:
                break
            offset += 100

        resolved = []
        sorted_artists = sorted(artists_found.items(), key=lambda x: -x[1]["count"])
        for aname, info in sorted_artists[:30]:
            existing = self.db.find_artist_by_name(aname)
            if existing:
                akey = existing
                if "musicbrainz" not in self.db.artists[akey].get("sources", []):
                    self.db.artists[akey].setdefault("sources", []).append("musicbrainz")
                if not self.db.artists[akey].get("mbid"):
                    self.db.artists[akey]["mbid"] = info["mbid"]
            else:
                akey = make_artist_key(aname, mbid=info["mbid"])
                self.db.add_artist(akey, aname, depth, source="musicbrainz", mbid=info["mbid"])
            self.db.add_edge(akey, label_key, info["count"], source="musicbrainz")
            resolved.append((akey, aname))
        return resolved

    # ── SPOTIFY ──────────────────────────────────

    def spotify_related(self, artist_key, artist_name, depth):
        """Spotify: Related Artists (algorithmisch) entdecken"""
        if artist_key in self.db.spotify_related_crawled:
            return []

        sp_artist = self.spotify.search_artist(artist_name)
        if not sp_artist:
            return []

        # Spotify-Daten im Artist speichern
        if artist_key in self.db.artists:
            self.db.artists[artist_key]["spotify_id"] = sp_artist["id"]
            self.db.artists[artist_key]["popularity"] = sp_artist.get("popularity")
            sp_genres = sp_artist.get("genres", [])
            if sp_genres:
                existing_genres = self.db.artists[artist_key].get("genres", [])
                self.db.artists[artist_key]["genres"] = list(set(existing_genres + sp_genres))

        self.db.spotify_related_crawled.add(artist_key)

        related = self.spotify.get_related_artists(sp_artist["id"])
        if not related:
            return []

        resolved = []
        for rel in related:
            if not self.spotify.is_electronic(rel):
                continue
            rname = rel["name"]
            existing = self.db.find_artist_by_name(rname)
            if existing:
                akey = existing
                self.db.artists[akey]["spotify_id"] = rel["id"]
                self.db.artists[akey]["popularity"] = rel.get("popularity")
                if "spotify" not in self.db.artists[akey].get("sources", []):
                    self.db.artists[akey].setdefault("sources", []).append("spotify")
                sp_genres = rel.get("genres", [])
                if sp_genres:
                    self.db.artists[akey]["genres"] = list(
                        set(self.db.artists[akey].get("genres", []) + sp_genres))
            else:
                akey = make_artist_key(rname, spotify_id=rel["id"])
                self.db.add_artist(akey, rname, depth, source="spotify",
                                   spotify_id=rel["id"], genres=rel.get("genres", []),
                                   popularity=rel.get("popularity"),
                                   url=rel.get("external_urls", {}).get("spotify"))
            resolved.append((akey, rname))
        return resolved

    # ── MAIN CRAWL ───────────────────────────────

    def resolve_seed(self, name):
        """Seed-Künstler über alle Quellen auflösen"""
        discogs_id = None
        mbid = None
        spotify_id = None
        genres = []
        popularity = None

        if self.discogs:
            result = self.discogs.search_artist(name)
            if result:
                discogs_id = result["id"]
                genres.extend(result.get("genre", []))

        if self.spotify:
            result = self.spotify.search_artist(name)
            if result:
                spotify_id = result["id"]
                genres.extend(result.get("genres", []))
                popularity = result.get("popularity")

        if not discogs_id and not spotify_id and self.mb:
            result = self.mb.search_artist(name)
            if result:
                mbid = result["id"]

        if not discogs_id and not mbid and not spotify_id:
            return None

        key = make_artist_key(name, discogs_id=discogs_id, mbid=mbid, spotify_id=spotify_id)
        self.db.add_artist(key, name, 0, source=self.sources[0] if self.sources else "unknown",
                           genres=list(set(genres)), discogs_id=discogs_id,
                           mbid=mbid, spotify_id=spotify_id, popularity=popularity)
        return key

    def crawl_artist(self, artist_key, artist_name, depth):
        """Einen Künstler über alle Quellen crawlen"""
        if artist_key in self.db.crawled_artists:
            return []
        print(f"  {'  ' * min(depth, 5)}🎵 [{depth}] {artist_name}")
        self.db.crawled_artists.add(artist_key)
        all_labels = []

        artist_data = self.db.artists.get(artist_key, {})
        discogs_id = artist_data.get("discogs_id")

        # 1. Discogs
        if self.discogs and discogs_id:
            labels = self.discogs_artist_labels(artist_key, discogs_id, artist_name, depth)
            all_labels.extend(labels)

        # 2. MusicBrainz (cross-check + neue Labels)
        if self.mb:
            mb_labels = self.mb_artist_labels(artist_key, artist_name, depth)
            existing_names = {self.db.labels.get(l[0], {}).get("name", "").lower() for l in all_labels}
            for ml in mb_labels:
                if ml[2].lower() not in existing_names:
                    all_labels.append(ml)

        # 3. Spotify Related Artists
        if self.spotify:
            self.spotify_related(artist_key, artist_name, depth)

        return all_labels

    def crawl_label(self, label_key, label_name, depth):
        """Ein Label über alle Quellen crawlen"""
        if label_key in self.db.crawled_labels:
            return []
        print(f"  {'  ' * min(depth, 5)}🏷️  [{depth}] {label_name}")
        self.db.crawled_labels.add(label_key)
        all_artists = []

        label_data = self.db.labels.get(label_key, {})
        discogs_id = label_data.get("discogs_id")

        # 1. Discogs
        if self.discogs and discogs_id:
            artists = self.discogs_label_artists(label_key, discogs_id, label_name, depth)
            all_artists.extend(artists)

        # 2. MusicBrainz
        if self.mb:
            mb_artists = self.mb_label_artists(label_key, label_name, depth)
            existing_names = {a[2].lower() if len(a) > 2 else a[1].lower() for a in all_artists}
            for ma in mb_artists:
                if ma[1].lower() not in existing_names:
                    all_artists.append(ma)

        return all_artists

    def crawl_recursive(self, seed_names):
        """Hauptloop: Seeds → Labels → Artists → Labels → ..."""
        self.db.stats["started_at"] = datetime.now().isoformat()
        self.db.stats["sources_used"] = self.sources

        # Phase 1: Seeds
        print(f"\n🌱 Phase 1: {len(seed_names)} Seeds resolven...")
        queue = []
        for i, name in enumerate(seed_names):
            # Skip bereits gecrawlte Seeds bei Resume
            existing = self.db.find_artist_by_name(name)
            if existing and existing in self.db.crawled_artists:
                print(f"  ⏭️  [{i+1}/{len(seed_names)}] {name} (bereits gecrawlt)")
                continue

            key = existing or self.resolve_seed(name)
            if key:
                queue.append(("artist", key, name, 0))
                print(f"  ✅ [{i+1}/{len(seed_names)}] {name}")
            else:
                print(f"  ❌ [{i+1}/{len(seed_names)}] {name} — nicht gefunden")
            if (i + 1) % 20 == 0:
                self.db.save()

        self.db.save()

        # Phase 1.5: Queue aus bestehender DB auffüllen (bei Resume)
        queued_keys = {item[1] for item in queue}

        uncrawled_artists = []
        for uid, data in self.db.artists.items():
            if (uid not in self.db.crawled_artists and
                    uid not in queued_keys and
                    data.get("depth", 99) < self.max_depth):
                if data.get("discogs_id") or data.get("mbid"):
                    uncrawled_artists.append((uid, data))

        uncrawled_labels = []
        for uid, data in self.db.labels.items():
            if (uid not in self.db.crawled_labels and
                    uid not in queued_keys and
                    data.get("depth", 99) < self.max_depth):
                if data.get("discogs_id") or data.get("mbid"):
                    relevance = compute_label_relevance(data, self.db)
                    if relevance >= MIN_RELEVANCE_SCORE:
                        uncrawled_labels.append((uid, data, relevance))

        # Sortieren: niedrigere Tiefe zuerst, dann nach Anzahl Verbindungen
        uncrawled_artists.sort(key=lambda x: (x[1].get("depth", 99),
                                               -len(x[1].get("label_ids", []))))
        uncrawled_labels.sort(key=lambda x: (x[1].get("depth", 99), -x[2]))

        for uid, data in uncrawled_artists:
            queue.append(("artist", uid, data["name"], data.get("depth", 1)))
        for uid, data, rel in uncrawled_labels:
            queue.append(("label", uid, data["name"], data.get("depth", 0)))

        if uncrawled_artists or uncrawled_labels:
            print(f"\n🔄 Queue aus DB aufgefüllt: {len(uncrawled_artists)} Künstler, "
                  f"{len(uncrawled_labels)} Labels")

        print(f"\n🔄 Phase 2: Rekursives Crawlen (max Tiefe: {self.max_depth}, "
              f"Quellen: {', '.join(self.sources)})...\n")
        print(f"  Queue: {len(queue)} items\n")

        # Phase 2: Rekursion
        processed = 0
        crawl_start = time.time()
        while queue:
            item_type, item_key, item_name, depth = queue.pop(0)

            if depth > self.max_depth:
                continue

            self.db.stats["max_depth_reached"] = max(
                self.db.stats.get("max_depth_reached", 0), depth)

            if item_type == "artist":
                labels = self.crawl_artist(item_key, item_name, depth)
                for label_info in labels:
                    lkey = label_info[0]
                    lname = label_info[2] if len(label_info) > 2 else "?"
                    if lkey not in self.db.crawled_labels:
                        label_data = self.db.labels.get(lkey, {})
                        relevance = compute_label_relevance(label_data, self.db)
                        if relevance >= MIN_RELEVANCE_SCORE or depth < 2:
                            queue.append(("label", lkey, lname, depth + 1))

                # Spotify Related Artists in die Queue
                if self.spotify:
                    for akey, adata in list(self.db.artists.items()):
                        if (akey not in self.db.crawled_artists and
                                "spotify" in adata.get("sources", []) and
                                adata.get("depth", 99) == depth and
                                not any(q[1] == akey for q in queue)):
                            queue.append(("artist", akey, adata["name"], depth + 1))

            elif item_type == "label":
                artists = self.crawl_label(item_key, item_name, depth)
                for artist_info in artists:
                    akey = artist_info[0]
                    aname = artist_info[2] if len(artist_info) > 2 else artist_info[1]
                    if akey not in self.db.crawled_artists:
                        if not any(q[1] == akey for q in queue[:200]):
                            queue.append(("artist", akey, aname, depth + 1))

            processed += 1

            if processed % SAVE_INTERVAL == 0:
                self._update_request_stats()
                self.db.save()
                d_req = self.discogs.request_count if self.discogs else 0
                mb_req = self.mb.request_count if self.mb else 0
                sp_req = self.spotify.request_count if self.spotify else 0
                elapsed = time.time() - crawl_start
                elapsed_str = f"{int(elapsed // 60)}m{int(elapsed % 60):02d}s"
                print(f"\n  📊 Processed: {processed} | Queue: {len(queue)} | "
                      f"Artists: {len(self.db.artists)} | Labels: {len(self.db.labels)} | "
                      f"Requests: D={d_req} MB={mb_req} SP={sp_req} | ⏱ {elapsed_str}\n")

                # Time-Budget Check
                if self.time_budget and elapsed >= self.time_budget:
                    print(f"\n  ⏰ Time-Budget ({self.time_budget // 60} Min) erreicht. "
                          f"Speichere und beende...")
                    print(f"     Verbleibende Queue: {len(queue)} Items → "
                          f"nächster Run mit --resume")
                    break

        # Final save
        self._update_request_stats()
        self.db.save()
        print(f"\n{'='*55}")
        print(f"  ✅ FERTIG!")
        print(f"  📊 {len(self.db.artists)} Artists | {len(self.db.labels)} Labels | "
              f"{len(self.db.edges)} Connections")
        print(f"  🔗 Quellen: {', '.join(self.sources)}")
        print(f"  📡 Requests: {self.db.stats.get('total_requests', 0)}")
        print(f"{'='*55}\n")

    def _update_request_stats(self):
        d = self.discogs.request_count if self.discogs else 0
        m = self.mb.request_count if self.mb else 0
        s = self.spotify.request_count if self.spotify else 0
        self.db.stats["discogs_requests"] = d
        self.db.stats["musicbrainz_requests"] = m
        self.db.stats["spotify_requests"] = s
        self.db.stats["total_requests"] = d + m + s


# ─────────────────────────────────────────────────
# SEED LOADER
# ─────────────────────────────────────────────────

def load_seed_data(path="seed_data.json"):
    with open(path, "r", encoding="utf-8") as f:
        data = json.load(f)
    artists = list(data.get("known_associations", {}).keys())
    artists += data.get("artists_without_labels", [])
    seen = set()
    unique = []
    for a in artists:
        a_clean = a.strip()
        if a_clean.lower() not in seen and a_clean:
            seen.add(a_clean.lower())
            unique.append(a_clean)
    return unique


def load_env(key):
    env_path = Path(".env")
    if env_path.exists():
        for line in env_path.read_text().splitlines():
            line = line.strip()
            if line.startswith(f"{key}="):
                return line.split("=", 1)[1].strip().strip('"\'')
    return os.environ.get(key)


# ─────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="🔍 Label Crawler v2.0 — Multi-Source")
    parser.add_argument("--token", help="Discogs API Token")
    parser.add_argument("--spotify", action="store_true", help="Spotify aktivieren")
    parser.add_argument("--spotify-id", help="Spotify Client ID")
    parser.add_argument("--spotify-secret", help="Spotify Client Secret")
    parser.add_argument("--no-musicbrainz", action="store_true", help="MusicBrainz deaktivieren")
    parser.add_argument("--no-discogs", action="store_true", help="Discogs deaktivieren")
    parser.add_argument("--max-depth", type=int, default=DEFAULT_MAX_DEPTH)
    parser.add_argument("--time-budget", type=int, default=0,
                        help="Max Laufzeit in Minuten (0=unbegrenzt). Speichert und beendet bei Überschreitung.")
    parser.add_argument("--resume", action="store_true")
    parser.add_argument("--seed", default="seed_data.json")
    parser.add_argument("--output", default="network_data.json")
    args = parser.parse_args()

    print("\n🔍 LABEL CRAWLER v2.0 — Multi-Source Edition")
    print("=" * 50)

    # ── Clients ──
    discogs = None
    mb = None
    spotify = None

    if not args.no_discogs:
        token = args.token or load_env("DISCOGS_TOKEN")
        if token:
            discogs = DiscogsClient(token)
            print("  ✅ Discogs bereit")
        else:
            print("  ⚠️  Kein Discogs Token → deaktiviert")
            print("     → discogs.com/settings/developers")

    if not args.no_musicbrainz:
        mb = MusicBrainzClient()
        print("  ✅ MusicBrainz bereit (kein Key nötig)")

    if args.spotify:
        sp_id = args.spotify_id or load_env("SPOTIFY_CLIENT_ID")
        sp_secret = args.spotify_secret or load_env("SPOTIFY_CLIENT_SECRET")
        if sp_id and sp_secret:
            spotify = SpotifyClient(sp_id, sp_secret)
            if spotify.token:
                print("  ✅ Spotify bereit")
            else:
                spotify = None
        else:
            print("  ⚠️  Spotify Credentials fehlen → deaktiviert")
            print("     → developer.spotify.com/dashboard")

    if not discogs and not mb and not spotify:
        print("\n❌ Keine Datenquelle verfügbar!")
        sys.exit(1)

    # ── DB ──
    db = NetworkDB(args.output)
    if args.resume and db.load():
        print("  📂 Vorherige Daten geladen")

    # ── Seeds ──
    seeds = load_seed_data(args.seed)
    source_names = []
    if discogs: source_names.append("Discogs")
    if mb: source_names.append("MusicBrainz")
    if spotify: source_names.append("Spotify")

    budget_str = f"{args.time_budget} Min" if args.time_budget else "unbegrenzt"
    print(f"""
╔══════════════════════════════════════════════════╗
║  Seeds:       {len(seeds):>5}                            ║
║  Max Depth:   {args.max_depth:>5}                            ║
║  Time-Budget: {budget_str:<33} ║
║  Sources:     {', '.join(source_names):<33} ║
║  Output:      {args.output:<33} ║
╚══════════════════════════════════════════════════╝
    """)

    crawler = MultiSourceCrawler(db, discogs, mb, spotify, args.max_depth,
                                 time_budget=args.time_budget)

    try:
        crawler.crawl_recursive(seeds)
    except KeyboardInterrupt:
        print("\n\n⚠️  Abgebrochen! Speichere...")
        crawler._update_request_stats()
        db.save()
        print("  💾 Gespeichert. Fortsetzen mit: python crawler.py --resume")


if __name__ == "__main__":
    main()
