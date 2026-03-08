# Valentina Release Radar v2 — Full Expansion Design

**Date:** 2026-03-09
**Status:** Approved
**Goal:** Expand from 2 active sources to 6+, add quality scoring, improve release relevance

## Problem

- Discogs delivered 93% of releases but is only legacy data (no active fetcher)
- Spotify is enabled but delivers 0 releases (needs debugging)
- Bandcamp covers only 71 labels — too narrow
- User wants Minimal, Deep House, Downtempo, Soulful, Broken Beat, Jazz-Electronic
- Too many irrelevant releases, not enough quality curation

## Architecture

```
network_data.json (5,262 artists, 20,611 labels)
        │
        ├── sources/discogs_source.py   NEW — active label release fetcher
        ├── sources/bandcamp.py         EXPAND — auto-expand labels from network
        ├── sources/spotify_source.py   FIX — debug 0 releases
        ├── sources/hardwax.py          NEW — curated shop scraper
        ├── sources/boomkat.py          NEW — curated shop scraper
        ├── sources/juno.py             NEW — genre-filtered shop scraper
        │
        ▼
fetch_multi.py  ──>  quality_score.py  ──>  releases.json
                     (score each release)
```

## New Components

### 1. Discogs Release Fetcher (`sources/discogs_source.py`)

- Uses Discogs API: `GET /labels/{id}/releases` and `GET /artists/{id}/releases`
- Filters labels from network_data.json: only labels with >= 2 seed-artist connections
- Respects 1 req/sec rate limit (uses DISCOGS_TOKEN from .env)
- Fetches releases from last 90 days
- Rich metadata: styles, genres, catalog_number, format, year

### 2. Hardwax Scraper (`sources/hardwax.py`)

- Scrapes hardwax.com new releases page
- Hardwax curates exactly the genres Mario likes (Minimal, Deep House, Dub, Basic Channel lineage)
- Categories to scrape: /new/, specific genre sections
- Extracts: artist, title, label, format, genre tags

### 3. Boomkat Scraper (`sources/boomkat.py`)

- Scrapes boomkat.com new releases
- Strong in: Experimental Electronic, Ambient, Downtempo, IDM
- Categories: /downloads/new, genre filters
- Extracts: artist, title, label, format, description, genre tags

### 4. Juno Scraper (`sources/juno.py`)

- Scrapes juno.co.uk with genre filtering
- Genres to scrape: Deep House, Minimal/Tech House, Downtempo/Balearic, Broken Beat/Nu Jazz
- Large catalog with good categorization
- Extracts: artist, title, label, catalog_number, format, bpm, genre

### 5. Bandcamp Auto-Expansion

- Script `expand_bandcamp_labels.py` that:
  1. Reads network_data.json
  2. Finds labels connected to seed artists (>= 2 connections)
  3. Checks if label has Bandcamp presence (via search or known mappings)
  4. Adds validated labels to reference_labels.txt
- Target: expand from 71 to 200-300 labels

### 6. Quality Score System (`quality_score.py`)

Each release gets a score (0-100) based on:
- **Label relevance** (0-30): Is label in reference_labels? How many seed-artist connections?
- **Artist relevance** (0-30): Is artist in network? Distance from seeds?
- **Genre match** (0-20): Does genre match user preferences?
- **Source trust** (0-10): Discogs/Hardwax > Bandcamp > Boomkat/Juno > Spotify
- **Multi-source bonus** (0-10): Found on multiple sources = higher quality signal

Score is stored in releases.json as `quality_score` field.
Frontend can sort/filter by score.

## Spotify Fix

Debug checklist:
1. Check if SPOTIFY_CLIENT_ID and SPOTIFY_CLIENT_SECRET are set
2. Check if token exchange works
3. Check if artist list from network_data.json has spotify_ids
4. Check rate limiting behavior
5. Test with a single known artist

## Integration into fetch_multi.py

- New `--sources` options: `discogs,hardwax,boomkat,juno` added to existing `bandcamp,spotify`
- Default sources: `bandcamp,spotify,discogs,hardwax,boomkat,juno`
- Quality scoring runs after all fetches complete, before saving
- Source priority for dedup: hardwax=5, discogs=4, boomkat=3, bandcamp=2, juno=2, spotify=1

## Files Changed/Created

### New Files
- `sources/discogs_source.py`
- `sources/hardwax.py`
- `sources/boomkat.py`
- `sources/juno.py`
- `quality_score.py`
- `expand_bandcamp_labels.py`

### Modified Files
- `fetch_multi.py` — add new sources, quality scoring integration
- `reference_labels.txt` — expanded via auto-expansion
- `sources/genre_map.py` — add mappings for new source genre tags
- `CLAUDE.md` — document new sources
- Skill SKILL.md — document new workflows

## Success Criteria

- At least 4 sources actively delivering releases
- Quality score correlates with user taste (top-scored releases = genres user likes)
- Total curated releases increase without noise increase
- New releases from labels user hasn't seen before but would like
