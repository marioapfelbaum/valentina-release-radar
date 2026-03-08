# Valentina Release Radar

Automatisierte Entdeckungsplattform fuer neue Electronic-Music-Releases.
Live: https://valentina-release-radar.pages.dev
Repo: https://github.com/marioapfelbaum/valentina-release-radar

## Architektur

```
crawler.py ──> network_data.json (Artist/Label-Netzwerk)
                     │
fetch_multi.py ──> releases.json (aktuelle Releases)
                     │
deploy.sh ──> Cloudflare Pages (Static Site)
```

GitHub Actions (`update-radar.yml`) fuehrt alle 3 Tage automatisch aus:
1. Crawler (Netzwerk erweitern, --resume --time-budget 300)
2. Fetch (Bandcamp + Spotify)
3. Deploy (Cloudflare Pages)

## Release-Quellen

### Aktive Quellen (Standard: `--sources bandcamp,spotify`)

- **Bandcamp** (`sources/bandcamp.py`): Holt Releases von Labels in `reference_labels.txt` via Mobile API. Zuverlaessigste Quelle.
- **Spotify** (`sources/spotify_source.py`): Holt Releases fuer alle Artists aus dem Netzwerk. Client Credentials Flow. Braucht SPOTIFY_CLIENT_ID + SPOTIFY_CLIENT_SECRET.

### Deaktivierte Quellen

- **Beatport** (`sources/beatport.py`): NICHT VERWENDEN. Liefert zu viele generische/irrelevante Releases, da der Artist-Filter zu breit greift. Nur aktivieren mit `--sources beatport,...` wenn explizit gewuenscht.
- **Discogs**: Kein eigener Release-Fetcher. Die ~1.800 Discogs-Releases in releases.json sind historische Daten vom Crawler.

## Netzwerk-Crawler

`crawler.py` baut ein Netzwerk aus Artists und Labels auf:
- **Quellen**: Discogs API + MusicBrainz (kein Spotify Related Artists — 403)
- **Daten**: `network_data.json` (~20MB, ~5.200 Artists, ~20.000 Labels)
- **Seeds**: `seed_data.json` (350 Seed-Artists)
- **Resume**: `--resume` laedt vorherigen Stand und macht weiter
- **Time-Budget**: `--time-budget 300` begrenzt Laufzeit auf 300 Minuten
- Discogs Rate Limit: 1 Request/Sekunde

## Filterung

### Label-Filter
- `reference_labels.txt` — 71 kuratierte Labels (Perlon, Cocoon, Kompakt, etc.)
- `label_blacklist.txt` — 61 Spam/Distributor-Labels (DistroKid, TuneCore, etc.)
- `reference_artists.txt` — 50 Seed-Artists fuer direkten Match

### Netzwerk-Filter
- Artists aus `network_data.json` werden fuer Beatport-Filterung verwendet
- Bandcamp holt NUR von reference_labels
- Spotify holt fuer ALLE Netzwerk-Artists

## Dateien

### Hauptskripte
- `crawler.py` — Netzwerk-Crawler (Discogs + MusicBrainz)
- `fetch_multi.py` — Release-Fetcher (Bandcamp + Spotify)
- `deploy.sh` — Cloudflare Pages Deploy

### Daten
- `network_data.json` — Artist/Label-Graph (~20MB)
- `releases.json` — Alle Releases (~1.1MB)
- `seed_data.json` — 350 Seed-Artists
- `last_checked.json` — Fetch-Tracking
- `bandcamp_labels.json` — Bandcamp Label-ID Mappings

### Konfiguration
- `reference_labels.txt` — Whitelist Labels
- `reference_artists.txt` — Whitelist Artists
- `label_blacklist.txt` — Blacklist Labels/Distributoren
- `.env` — API Credentials (DISCOGS_TOKEN, SPOTIFY_CLIENT_ID, SPOTIFY_CLIENT_SECRET)

### Frontend
- `release_radar.html` — Haupt-Radar (Single-Page App, laedt releases.json)
- `event_radar.html` — Event-Radar
- `network_explorer.html` — Netzwerk-Visualisierung

### Sources (Python-Module)
- `sources/bandcamp.py` — Bandcamp Mobile API
- `sources/beatport.py` — Beatport HTML Scraper (deaktiviert)
- `sources/spotify_source.py` — Spotify Web API
- `sources/base.py` — Base-Klasse fuer Fetcher
- `sources/genre_map.py` — Genre-Klassifikation

## Credentials

Gespeichert in `.env` (lokal) und GitHub Secrets (CI):
- `DISCOGS_TOKEN` — Discogs API Token (kostenlos, discogs.com/settings/developers)
- `SPOTIFY_CLIENT_ID` — Spotify App Client ID
- `SPOTIFY_CLIENT_SECRET` — Spotify App Client Secret
- `CLOUDFLARE_API_TOKEN` — Fuer Wrangler Deploy (Cloudflare Dashboard)
- `CLOUDFLARE_ACCOUNT_ID` — Cloudflare Account ID

## Deployment

Cloudflare Pages, Projekt: `valentina-release-radar`
- Deploy: `bash deploy.sh` (baut dist/, ruft `npx wrangler pages deploy`)
- Automatisch via GitHub Actions alle 3 Tage
- Manuell: "Quick Update" Workflow in GitHub Actions (nur Fetch + Deploy)

## Haeufige Befehle

```bash
# Releases holen (Bandcamp + Spotify)
python3 fetch_multi.py

# Nur Bandcamp
python3 fetch_multi.py --sources bandcamp

# Crawler fortsetzen (max 2 Stunden)
python3 crawler.py --resume --max-depth 2 --time-budget 120

# Deploy
bash deploy.sh

# Neues Label hinzufuegen
# → reference_labels.txt editieren, dann fetch_multi.py laufen lassen
```

## Wichtige Hinweise

- Beatport ist deaktiviert. Der Artist-Filter greift zu breit (5.000+ Artists) und laesst generische Releases durch.
- Spotify Related Artists API gibt 403 zurueck (Client Credentials reichen nicht). Aber Album/Single-Fetch funktioniert.
- Bandcamp blockiert Python requests via TLS-Fingerprinting. bandcamp.py nutzt curl als Fallback.
- network_data.json waechst mit jedem Crawler-Run. Bei >50MB auf Git LFS umstellen.
- Der User bevorzugt: Minimal, Deep House, Downtempo, Soulful, Broken Beat, Jazz-Electronic. Keine Mainstream-EDM.
