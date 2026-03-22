# Valentina Release Radar

Automatisierte Entdeckungsplattform fuer neue Electronic-Music-Releases.
Live: https://valentina-release-radar.pages.dev
Repo: https://github.com/marioapfelbaum/valentina-release-radar

## Architektur

```
crawler.py ──> network_data.json (Artist/Label-Netzwerk)
                     │
fetch_multi.py ──> quality_score.py ──> releases.json (aktuelle Releases mit Score)
                     │
deploy.sh ──> Cloudflare Pages (Static Site)
```

Hetzner Cronjobs:
- Shops (10 Quellen ohne Spotify) alle 4h: 00:00, 04:00, 08:00, 12:00, 16:00, 20:00
- Spotify separat 3x/Tag: 02:30, 10:30, 18:30 (30 Artists/Run, Rate-Limit-schonend)
- Crawler taeglich 03:00 UTC (crawl_and_push.sh, 3h Budget)
- Lockfiles: `/tmp/valentina-fetch.lock`, `/tmp/valentina-crawl.lock`

GitHub Actions (`update-radar.yml`): Nur noch manueller Backup-Trigger (Fetch + Deploy).
Auto-Deploy bei jedem Push: `deploy-on-push.yml`

## Release-Quellen

### Aktive Quellen (Standard: `--sources bandcamp,spotify,discogs,hardwax,boomkat,juno,clone,rushhour,deejay,phonica,redeye`)

- **Bandcamp** (`sources/bandcamp.py`): Holt Releases von Labels in `reference_labels.txt` via Mobile API. Zuverlaessigste Quelle.
- **Spotify** (`sources/spotify_source.py`): Holt Releases fuer Netzwerk-Artists. Cached spotify_ids in network_data.json. Max 30 Artists/Run, 3.0s Delay. Laeuft separat 3x/Tag um Rate-Limits zu vermeiden.
- **Discogs** (`sources/discogs_source.py`): Holt aktiv neue Releases von Top-Labels im Netzwerk (2+ Seed-Artist-Verbindungen). Braucht DISCOGS_TOKEN.
- **Hardwax** (`sources/hardwax.py`): JSON-Feed + /this-week/ + /last-week/. Berliner Plattenladen, kuratiert fuer Minimal/Deep House/Dub. Braucht beautifulsoup4.
- **Boomkat** (`sources/boomkat.py`): RSS-Feed (boomkat.com/new-releases.rss), Fallback auf rss2json.com Proxy wenn Cloudflare blockt. Kuratiert fuer Experimental/Electronic/Ambient.
- **Juno** (`sources/juno.py`): Scrapt juno.co.uk mit Genre-Filter. Braucht beautifulsoup4 + cloudscraper (Cloudflare-Bypass).
- **Clone.nl** (`sources/clone.py`): RSS-Feeds (clone.nl/rss/new + Genre-Feeds). Amsterdamer Plattenladen, stark fuer Detroit Techno/Electro/House.
- **Rush Hour** (`sources/rushhour.py`): RSS-Feed (rushhour.nl/rss.xml). Amsterdamer Plattenladen, Soulful/Jazz-Electronic/Deep House.
- **Deejay.de** (`sources/deejay.py`): HTML-Scraper. Grosser deutscher Vinyl-Shop, 4 Genre-Seiten (House/Techno/Beats/Electro), 40 Releases/Seite. Kein Cloudflare.
- **Phonica** (`sources/phonica.py`): RSS-Feed via rss2json.com Proxy. Kuratierter Londoner Plattenladen, 10 Items/Feed.
- **Redeye** (`sources/redeye.py`): HTML-Scraper. Bristoler Distributor/Shop, 4 Genre-Seiten, nur Releases mit "Exp."-Datum (Pre-Orders). Out-of-Stock und undatierte Restocks gefiltert. 10s Crawl-Delay (robots.txt).

### Deaktivierte Quellen

- **Beatport** (`sources/beatport.py`): NICHT VERWENDEN. Liefert zu viele generische/irrelevante Releases, da der Artist-Filter zu breit greift.

## Quality Scoring

`quality_score.py` bewertet jeden Release (0-100 Punkte, Cap bei 100):
- Label-Relevanz (0-30): Reference Label? Seed-Artist-Verbindungen?
- Artist-Relevanz (0-30): Im Netzwerk? Seed-Artist? Tiefe?
- Genre-Match (0-20): Passt zum User-Geschmack?
- Source-Trust (0-10): Hardwax/Clone/Rush Hour > Boomkat/Discogs > Bandcamp/Juno > Spotify
- Multi-Source-Bonus (0-10): Auf mehreren Quellen gefunden?
- Niche-Bonus (0-10): Inverse Style-Popularity aus Discogs-Styles (inspiriert von BlackTape). Artists mit seltenen Electronic-Subgenres (Microhouse, Berlin-School, Illbient) werden bevorzugt. Styles mit <15 Artists im Netzwerk werden ignoriert (Noise-Filter). "Various" etc. blacklisted.

## Freshness-System

Unterscheidet zwischen echten neuen Releases und Shop-Restocks:
- **date_verified**: `true` bei Bandcamp/Spotify/Discogs/Redeye (echte Release-Dates), `false` bei Shop-Quellen
- **Discogs-Gegencheck** (Phase 7b in fetch_multi.py): Fuer unverified Releases wird das echte Release-Jahr via Discogs Search API geprueft. Aelter als 6 Monate → `is_restock: true`
- **Frontend-Badges**: VER (gruen) = Verified New, SHOP (gelb) = Shop Pick, RE (grau) = Restock
- **Frontend-Filter**: NEW (Default, ohne Restocks) / ALL / VERIFIED / SHOP / RESTOCK Buttons

## Genre-Klassifikation

`sources/genre_map.py` klassifiziert Releases anhand von Style-Tags:
- **Specificity-Scoring**: Spezifische Genres (Detroit Techno, Soulful House = 5) schlagen generische (House = 1, Electronic = 0)
- **Noise-Filter**: Tags wie "label catalog", "merchandise" werden ignoriert
- **Artist-Enrichment**: Releases ohne Genre bekommen das haeufigste Genre des gleichen Artists
- **Genres**: Minimal House, Deep House, Detroit Techno, Chicago House, Soulful House, Broken Beat, Jazz-Funk, Nu Jazz, Garage House, UK Garage, Acid, Dub, Downtempo, Trip Hop, etc.
- **Luecke**: Clone/Rush Hour/Phonica liefern keine Style-Tags → ~675 Releases als "Other"/"Electronic"

## Netzwerk-Crawler

`crawler.py` baut ein Netzwerk aus Artists und Labels auf:
- **Quellen**: Discogs API + MusicBrainz (kein Spotify Related Artists — 403)
- **Daten**: `network_data.json` (~36MB, ~5.200 Artists, ~20.000 Labels)
- **Seeds**: `seed_data.json` (387 Seed-Artists)
- **Resume**: `--resume` laedt vorherigen Stand und macht weiter
- **Time-Budget**: `--time-budget 300` begrenzt Laufzeit auf 300 Minuten

## Bandcamp Label-Expansion

`expand_bandcamp_labels.py` erweitert reference_labels.txt automatisch:
- Findet Labels im Netzwerk mit 2+ Seed-Artist-Verbindungen
- Filtert Medien, Distributoren und Spam raus
- Kann Bandcamp-Praesenz pruefen (--check-bandcamp)
- Dry-Run: `python3 expand_bandcamp_labels.py --dry-run --min-connections 3`

## Filterung

### Label-Filter
- `reference_labels.txt` — Kuratierte Labels (erweiterbar via expand_bandcamp_labels.py)
- `label_blacklist.txt` — Spam/Distributor-Labels (DistroKid, TuneCore, etc.)
- `reference_artists.txt` — 50 Seed-Artists fuer direkten Match

### Quell-spezifische Filterung
- Bandcamp: Holt NUR von reference_labels
- Spotify: Holt fuer Netzwerk-Artists (max 500/Run, cached IDs)
- Discogs: Holt von Top-Labels im Netzwerk (2+ Seed-Connections)
- Hardwax/Boomkat/Juno: Vorgefiltert durch Shop-Kuration
- Beatport: Label-Blacklist + Netzwerk-Artist-Filter (deaktiviert)

## Dateien

### Hauptskripte
- `crawler.py` — Netzwerk-Crawler (Discogs + MusicBrainz)
- `fetch_multi.py` — Release-Fetcher (11 Quellen + Scoring)
- `quality_score.py` — Quality-Scoring-System
- `expand_bandcamp_labels.py` — Bandcamp-Label-Expansion
- `deploy.sh` — Cloudflare Pages Deploy
- `fetch_and_push.sh` — Hetzner: Release-Fetch + Git Push
- `crawl_and_push.sh` — Hetzner: Crawler + Git Push (taeglich)

### Daten
- `network_data.json` — Artist/Label-Graph (~36MB)
- `releases.json` — Alle Releases mit Quality Scores
- `seed_data.json` — 387 Seed-Artists
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
- `sources/spotify_source.py` — Spotify Web API (mit ID-Caching)
- `sources/discogs_source.py` — Discogs API (aktiver Release-Fetcher)
- `sources/hardwax.py` — Hardwax.com JSON-Feed + HTML
- `sources/boomkat.py` — Boomkat.com RSS-Feed
- `sources/juno.py` — Juno.co.uk Scraper (cloudscraper fuer Cloudflare-Bypass)
- `sources/clone.py` — Clone.nl RSS-Feeds (new + genre)
- `sources/rushhour.py` — Rush Hour RSS-Feed
- `sources/deejay.py` — Deejay.de HTML Scraper (House/Techno/Beats)
- `sources/phonica.py` — Phonica Records RSS (via rss2json Proxy)
- `sources/redeye.py` — Redeye Records HTML Scraper (4 Genres)
- `sources/beatport.py` — Beatport HTML Scraper (deaktiviert)
- `sources/base.py` — Base-Klasse fuer Fetcher
- `sources/genre_map.py` — Genre-Klassifikation

## Credentials

Gespeichert in `.env` (lokal) und GitHub Secrets (CI):
- `DISCOGS_TOKEN` — Discogs API Token (kostenlos, discogs.com/settings/developers)
- `SPOTIFY_CLIENT_ID` — Spotify App Client ID
- `SPOTIFY_CLIENT_SECRET` — Spotify App Client Secret
- `CLOUDFLARE_API_TOKEN` — Fuer Wrangler Deploy (Cloudflare Dashboard)
- `CLOUDFLARE_ACCOUNT_ID` — Cloudflare Account ID

## Dependencies

Core: `requests`
Scrapers: `beautifulsoup4`, `cloudscraper` (fuer Juno Cloudflare-Bypass)
Install: `pip install requests beautifulsoup4 cloudscraper`

## Deployment

Cloudflare Pages, Projekt: `valentina-release-radar`
- Deploy: `bash deploy.sh` (baut dist/, ruft `npx wrangler pages deploy`)
- **Auto-Deploy bei jedem Push**: `deploy-on-push.yml` triggert wenn releases.json gepusht wird
- **Hetzner → Auto-Deploy**: Hetzner pusht ohne [skip ci], deploy-on-push deployt → alle 4h frisch
- Crawler + Fetch + Deploy: `update-radar.yml` alle 3 Tage (mit Push-Retry und Pipeline-Resilienz)
- Manuell: "Quick Update" Workflow in GitHub Actions (nur Fetch + Deploy)

## Haeufige Befehle

```bash
# Releases holen (alle 8 Quellen)
python3 fetch_multi.py

# Nur Bandcamp
python3 fetch_multi.py --sources bandcamp

# Nur kuratierte Shops
python3 fetch_multi.py --sources hardwax,boomkat,juno,clone,rushhour

# Nur Discogs
python3 fetch_multi.py --sources discogs

# Test-Modus (wenige Requests)
python3 fetch_multi.py --limit 2

# Bandcamp Labels erweitern (Dry Run)
python3 expand_bandcamp_labels.py --dry-run --min-connections 3

# Quality Scores berechnen
python3 quality_score.py

# Crawler fortsetzen (max 2 Stunden)
python3 crawler.py --resume --max-depth 2 --time-budget 120

# Deploy
bash deploy.sh
```

## Wichtige Hinweise

- Beatport ist deaktiviert. Der Artist-Filter greift zu breit und laesst generische Releases durch.
- Spotify cached jetzt spotify_ids in network_data.json — kuenftige Runs sind schneller.
- Spotify Related Artists API gibt 403 zurueck (Client Credentials reichen nicht).
- Bandcamp blockiert Python requests via TLS-Fingerprinting. bandcamp.py nutzt curl als Fallback.
- Hardwax nutzt JSON-Feed (stabil). Boomkat nutzt RSS-Feed mit rss2json.com Proxy-Fallback fuer Hetzner. Clone/Rush Hour nutzen RSS-Feeds (stabil). Juno ist HTML-Scraper mit cloudscraper — Genre-Slugs muessen gepflegt werden (zuletzt aktualisiert 2026-03-21).
- network_data.json waechst mit jedem Crawler-Run (~36MB). Bei >50MB auf Git LFS umstellen.
- Der User bevorzugt: Minimal, Deep House, Downtempo, Soulful, Broken Beat, Jazz-Electronic. Keine Mainstream-EDM.
