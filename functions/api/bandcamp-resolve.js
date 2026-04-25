// Cloudflare Pages Function — Bandcamp Album-ID Resolver
// Browser kann Bandcamp-Pages wegen CORS nicht direkt scrapen.
// Diese Function nimmt eine Bandcamp-URL und liefert {album_id} zurueck,
// das das Frontend dann fuer den iframe-Embed nutzt.
//
// Usage: GET /api/bandcamp-resolve?url=https://label.bandcamp.com/album/foo
// Returns: { album_id: "123456789", track_id: "..." (optional) }

function json(body, status = 200) {
  return new Response(JSON.stringify(body), {
    status,
    headers: {
      "Content-Type": "application/json",
      "Access-Control-Allow-Origin": "*",
      "Cache-Control": "public, max-age=86400", // 24h cache (album-IDs sind stabil)
    },
  });
}

export async function onRequestOptions() {
  return new Response(null, {
    headers: {
      "Access-Control-Allow-Origin": "*",
      "Access-Control-Allow-Methods": "GET, OPTIONS",
    },
  });
}

export async function onRequestGet({ request }) {
  const url = new URL(request.url).searchParams.get("url") || "";
  if (!url || !/^https?:\/\/[a-z0-9-]+\.bandcamp\.com\//i.test(url)) {
    return json({ error: "invalid bandcamp url" }, 400);
  }

  let html;
  try {
    const resp = await fetch(url, {
      headers: { "User-Agent": "Mozilla/5.0 (Valentina Release Radar)" },
      cf: { cacheTtl: 86400 },
    });
    if (!resp.ok) {
      return json({ error: "bandcamp returned " + resp.status }, 502);
    }
    html = await resp.text();
  } catch (e) {
    return json({ error: "fetch failed: " + (e?.message || "unknown") }, 502);
  }

  // Pattern 1: data-tralbum JSON enthaelt {"current": {"id": ...,"type":"album|track"}}
  let albumId = null;
  let trackId = null;
  let kind = null;

  const tralbum = html.match(/data-tralbum="([^"]+)"/);
  if (tralbum) {
    try {
      const decoded = tralbum[1].replace(/&quot;/g, '"').replace(/&amp;/g, "&");
      const data = JSON.parse(decoded);
      if (data?.current?.type === "album") {
        albumId = data.current.id;
        kind = "album";
      } else if (data?.current?.type === "track") {
        trackId = data.current.id;
        kind = "track";
        // Track-Releases: bevorzugt Album-ID falls verfuegbar (mehr Kontext)
        if (data.album_id) {
          albumId = data.album_id;
          kind = "album";
        }
      }
    } catch {}
  }

  // Pattern 2: meta tag <meta name="bc-page-properties" content='{"item_id":...,"item_type":"a|t"}' />
  if (!albumId && !trackId) {
    const meta = html.match(/<meta\s+name="bc-page-properties"\s+content='([^']+)'/);
    if (meta) {
      try {
        const data = JSON.parse(meta[1]);
        if (data.item_type === "a") { albumId = data.item_id; kind = "album"; }
        else if (data.item_type === "t") { trackId = data.item_id; kind = "track"; }
      } catch {}
    }
  }

  // Pattern 3: direkter Embed-Link im HTML
  if (!albumId && !trackId) {
    const embed = html.match(/EmbeddedPlayer\/(album|track)=(\d+)/);
    if (embed) {
      if (embed[1] === "album") { albumId = embed[2]; kind = "album"; }
      else { trackId = embed[2]; kind = "track"; }
    }
  }

  if (!albumId && !trackId) {
    return json({ error: "no album/track id found" }, 404);
  }

  return json({ album_id: albumId, track_id: trackId, kind });
}
