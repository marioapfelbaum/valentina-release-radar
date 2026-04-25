// Cloudflare Pages Function — Bandcamp-Search-Fallback
// Sucht ein Release auf Bandcamp wenn die Source-Quelle keinen iframe-Player hat
// (Hardwax/Redeye/Clone/etc.). Browser kann das wegen CORS nicht selbst.
//
// Usage: GET /api/bandcamp-search?q=artist+title&label=labelname
// Returns: { url, album_id, track_id, kind, score }
//
// Strategie: HTML-Scrape von https://bandcamp.com/search?q=...&item_type=a (album)
// Erstes Album-Match wird als Treffer genommen, danach via bandcamp-resolve verifiziert.

function json(body, status = 200) {
  return new Response(JSON.stringify(body), {
    status,
    headers: {
      "Content-Type": "application/json",
      "Access-Control-Allow-Origin": "*",
      "Cache-Control": "public, max-age=86400",
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

async function resolveBandcampUrl(url) {
  try {
    const resp = await fetch(url, {
      headers: { "User-Agent": "Mozilla/5.0 (Valentina Release Radar)" },
      cf: { cacheTtl: 86400 },
    });
    if (!resp.ok) return null;
    const html = await resp.text();
    // Pattern: data-tralbum oder bc-page-properties
    const tralbum = html.match(/data-tralbum="([^"]+)"/);
    if (tralbum) {
      try {
        const decoded = tralbum[1].replace(/&quot;/g, '"').replace(/&amp;/g, "&");
        const data = JSON.parse(decoded);
        if (data?.current?.type === "album") return { album_id: data.current.id, kind: "album" };
        if (data?.current?.type === "track") {
          if (data.album_id) return { album_id: data.album_id, kind: "album" };
          return { track_id: data.current.id, kind: "track" };
        }
      } catch {}
    }
    const meta = html.match(/<meta\s+name="bc-page-properties"\s+content='([^']+)'/);
    if (meta) {
      try {
        const data = JSON.parse(meta[1]);
        if (data.item_type === "a") return { album_id: data.item_id, kind: "album" };
        if (data.item_type === "t") return { track_id: data.item_id, kind: "track" };
      } catch {}
    }
    const embed = html.match(/EmbeddedPlayer\/(album|track)=(\d+)/);
    if (embed) {
      if (embed[1] === "album") return { album_id: embed[2], kind: "album" };
      return { track_id: embed[2], kind: "track" };
    }
  } catch {}
  return null;
}

export async function onRequestGet({ request }) {
  const params = new URL(request.url).searchParams;
  const q = (params.get("q") || "").trim();
  const label = (params.get("label") || "").trim();
  if (!q) return json({ error: "missing q" }, 400);

  // Bandcamp Search HTML scrapen — Album-Suche zuerst, dann Track
  const candidates = [];
  for (const itemType of ["a", "t"]) {
    const searchUrl = `https://bandcamp.com/search?q=${encodeURIComponent(q)}&item_type=${itemType}`;
    let html;
    try {
      const resp = await fetch(searchUrl, {
        headers: { "User-Agent": "Mozilla/5.0 (Valentina Release Radar)" },
        cf: { cacheTtl: 86400 },
      });
      if (!resp.ok) continue;
      html = await resp.text();
    } catch {
      continue;
    }

    // Search-Result-Items haben <a class="artcont" href="...">
    // oder <a class="heading" href="..."> (variiert nach Layout)
    // Wir extrahieren alle bandcamp.com URLs und filtern
    const urlMatches = [...html.matchAll(/href="(https?:\/\/[a-z0-9-]+\.bandcamp\.com\/(?:album|track)\/[^"?#]+)/gi)];
    for (const m of urlMatches) {
      const url = m[1];
      if (candidates.find(c => c.url === url)) continue;
      // Score: bevorzuge URLs deren Subdomain (Label) zum Label des Releases passt
      let score = itemType === "a" ? 10 : 5;
      if (label) {
        const labelSlug = label.toLowerCase().replace(/[^a-z0-9]/g, "");
        const sub = url.match(/\/\/([^.]+)\.bandcamp/);
        if (sub) {
          const subSlug = sub[1].toLowerCase().replace(/[^a-z0-9]/g, "");
          if (subSlug.includes(labelSlug) || labelSlug.includes(subSlug)) score += 20;
        }
      }
      candidates.push({ url, score });
      if (candidates.length >= 5) break;
    }
    if (candidates.length >= 5) break;
  }

  if (candidates.length === 0) {
    return json({ error: "no bandcamp results" }, 404);
  }

  // Sortiere nach Score, nimm besten Treffer und resolve
  candidates.sort((a, b) => b.score - a.score);
  for (const c of candidates.slice(0, 3)) {
    const resolved = await resolveBandcampUrl(c.url);
    if (resolved && (resolved.album_id || resolved.track_id)) {
      return json({ url: c.url, score: c.score, ...resolved });
    }
  }
  return json({ error: "found URLs but resolve failed" }, 404);
}
