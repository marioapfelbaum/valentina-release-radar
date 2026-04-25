// Cloudflare Pages Function — SoundCloud-Search-Fallback
// Sucht ein Release auf SoundCloud (3rd Player-Quelle nach Bandcamp/Spotify, vor YouTube).
// Browser kann SoundCloud-Search wegen CORS nicht selbst.
//
// Usage: GET /api/soundcloud-search?q=artist+title
// Returns: { url, embed_url }

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

export async function onRequestGet({ request }) {
  const q = (new URL(request.url).searchParams.get("q") || "").trim();
  if (!q) return json({ error: "missing q" }, 400);

  const searchUrl = `https://soundcloud.com/search/sounds?q=${encodeURIComponent(q)}`;
  let html;
  try {
    const resp = await fetch(searchUrl, {
      headers: { "User-Agent": "Mozilla/5.0 (Valentina Release Radar)" },
      cf: { cacheTtl: 86400 },
    });
    if (!resp.ok) return json({ error: "soundcloud returned " + resp.status }, 502);
    html = await resp.text();
  } catch (e) {
    return json({ error: "fetch failed: " + (e?.message || "unknown") }, 502);
  }

  // Track-URLs liegen als <a href="/USER/TRACK_SLUG"> im <noscript>-HTML
  // Filter Navigation/Search-URLs raus
  const blocked = new Set(["search", "discover", "feed", "you", "home", "stream", "upload", "stations"]);
  const matches = [...html.matchAll(/href="\/([a-z0-9_-]+)\/([a-z0-9_-]+)"/gi)];
  const seen = new Set();
  const tracks = [];
  for (const m of matches) {
    const user = m[1].toLowerCase();
    const track = m[2].toLowerCase();
    if (blocked.has(user)) continue;
    // Skip secondary navigation-Pfade (z.B. /search/sounds, /search/sets)
    if (user.startsWith("search")) continue;
    const path = `/${m[1]}/${m[2]}`;
    if (seen.has(path)) continue;
    seen.add(path);
    tracks.push({ user: m[1], track: m[2], path });
    if (tracks.length >= 5) break;
  }

  if (tracks.length === 0) {
    return json({ error: "no soundcloud results" }, 404);
  }

  const first = tracks[0];
  const trackUrl = `https://soundcloud.com${first.path}`;
  // Offizieller SC iframe-Player
  const embedUrl = `https://w.soundcloud.com/player/?url=${encodeURIComponent(trackUrl)}&color=%23d4915e&auto_play=false&hide_related=true&show_comments=false&show_user=true&show_reposts=false&show_teaser=false`;

  return json({ url: trackUrl, embed_url: embedUrl, alternates: tracks.slice(1, 4).map(t => `https://soundcloud.com${t.path}`) });
}
