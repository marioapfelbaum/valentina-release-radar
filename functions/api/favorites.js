// Cloudflare Pages Function — Favoriten-Sync via KV
// Routes: /api/favorites (GET, POST)
// Storage: KV-Binding "valentina_favorites", Key "v1"
//
// Auth: Optional shared secret. Wenn env.FAVORITES_KEY gesetzt ist,
// muss der Request den Header X-Auth-Key mit dem gleichen Wert senden
// (write-only — GET ist ohne Auth).

const KEY = "v1";

function json(body, status = 200) {
  return new Response(JSON.stringify(body), {
    status,
    headers: {
      "Content-Type": "application/json",
      "Access-Control-Allow-Origin": "*",
      "Access-Control-Allow-Methods": "GET, POST, OPTIONS",
      "Access-Control-Allow-Headers": "Content-Type, X-Auth-Key",
      "Cache-Control": "no-store",
    },
  });
}

export async function onRequestOptions() {
  return new Response(null, {
    headers: {
      "Access-Control-Allow-Origin": "*",
      "Access-Control-Allow-Methods": "GET, POST, OPTIONS",
      "Access-Control-Allow-Headers": "Content-Type, X-Auth-Key",
    },
  });
}

export async function onRequestGet({ env }) {
  if (!env.valentina_favorites) {
    return json({ error: "KV not bound" }, 500);
  }
  const data = await env.valentina_favorites.get(KEY, { type: "json" });
  return json({
    favorites: Array.isArray(data?.favorites) ? data.favorites : [],
    updated_at: data?.updated_at || null,
  });
}

export async function onRequestPost({ request, env }) {
  if (!env.valentina_favorites) {
    return json({ error: "KV not bound" }, 500);
  }

  // Optional auth — only enforced when FAVORITES_KEY is set
  if (env.FAVORITES_KEY) {
    const provided = request.headers.get("X-Auth-Key");
    if (provided !== env.FAVORITES_KEY) {
      return json({ error: "unauthorized" }, 401);
    }
  }

  let body;
  try {
    body = await request.json();
  } catch {
    return json({ error: "invalid json" }, 400);
  }

  const favorites = Array.isArray(body?.favorites) ? body.favorites : null;
  if (!favorites) {
    return json({ error: "missing favorites array" }, 400);
  }

  // Sanity-Limit: max 5000 IDs
  if (favorites.length > 5000) {
    return json({ error: "too many favorites" }, 400);
  }

  const payload = {
    favorites,
    updated_at: new Date().toISOString(),
  };
  await env.valentina_favorites.put(KEY, JSON.stringify(payload));
  return json({ ok: true, count: favorites.length, updated_at: payload.updated_at });
}
