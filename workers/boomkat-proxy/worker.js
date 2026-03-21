/**
 * Cloudflare Worker: Boomkat RSS Proxy
 * Proxies requests to boomkat.com RSS feed.
 * Needed because Boomkat blocks Hetzner server IPs via Cloudflare.
 */

const ALLOWED_URLS = [
  'https://boomkat.com/new-releases.rss',
  'https://boomkat.com/pre-orders.rss',
];

export default {
  async fetch(request) {
    const url = new URL(request.url);
    const target = url.searchParams.get('url');

    if (!target) {
      return new Response('Usage: ?url=https://boomkat.com/new-releases.rss', {
        status: 400,
      });
    }

    // Only allow known Boomkat URLs
    if (!ALLOWED_URLS.some(allowed => target.startsWith(allowed))) {
      return new Response('URL not allowed', { status: 403 });
    }

    try {
      const resp = await fetch(target, {
        headers: {
          'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36',
          'Accept': 'application/rss+xml,application/xml,text/xml,*/*',
        },
      });

      const body = await resp.text();
      return new Response(body, {
        status: resp.status,
        headers: {
          'Content-Type': resp.headers.get('Content-Type') || 'application/xml',
          'Cache-Control': 'public, max-age=3600',
        },
      });
    } catch (err) {
      return new Response(`Proxy error: ${err.message}`, { status: 502 });
    }
  },
};
