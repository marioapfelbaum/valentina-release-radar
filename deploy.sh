#!/bin/bash
# Deploy Valentina Release Radar to Cloudflare Pages
# Usage: ./deploy.sh
#
# Live URL: https://valentina-release-radar.pages.dev

set -e
DIR="$(cd "$(dirname "$0")" && pwd)"
DIST="$DIR/dist"

echo "📦 Preparing deploy directory..."
rm -rf "$DIST"
mkdir -p "$DIST"

# Copy static HTML files
cp "$DIR/release_radar.html" "$DIST/"
cp "$DIR/event_radar.html" "$DIST/"
cp "$DIR/network_explorer.html" "$DIST/" 2>/dev/null || true

# Copy data files
cp "$DIR/releases.json" "$DIST/"
cp "$DIR/events.json" "$DIST/" 2>/dev/null || true

# Cloudflare Pages redirect: / → /release_radar.html
cat > "$DIST/_redirects" << 'EOF'
/ /release_radar.html 200
EOF

# Cloudflare Pages headers (CORS for JSON, caching)
cat > "$DIST/_headers" << 'EOF'
/*.json
  Access-Control-Allow-Origin: *
  Cache-Control: public, max-age=300

/*.html
  Cache-Control: public, max-age=60
EOF

echo "✅ Prepared $(ls "$DIST" | wc -l | tr -d ' ') files for deploy"
ls -lh "$DIST"

echo ""
echo "🚀 Deploying to Cloudflare Pages..."
cd "$DIR"
npx wrangler pages deploy dist --project-name valentina-release-radar

echo ""
echo "✅ Done! Live at: https://valentina-release-radar.pages.dev"
