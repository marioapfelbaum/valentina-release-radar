#!/usr/bin/env python3
"""
Generates a small network_artists.json with just the artist names,
for the frontend (event_radar.html) which only needs to mark
"is this artist in my network?".
"""
import json
from pathlib import Path

BASE = Path(__file__).parent
network = BASE / "network_data.json"
out = BASE / "network_artists.json"

with open(network) as f:
    data = json.load(f)

names = sorted({a["name"] for a in data["artists"].values() if a.get("name")})

with open(out, "w") as f:
    json.dump(names, f, ensure_ascii=False)

size_kb = out.stat().st_size / 1024
print(f"Wrote {len(names)} artist names to network_artists.json ({size_kb:.0f} KB)")
