import csv
import time
from pathlib import Path
from typing import Dict, List, Optional

import requests
from google_play_scraper import search


BASE_DIR = Path(__file__).resolve().parents[1]
SEED_PATH = BASE_DIR / "seeds" / "apps_seed_300.csv"
TARGET_COUNT = 300


QUERIES = [
    "productivity app",
    "project management",
    "crm app",
    "sales app",
    "notetaking app",
    "to do list",
    "calendar app",
    "email app",
    "team chat",
    "customer support",
    "helpdesk software",
    "hr software",
    "accounting app",
    "bookkeeping app",
    "invoice app",
    "expense tracker",
    "time tracking",
    "analytics dashboard",
    "data visualization",
    "marketing automation",
    "seo tools",
    "social media management",
    "content creation app",
    "video editor",
    "photo editor",
    "education app",
    "healthcare app",
    "fitness app",
    "meditation app",
    "legal app",
    "ecommerce app",
    "shopify tools",
    "dropshipping app",
    "delivery app",
    "logistics app",
    "travel planner",
    "finance app",
    "trading app",
    "crypto app",
    "banking app",
    "ai chatbot",
    "ai writer",
    "automation app",
    "no code builder",
    "website builder",
    "wordpress app",
    "developer tools",
    "cloud app",
    "security app",
    "vpn app",
]


def normalize_genre(genre: str) -> str:
    cleaned = (genre or "").strip()
    if not cleaned:
        return "Unknown"
    return cleaned


def find_apple_id(session: requests.Session, app_name: str) -> str:
    try:
        resp = session.get(
            "https://itunes.apple.com/search",
            params={"term": app_name, "entity": "software", "country": "us", "limit": 3},
            timeout=15,
        )
        resp.raise_for_status()
        payload = resp.json()
        results = payload.get("results", [])
        if not results:
            return ""
        # Prefer closest textual match, then fallback to first.
        name_lower = app_name.lower()
        best = None
        for row in results:
            track = str(row.get("trackName", "")).lower()
            if name_lower in track or track in name_lower:
                best = row
                break
        if best is None:
            best = results[0]
        track_id = best.get("trackId")
        return str(track_id) if track_id is not None else ""
    except Exception:
        return ""


def collect_play_apps() -> List[Dict[str, str]]:
    seen = set()
    collected: List[Dict[str, str]] = []
    for query in QUERIES:
        try:
            rows = search(query=query, n_hits=50, lang="en", country="us")
        except Exception:
            continue
        for row in rows:
            app_id = str(row.get("appId", "")).strip()
            if not app_id or app_id in seen:
                continue
            seen.add(app_id)
            collected.append(
                {
                    "app_name": str(row.get("title", "")).strip() or app_id,
                    "play_id": app_id,
                    "apple_id": "",
                    "category": normalize_genre(str(row.get("genre", "")).strip()),
                    "enabled": "1",
                }
            )
            if len(collected) >= TARGET_COUNT:
                return collected
    return collected


def main() -> None:
    SEED_PATH.parent.mkdir(parents=True, exist_ok=True)
    apps = collect_play_apps()
    if not apps:
        raise RuntimeError("No apps collected from Play search.")

    session = requests.Session()
    session.headers.update({"User-Agent": "startupideadb-seed-populator/1.0"})

    for idx, app in enumerate(apps[:TARGET_COUNT], start=1):
        app["apple_id"] = find_apple_id(session, app["app_name"])
        if idx % 25 == 0:
            time.sleep(0.2)

    rows = apps[:TARGET_COUNT]
    if len(rows) < TARGET_COUNT:
        for i in range(len(rows) + 1, TARGET_COUNT + 1):
            rows.append(
                {
                    "app_name": f"filler_app_{i:03d}",
                    "play_id": "",
                    "apple_id": "",
                    "category": "Unknown",
                    "enabled": "0",
                }
            )

    with SEED_PATH.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=["app_name", "play_id", "apple_id", "category", "enabled"])
        writer.writeheader()
        writer.writerows(rows)

    with_apple = sum(1 for r in rows if r.get("apple_id"))
    enabled = sum(1 for r in rows if r.get("enabled") == "1")
    print(f"Wrote {len(rows)} rows to {SEED_PATH}")
    print(f"Enabled rows: {enabled}")
    print(f"Rows with Apple IDs: {with_apple}")


if __name__ == "__main__":
    main()
