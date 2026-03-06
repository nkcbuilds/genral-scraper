import argparse
import csv
import random
import time
from pathlib import Path
from typing import Dict, List, Set
from urllib.parse import quote_plus

import requests
from google_play_scraper import search as play_search


BASE_DIR = Path(__file__).resolve().parents[1]
SEEDS_DIR = BASE_DIR / "seeds"

PAIN_KEYWORDS = "wish|hate|frustrated|need better|alternative to|struggling with|pain point"

STARTER_REDDIT = [
    "startups", "entrepreneur", "smallbusiness", "indiehackers", "saas", "microsaas", "productivity",
    "marketing", "sales", "webdev", "devops", "programming", "python", "javascript", "reactjs", "node",
    "datascience", "dataanalytics", "machinelearning", "accounting", "bookkeeping", "legaladvice",
    "developersindia", "indianstartup", "smallbusinessindia", "india", "automation", "crm", "hr", "nocode",
]

PLAY_QUERY_TERMS = [
    "productivity", "project management", "crm", "sales", "marketing", "email", "customer support", "helpdesk",
    "notetaking", "calendar", "todo", "task management", "automation", "workflow", "analytics", "dashboard",
    "bookkeeping", "accounting", "invoice", "expense", "inventory", "warehouse", "logistics", "shipping",
    "ecommerce", "shopify", "woocommerce", "dropshipping", "social media", "seo", "ppc", "content",
    "developer tools", "devops", "cloud", "security", "vpn", "ai assistant", "ai chatbot", "ai writing",
    "no code", "website builder", "wordpress", "notion", "slack", "jira", "kanban", "collaboration", "remote work",
    "education", "healthcare", "fitness", "finance", "banking", "payments", "subscription", "saas",
    "recruiting", "hr", "payroll", "scheduling", "appointment", "point of sale", "restaurant", "hotel", "travel",
]

PLAY_QUERY_SUFFIXES = [
    "app", "software", "tool", "platform", "for business", "for startups", "for teams", "for agencies",
    "for freelancers", "for creators", "for ecommerce", "for smb", "for enterprise", "india", "global",
]

REDDIT_DISCOVERY_QUERIES = [
    "startup", "saas", "micro saas", "small business", "indie hacker", "b2b", "operations", "automation",
    "crm", "customer support", "marketing", "growth", "seo", "sales", "project management", "developer",
    "web development", "python", "javascript", "react", "node", "devops", "cloud", "ai", "machine learning",
    "data science", "analytics", "finance", "accounting", "bookkeeping", "tax", "legal", "hr", "recruiting",
    "ecommerce", "shopify", "wordpress", "nocode", "freelance", "agency", "remote", "productivity", "notion",
    "excel", "healthcare", "education", "logistics", "supply chain", "india startup", "india business",
]

UPWORK_VERBS = [
    "automation", "management", "tracking", "reporting", "optimization", "cleanup", "migration", "integration",
    "consolidation", "analysis", "monitoring", "validation", "synchronization", "auditing", "reconciliation",
]

UPWORK_TASKS = [
    "manual data entry", "copy paste", "lead generation", "crm updates", "invoice processing", "bookkeeping",
    "inventory updates", "order processing", "email outreach", "support ticket triage", "calendar coordination",
    "excel reporting", "dashboard updates", "social media reporting", "seo reporting", "recruiting pipeline",
    "project status updates", "timesheet processing", "billing operations", "vendor management", "qa checklist",
]

UPWORK_DOMAINS = [
    "for shopify stores", "for agencies", "for startups", "for ecommerce brands", "for saas teams",
    "for small businesses", "for sales teams", "for support teams", "for operations teams", "for finance teams",
    "for recruitment agencies", "for healthcare clinics", "for education businesses", "for logistics companies",
]


def normalize_text(value: str) -> str:
    return " ".join((value or "").replace("\x00", " ").split()).strip()


def play_queries() -> List[str]:
    out: List[str] = []
    for term in PLAY_QUERY_TERMS:
        for suffix in PLAY_QUERY_SUFFIXES:
            out.append(f"{term} {suffix}")
    random.Random(42).shuffle(out)
    return out


def collect_apps(target: int, apple_lookup_limit: int, countries: List[str]) -> List[Dict[str, str]]:
    found: Dict[str, Dict[str, str]] = {}
    queries = play_queries()

    for query in queries:
        if len(found) >= target:
            break
        for country in countries:
            if len(found) >= target:
                break
            try:
                rows = play_search(query=query, n_hits=120, lang="en", country=country)
            except Exception:
                continue
            for row in rows:
                app_id = normalize_text(str(row.get("appId", "")))
                if not app_id or app_id in found:
                    continue
                found[app_id] = {
                    "app_name": normalize_text(str(row.get("title", ""))) or app_id,
                    "play_id": app_id,
                    "apple_id": "",
                    "category": normalize_text(str(row.get("genre", ""))) or "Unknown",
                    "enabled": "1",
                }
                if len(found) >= target:
                    break

    apps = list(found.values())[:target]

    if apple_lookup_limit > 0:
        session = requests.Session()
        session.headers.update({"User-Agent": "startupideadb-seed-populator/2.0"})
        for idx, row in enumerate(apps[:apple_lookup_limit], start=1):
            try:
                resp = session.get(
                    "https://itunes.apple.com/search",
                    params={"term": row["app_name"], "entity": "software", "country": "us", "limit": 1},
                    timeout=15,
                )
                resp.raise_for_status()
                payload = resp.json()
                results = payload.get("results", [])
                if results:
                    track_id = results[0].get("trackId")
                    if track_id is not None:
                        row["apple_id"] = str(track_id)
            except Exception:
                pass
            if idx % 30 == 0:
                time.sleep(0.1)
    return apps


def collect_reddit_subreddits(target: int) -> List[str]:
    names: Set[str] = {x.lower() for x in STARTER_REDDIT}
    session = requests.Session()
    session.headers.update({"User-Agent": "startupideadb-seed-populator/2.0"})

    for q in REDDIT_DISCOVERY_QUERIES:
        if len(names) >= target:
            break
        try:
            resp = session.get(
                "https://www.reddit.com/subreddits/search.json",
                params={"q": q, "limit": 100, "sort": "relevance"},
                timeout=20,
            )
            resp.raise_for_status()
            payload = resp.json()
        except Exception:
            continue
        children = payload.get("data", {}).get("children", [])
        for child in children:
            data = child.get("data", {})
            name = normalize_text(str(data.get("display_name", ""))).lower()
            if not name:
                continue
            if not all(c.isalnum() or c == "_" for c in name):
                continue
            if data.get("over18") is True:
                continue
            names.add(name)
            if len(names) >= target:
                break
        time.sleep(0.2)
    return sorted(names)


def meta_name_pool(apps: List[Dict[str, str]], target: int) -> List[Dict[str, str]]:
    seen = set()
    pool: List[Dict[str, str]] = []
    for row in apps:
        name = normalize_text(row["app_name"])
        if not name:
            continue
        key = name.lower()
        if key in seen:
            continue
        seen.add(key)
        pool.append({"product_name": name, "category": normalize_text(row["category"]) or "Unknown", "enabled": "1"})
        if len(pool) >= target:
            return pool

    extra = [
        ("HubSpot", "CRM"), ("Salesforce", "CRM"), ("Pipedrive", "CRM"), ("Zoho CRM", "CRM"),
        ("Freshsales", "CRM"), ("Zendesk", "Support"), ("Freshdesk", "Support"), ("Intercom", "Support"),
        ("Asana", "Project Management"), ("ClickUp", "Project Management"), ("Monday.com", "Project Management"),
        ("Notion", "Productivity"), ("Airtable", "Database"), ("Trello", "Project Management"),
        ("Jira", "Development"), ("Linear", "Development"), ("Confluence", "Documentation"),
        ("Zapier", "Automation"), ("Make", "Automation"), ("Calendly", "Scheduling"),
        ("Mailchimp", "Marketing"), ("Klaviyo", "Marketing"), ("Semrush", "SEO"), ("Ahrefs", "SEO"),
        ("Shopify", "Ecommerce"), ("WooCommerce", "Ecommerce"), ("Stripe", "Payments"),
        ("QuickBooks", "Accounting"), ("Xero", "Accounting"), ("Wave", "Accounting"),
    ]
    for name, category in extra:
        key = name.lower()
        if key in seen:
            continue
        seen.add(key)
        pool.append({"product_name": name, "category": category, "enabled": "1"})
        if len(pool) >= target:
            break
    return pool


def upwork_queries(target: int) -> List[Dict[str, str]]:
    seen = set()
    rows: List[Dict[str, str]] = []

    starter = [
        ("manual data entry excel", "Operations"),
        ("copy paste crm", "CRM"),
        ("lead generation spreadsheet", "Sales"),
        ("shopify order processing", "Ecommerce"),
        ("invoice processing", "Finance"),
        ("social media reporting", "Marketing"),
    ]
    for q, c in starter:
        key = q.lower()
        if key in seen:
            continue
        seen.add(key)
        rows.append({"query": q, "enabled": "1", "category": c})

    combos = []
    for task in UPWORK_TASKS:
        for verb in UPWORK_VERBS:
            for domain in UPWORK_DOMAINS:
                combos.append(f"{task} {verb} {domain}")
    random.Random(42).shuffle(combos)

    category_map = {
        "crm": "CRM", "invoice": "Finance", "bookkeeping": "Finance", "support": "Support", "seo": "Marketing",
        "social": "Marketing", "shopify": "Ecommerce", "inventory": "Operations", "recruit": "HR", "data": "Operations",
    }

    for query in combos:
        if len(rows) >= target:
            break
        norm = normalize_text(query)
        key = norm.lower()
        if key in seen:
            continue
        seen.add(key)
        category = "Operations"
        lk = key
        for token, cat in category_map.items():
            if token in lk:
                category = cat
                break
        rows.append({"query": norm, "enabled": "1", "category": category})
    return rows[:target]


def write_csv(path: Path, fieldnames: List[str], rows: List[Dict[str, str]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        w.writerows(rows)


def main() -> None:
    parser = argparse.ArgumentParser(description="Populate large seed files for all sources.")
    parser.add_argument("--apps", type=int, default=3000)
    parser.add_argument("--reddit", type=int, default=1200)
    parser.add_argument("--meta", type=int, default=2500, help="Rows for each of G2 and Capterra.")
    parser.add_argument("--upwork", type=int, default=3000)
    parser.add_argument("--apple-lookup-limit", type=int, default=1000)
    parser.add_argument("--countries", type=str, default="us,in,gb,ca,au")
    args = parser.parse_args()

    random.seed(42)
    countries = [c.strip().lower() for c in args.countries.split(",") if c.strip()]

    apps = collect_apps(args.apps, args.apple_lookup_limit, countries)
    if not apps:
        raise RuntimeError("No apps collected from Play Store search. Check network and try again.")

    reddit_names = collect_reddit_subreddits(args.reddit)
    meta_pool = meta_name_pool(apps, args.meta)
    upwork = upwork_queries(args.upwork)

    apps_path = SEEDS_DIR / "apps_seed_300.csv"
    reddit_path = SEEDS_DIR / "reddit_seed.csv"
    g2_path = SEEDS_DIR / "g2_products.csv"
    cap_path = SEEDS_DIR / "capterra_products.csv"
    upwork_path = SEEDS_DIR / "upwork_queries.csv"

    write_csv(
        apps_path,
        ["app_name", "play_id", "apple_id", "category", "enabled"],
        apps,
    )
    write_csv(
        reddit_path,
        ["subreddit", "enabled", "keywords"],
        [{"subreddit": s, "enabled": "1", "keywords": PAIN_KEYWORDS} for s in reddit_names],
    )
    write_csv(
        g2_path,
        ["product_name", "url", "category", "enabled"],
        [
            {
                "product_name": row["product_name"],
                "url": f"https://www.g2.com/search?query={quote_plus(row['product_name'])}",
                "category": row["category"],
                "enabled": row["enabled"],
            }
            for row in meta_pool
        ],
    )
    write_csv(
        cap_path,
        ["product_name", "url", "category", "enabled"],
        [
            {
                "product_name": row["product_name"],
                "url": f"https://www.capterra.com/search/?query={quote_plus(row['product_name'])}",
                "category": row["category"],
                "enabled": row["enabled"],
            }
            for row in meta_pool
        ],
    )
    write_csv(
        upwork_path,
        ["query", "enabled", "category"],
        upwork,
    )

    with_apple = sum(1 for row in apps if row.get("apple_id"))
    print(f"apps_seed_300.csv rows={len(apps)} apple_ids={with_apple}")
    print(f"reddit_seed.csv rows={len(reddit_names)}")
    print(f"g2_products.csv rows={len(meta_pool)}")
    print(f"capterra_products.csv rows={len(meta_pool)}")
    print(f"upwork_queries.csv rows={len(upwork)}")


if __name__ == "__main__":
    main()
