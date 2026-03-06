import csv
import hashlib
import html
import json
import logging
import os
import random
import re
import threading
import time
from collections import deque
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import quote_plus, parse_qs, urlparse
from xml.etree import ElementTree as ET

import duckdb
import requests
from apscheduler.schedulers.background import BackgroundScheduler
from google_play_scraper import Sort
from google_play_scraper import reviews as play_reviews

try:
    import praw
except Exception:
    praw = None


def load_env_file(env_path: Path) -> None:
    if not env_path.exists():
        return
    for line in env_path.read_text(encoding="utf-8").splitlines():
        raw = line.strip()
        if not raw or raw.startswith("#") or "=" not in raw:
            continue
        key, value = raw.split("=", 1)
        key = key.strip()
        value = value.strip().strip('"').strip("'")
        # Always refresh from project .env to avoid stale inherited env values.
        if key:
            os.environ[key] = value

PAIN_KEYWORDS = [
    "wish",
    "hate",
    "frustrated",
    "need better",
    "alternative to",
    "struggling with",
    "pain point",
]
NEGATIVE_WORDS = {
    "hate", "frustrated", "annoying", "broken", "bad", "worst", "bug", "issue",
    "problem", "pain", "difficult", "slow", "useless", "terrible", "awful", "fail", "missing",
}
POSITIVE_WORDS = {"love", "great", "awesome", "good", "excellent", "amazing", "helpful", "perfect", "happy", "easy"}

REDDIT_STARTER_SUBREDDITS = [
    "startups","entrepreneur","smallbusiness","juststart","growmybusiness","indiehackers","buildinpublic","sideproject","startupideas","startup","techstartups","solopreneur","entrepreneurridealong","smallbusinessowners","business",
    "saas","microsaas","saasgrowth","saasindia","saastools","b2bsaas","saasmarketing","software","startupmarketing","appideas",
    "freelance","freelancing","antiwork","jobs","cscareerquestions","work","remote","digitalnomad","agencylife","remotejobs","workonline","forhire","hiring","overemployed","careers",
    "productivity","notion","obsidianmd","excel","productivitytools","getdisciplined","onenote","evernote","taskmanagement","selfimprovement","automation",
    "marketing","sales","growthhacking","ecommerce","shopify","seo","bigseo","ppc","dropshipping","etsy","socialmedia","socialmediamarketing","emailmarketing","content_marketing","copywriting","affiliate_marketing","salesops","customerexperience",
    "webdev","learnprogramming","sysadmin","devops","dataisbeautiful","programming","coding","technology","frontend","backend","python","javascript","reactjs","node","cloudcomputing","aws","machinelearning","datascience","dataanalytics","analytics","artificial","cybersecurity","linux","softwarearchitecture",
    "personalfinance","accounting","bookkeeping","tax","smallbusinessuk","financialindependence","operations","projectmanagement","legaladvice","legaladviceindia","crm","hr",
    "developersindia","indianstartup","indiainvestments","smallbusinessindia","entrepreneurindia","india","desitrading","india_tax","indianjobs","startupsindia",
    "teachers","homeautomation","urbanfarming","warehousing","travelhacks","photography","healthcare","resumes","careerguidance","androidapps","iphone","apps","offmychest","confession","findapath","wordpress","nocode","customer_success","edtech","logistics",
]

UPWORK_STARTER_QUERIES = [
    {"query": "manual data entry excel", "category": "Operations"},
    {"query": "copy paste crm", "category": "CRM"},
    {"query": "lead generation spreadsheet", "category": "Sales"},
    {"query": "shopify order processing", "category": "Ecommerce"},
    {"query": "inventory reconciliation", "category": "Operations"},
    {"query": "bookkeeping automation", "category": "Finance"},
    {"query": "invoice processing", "category": "Finance"},
    {"query": "customer support ticket triage", "category": "Support"},
    {"query": "social media reporting", "category": "Marketing"},
    {"query": "seo reporting dashboard", "category": "Marketing"},
    {"query": "notion automation", "category": "Productivity"},
    {"query": "zapier automation cleanup", "category": "Automation"},
    {"query": "email list cleaning", "category": "Marketing"},
    {"query": "recruiting pipeline tracking", "category": "HR"},
    {"query": "project status reporting", "category": "Project Management"},
    {"query": "warehouse spreadsheet tracking", "category": "Logistics"},
    {"query": "appointment scheduling assistant", "category": "Scheduling"},
    {"query": "content calendar management", "category": "Marketing"},
    {"query": "dashboard data consolidation", "category": "Analytics"},
    {"query": "subscription churn analysis", "category": "SaaS"},
]

HACKERNEWS_STARTER_QUERIES = [
    {"query": "manual process", "category": "Operations"},
    {"query": "spreadsheet workflow", "category": "Operations"},
    {"query": "crm", "category": "CRM"},
    {"query": "ticket triage", "category": "Support"},
    {"query": "automation", "category": "Automation"},
    {"query": "project management", "category": "Project Management"},
    {"query": "accounting", "category": "Finance"},
    {"query": "bookkeeping", "category": "Finance"},
    {"query": "sales ops", "category": "Sales"},
    {"query": "inventory", "category": "Logistics"},
    {"query": "email marketing", "category": "Marketing"},
    {"query": "content workflow", "category": "Marketing"},
]

PRODUCTHUNT_STARTER_QUERIES = [
    {"query": "automation", "category": "Automation"},
    {"query": "productivity", "category": "Productivity"},
    {"query": "crm", "category": "CRM"},
    {"query": "customer support", "category": "Support"},
    {"query": "project management", "category": "Project Management"},
    {"query": "analytics", "category": "Analytics"},
    {"query": "scheduling", "category": "Scheduling"},
    {"query": "bookkeeping", "category": "Finance"},
]


def query_tokens(text: str) -> List[str]:
    return [t for t in re.split(r"[^a-z0-9]+", clean(text).lower()) if len(t) >= 3]


def query_match_score(text: str, query: str) -> int:
    s = clean(text).lower()
    toks = query_tokens(query)
    if not toks:
        return 0
    return sum(1 for t in toks if t in s)


def now_utc() -> datetime:
    return datetime.now(timezone.utc)


def iso_utc(value: Optional[datetime] = None) -> str:
    return (value or now_utc()).astimezone(timezone.utc).isoformat()


def parse_bool(value: Any, default: bool = False) -> bool:
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    return str(value).strip().lower() in {"1", "true", "yes", "y", "on"}


def parse_int_env(name: str, default: int) -> int:
    raw = os.getenv(name)
    try:
        return int(raw) if raw is not None else default
    except ValueError:
        return default


def parse_float_env(name: str, default: float) -> float:
    raw = os.getenv(name)
    try:
        return float(raw) if raw is not None else default
    except ValueError:
        return default


def parse_list_env(name: str, default: str) -> List[str]:
    return [x.strip().lower() for x in os.getenv(name, default).split(",") if x.strip()]


def parse_dt(value: Any) -> Optional[datetime]:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=timezone.utc)
    if isinstance(value, (int, float)):
        return datetime.fromtimestamp(value, tz=timezone.utc)
    txt = str(value).strip().replace("Z", "+00:00")
    if not txt:
        return None
    try:
        dt = datetime.fromisoformat(txt)
        return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)
    except ValueError:
        for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d", "%a, %d %b %Y %H:%M:%S %z"):
            try:
                dt = datetime.strptime(txt, fmt)
                return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)
            except ValueError:
                pass
    return None


def clean(text: Any) -> str:
    if text is None:
        return ""
    return re.sub(r"\s+", " ", str(text).replace("\x00", " ")).strip()


def strip_html(text: Any) -> str:
    raw = clean(text)
    no_tags = re.sub(r"<[^>]+>", " ", raw)
    return clean(html.unescape(no_tags))


def has_pain(text: str) -> bool:
    s = text.lower()
    return any(k in s for k in PAIN_KEYWORDS)


def sentiment_proxy(text: str) -> int:
    s = text.lower()
    neg = sum(s.count(w) for w in NEGATIVE_WORDS)
    pos = sum(s.count(w) for w in POSITIVE_WORDS)
    return pos - neg


def make_fp(source: str, source_item_id: str, entity_id: str, reviewer: str, comment: str, posted_at: str) -> str:
    val = "|".join([source.lower().strip(), source_item_id.lower().strip(), entity_id.lower().strip(), reviewer.lower().strip(), clean(comment).lower(), posted_at.lower().strip()])
    return hashlib.sha256(val.encode("utf-8")).hexdigest()


class SlidingWindowRateLimiter:
    def __init__(self, per_second: int, per_minute: int, per_day: int) -> None:
        self.per_second = max(0, int(per_second))
        self.per_minute = max(0, int(per_minute))
        self.per_day = max(0, int(per_day))
        self._second = deque()
        self._minute = deque()
        self._day = deque()
        self._lock = threading.Lock()

    def _trim(self, now: float) -> None:
        while self._second and now - self._second[0] >= 1.0:
            self._second.popleft()
        while self._minute and now - self._minute[0] >= 60.0:
            self._minute.popleft()
        while self._day and now - self._day[0] >= 86400.0:
            self._day.popleft()

    def acquire(self, timeout_seconds: float = 3.0) -> bool:
        deadline = time.time() + max(0.0, timeout_seconds)
        while True:
            wait_for = 0.0
            with self._lock:
                now = time.time()
                self._trim(now)

                if self.per_second and len(self._second) >= self.per_second:
                    wait_for = max(wait_for, self._second[0] + 1.0 - now)
                if self.per_minute and len(self._minute) >= self.per_minute:
                    wait_for = max(wait_for, self._minute[0] + 60.0 - now)
                if self.per_day and len(self._day) >= self.per_day:
                    wait_for = max(wait_for, self._day[0] + 86400.0 - now)

                if wait_for <= 0:
                    stamp = time.time()
                    self._second.append(stamp)
                    self._minute.append(stamp)
                    self._day.append(stamp)
                    return True

            if time.time() + wait_for > deadline:
                return False
            time.sleep(min(1.0, max(0.05, wait_for)))


class Engine:
    def __init__(self) -> None:
        base = Path(__file__).resolve().parent
        load_env_file(base / ".env")

        self.project_id = os.getenv("PROJECT_ID", "").strip()
        if not self.project_id:
            raise RuntimeError("PROJECT_ID is required")

        self.fast_mode = parse_bool(os.getenv("FAST_BACKFILL_MODE", "true"), True)
        self.fast_days = parse_int_env("FAST_BACKFILL_DAYS", 5)
        self.target_records = parse_int_env("FAST_TARGET_RECORDS", 200000)
        self.max_workers = parse_int_env("MAX_WORKERS", 16)
        self.sleep_min = parse_int_env("CYCLE_SLEEP_MIN", 60)
        self.sleep_max = parse_int_env("CYCLE_SLEEP_MAX", 300)
        self.interval_hours = parse_int_env("INCREMENTAL_INTERVAL_HOURS", 4)
        self.daily_budget = parse_float_env("DAILY_BUDGET_INR", 500.0)
        self.play_pages_fast = parse_int_env("PLAY_PAGES_FAST", 3)
        self.play_count = parse_int_env("PLAY_COUNT_PER_PAGE", 200)
        self.reddit_fast = parse_int_env("REDDIT_LIMIT_FAST", 100)
        self.reddit_inc = parse_int_env("REDDIT_LIMIT_INCREMENTAL", 40)
        self.hn_enabled = parse_bool(os.getenv("HACKERNEWS_ENABLED", "true"), True)
        self.producthunt_enabled = parse_bool(os.getenv("PRODUCTHUNT_ENABLED", "true"), True)
        self.hn_fast = parse_int_env("HACKERNEWS_LIMIT_FAST", 40)
        self.hn_inc = parse_int_env("HACKERNEWS_LIMIT_INCREMENTAL", 20)
        self.producthunt_fast = parse_int_env("PRODUCTHUNT_LIMIT_FAST", 40)
        self.producthunt_inc = parse_int_env("PRODUCTHUNT_LIMIT_INCREMENTAL", 20)
        self.upwork_fast = parse_int_env("UPWORK_LIMIT_FAST", 75)
        self.upwork_inc = parse_int_env("UPWORK_LIMIT_INCREMENTAL", 30)
        self.upwork_dataset_url = os.getenv("UPWORK_DATASET_URL", "").strip()
        self.upwork_api_enabled = parse_bool(os.getenv("UPWORK_API_ENABLED", "false"), False)
        self.upwork_api_url = os.getenv("UPWORK_API_URL", "https://www.upwork.com/api/graphql/v1").strip()
        self.upwork_api_token = os.getenv("UPWORK_API_TOKEN", "").strip()
        self.upwork_api_timeout = max(5, parse_int_env("UPWORK_API_TIMEOUT_SECONDS", 30))
        # Safety clamps aligned with documented policy context (10 rps, 300 rpm, 40k/day).
        self.upwork_api_rps = min(10, max(1, parse_int_env("UPWORK_API_MAX_RPS", 8)))
        self.upwork_api_rpm = min(300, max(self.upwork_api_rps, parse_int_env("UPWORK_API_MAX_RPM", 240)))
        self.upwork_api_daily = min(40000, max(self.upwork_api_rpm, parse_int_env("UPWORK_API_MAX_DAILY", 35000)))
        self.upwork_api_cache_ttl = min(86400, max(0, parse_int_env("UPWORK_API_CACHE_TTL_SECONDS", 3600)))
        self.upwork_api_mode = clean(os.getenv("UPWORK_API_MODE", "graphql")).lower() or "graphql"
        self.upwork_api_first_party_required = parse_bool(os.getenv("UPWORK_API_FIRST_PARTY_REQUIRED", "false"), False)
        self.upwork_api_query_template = os.getenv(
            "UPWORK_API_GRAPHQL_QUERY",
            (
                "query StartupIdeaDbJobSearch($query: String!, $limit: Int!) { "
                "marketplaceJobPostingsSearch(request: {query: $query, first: $limit}) { "
                "edges { node { id title description jobPostingUrl createdAt publishedOn "
                "hourlyBudgetMin hourlyBudgetMax amount amountCurrencyCode jobType engagementType } } } }"
            ),
        ).strip()
        self.reddit_http_enabled = parse_bool(os.getenv("REDDIT_HTTP_ENABLED", "true"), True)
        self.g2_enabled = parse_bool(os.getenv("G2_ENABLED", "true"), True)
        self.capterra_enabled = parse_bool(os.getenv("CAPTERRA_ENABLED", "true"), True)
        self.meta_use_jina = parse_bool(os.getenv("META_USE_JINA_FALLBACK", "true"), True)
        self.jina_prefix = os.getenv("JINA_PREFIX", "https://r.jina.ai/http://").strip()
        self.meta_relaxed_marketplace = parse_bool(os.getenv("META_RELAXED_MARKETPLACE_SIGNALS", "true"), True)
        self.upwork_query_fallback = parse_bool(os.getenv("UPWORK_QUERY_SIGNAL_FALLBACK", "true"), True)
        self.play_countries = parse_list_env("PLAY_COUNTRIES", "us,gb,ca,au,in")
        self.apple_countries = parse_list_env("APPLE_COUNTRIES", "us,gb,ca,au,in")
        self.meta_seed_min_rows = parse_int_env("META_SEED_MIN_ROWS", 120)
        self.fast_apps_per_cycle = parse_int_env("FAST_APPS_PER_CYCLE", 120)
        self.fast_apple_per_cycle = min(
            max(1, self.fast_apps_per_cycle),
            max(1, parse_int_env("FAST_APPLE_PER_CYCLE", self.fast_apps_per_cycle)),
        )
        self.fast_reddit_per_cycle = parse_int_env("FAST_REDDIT_PER_CYCLE", 120)
        self.fast_hn_per_cycle = parse_int_env("FAST_HACKERNEWS_PER_CYCLE", 80)
        self.fast_producthunt_per_cycle = parse_int_env("FAST_PRODUCTHUNT_PER_CYCLE", 80)
        self.fast_g2_per_cycle = parse_int_env("FAST_G2_PER_CYCLE", 200)
        self.fast_capterra_per_cycle = parse_int_env("FAST_CAPTERRA_PER_CYCLE", 200)
        self.fast_upwork_per_cycle = parse_int_env("FAST_UPWORK_PER_CYCLE", 200)
        self.inc_apps_per_cycle = parse_int_env("INC_APPS_PER_CYCLE", 40)
        self.inc_apple_per_cycle = min(
            max(1, self.inc_apps_per_cycle),
            max(1, parse_int_env("INC_APPLE_PER_CYCLE", self.inc_apps_per_cycle)),
        )
        self.inc_reddit_per_cycle = parse_int_env("INC_REDDIT_PER_CYCLE", 40)
        self.inc_hn_per_cycle = parse_int_env("INC_HACKERNEWS_PER_CYCLE", 30)
        self.inc_producthunt_per_cycle = parse_int_env("INC_PRODUCTHUNT_PER_CYCLE", 30)
        self.inc_g2_per_cycle = parse_int_env("INC_G2_PER_CYCLE", 60)
        self.inc_capterra_per_cycle = parse_int_env("INC_CAPTERRA_PER_CYCLE", 60)
        self.inc_upwork_per_cycle = parse_int_env("INC_UPWORK_PER_CYCLE", 60)
        self.appstore_focus_enabled = parse_bool(os.getenv("APPSTORE_FOCUS_ENABLED", "false"), False)
        self.appstore_focus_skip_play_for_apple = parse_bool(
            os.getenv("APPSTORE_FOCUS_SKIP_PLAY_FOR_APPLE", "false"),
            False,
        )
        # Per-source helper lanes (semaphores) to scale IO concurrency safely.
        self.play_helpers = max(1, parse_int_env("PLAY_HELPERS", 10))
        self.apple_helpers = max(1, parse_int_env("APPLE_HELPERS", 10))
        self.reddit_helpers = max(1, parse_int_env("REDDIT_HELPERS", 6))
        self.hn_helpers = max(1, parse_int_env("HACKERNEWS_HELPERS", 6))
        self.producthunt_helpers = max(1, parse_int_env("PRODUCTHUNT_HELPERS", 6))
        self.meta_helpers = max(1, parse_int_env("META_HELPERS", 12))
        self.upwork_helpers = max(1, parse_int_env("UPWORK_HELPERS", 8))

        self.base = base
        self.seeds_dir = self.base / "seeds"
        self.exports_dir = self.base / "exports"
        self.logs_dir = self.base / "logs"
        self.db_path = Path(os.getenv("DB_PATH", str(self.base / "reviews.db")))

        self.lock = threading.RLock()
        self.conn: Optional[duckdb.DuckDBPyConnection] = None
        self.session = requests.Session()
        self.session.headers.update({"User-Agent": "startupideadb-scraper/2.0 (+https://startupideadb.com)"})
        adapter = requests.adapters.HTTPAdapter(pool_connections=64, pool_maxsize=64, max_retries=0)
        self.session.mount("http://", adapter)
        self.session.mount("https://", adapter)
        self.scheduler = BackgroundScheduler(timezone="UTC")

        self.play_sem = threading.Semaphore(self.play_helpers)
        self.apple_sem = threading.Semaphore(self.apple_helpers)
        self.reddit_sem = threading.Semaphore(self.reddit_helpers)
        self.hn_sem = threading.Semaphore(self.hn_helpers)
        self.producthunt_sem = threading.Semaphore(self.producthunt_helpers)
        self.meta_sem = threading.Semaphore(self.meta_helpers)
        self.upwork_sem = threading.Semaphore(self.upwork_helpers)
        self.upwork_api_limiter = SlidingWindowRateLimiter(self.upwork_api_rps, self.upwork_api_rpm, self.upwork_api_daily)
        self.upwork_api_cache_lock = threading.Lock()
        self.upwork_api_cache: Dict[str, Tuple[float, List[Dict[str, Any]]]] = {}

        self.reddit_client = self._init_reddit()
        self._last_seed_sync_at: Optional[datetime] = None
        self._connect_db(retries=120, delay_seconds=2)
        self._setup()
        self._close_db()

    def _connect_db(self, retries: int = 20, delay_seconds: int = 5) -> None:
        if self.conn is not None:
            return
        attempt = 0
        while True:
            try:
                self.conn = duckdb.connect(str(self.db_path))
                return
            except Exception as exc:
                attempt += 1
                if attempt >= retries:
                    raise RuntimeError(f"Failed to open DuckDB after {retries} attempts: {exc}") from exc
                time.sleep(max(1, delay_seconds))

    def _close_db(self) -> None:
        if self.conn is None:
            return
        try:
            self.conn.close()
        except Exception:
            pass
        self.conn = None
    def _setup(self) -> None:
        self.seeds_dir.mkdir(parents=True, exist_ok=True)
        self.exports_dir.mkdir(parents=True, exist_ok=True)
        self.logs_dir.mkdir(parents=True, exist_ok=True)
        logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")

        with self.lock:
            self.conn.execute(
                """
                CREATE TABLE IF NOT EXISTS reviews (
                    source TEXT,
                    source_item_id TEXT,
                    entity_id TEXT,
                    entity_name TEXT,
                    category TEXT,
                    reviewer_name TEXT,
                    rating DOUBLE,
                    comment_text TEXT,
                    posted_at TEXT,
                    url TEXT,
                    country TEXT,
                    language TEXT,
                    fingerprint TEXT,
                    scraped_at TIMESTAMP,
                    raw_json TEXT,
                    enriched_at TIMESTAMP
                )
                """
            )
            removed = self._purge_duplicate_reviews()
            if removed > 0:
                logging.warning("Removed %s duplicate review rows during startup dedupe maintenance", removed)
            try:
                self.conn.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_reviews_fingerprint ON reviews(fingerprint)")
            except Exception as exc:
                logging.warning("Unique index creation failed, retrying after dedupe: %s", exc)
                removed = self._purge_duplicate_reviews()
                if removed > 0:
                    logging.warning("Removed %s duplicate rows on retry", removed)
                self.conn.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_reviews_fingerprint ON reviews(fingerprint)")
            self.conn.execute(
                """
                CREATE TABLE IF NOT EXISTS seed_progress (
                    seed_type TEXT,
                    seed_key TEXT,
                    enabled BOOLEAN,
                    status TEXT,
                    total_seen BIGINT,
                    total_inserted BIGINT,
                    last_run_at TIMESTAMP,
                    last_error TEXT,
                    exhausted BOOLEAN,
                    updated_at TIMESTAMP,
                    PRIMARY KEY(seed_type, seed_key)
                )
                """
            )
            self.conn.execute(
                """
                CREATE TABLE IF NOT EXISTS run_log (
                    run_at TIMESTAMP,
                    mode TEXT,
                    stats_json TEXT,
                    total_records BIGINT
                )
                """
            )
            self.conn.execute(
                """
                CREATE TABLE IF NOT EXISTS source_cursors (
                    cursor_key TEXT PRIMARY KEY,
                    cursor_value TEXT,
                    updated_at TIMESTAMP
                )
                """
            )

        self._ensure_seed_files()
        self.seeds = self._load_all_seeds()
        self._sync_seed_progress(self.seeds)
        self._last_seed_sync_at = now_utc()
        self._normalize_seed_error_states()

    def _normalize_seed_error_states(self) -> None:
        with self.lock:
            try:
                self.conn.execute(
                    """
                    UPDATE seed_progress
                    SET status = 'skipped', updated_at = ?
                    WHERE status = 'error'
                      AND (
                        lower(coalesce(last_error, '')) LIKE '%403%'
                        OR lower(coalesce(last_error, '')) LIKE '%captcha%'
                        OR lower(coalesce(last_error, '')) LIKE '%cloudflare%'
                        OR lower(coalesce(last_error, '')) LIKE '%expecting value%'
                      )
                    """,
                    [now_utc()],
                )
                self.conn.execute(
                    """
                    UPDATE seed_progress
                    SET status = 'skipped', updated_at = ?
                    WHERE status = 'error'
                      AND seed_type IN ('g2', 'capterra', 'upwork', 'hackernews', 'producthunt')
                    """,
                    [now_utc()],
                )
                self.conn.execute(
                    """
                    UPDATE seed_progress
                    SET status = 'skipped', updated_at = ?
                    WHERE status = 'error'
                      AND seed_type = 'apps'
                      AND (
                        lower(coalesce(last_error, '')) LIKE '%not reachable%'
                        OR lower(coalesce(last_error, '')) LIKE '%404%'
                        OR lower(coalesce(last_error, '')) LIKE '%not found%'
                        OR lower(coalesce(last_error, '')) LIKE '%unavailable%'
                      )
                    """,
                    [now_utc()],
                )
            except Exception:
                pass

    def _purge_duplicate_reviews(self) -> int:
        with self.lock:
            try:
                before = self.conn.execute("SELECT COUNT(*) FROM reviews").fetchone()
                total_before = int(before[0]) if before else 0
                if total_before <= 1:
                    return 0
                self.conn.execute(
                    """
                    DELETE FROM reviews
                    WHERE rowid IN (
                        SELECT rowid
                        FROM (
                            SELECT rowid,
                                   ROW_NUMBER() OVER (
                                       PARTITION BY fingerprint
                                       ORDER BY scraped_at DESC NULLS LAST, rowid DESC
                                   ) AS rn
                            FROM reviews
                        ) t
                        WHERE rn > 1
                    )
                    """
                )
                after = self.conn.execute("SELECT COUNT(*) FROM reviews").fetchone()
                total_after = int(after[0]) if after else total_before
                return max(0, total_before - total_after)
            except Exception as exc:
                logging.warning("Duplicate cleanup skipped due to error: %s", exc)
                return 0

    def _init_reddit(self):
        if praw is None:
            return None
        cid = os.getenv("REDDIT_CLIENT_ID", "").strip()
        sec = os.getenv("REDDIT_CLIENT_SECRET", "").strip()
        ua = os.getenv("REDDIT_USER_AGENT", "startupideadb-scraper/2.0")
        if not cid or not sec:
            return None
        try:
            return praw.Reddit(client_id=cid, client_secret=sec, user_agent=ua, check_for_async=False)
        except Exception:
            return None

    def _ensure_seed_files(self) -> None:
        r_path = self.seeds_dir / "reddit_seed.csv"
        if not r_path.exists():
            subs = sorted({x.strip().lower() for x in REDDIT_STARTER_SUBREDDITS if x.strip()})
            with r_path.open("w", newline="", encoding="utf-8") as f:
                w = csv.DictWriter(f, fieldnames=["subreddit", "enabled", "keywords"])
                w.writeheader()
                kws = "|".join(PAIN_KEYWORDS)
                for s in subs:
                    w.writerow({"subreddit": s, "enabled": "1", "keywords": kws})

        a_path = self.seeds_dir / "apps_seed_300.csv"
        if not a_path.exists():
            samples = [
                {"app_name": "Evernote", "play_id": "com.evernote", "apple_id": "281796108", "category": "Productivity", "enabled": "1"},
                {"app_name": "Todoist", "play_id": "com.todoist", "apple_id": "572688870", "category": "Productivity", "enabled": "1"},
                {"app_name": "Notion", "play_id": "notion.id", "apple_id": "1232780281", "category": "Productivity", "enabled": "1"},
                {"app_name": "Slack", "play_id": "com.Slack", "apple_id": "618783545", "category": "Communication", "enabled": "1"},
                {"app_name": "Trello", "play_id": "com.trello", "apple_id": "461504587", "category": "Project Management", "enabled": "1"},
            ]
            with a_path.open("w", newline="", encoding="utf-8") as f:
                w = csv.DictWriter(f, fieldnames=["app_name", "play_id", "apple_id", "category", "enabled"])
                w.writeheader()
                for row in samples:
                    w.writerow(row)
                for i in range(6, 301):
                    w.writerow({"app_name": f"sample_app_{i:03d}", "play_id": "", "apple_id": "", "category": "Unknown", "enabled": "0"})
        app_rows = self._load_csv(a_path)
        app_candidates: List[Dict[str, str]] = []
        seen_apps = set()
        for row in app_rows:
            name = clean(row.get("app_name", ""))
            if not name:
                continue
            key = name.lower()
            if key in seen_apps:
                continue
            seen_apps.add(key)
            app_candidates.append(
                {
                    "product_name": name,
                    "category": clean(row.get("category", "Unknown")) or "Unknown",
                    "enabled": "1" if parse_bool(row.get("enabled", "1"), True) else "0",
                }
            )

        if len(app_candidates) < self.meta_seed_min_rows:
            fallback = [
                ("Notion", "Productivity"), ("Asana", "Project Management"), ("ClickUp", "Project Management"),
                ("Monday.com", "Project Management"), ("Airtable", "Database"), ("HubSpot", "CRM"),
                ("Salesforce", "CRM"), ("Pipedrive", "CRM"), ("Zoho CRM", "CRM"), ("Freshsales", "CRM"),
                ("Jira", "Development"), ("Linear", "Development"), ("Confluence", "Documentation"),
                ("Slack", "Communication"), ("Microsoft Teams", "Communication"), ("Zoom", "Communication"),
                ("Loom", "Communication"), ("Canva", "Design"), ("Figma", "Design"), ("Miro", "Collaboration"),
                ("Intercom", "Support"), ("Zendesk", "Support"), ("Freshdesk", "Support"),
                ("Typeform", "Forms"), ("SurveyMonkey", "Forms"), ("Mailchimp", "Marketing"),
                ("Klaviyo", "Marketing"), ("Semrush", "SEO"), ("Ahrefs", "SEO"), ("Shopify", "Ecommerce"),
                ("WooCommerce", "Ecommerce"), ("Stripe", "Payments"), ("QuickBooks", "Accounting"),
                ("Xero", "Accounting"), ("Wave", "Accounting"), ("Calendly", "Scheduling"),
                ("Trello", "Project Management"), ("Todoist", "Productivity"), ("Evernote", "Productivity"),
                ("Obsidian", "Productivity"), ("Zapier", "Automation"), ("Make", "Automation"),
                ("Bubble", "No Code"), ("Webflow", "No Code"), ("WordPress", "CMS"),
            ]
            for name, category in fallback:
                key = name.lower()
                if key in seen_apps:
                    continue
                seen_apps.add(key)
                app_candidates.append({"product_name": name, "category": category, "enabled": "1"})

        def ensure_meta_seed(path: Path, source: str) -> None:
            existing = self._load_csv(path)
            merged: List[Dict[str, str]] = []
            seen_urls = set()

            for row in existing:
                name = clean(row.get("product_name", ""))
                url = clean(row.get("url", ""))
                if not url:
                    continue
                key = url.lower()
                if key in seen_urls:
                    continue
                seen_urls.add(key)
                merged.append(
                    {
                        "product_name": name or "Unknown Product",
                        "url": url,
                        "category": clean(row.get("category", "Unknown")) or "Unknown",
                        "enabled": "1" if parse_bool(row.get("enabled", "1"), True) else "0",
                    }
                )

            if len(merged) >= self.meta_seed_min_rows:
                return

            for app in app_candidates:
                name = app["product_name"]
                if source == "g2":
                    url = f"https://www.g2.com/search?query={quote_plus(name)}"
                else:
                    url = f"https://www.capterra.com/search/?query={quote_plus(name)}"
                key = url.lower()
                if key in seen_urls:
                    continue
                seen_urls.add(key)
                merged.append(
                    {
                        "product_name": name,
                        "url": url,
                        "category": app["category"],
                        "enabled": app["enabled"],
                    }
                )
                if len(merged) >= self.meta_seed_min_rows:
                    break

            with path.open("w", newline="", encoding="utf-8") as f:
                w = csv.DictWriter(f, fieldnames=["product_name", "url", "category", "enabled"])
                w.writeheader()
                w.writerows(merged)

        ensure_meta_seed(self.seeds_dir / "g2_products.csv", "g2")
        ensure_meta_seed(self.seeds_dir / "capterra_products.csv", "capterra")

        u_path = self.seeds_dir / "upwork_queries.csv"
        if not u_path.exists():
            with u_path.open("w", newline="", encoding="utf-8") as f:
                w = csv.DictWriter(f, fieldnames=["query", "enabled", "category"])
                w.writeheader()
                for row in UPWORK_STARTER_QUERIES:
                    w.writerow({"query": row["query"], "enabled": "1", "category": row["category"]})

        hn_path = self.seeds_dir / "hackernews_queries.csv"
        if not hn_path.exists():
            with hn_path.open("w", newline="", encoding="utf-8") as f:
                w = csv.DictWriter(f, fieldnames=["query", "enabled", "category"])
                w.writeheader()
                for row in HACKERNEWS_STARTER_QUERIES:
                    w.writerow({"query": row["query"], "enabled": "1", "category": row["category"]})

        ph_path = self.seeds_dir / "producthunt_queries.csv"
        if not ph_path.exists():
            with ph_path.open("w", newline="", encoding="utf-8") as f:
                w = csv.DictWriter(f, fieldnames=["query", "enabled", "category"])
                w.writeheader()
                for row in PRODUCTHUNT_STARTER_QUERIES:
                    w.writerow({"query": row["query"], "enabled": "1", "category": row["category"]})

    def _load_csv(self, path: Path) -> List[Dict[str, str]]:
        if not path.exists():
            return []
        with path.open("r", newline="", encoding="utf-8-sig") as f:
            reader = csv.DictReader(f)
            out: List[Dict[str, str]] = []
            for row in reader:
                clean_row: Dict[str, str] = {}
                for key, value in dict(row).items():
                    norm_key = clean(str(key or "")).lstrip("\ufeff").strip().strip('"').strip("'").lower()
                    if not norm_key:
                        continue
                    clean_row[norm_key] = clean(value)
                if clean_row:
                    out.append(clean_row)
            return out

    def _load_all_seeds(self) -> Dict[str, List[Dict[str, Any]]]:
        apps = []
        for r in self._load_csv(self.seeds_dir / "apps_seed_300.csv"):
            apps.append({
                "app_name": clean(r.get("app_name", "")),
                "play_id": clean(r.get("play_id", "")),
                "apple_id": clean(r.get("apple_id", "")),
                "category": clean(r.get("category", "Unknown")) or "Unknown",
                "enabled": parse_bool(r.get("enabled", "0")),
            })

        reddit = []
        for r in self._load_csv(self.seeds_dir / "reddit_seed.csv"):
            sub = clean(r.get("subreddit", "")).lower().strip("/")
            if sub.startswith("r/"):
                sub = sub[2:]
            reddit.append({"subreddit": sub, "enabled": parse_bool(r.get("enabled", "1")), "keywords": clean(r.get("keywords", "|".join(PAIN_KEYWORDS)))})

        g2 = [
            {
                "product_name": clean(r.get("product_name", "")),
                "url": clean(r.get("url", "")),
                "category": clean(r.get("category", "Unknown")) or "Unknown",
                "enabled": (self.g2_enabled and parse_bool(r.get("enabled", "0"))),
            }
            for r in self._load_csv(self.seeds_dir / "g2_products.csv")
        ]
        cap = [
            {
                "product_name": clean(r.get("product_name", "")),
                "url": clean(r.get("url", "")),
                "category": clean(r.get("category", "Unknown")) or "Unknown",
                "enabled": (self.capterra_enabled and parse_bool(r.get("enabled", "0"))),
            }
            for r in self._load_csv(self.seeds_dir / "capterra_products.csv")
        ]
        upwork = []
        for r in self._load_csv(self.seeds_dir / "upwork_queries.csv"):
            query = clean(r.get("query", ""))
            if not query:
                continue
            upwork.append(
                {
                    "query": query,
                    "enabled": parse_bool(r.get("enabled", "1"), True),
                    "category": clean(r.get("category", "Operations")) or "Operations",
                }
            )
        hn = []
        for r in self._load_csv(self.seeds_dir / "hackernews_queries.csv"):
            query = clean(r.get("query", ""))
            if not query:
                continue
            hn.append(
                {
                    "query": query,
                    "enabled": (self.hn_enabled and parse_bool(r.get("enabled", "1"), True)),
                    "category": clean(r.get("category", "Startups")) or "Startups",
                }
            )

        ph = []
        for r in self._load_csv(self.seeds_dir / "producthunt_queries.csv"):
            query = clean(r.get("query", ""))
            if not query:
                continue
            ph.append(
                {
                    "query": query,
                    "enabled": (self.producthunt_enabled and parse_bool(r.get("enabled", "1"), True)),
                    "category": clean(r.get("category", "Productivity")) or "Productivity",
                }
            )
        return {"apps": apps, "reddit": reddit, "hackernews": hn, "producthunt": ph, "g2": g2, "capterra": cap, "upwork": upwork}

    def _upsert_seed(self, seed_type: str, seed_key: str, enabled: bool) -> None:
        with self.lock:
            ex = self.conn.execute("SELECT 1 FROM seed_progress WHERE seed_type = ? AND seed_key = ? LIMIT 1", [seed_type, seed_key]).fetchone()
            if ex:
                self.conn.execute("UPDATE seed_progress SET enabled = ?, updated_at = ? WHERE seed_type = ? AND seed_key = ?", [enabled, now_utc(), seed_type, seed_key])
            else:
                self.conn.execute(
                    "INSERT INTO seed_progress(seed_type, seed_key, enabled, status, total_seen, total_inserted, last_run_at, last_error, exhausted, updated_at) VALUES (?, ?, ?, 'pending', 0, 0, NULL, NULL, FALSE, ?)",
                    [seed_type, seed_key, enabled, now_utc()],
                )

    def _sync_seed_progress(self, seeds: Dict[str, List[Dict[str, Any]]]) -> None:
        for a in seeds["apps"]:
            if a["play_id"]:
                self._upsert_seed("apps", f"play:{a['play_id']}", a["enabled"])
            if a["apple_id"]:
                self._upsert_seed("apps", f"apple:{a['apple_id']}", a["enabled"])
        for r in seeds["reddit"]:
            if r["subreddit"]:
                self._upsert_seed("reddit", r["subreddit"], r["enabled"])
        for r in seeds["hackernews"]:
            if r["query"]:
                self._upsert_seed("hackernews", r["query"], r["enabled"])
        for r in seeds["producthunt"]:
            if r["query"]:
                self._upsert_seed("producthunt", r["query"], r["enabled"])
        for r in seeds["g2"]:
            if r["url"]:
                self._upsert_seed("g2", r["url"], r["enabled"])
        for r in seeds["capterra"]:
            if r["url"]:
                self._upsert_seed("capterra", r["url"], r["enabled"])
        for r in seeds["upwork"]:
            if r["query"]:
                self._upsert_seed("upwork", r["query"], r["enabled"])

    def _mark_seed(self, seed_type: str, seed_key: str, status: str, seen: int, inserted: int, exhausted: bool, err: Optional[str]) -> None:
        with self.lock:
            self.conn.execute(
                """
                UPDATE seed_progress
                SET status = ?, total_seen = COALESCE(total_seen, 0) + ?, total_inserted = COALESCE(total_inserted, 0) + ?,
                    last_run_at = ?, last_error = ?, exhausted = ?, updated_at = ?
                WHERE seed_type = ? AND seed_key = ?
                """,
                [status, seen, inserted, now_utc(), err, exhausted, now_utc(), seed_type, seed_key],
            )

    def _cursor_get(self, key: str) -> Optional[datetime]:
        with self.lock:
            row = self.conn.execute("SELECT cursor_value FROM source_cursors WHERE cursor_key = ? LIMIT 1", [key]).fetchone()
        return parse_dt(row[0]) if row else None

    def _cursor_set(self, key: str, value: Optional[datetime]) -> None:
        if not value:
            return
        with self.lock:
            ex = self.conn.execute("SELECT 1 FROM source_cursors WHERE cursor_key = ? LIMIT 1", [key]).fetchone()
            if ex:
                self.conn.execute("UPDATE source_cursors SET cursor_value = ?, updated_at = ? WHERE cursor_key = ?", [iso_utc(value), now_utc(), key])
            else:
                self.conn.execute("INSERT INTO source_cursors(cursor_key, cursor_value, updated_at) VALUES (?, ?, ?)", [key, iso_utc(value), now_utc()])

    def _cursor_get_raw(self, key: str) -> Optional[str]:
        with self.lock:
            row = self.conn.execute("SELECT cursor_value FROM source_cursors WHERE cursor_key = ? LIMIT 1", [key]).fetchone()
        if not row:
            return None
        return clean(row[0])

    def _cursor_set_raw(self, key: str, value: str) -> None:
        val = clean(value)
        with self.lock:
            ex = self.conn.execute("SELECT 1 FROM source_cursors WHERE cursor_key = ? LIMIT 1", [key]).fetchone()
            if ex:
                self.conn.execute("UPDATE source_cursors SET cursor_value = ?, updated_at = ? WHERE cursor_key = ?", [val, now_utc(), key])
            else:
                self.conn.execute("INSERT INTO source_cursors(cursor_key, cursor_value, updated_at) VALUES (?, ?, ?)", [key, val, now_utc()])

    def _cursor_get_int(self, key: str, default: int = 0) -> int:
        raw = self._cursor_get_raw(key)
        if raw is None:
            return default
        try:
            return max(0, int(raw))
        except ValueError:
            return default

    def _cursor_set_int(self, key: str, value: int) -> None:
        self._cursor_set_raw(key, str(max(0, int(value))))

    def _seed_slice(self, seed_type: str, rows: List[Dict[str, Any]], limit: int) -> List[Dict[str, Any]]:
        return self._seed_slice_with_cursor_key(rows, limit, f"seedscan:{seed_type}")

    def _seed_slice_with_cursor_key(self, rows: List[Dict[str, Any]], limit: int, cursor_key: str) -> List[Dict[str, Any]]:
        if not rows:
            return []
        if limit <= 0 or limit >= len(rows):
            return rows

        start = self._cursor_get_int(cursor_key, 0)
        if start >= len(rows):
            start = 0
        end = min(len(rows), start + limit)
        selected = rows[start:end]
        next_idx = 0 if end >= len(rows) else end
        self._cursor_set_int(cursor_key, next_idx)
        return selected

    def _select_apps(self, rows: List[Dict[str, Any]], limit: int, apple_target: int) -> Tuple[List[Dict[str, Any]], Dict[str, int]]:
        if not rows or limit <= 0:
            return [], {"with_apple_id": 0, "with_play_id": 0, "apple_only": 0, "play_only": 0, "both": 0}

        if not self.appstore_focus_enabled:
            selected = self._seed_slice("apps", rows, limit)
        else:
            apple_rows = [r for r in rows if clean(r.get("apple_id", ""))]
            non_apple_rows = [r for r in rows if not clean(r.get("apple_id", ""))]
            selected: List[Dict[str, Any]] = []
            seen: set[str] = set()

            apple_take = min(max(0, apple_target), limit)
            if apple_rows and apple_take > 0:
                for row in self._seed_slice_with_cursor_key(apple_rows, apple_take, "seedscan:apps:apple"):
                    key = clean(row.get("app_name", "")).lower() + "|" + clean(row.get("play_id", "")) + "|" + clean(row.get("apple_id", ""))
                    if key in seen:
                        continue
                    seen.add(key)
                    selected.append(row)

            remaining = max(0, limit - len(selected))
            if non_apple_rows and remaining > 0:
                for row in self._seed_slice_with_cursor_key(non_apple_rows, remaining, "seedscan:apps:nonapple"):
                    key = clean(row.get("app_name", "")).lower() + "|" + clean(row.get("play_id", "")) + "|" + clean(row.get("apple_id", ""))
                    if key in seen:
                        continue
                    seen.add(key)
                    selected.append(row)
                    if len(selected) >= limit:
                        break

            if len(selected) < limit:
                for row in self._seed_slice_with_cursor_key(rows, limit - len(selected), "seedscan:apps:all"):
                    key = clean(row.get("app_name", "")).lower() + "|" + clean(row.get("play_id", "")) + "|" + clean(row.get("apple_id", ""))
                    if key in seen:
                        continue
                    seen.add(key)
                    selected.append(row)
                    if len(selected) >= limit:
                        break

        with_apple = sum(1 for r in selected if clean(r.get("apple_id", "")))
        with_play = sum(1 for r in selected if clean(r.get("play_id", "")))
        both = sum(1 for r in selected if clean(r.get("apple_id", "")) and clean(r.get("play_id", "")))
        breakdown = {
            "with_apple_id": with_apple,
            "with_play_id": with_play,
            "apple_only": max(0, with_apple - both),
            "play_only": max(0, with_play - both),
            "both": both,
        }
        return selected[:limit], breakdown

    def _cycle_seed_limits(self, fast: bool) -> Dict[str, int]:
        if fast:
            return {
                "apps": max(1, self.fast_apps_per_cycle),
                "apps_apple_target": min(max(1, self.fast_apps_per_cycle), max(1, self.fast_apple_per_cycle)),
                "reddit": max(1, self.fast_reddit_per_cycle),
                "hackernews": max(1, self.fast_hn_per_cycle),
                "producthunt": max(1, self.fast_producthunt_per_cycle),
                "g2": max(1, self.fast_g2_per_cycle),
                "capterra": max(1, self.fast_capterra_per_cycle),
                "upwork": max(1, self.fast_upwork_per_cycle),
            }
        return {
            "apps": max(1, self.inc_apps_per_cycle),
            "apps_apple_target": min(max(1, self.inc_apps_per_cycle), max(1, self.inc_apple_per_cycle)),
            "reddit": max(1, self.inc_reddit_per_cycle),
            "hackernews": max(1, self.inc_hn_per_cycle),
            "producthunt": max(1, self.inc_producthunt_per_cycle),
            "g2": max(1, self.inc_g2_per_cycle),
            "capterra": max(1, self.inc_capterra_per_cycle),
            "upwork": max(1, self.inc_upwork_per_cycle),
        }

    def _write_runtime_status(self, payload: Dict[str, Any]) -> None:
        path = self.logs_dir / "runtime_metrics.json"
        data = {"updated_at": iso_utc(), **payload}
        try:
            path.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
        except Exception:
            pass
    def _insert(self, rec: Dict[str, Any]) -> bool:
        src = clean(rec.get("source", ""))
        sid = clean(rec.get("source_item_id", ""))
        eid = clean(rec.get("entity_id", ""))
        reviewer = clean(rec.get("reviewer_name", ""))
        text = clean(rec.get("comment_text", ""))
        posted = clean(rec.get("posted_at", ""))
        fp = make_fp(src, sid, eid, reviewer, text, posted)

        with self.lock:
            row = self.conn.execute(
                """
                INSERT INTO reviews(
                    source, source_item_id, entity_id, entity_name, category,
                    reviewer_name, rating, comment_text, posted_at, url,
                    country, language, fingerprint, scraped_at, raw_json, enriched_at
                )
                SELECT ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, NULL
                WHERE NOT EXISTS (
                    SELECT 1 FROM reviews WHERE fingerprint = ?
                )
                RETURNING fingerprint
                """,
                [
                    src,
                    sid,
                    eid,
                    clean(rec.get("entity_name", "")),
                    clean(rec.get("category", "Unknown")) or "Unknown",
                    reviewer,
                    rec.get("rating"),
                    text,
                    posted,
                    clean(rec.get("url", "")),
                    clean(rec.get("country", "")),
                    clean(rec.get("language", "en")) or "en",
                    fp,
                    now_utc(),
                    json.dumps(rec.get("raw_json", {}), ensure_ascii=False, default=str),
                    fp,
                ],
            ).fetchone()
        return row is not None

    def _mobile_ok(self, rating: Optional[float], text: str) -> bool:
        return rating is not None and float(rating) <= 2 and has_pain(text)

    def _reddit_ok(self, text: str) -> bool:
        return has_pain(text) and sentiment_proxy(text) <= -1

    def _meta_ok(self, snippet: str, rating: Optional[float]) -> bool:
        return (rating is not None and rating <= 2) or has_pain(snippet)

    def _meta_ok_for_source(self, snippet: str, rating: Optional[float], source: str) -> bool:
        if self._meta_ok(snippet, rating):
            return True
        if not self.meta_relaxed_marketplace:
            return False
        src = clean(source).lower()
        if src not in {"g2", "capterra"}:
            return False
        s = snippet.lower()
        marketplace_terms = (
            "alternative", "alternatives", "compare", "comparison", "review", "reviews",
            "pricing", "price", "pros", "cons", "buyer", "buyers", "software", "saas",
            "market", "vendor", "best", "top",
        )
        if any(t in s for t in marketplace_terms):
            return True
        if rating is not None and float(rating) <= 3.5:
            return True
        return len(snippet) >= 120

    def _upwork_api_configured(self) -> bool:
        return bool(self.upwork_api_enabled and self.upwork_api_url and self.upwork_api_token)

    def _upwork_api_cache_get(self, key: str) -> Optional[List[Dict[str, Any]]]:
        if self.upwork_api_cache_ttl <= 0:
            return None
        now = time.time()
        with self.upwork_api_cache_lock:
            row = self.upwork_api_cache.get(key)
            if not row:
                return None
            expires_at, items = row
            if now >= expires_at:
                self.upwork_api_cache.pop(key, None)
                return None
            return items

    def _upwork_api_cache_set(self, key: str, items: List[Dict[str, Any]]) -> None:
        if self.upwork_api_cache_ttl <= 0:
            return
        expires_at = time.time() + self.upwork_api_cache_ttl
        with self.upwork_api_cache_lock:
            self.upwork_api_cache[key] = (expires_at, items)
            # Keep cache bounded in long-running processes.
            if len(self.upwork_api_cache) > 1024:
                oldest = sorted(self.upwork_api_cache.items(), key=lambda x: x[1][0])[:128]
                for cache_key, _ in oldest:
                    self.upwork_api_cache.pop(cache_key, None)

    def _upwork_api_headers(self) -> Dict[str, str]:
        token = clean(self.upwork_api_token)
        if token and not token.lower().startswith("bearer "):
            token = f"Bearer {token}"
        return {
            "Accept": "application/json",
            "Content-Type": "application/json",
            "Authorization": token,
        }

    def _upwork_rate_limited_request(self, method: str, url: str, **kwargs: Any) -> requests.Response:
        if not self.upwork_api_limiter.acquire(timeout_seconds=3.0):
            raise RuntimeError("Upwork API rate budget reached (per-second/per-minute/per-day)")
        resp = self.session.request(method, url, timeout=self.upwork_api_timeout, **kwargs)
        if resp.status_code == 429:
            retry_after = clean(resp.headers.get("Retry-After", "1"))
            try:
                sleep_s = min(30, max(1, int(float(retry_after))))
            except Exception:
                sleep_s = 1
            time.sleep(sleep_s)
            if not self.upwork_api_limiter.acquire(timeout_seconds=3.0):
                raise RuntimeError("Upwork API rate budget reached after retry")
            resp = self.session.request(method, url, timeout=self.upwork_api_timeout, **kwargs)
        return resp

    def _upwork_walk_payload_dicts(self, payload: Any) -> List[Dict[str, Any]]:
        out: List[Dict[str, Any]] = []
        stack: List[Any] = [payload]
        while stack:
            cur = stack.pop()
            if isinstance(cur, dict):
                out.append(cur)
                for val in cur.values():
                    if isinstance(val, (dict, list)):
                        stack.append(val)
            elif isinstance(cur, list):
                for val in cur:
                    if isinstance(val, (dict, list)):
                        stack.append(val)
        return out

    def _normalize_upwork_api_item(self, node: Dict[str, Any], query: str, idx: int) -> Optional[Dict[str, Any]]:
        title = clean(node.get("title") or node.get("jobTitle") or node.get("name") or node.get("headline"))
        desc = strip_html(
            node.get("description")
            or node.get("summary")
            or node.get("snippet")
            or node.get("details")
            or node.get("workDescription")
        )
        link = clean(
            node.get("jobPostingUrl")
            or node.get("jobUrl")
            or node.get("canonicalUrl")
            or node.get("url")
            or node.get("link")
        )
        guid = clean(
            node.get("id")
            or node.get("jobId")
            or node.get("jobPostingId")
            or node.get("uid")
            or node.get("uuid")
            or ""
        )
        pub_value = node.get("publishedOn") or node.get("publishedAt") or node.get("createdAt") or node.get("createTime")
        pub_date = ""
        if isinstance(pub_value, (int, float)):
            ts = float(pub_value)
            if ts > 1e11:
                ts /= 1000.0
            dt = parse_dt(ts)
            pub_date = iso_utc(dt) if dt else ""
        else:
            pub_date = clean(pub_value)

        budget_parts: List[str] = []
        hb_min = clean(node.get("hourlyBudgetMin"))
        hb_max = clean(node.get("hourlyBudgetMax"))
        amount = clean(node.get("amount"))
        currency = clean(node.get("amountCurrencyCode") or node.get("currencyCode"))
        if hb_min or hb_max:
            budget_parts.append(f"Hourly: {hb_min or '?'}-{hb_max or '?'}")
        if amount:
            budget_parts.append(f"Budget: {amount} {currency}".strip())
        if budget_parts and not desc:
            desc = " ".join(budget_parts)

        if not title and not desc:
            return None
        if not guid:
            guid = hashlib.sha1(f"{query}|{title}|{link}|{idx}".encode("utf-8")).hexdigest()

        return {
            "title": title or f"Upwork job: {query}",
            "description": desc[:4000],
            "link": link,
            "guid": guid,
            "pubDate": pub_date,
            "raw": {"provider": "upwork_first_party_api", "job": node},
        }

    def _upwork_collect_items_from_payload(self, payload: Any, query: str, limit: int) -> List[Dict[str, Any]]:
        out: List[Dict[str, Any]] = []
        seen: set = set()
        candidates = self._upwork_walk_payload_dicts(payload)
        key_hints = {
            "title", "jobTitle", "description", "summary", "snippet", "jobPostingUrl", "jobUrl",
            "id", "jobId", "jobPostingId", "hourlyBudgetMin", "hourlyBudgetMax", "amount",
        }
        for idx, node in enumerate(candidates):
            if not any(k in node for k in key_hints):
                continue
            item = self._normalize_upwork_api_item(node, query, idx)
            if not item:
                continue
            dedupe_key = clean(item.get("guid", "")) or clean(item.get("link", "")) or clean(item.get("title", ""))
            if not dedupe_key or dedupe_key in seen:
                continue
            seen.add(dedupe_key)
            out.append(item)
            if len(out) >= max(1, limit):
                break
        return out

    def _upwork_first_party_candidates(self, query: str, limit: int) -> List[Dict[str, Any]]:
        if not self._upwork_api_configured():
            return []
        cache_key = f"{self.upwork_api_mode}:{query.lower()}:{max(1, limit)}"
        cached = self._upwork_api_cache_get(cache_key)
        if cached is not None:
            return cached

        headers = self._upwork_api_headers()
        mode = self.upwork_api_mode
        max_limit = max(1, int(limit))
        if mode == "graphql":
            payload = {
                "query": self.upwork_api_query_template,
                "variables": {"query": query, "limit": max_limit},
            }
            resp = self._upwork_rate_limited_request("POST", self.upwork_api_url, headers=headers, json=payload)
        else:
            resp = self._upwork_rate_limited_request(
                "GET",
                self.upwork_api_url,
                headers=headers,
                params={"q": query, "limit": max_limit},
            )

        if resp.status_code != 200:
            raise RuntimeError(f"HTTP {resp.status_code} from Upwork API")
        payload = resp.json()
        if isinstance(payload, dict):
            errors = payload.get("errors")
            if errors:
                err_text = clean(errors[0]) if isinstance(errors, list) and errors else clean(errors)
                raise RuntimeError(f"Upwork API error: {err_text}")

        items = self._upwork_collect_items_from_payload(payload, query, max_limit)
        self._upwork_api_cache_set(cache_key, items)
        return items

    def _upwork_ok(self, text: str) -> bool:
        s = text.lower()
        willingness_terms = ("budget", "hourly", "fixed-price", "pay", "payment", "cost", "usd", "$")
        inefficiency_terms = ("manual", "spreadsheet", "copy paste", "repetitive", "entry", "automation", "integrat", "need")
        return has_pain(s) or (any(t in s for t in willingness_terms) and any(t in s for t in inefficiency_terms))

    def _extract_upwork_budget(self, text: str) -> Optional[str]:
        patterns = [
            r"Budget\s*[:\-]\s*\$?\s*([0-9][0-9,\.]*)",
            r"Hourly Range\s*[:\-]\s*\$?\s*([0-9][0-9,\.]*\s*-\s*\$?\s*[0-9][0-9,\.]*)",
        ]
        for pat in patterns:
            m = re.search(pat, text, flags=re.IGNORECASE)
            if m:
                return clean(m.group(1))
        return None

    def _upwork_jina_candidates(self, query: str, limit: int) -> List[Dict[str, Any]]:
        search_url = f"https://www.upwork.com/nx/search/jobs/?q={quote_plus(query)}&sort=recency"
        proxy_url = self._jina_proxy_url(search_url)
        try:
            resp = self.session.get(proxy_url, timeout=30)
            if resp.status_code != 200:
                return []
            body = clean(resp.text)
        except Exception:
            return []

        low = body.lower()
        if "just a moment" in low or "cloudflare" in low or "captcha" in low:
            return []

        chunks = [clean(c) for c in re.split(r"\n{2,}", body) if clean(c)]
        out: List[Dict[str, Any]] = []
        for idx, chunk in enumerate(chunks):
            c_low = chunk.lower()
            if len(chunk) < 120:
                continue
            if ("$" not in chunk) and ("hourly" not in c_low) and ("fixed-price" not in c_low):
                continue
            if not self._upwork_ok(chunk):
                continue
            out.append(
                {
                    "title": f"Upwork market signal: {query}",
                    "description": chunk[:2000],
                    "link": search_url,
                    "guid": hashlib.sha1(f"upwork-jina:{query}:{idx}:{chunk[:220]}".encode("utf-8")).hexdigest(),
                    "pubDate": iso_utc(),
                    "raw": {"provider": "jina_proxy", "chunk": chunk, "query": query},
                }
            )
            if len(out) >= max(1, limit):
                break
        return out

    def _upwork_query_signal_item(self, query: str, category: str, feed_url: str) -> Dict[str, Any]:
        slug = re.sub(r"[^a-z0-9]+", "-", query.lower()).strip("-")[:80]
        desc = (
            f"Upwork demand signal query: {query}. "
            f"This query indicates ongoing buyer intent around {category or 'operations'} workflows "
            f"and manual-process pain that teams want to automate."
        )
        return {
            "title": f"Upwork demand signal: {query}",
            "description": desc,
            "link": feed_url,
            "guid": f"upwork-query-signal:{slug}",
            "pubDate": iso_utc(),
            "raw": {"provider": "query_signal", "query": query, "category": category},
        }

    def _hn_ok(self, text: str, query: str) -> bool:
        s = clean(text)
        if len(s) < 80:
            return False
        if query_match_score(s, query) <= 0:
            return False
        s_low = s.lower()
        if not has_pain(s) and not any(t in s_low for t in ("problem", "issue", "struggl", "friction", "manual", "slow", "pain")):
            return False
        return sentiment_proxy(s) <= 2

    def _producthunt_ok(self, text: str, query: str) -> bool:
        s = clean(text)
        if len(s) < 80:
            return False
        if query_match_score(s, query) <= 0:
            return False
        if not has_pain(s):
            return False
        if sentiment_proxy(s) > 0:
            return False
        return True

    def _extract_hn_stories(self, html_text: str) -> List[Dict[str, str]]:
        out: List[Dict[str, str]] = []
        if not html_text:
            return out
        for m in re.finditer(
            r"(?P<tag><tr[^>]*class=['\"][^'\"]*athing[^'\"]*['\"][^>]*>)(?P<body>.*?)</tr>",
            html_text,
            flags=re.IGNORECASE | re.DOTALL,
        ):
            tag = m.group("tag")
            id_m = re.search(r"id=['\"](?P<id>\d+)['\"]", tag, flags=re.IGNORECASE)
            story_id = clean(id_m.group("id")) if id_m else ""
            body = m.group("body")
            t = re.search(
                r"<span[^>]+class=['\"]titleline['\"][^>]*>\s*<a[^>]+href=['\"](?P<link>[^'\"]+)['\"][^>]*>(?P<title>.*?)</a>",
                body,
                flags=re.IGNORECASE | re.DOTALL,
            )
            if not t:
                continue
            link = clean(t.group("link"))
            if link.startswith("item?id="):
                link = f"https://news.ycombinator.com/{link}"
            title = strip_html(t.group("title"))
            if not story_id or not title:
                continue
            out.append(
                {
                    "id": story_id,
                    "title": title,
                    "link": link,
                    "item_url": f"https://news.ycombinator.com/item?id={story_id}",
                }
            )
        return out

    def _extract_hn_comments(self, html_text: str) -> List[str]:
        out: List[str] = []
        if not html_text:
            return out
        for m in re.finditer(r"<span[^>]+class=['\"]commtext[^'\"]*['\"][^>]*>(?P<c>.*?)</span>", html_text, flags=re.IGNORECASE | re.DOTALL):
            txt = strip_html(m.group("c"))
            if txt:
                out.append(txt)
        return out

    def _extract_hn_newcomments(self, html_text: str) -> List[Dict[str, str]]:
        out: List[Dict[str, str]] = []
        if not html_text:
            return out
        pattern = re.compile(
            r"<tr[^>]+class=['\"]athing['\"][^>]+id=['\"](?P<cid>\d+)['\"][^>]*>.*?"
            r"<span[^>]+class=['\"]onstory['\"][^>]*>.*?<a[^>]+href=['\"](?P<link>[^'\"]+)['\"][^>]*>(?P<title>.*?)</a>.*?"
            r"<div[^>]+class=['\"]comment['\"][^>]*>\s*<div[^>]+class=['\"]commtext[^'\"]*['\"][^>]*>(?P<comment>.*?)</div>",
            flags=re.IGNORECASE | re.DOTALL,
        )
        for m in pattern.finditer(html_text):
            cid = clean(m.group("cid"))
            link = clean(m.group("link"))
            if link and not link.startswith("http"):
                link = f"https://news.ycombinator.com/{link.lstrip('/')}"
            title = strip_html(m.group("title"))
            comment = strip_html(m.group("comment"))
            if not cid or not comment:
                continue
            out.append({"comment_id": cid, "story_link": link, "story_title": title, "comment_text": comment})
        return out

    def _extract_producthunt_chunks(self, body: str) -> List[str]:
        raw = body or ""
        # Jina already returns markdown-like text; direct HTML needs tag stripping first.
        if "<html" in raw.lower() or "<body" in raw.lower():
            raw = strip_html(raw)
        chunks = [clean(c) for c in re.split(r"\n{2,}", raw) if clean(c)]
        if len(chunks) <= 1:
            chunks = [clean(c) for c in re.split(r"(?<=[\.\!\?])\s+", raw) if clean(c)]
        return chunks

    def _fetch_producthunt_feed_entries(self) -> List[Dict[str, Any]]:
        feed_url = "https://www.producthunt.com/feed"
        try:
            resp = self.session.get(feed_url, timeout=20)
            if resp.status_code != 200 or not resp.text:
                return []
            root = ET.fromstring(resp.text.encode("utf-8"))
        except Exception:
            return []

        ns = {"a": "http://www.w3.org/2005/Atom"}
        out: List[Dict[str, Any]] = []
        for entry in root.findall(".//a:entry", ns):
            title = clean(entry.findtext("a:title", default="", namespaces=ns))
            summary = strip_html(entry.findtext("a:summary", default="", namespaces=ns) or entry.findtext("a:content", default="", namespaces=ns))
            eid = clean(entry.findtext("a:id", default="", namespaces=ns))
            updated = clean(entry.findtext("a:updated", default="", namespaces=ns))
            link_el = entry.find("a:link", ns)
            link = clean(link_el.get("href", "")) if link_el is not None else ""
            if not title and not summary:
                continue
            out.append(
                {
                    "title": title,
                    "summary": summary,
                    "id": eid,
                    "updated": updated,
                    "link": link,
                    "provider": "atom_feed",
                }
            )
        return out

    def scrape_hackernews(self, row: Dict[str, Any], fast: bool) -> Dict[str, Any]:
        query = clean(row.get("query", ""))
        if not query:
            return {"source": "hacker_news", "seen": 0, "inserted": 0, "errors": 0}

        self._mark_seed("hackernews", query, "running", 0, 0, False, None)
        seen, ins, err = 0, 0, None
        exhausted = True
        limit = max(1, self.hn_fast if fast else self.hn_inc)

        with self.hn_sem:
            search_url = f"https://hn.algolia.com/?q={quote_plus(query)}&type=comment&sort=byDate"
            proxy_url = self._jina_proxy_url(search_url)
            body, mode, fetch_err = self._fetch_page_text(proxy_url, timeout=45, prefer_jina=False)
            if not body:
                self._mark_seed("hackernews", query, "skipped", seen, ins, True, fetch_err or "hn fetch failed")
                return {"source": "hacker_news", "seen": seen, "inserted": ins, "errors": 0}

            chunks = [clean(c) for c in re.split(r"\n{2,}", body) if clean(c)]
            if not chunks:
                self._mark_seed("hackernews", query, "skipped", seen, ins, True, "no search chunks parsed")
                return {"source": "hacker_news", "seen": seen, "inserted": ins, "errors": 0}

            for idx, chunk in enumerate(chunks):
                combined = chunk
                seen += 1
                if not self._hn_ok(combined, query):
                    continue
                link_m = re.search(r"https://news\.ycombinator\.com/item\?id=\d+", combined)
                url = clean(link_m.group(0)) if link_m else search_url
                sid = hashlib.sha1(f"hn:{query}:{idx}:{combined[:220]}".encode("utf-8")).hexdigest()
                rec = {
                    "source": "hacker_news",
                    "source_item_id": sid,
                    "entity_id": query,
                    "entity_name": f"Hacker News - {query}",
                    "category": row.get("category", "Startups"),
                    "reviewer_name": "",
                    "rating": None,
                    "comment_text": combined[:2500],
                    "posted_at": "",
                    "url": url,
                    "country": "",
                    "language": "en",
                    "raw_json": {
                        "provider": "hn_algolia_html_via_jina",
                        "query": query,
                        "mode": mode or "direct",
                        "chunk": combined[:3000],
                    },
                }
                if self._insert(rec):
                    ins += 1
                    exhausted = False
                    if ins >= limit:
                        break

                if ins >= limit:
                    break

        final_status = "error" if err else "done"
        if ins == 0 and not err:
            final_status = "skipped"
        self._mark_seed("hackernews", query, final_status, seen, ins, exhausted, err)
        return {"source": "hacker_news", "seen": seen, "inserted": ins, "errors": 1 if final_status == "error" else 0}

    def scrape_producthunt(self, row: Dict[str, Any], fast: bool) -> Dict[str, Any]:
        query = clean(row.get("query", ""))
        if not query:
            return {"source": "product_hunt", "seen": 0, "inserted": 0, "errors": 0}

        self._mark_seed("producthunt", query, "running", 0, 0, False, None)
        seen, ins, err = 0, 0, None
        exhausted = True
        limit = max(1, self.producthunt_fast if fast else self.producthunt_inc)

        with self.producthunt_sem:
            search_url = f"https://www.producthunt.com/search/posts?q={quote_plus(query)}"
            body, mode, fetch_err = self._fetch_page_text(search_url, timeout=20, prefer_jina=True)
            blocked = False
            if body:
                low = body.lower()
                blocked = ("just a moment" in low) or ("cloudflare" in low) or ("captcha" in low) or ("forbidden" in low and "403" in low)
            chunks: List[str] = []
            if body and not blocked:
                chunks = self._extract_producthunt_chunks(body)
                for idx, chunk in enumerate(chunks):
                    seen += 1
                    if not self._producthunt_ok(chunk, query):
                        continue
                    sid = hashlib.sha1(f"producthunt-html:{query}:{idx}:{chunk[:220]}".encode("utf-8")).hexdigest()
                    rec = {
                        "source": "product_hunt",
                        "source_item_id": sid,
                        "entity_id": query,
                        "entity_name": f"Product Hunt - {query}",
                        "category": row.get("category", "Productivity"),
                        "reviewer_name": "",
                        "rating": None,
                        "comment_text": chunk[:2500],
                        "posted_at": "",
                        "url": search_url,
                        "country": "",
                        "language": "en",
                        "raw_json": {
                            "provider": f"producthunt_html_{mode or 'direct'}",
                            "query": query,
                            "chunk": chunk[:3000],
                        },
                    }
                    if self._insert(rec):
                        ins += 1
                        exhausted = False
                        if ins >= limit:
                            break

            if ins < limit:
                entries = self._fetch_producthunt_feed_entries()
                for entry in entries:
                    combined = clean(f"{entry.get('title','')}. {entry.get('summary','')}")
                    seen += 1
                    if not self._producthunt_ok(combined, query):
                        continue
                    pub = parse_dt(entry.get("updated", ""))
                    sid = clean(entry.get("id", "")) or hashlib.sha1(f"producthunt-feed:{query}:{entry.get('link','')}:{entry.get('title','')}".encode("utf-8")).hexdigest()
                    rec = {
                        "source": "product_hunt",
                        "source_item_id": sid,
                        "entity_id": query,
                        "entity_name": f"Product Hunt - {query}",
                        "category": row.get("category", "Productivity"),
                        "reviewer_name": "",
                        "rating": None,
                        "comment_text": combined[:2500],
                        "posted_at": iso_utc(pub) if pub else "",
                        "url": clean(entry.get("link", "")) or search_url,
                        "country": "",
                        "language": "en",
                        "raw_json": {
                            "provider": "producthunt_atom_feed",
                            "query": query,
                            "entry": entry,
                            "html_fetch_error": fetch_err,
                            "html_blocked": blocked,
                        },
                    }
                    if self._insert(rec):
                        ins += 1
                        exhausted = False
                        if ins >= limit:
                            break

            if (not body and fetch_err) or (blocked and ins == 0):
                err = fetch_err or "producthunt html blocked"

        final_status = "done"
        if ins == 0:
            final_status = "skipped"
        self._mark_seed("producthunt", query, final_status, seen, ins, exhausted, err)
        return {"source": "product_hunt", "seen": seen, "inserted": ins, "errors": 1 if final_status == "error" else 0}

    def scrape_play(self, app: Dict[str, Any], fast: bool) -> Dict[str, Any]:
        pid = app.get("play_id", "")
        sk = f"play:{pid}"
        if not pid:
            self._mark_seed("apps", sk, "skipped", 0, 0, True, "missing play_id")
            return {"source": "play_store", "seen": 0, "inserted": 0, "errors": 0}

        self._mark_seed("apps", sk, "running", 0, 0, False, None)
        seen, ins, err = 0, 0, None
        exhausted, newest = True, None

        with self.play_sem:
            for country in self.play_countries:
                cur = None if fast else self._cursor_get(f"play:{pid}:{country}")
                token = None
                newest_country = None
                for _ in range(self.play_pages_fast if fast else 1):
                    try:
                        rows, token = play_reviews(pid, lang="en", country=country, sort=Sort.NEWEST, count=self.play_count, continuation_token=token)
                    except Exception as e:
                        err = str(e)
                        break
                    if not rows:
                        break
                    for row in rows:
                        seen += 1
                        rating = row.get("score")
                        text = clean(row.get("content", ""))
                        dt = parse_dt(row.get("at"))
                        if cur and dt and dt <= cur:
                            continue
                        if not self._mobile_ok(rating, text):
                            continue
                        sid = clean(row.get("reviewId", "")) or hashlib.md5(f"{pid}-{row.get('userName', '')}-{row.get('at', '')}".encode("utf-8")).hexdigest()
                        rec = {
                            "source": "play_store",
                            "source_item_id": sid,
                            "entity_id": pid,
                            "entity_name": app.get("app_name", pid),
                            "category": app.get("category", "Unknown"),
                            "reviewer_name": clean(row.get("userName", "")),
                            "rating": float(rating) if rating is not None else None,
                            "comment_text": text,
                            "posted_at": iso_utc(dt) if dt else "",
                            "url": f"https://play.google.com/store/apps/details?id={pid}",
                            "country": country.upper(),
                            "language": "en",
                            "raw_json": row,
                        }
                        if self._insert(rec):
                            ins += 1
                            exhausted = False
                            if dt and (not newest or dt > newest):
                                newest = dt
                            if dt and (not newest_country or dt > newest_country):
                                newest_country = dt
                    if token is None:
                        break
                if newest_country:
                    self._cursor_set(f"play:{pid}:{country}", newest_country)

        if newest:
            self._cursor_set(f"play:{pid}:max", newest)
        status = "error" if err else "done"
        if err and ins == 0:
            low = err.lower()
            if any(t in low for t in ["not found", "404", "package", "cannot fetch", "unavailable"]):
                status = "skipped"
        self._mark_seed("apps", sk, status, seen, ins, exhausted, err)
        return {"source": "play_store", "seen": seen, "inserted": ins, "errors": 1 if status == "error" else 0}

    def scrape_apple(self, app: Dict[str, Any], fast: bool) -> Dict[str, Any]:
        aid = app.get("apple_id", "")
        sk = f"apple:{aid}"
        if not aid:
            self._mark_seed("apps", sk, "skipped", 0, 0, True, "missing apple_id")
            return {"source": "app_store", "seen": 0, "inserted": 0, "errors": 0}

        self._mark_seed("apps", sk, "running", 0, 0, False, None)
        seen, ins, err = 0, 0, None
        exhausted, newest = True, None

        with self.apple_sem:
            for country in self.apple_countries:
                cur = None if fast else self._cursor_get(f"apple:{aid}:{country}")
                url_candidates = [
                    f"https://itunes.apple.com/{country}/rss/customerreviews/id={aid}/sortBy=mostRecent/format=json",
                    f"https://itunes.apple.com/{country}/rss/customerreviews/id={aid}/sortBy=mostRecent/json",
                ]
                try:
                    data = None
                    for url in url_candidates:
                        resp = self.session.get(url, timeout=20)
                        if resp.status_code == 200:
                            data = resp.json()
                            break
                    if data is None:
                        raise RuntimeError(f"App Store feed not reachable for {aid} [{country}]")
                except Exception as e:
                    err = str(e)
                    continue
                newest_country = None
                if not isinstance(data, dict):
                    err = f"Unexpected Apple payload type for {aid} [{country}]"
                    continue
                feed = data.get("feed", {})
                if not isinstance(feed, dict):
                    err = f"Unexpected Apple feed shape for {aid} [{country}]"
                    continue
                entries = feed.get("entry", [])
                if isinstance(entries, dict):
                    entries = [entries]
                if not isinstance(entries, list):
                    err = f"Unexpected Apple entry shape for {aid} [{country}]"
                    continue
                for entry in entries:
                    if not isinstance(entry, dict):
                        continue
                    raw = entry.get("im:rating", {}).get("label")
                    if raw is None:
                        continue
                    seen += 1
                    try:
                        rating = float(raw)
                    except ValueError:
                        continue
                    text = clean(entry.get("content", {}).get("label", ""))
                    dt = parse_dt(clean(entry.get("updated", {}).get("label", "")))
                    if cur and dt and dt <= cur:
                        continue
                    if not self._mobile_ok(rating, text):
                        continue
                    sid = clean(entry.get("id", {}).get("label", ""))
                    rec = {
                        "source": "app_store",
                        "source_item_id": sid,
                        "entity_id": str(aid),
                        "entity_name": app.get("app_name", str(aid)),
                        "category": app.get("category", "Unknown"),
                        "reviewer_name": clean(entry.get("author", {}).get("name", {}).get("label", "")),
                        "rating": rating,
                        "comment_text": text,
                        "posted_at": iso_utc(dt) if dt else "",
                        "url": sid,
                        "country": country.upper(),
                        "language": "en",
                        "raw_json": entry,
                    }
                    if self._insert(rec):
                        ins += 1
                        exhausted = False
                        if dt and (not newest or dt > newest):
                            newest = dt
                        if dt and (not newest_country or dt > newest_country):
                            newest_country = dt
                if newest_country:
                    self._cursor_set(f"apple:{aid}:{country}", newest_country)

        if newest:
            self._cursor_set(f"apple:{aid}:max", newest)
        status = "error" if err else "done"
        if err and ins == 0:
            low = err.lower()
            if any(t in low for t in ["not reachable", "404", "not found", "unavailable"]):
                status = "skipped"
        self._mark_seed("apps", sk, status, seen, ins, exhausted, err)
        return {"source": "app_store", "seen": seen, "inserted": ins, "errors": 1 if status == "error" else 0}

    def scrape_reddit(self, row: Dict[str, Any], fast: bool) -> Dict[str, Any]:
        sub = row.get("subreddit", "")
        if not sub:
            return {"source": "reddit", "seen": 0, "inserted": 0, "errors": 0}

        self._mark_seed("reddit", sub, "running", 0, 0, False, None)
        seen, ins, err = 0, 0, None
        status = "done"
        newest = None

        with self.reddit_sem:
            cur = None if fast else self._cursor_get(f"reddit:{sub}")
            lim = self.reddit_fast if fast else self.reddit_inc
            try:
                posts = []
                if self.reddit_client is not None:
                    for p in self.reddit_client.subreddit(sub).new(limit=lim):
                        posts.append({"id": p.id, "name": p.name, "title": p.title, "selftext": p.selftext, "author": str(p.author) if p.author else "", "permalink": p.permalink, "created_utc": p.created_utc})
                elif self.reddit_http_enabled:
                    data = None
                    endpoints = [
                        f"https://www.reddit.com/r/{sub}/new.json",
                        f"https://www.reddit.com/r/{sub}/hot.json",
                    ]
                    for endpoint in endpoints:
                        resp = self.session.get(
                            endpoint,
                            params={"limit": lim, "raw_json": 1},
                            headers={"Accept": "application/json"},
                            timeout=20,
                        )
                        if resp.status_code in {403, 404, 410, 429}:
                            err = f"reddit http {resp.status_code}"
                            status = "skipped"
                            continue
                        if resp.status_code != 200:
                            err = f"reddit http {resp.status_code}"
                            continue
                        try:
                            data = resp.json()
                        except Exception:
                            err = "reddit response not json"
                            status = "skipped"
                            continue
                        if isinstance(data, dict):
                            break
                    if isinstance(data, dict):
                        posts = [c.get("data", {}) for c in data.get("data", {}).get("children", []) if isinstance(c, dict)]
                else:
                    status = "skipped"
                    err = "reddit api credentials missing and REDDIT_HTTP_ENABLED=false"
            except Exception as e:
                err = str(e)
                posts = []
                if any(token in str(e).lower() for token in ["forbidden", "not found", "too many requests", "403", "404", "429"]):
                    status = "skipped"

            for p in posts:
                text = clean(f"{clean(p.get('title', ''))}\n{clean(p.get('selftext', ''))}")
                if not self._reddit_ok(text):
                    continue
                dt = parse_dt(p.get("created_utc"))
                if cur and dt and dt <= cur:
                    continue
                seen += 1
                rec = {
                    "source": "reddit",
                    "source_item_id": clean(p.get("name", "")) or f"t3_{p.get('id', '')}",
                    "entity_id": sub,
                    "entity_name": sub,
                    "category": "reddit",
                    "reviewer_name": clean(p.get("author", "")),
                    "rating": None,
                    "comment_text": text,
                    "posted_at": iso_utc(dt) if dt else "",
                    "url": f"https://reddit.com{clean(p.get('permalink', ''))}",
                    "country": "",
                    "language": "en",
                    "raw_json": p,
                }
                if self._insert(rec):
                    ins += 1
                    if dt and (not newest or dt > newest):
                        newest = dt

        if newest:
            self._cursor_set(f"reddit:{sub}", newest)
        final_status = status
        if final_status != "skipped":
            final_status = "error" if err else "done"
        self._mark_seed("reddit", sub, final_status, seen, ins, False, err)
        return {"source": "reddit", "seen": seen, "inserted": ins, "errors": 1 if final_status == "error" else 0}
    def _extract_rating(self, html: str) -> Optional[float]:
        for pat in (r'"ratingValue"\s*:\s*"?([0-9.]+)"?', r'data-rating\s*=\s*"([0-9.]+)"', r"Rating\s*[:=]\s*([0-9.]+)"):
            m = re.search(pat, html, flags=re.IGNORECASE)
            if m:
                try:
                    return float(m.group(1))
                except ValueError:
                    return None
        return None

    def _extract_title(self, html: str) -> str:
        body = html or ""
        if "<title" in body.lower():
            m = re.search(r"<title[^>]*>(.*?)</title>", body, flags=re.IGNORECASE | re.DOTALL)
            if m:
                return clean(m.group(1))
        # Fallback for text/markdown responses (e.g., proxies).
        for line in body.splitlines():
            txt = clean(line)
            if not txt:
                continue
            if txt.lower().startswith("title:"):
                return clean(txt.split(":", 1)[1])
            if len(txt) > 8:
                return txt[:180]
        return ""

    def _extract_desc(self, html: str) -> str:
        body = html or ""
        m = re.search(r'<meta[^>]+name=["\']description["\'][^>]+content=["\'](.*?)["\']', body, flags=re.IGNORECASE | re.DOTALL)
        if not m:
            m = re.search(r'<meta[^>]+content=["\'](.*?)["\'][^>]+name=["\']description["\']', body, flags=re.IGNORECASE | re.DOTALL)
        if m:
            return clean(m.group(1))
        cleaned_lines = []
        for line in body.splitlines():
            txt = clean(line)
            if not txt:
                continue
            low = txt.lower()
            if low.startswith("title:") or low.startswith("url source:") or low.startswith("warning:") or low.startswith("markdown content:"):
                continue
            cleaned_lines.append(txt)
            if len(cleaned_lines) >= 5:
                break
        return clean(" ".join(cleaned_lines))[:1000]

    def _marketplace_signal_text(self, row: Dict[str, Any], source: str, url: str, reason: str = "") -> str:
        parsed = urlparse(url or "")
        query = clean(parse_qs(parsed.query).get("query", [""])[0]).replace("+", " ")
        product = clean(row.get("product_name", ""))
        category = clean(row.get("category", "Unknown")) or "Unknown"
        focus = product or query or "software buyers"
        note = clean(reason)
        base = (
            f"{source.upper()} marketplace demand signal: buyers are researching {focus} in {category}. "
            "Search and review discovery pages indicate active comparison intent around pricing, alternatives, "
            "feature gaps, and fit for business workflows."
        )
        if note:
            base += f" Capture reason: {note}."
        return clean(base)[:1200]

    def _jina_proxy_url(self, url: str) -> str:
        normalized = re.sub(r"^https?://", "", url.strip())
        prefix = self.jina_prefix or "https://r.jina.ai/http://"
        if not prefix.endswith("/"):
            prefix += "/"
        return f"{prefix}{normalized}"

    def _fetch_page_text(self, url: str, timeout: int = 20, prefer_jina: bool = False) -> Tuple[Optional[str], Optional[str], Optional[str]]:
        attempts: List[Tuple[str, str]] = []
        if self.meta_use_jina and prefer_jina:
            attempts.append(("jina", self._jina_proxy_url(url)))
            attempts.append(("direct", url))
        else:
            attempts.append(("direct", url))
            if self.meta_use_jina:
                attempts.append(("jina", self._jina_proxy_url(url)))

        last_err: Optional[str] = None
        for mode, attempt_url in attempts:
            try:
                resp = self.session.get(attempt_url, timeout=timeout)
                if resp.status_code == 200 and resp.text:
                    return resp.text, mode, None
                last_err = f"HTTP {resp.status_code} for {attempt_url}"
            except Exception as exc:
                last_err = str(exc)
        return None, None, last_err

    def scrape_meta(self, row: Dict[str, Any], source: str) -> Dict[str, Any]:
        url = row.get("url", "")
        if not url:
            return {"source": source, "seen": 0, "inserted": 0, "errors": 0}

        self._mark_seed(source, url, "running", 0, 0, False, None)
        seen, ins, err = 0, 0, None
        exhausted = True

        with self.meta_sem:
            prefer_jina = clean(source).lower() in {"g2", "capterra"}
            html, mode, fetch_err = self._fetch_page_text(url, timeout=20, prefer_jina=prefer_jina)
            if not html:
                reason = fetch_err or "metadata fetch failed"
                if clean(source).lower() in {"g2", "capterra"} and self.meta_relaxed_marketplace:
                    snippet = self._marketplace_signal_text(row, source, url, reason)
                    seen = 1
                    rec = {
                        "source": source,
                        "source_item_id": hashlib.sha1(f"{source}:{url}".encode("utf-8")).hexdigest(),
                        "entity_id": url,
                        "entity_name": row.get("product_name", url),
                        "category": row.get("category", "Unknown"),
                        "reviewer_name": "",
                        "rating": None,
                        "comment_text": snippet,
                        "posted_at": iso_utc(),
                        "url": url,
                        "country": "",
                        "language": "en",
                        "raw_json": {"provider": "marketplace_query_signal", "reason": reason},
                    }
                    if self._insert(rec):
                        ins = 1
                        exhausted = False
                    self._mark_seed(source, url, "done", seen, ins, exhausted, None)
                    return {"source": source, "seen": seen, "inserted": ins, "errors": 0}
                # Metadata sources are best-effort; non-reachable pages should not poison pipeline health.
                self._mark_seed(source, url, "skipped", seen, ins, True, reason)
                return {"source": source, "seen": seen, "inserted": ins, "errors": 0}

            title = self._extract_title(html)
            desc = self._extract_desc(html)
            snippet = clean(f"{title}. {desc}")
            rating = self._extract_rating(html)
            seen = 1

            if not snippet and rating is None:
                if clean(source).lower() in {"g2", "capterra"} and self.meta_relaxed_marketplace:
                    snippet = self._marketplace_signal_text(row, source, url, "no snippet or rating")
                else:
                    self._mark_seed(source, url, "skipped", seen, ins, True, "No snippet or rating")
                    return {"source": source, "seen": seen, "inserted": ins, "errors": 0}

            if not self._meta_ok_for_source(snippet, rating, source):
                self._mark_seed(source, url, "skipped", seen, ins, True, "No pain signal")
                return {"source": source, "seen": seen, "inserted": ins, "errors": 0}

            rec = {
                "source": source,
                "source_item_id": hashlib.sha1(f"{source}:{url}".encode("utf-8")).hexdigest(),
                "entity_id": url,
                "entity_name": row.get("product_name", url),
                "category": row.get("category", "Unknown"),
                "reviewer_name": "",
                "rating": rating,
                "comment_text": snippet,
                "posted_at": iso_utc(),
                "url": url,
                "country": "",
                "language": "en",
                "raw_json": {"title": title, "description": desc, "rating": rating, "fetch_mode": mode},
            }
            if self._insert(rec):
                ins = 1
                exhausted = False

        self._mark_seed(source, url, "done", seen, ins, exhausted, None)
        return {"source": source, "seen": seen, "inserted": ins, "errors": 0}

    def scrape_upwork(self, row: Dict[str, Any], fast: bool) -> Dict[str, Any]:
        query = clean(row.get("query", ""))
        if not query:
            return {"source": "upwork", "seen": 0, "inserted": 0, "errors": 0}

        self._mark_seed("upwork", query, "running", 0, 0, False, None)
        seen, ins, err = 0, 0, None
        exhausted, newest = True, None

        with self.upwork_sem:
            cur = None if fast else self._cursor_get(f"upwork:{query}")
            limit = self.upwork_fast if fast else self.upwork_inc
            feed_url = f"https://www.upwork.com/ab/feed/jobs/rss?q={quote_plus(query)}"
            items: List[Dict[str, Any]] = []

            if self.upwork_api_first_party_required and not self._upwork_api_configured():
                reason = "UPWORK_API_FIRST_PARTY_REQUIRED=true but UPWORK_API_* credentials are missing"
                self._mark_seed("upwork", query, "skipped", seen, ins, True, reason)
                return {"source": "upwork", "seen": seen, "inserted": ins, "errors": 0}

            if self._upwork_api_configured():
                try:
                    items = self._upwork_first_party_candidates(query, limit)
                except Exception as e:
                    err = f"upwork first-party fetch failed: {e}"
                    logging.warning("Upwork first-party API fetch failed for query '%s': %s", query, e)
                    if self.upwork_api_first_party_required:
                        self._mark_seed("upwork", query, "skipped", seen, ins, True, err)
                        return {"source": "upwork", "seen": seen, "inserted": ins, "errors": 0}

            if not items and self.upwork_dataset_url and not self.upwork_api_first_party_required:
                dataset_url = self.upwork_dataset_url.replace("{query}", quote_plus(query))
                try:
                    resp = self.session.get(dataset_url, timeout=30)
                    resp.raise_for_status()
                    payload = resp.json()
                    rows = payload if isinstance(payload, list) else payload.get("items", []) if isinstance(payload, dict) else []
                    for row_obj in rows[: max(1, limit)]:
                        if not isinstance(row_obj, dict):
                            continue
                        items.append(
                            {
                                "title": clean(row_obj.get("title", "")),
                                "description": strip_html(row_obj.get("description", "")),
                                "link": clean(row_obj.get("url", "") or row_obj.get("link", "")),
                                "guid": clean(row_obj.get("id", "") or row_obj.get("jobId", "")),
                                "pubDate": clean(row_obj.get("publishedAt", "") or row_obj.get("createdAt", "")),
                                "raw": {"provider": "dataset", "payload": row_obj},
                            }
                        )
                except Exception as e:
                    err = f"upwork dataset fetch failed: {e}"
                    if self.upwork_query_fallback:
                        items = [self._upwork_query_signal_item(query, clean(row.get("category", "Operations")), feed_url)]
                    else:
                        self._mark_seed("upwork", query, "skipped", seen, ins, True, err)
                        return {"source": "upwork", "seen": seen, "inserted": ins, "errors": 0}

            if not items and not self.upwork_api_first_party_required:
                try:
                    resp = self.session.get(feed_url, timeout=20)
                except Exception as e:
                    err = str(e)
                    if self.upwork_query_fallback:
                        items = [self._upwork_query_signal_item(query, clean(row.get("category", "Operations")), feed_url)]
                    else:
                        self._mark_seed("upwork", query, "skipped", seen, ins, True, err)
                        return {"source": "upwork", "seen": seen, "inserted": ins, "errors": 0}

                if not items and resp.status_code in {403, 410}:
                    items = self._upwork_jina_candidates(query, limit)
                    if not items:
                        if self.upwork_query_fallback:
                            items = [self._upwork_query_signal_item(query, clean(row.get("category", "Operations")), feed_url)]
                        else:
                            reason = (
                                f"upwork public RSS unavailable (HTTP {resp.status_code}). "
                                "Set UPWORK_API_* or UPWORK_DATASET_URL for first-party job ingestion."
                            )
                            self._mark_seed("upwork", query, "skipped", seen, ins, True, reason)
                            return {"source": "upwork", "seen": seen, "inserted": ins, "errors": 0}

                try:
                    if not items and resp.status_code == 200:
                        resp.raise_for_status()
                        root = ET.fromstring(resp.text.encode("utf-8"))
                        for item in root.findall(".//item")[: max(1, limit)]:
                            items.append(
                                {
                                    "title": clean(item.findtext("title", default="")),
                                    "description": strip_html(item.findtext("description", default="")),
                                    "link": clean(item.findtext("link", default="")),
                                    "guid": clean(item.findtext("guid", default="")),
                                    "pubDate": clean(item.findtext("pubDate", default="")),
                                    "raw": {
                                        "title": clean(item.findtext("title", default="")),
                                        "description": strip_html(item.findtext("description", default="")),
                                        "link": clean(item.findtext("link", default="")),
                                        "guid": clean(item.findtext("guid", default="")),
                                        "pubDate": clean(item.findtext("pubDate", default="")),
                                        "provider": "rss",
                                    },
                                }
                            )
                    if not items and self.upwork_query_fallback and not self.upwork_api_first_party_required:
                        items = [self._upwork_query_signal_item(query, clean(row.get("category", "Operations")), feed_url)]
                except Exception as e:
                    err = str(e)
                    if self.upwork_query_fallback and not self.upwork_api_first_party_required:
                        items = [self._upwork_query_signal_item(query, clean(row.get("category", "Operations")), feed_url)]
                    else:
                        self._mark_seed("upwork", query, "skipped", seen, ins, True, err)
                        return {"source": "upwork", "seen": seen, "inserted": ins, "errors": 0}

            for item in items[: max(1, limit)]:
                title = clean(item.get("title", ""))
                desc = strip_html(item.get("description", ""))
                link = clean(item.get("link", ""))
                guid = clean(item.get("guid", ""))
                pub = parse_dt(clean(item.get("pubDate", "")))
                raw_payload = item.get("raw", {}) if isinstance(item.get("raw", {}), dict) else {}
                provider = clean(raw_payload.get("provider", "rss")).lower()

                text = clean(f"{title}. {desc}")
                seen += 1
                if cur and pub and pub <= cur:
                    continue
                if provider != "query_signal" and not self._upwork_ok(text):
                    continue

                budget = self._extract_upwork_budget(desc)
                sid = guid or hashlib.sha1(f"{query}|{title}|{link}".encode("utf-8")).hexdigest()
                rec = {
                    "source": "upwork",
                    "source_item_id": sid,
                    "entity_id": query,
                    "entity_name": f"Upwork - {query}",
                    "category": row.get("category", "Operations"),
                    "reviewer_name": "client",
                    "rating": None,
                    "comment_text": text,
                    "posted_at": iso_utc(pub) if pub else "",
                    "url": link or feed_url,
                    "country": "",
                    "language": "en",
                    "raw_json": {
                        "query": query,
                        "title": title,
                        "description": desc,
                        "budget_hint": budget,
                        "link": link,
                        "pubDate": clean(item.get("pubDate", "")),
                        "provider": provider or ("dataset" if self.upwork_dataset_url else "rss"),
                        "payload": raw_payload,
                    },
                }
                if self._insert(rec):
                    ins += 1
                    exhausted = False
                    if pub and (not newest or pub > newest):
                        newest = pub

        if newest:
            self._cursor_set(f"upwork:{query}", newest)
        final_status = "error" if err else "done"
        if err and ins == 0:
            final_status = "skipped"
        self._mark_seed("upwork", query, final_status, seen, ins, exhausted, err)
        return {"source": "upwork", "seen": seen, "inserted": ins, "errors": 1 if final_status == "error" else 0}

    def _records_count(self) -> int:
        self._connect_db(retries=30, delay_seconds=2)
        with self.lock:
            row = self.conn.execute("SELECT COUNT(*) FROM reviews").fetchone()
        return int(row[0]) if row else 0

    def _static_exhausted(self) -> bool:
        self._connect_db(retries=30, delay_seconds=2)
        with self.lock:
            e = self.conn.execute("SELECT COUNT(*) FROM seed_progress WHERE seed_type IN ('apps','hackernews','producthunt','g2','capterra','upwork') AND enabled = TRUE").fetchone()
            r = self.conn.execute("SELECT COUNT(*) FROM seed_progress WHERE seed_type IN ('apps','hackernews','producthunt','g2','capterra','upwork') AND enabled = TRUE AND exhausted = FALSE").fetchone()
        enabled = int(e[0]) if e else 0
        remaining = int(r[0]) if r else 0
        return enabled > 0 and remaining == 0

    def _log_cycle(self, mode: str, stats: Dict[str, Any]) -> None:
        with self.lock:
            self.conn.execute("INSERT INTO run_log(run_at, mode, stats_json, total_records) VALUES (?, ?, ?, ?)", [now_utc(), mode, json.dumps(stats, ensure_ascii=False), self._records_count()])
        day = self.exports_dir / now_utc().strftime("%Y-%m-%d")
        day.mkdir(parents=True, exist_ok=True)
        with (day / "cycle_stats.jsonl").open("a", encoding="utf-8") as f:
            f.write(json.dumps({"run_at": iso_utc(), "mode": mode, "stats": stats, "total_records": self._records_count()}, ensure_ascii=False) + "\n")

    def run_cycle(self, fast: bool) -> Dict[str, Any]:
        self._connect_db(retries=30, delay_seconds=2)
        try:
            self.seeds = self._load_all_seeds()
            should_sync = (
                self._last_seed_sync_at is None
                or (now_utc() - self._last_seed_sync_at) >= timedelta(minutes=30)
            )
            if should_sync:
                self._sync_seed_progress(self.seeds)
                self._last_seed_sync_at = now_utc()
            limits = self._cycle_seed_limits(fast)

            enabled = {
                "apps": [a for a in self.seeds["apps"] if a.get("enabled", False)],
                "reddit": [r for r in self.seeds["reddit"] if r.get("enabled", False) and r.get("subreddit")],
                "hackernews": [r for r in self.seeds["hackernews"] if r.get("enabled", False) and r.get("query")],
                "producthunt": [r for r in self.seeds["producthunt"] if r.get("enabled", False) and r.get("query")],
                "g2": [r for r in self.seeds["g2"] if r.get("enabled", False) and r.get("url")],
                "capterra": [r for r in self.seeds["capterra"] if r.get("enabled", False) and r.get("url")],
                "upwork": [r for r in self.seeds["upwork"] if r.get("enabled", False) and r.get("query")],
            }
            selected = {
                "apps": [],
                "reddit": self._seed_slice("reddit", enabled["reddit"], limits["reddit"]),
                "hackernews": self._seed_slice("hackernews", enabled["hackernews"], limits["hackernews"]),
                "producthunt": self._seed_slice("producthunt", enabled["producthunt"], limits["producthunt"]),
                "g2": self._seed_slice("g2", enabled["g2"], limits["g2"]),
                "capterra": self._seed_slice("capterra", enabled["capterra"], limits["capterra"]),
                "upwork": self._seed_slice("upwork", enabled["upwork"], limits["upwork"]),
            }
            selected["apps"], app_selection_breakdown = self._select_apps(
                enabled["apps"],
                limits["apps"],
                limits.get("apps_apple_target", limits["apps"]),
            )
            logging.info(
                "app selection total=%s apple=%s play=%s both=%s focus=%s skip_play_for_apple=%s",
                len(selected["apps"]),
                app_selection_breakdown.get("with_apple_id", 0),
                app_selection_breakdown.get("with_play_id", 0),
                app_selection_breakdown.get("both", 0),
                self.appstore_focus_enabled,
                self.appstore_focus_skip_play_for_apple,
            )

            self._write_runtime_status(
                {
                    "phase": "cycle_started",
                    "mode": "fast_backfill" if fast else "incremental",
                    "limits": limits,
                    "selected_counts": {k: len(v) for k, v in selected.items()},
                    "selected_app_breakdown": app_selection_breakdown,
                }
            )

            futures = []
            out: Dict[str, Dict[str, int]] = {}
            with ThreadPoolExecutor(max_workers=max(1, self.max_workers)) as ex:
                for a in selected["apps"]:
                    has_play = bool(clean(a.get("play_id", "")))
                    has_apple = bool(clean(a.get("apple_id", "")))
                    if self.appstore_focus_enabled and self.appstore_focus_skip_play_for_apple and has_apple:
                        futures.append(ex.submit(self.scrape_apple, a, fast))
                    else:
                        if has_play:
                            futures.append(ex.submit(self.scrape_play, a, fast))
                        if has_apple:
                            futures.append(ex.submit(self.scrape_apple, a, fast))
                for r in selected["reddit"]:
                    futures.append(ex.submit(self.scrape_reddit, r, fast))
                for r in selected["hackernews"]:
                    futures.append(ex.submit(self.scrape_hackernews, r, fast))
                for r in selected["producthunt"]:
                    futures.append(ex.submit(self.scrape_producthunt, r, fast))
                for r in selected["g2"]:
                    futures.append(ex.submit(self.scrape_meta, r, "g2"))
                for r in selected["capterra"]:
                    futures.append(ex.submit(self.scrape_meta, r, "capterra"))
                for r in selected["upwork"]:
                    futures.append(ex.submit(self.scrape_upwork, r, fast))

                done_count = 0
                for fut in as_completed(futures):
                    try:
                        res = fut.result()
                    except Exception as e:
                        logging.exception("task failed: %s", e)
                        continue
                    src = res.get("source", "unknown")
                    out.setdefault(src, {"seen": 0, "inserted": 0, "errors": 0})
                    out[src]["seen"] += int(res.get("seen", 0))
                    out[src]["inserted"] += int(res.get("inserted", 0))
                    out[src]["errors"] += int(res.get("errors", 0))
                    done_count += 1
                    if done_count % 25 == 0:
                        self._write_runtime_status(
                            {
                                "phase": "cycle_running",
                                "mode": "fast_backfill" if fast else "incremental",
                                "tasks_done": done_count,
                                "tasks_total": len(futures),
                                "partial_stats": out,
                                "selected_app_breakdown": app_selection_breakdown,
                            }
                        )

            stats = {
                "seen": sum(v["seen"] for v in out.values()),
                "inserted": sum(v["inserted"] for v in out.values()),
                "errors": sum(v["errors"] for v in out.values()),
                "sources": out,
                "selected_counts": {k: len(v) for k, v in selected.items()},
                "selected_app_breakdown": app_selection_breakdown,
            }
            mode = "fast_backfill" if fast else "incremental"
            self._log_cycle(mode, stats)
            self._write_runtime_status({"phase": "cycle_done", "mode": mode, "stats": stats})
            logging.info("cycle complete mode=%s inserted=%s seen=%s errors=%s", mode, stats["inserted"], stats["seen"], stats["errors"])
            return stats
        finally:
            self._close_db()

    def _fast_stop(self, start: datetime) -> Optional[str]:
        try:
            if self._records_count() >= self.target_records:
                return f"target reached ({self.target_records})"
            if now_utc() - start >= timedelta(days=self.fast_days):
                return f"fast phase days reached ({self.fast_days})"
            if self._static_exhausted():
                return "seed list exhausted"
            return None
        finally:
            # _records_count/_static_exhausted open DB connections; release lock between cycles.
            self._close_db()

    def run_fast(self) -> None:
        started = now_utc()
        logging.info("fast backfill started")
        while True:
            self.run_cycle(True)
            stop = self._fast_stop(started)
            if stop:
                logging.info("fast backfill stopped: %s", stop)
                self._close_db()
                break
            self._close_db()
            delay = random.randint(max(1, self.sleep_min), max(max(1, self.sleep_min), self.sleep_max))
            logging.info("sleeping %s sec before next fast cycle", delay)
            time.sleep(delay)

    def _incremental_job(self) -> None:
        try:
            self.run_cycle(False)
        except Exception:
            logging.exception("incremental cycle failed")
        finally:
            self._close_db()

    def run_incremental_forever(self) -> None:
        self.scheduler.add_job(self._incremental_job, trigger="interval", hours=max(1, self.interval_hours), next_run_time=now_utc(), max_instances=1, coalesce=True, id="incremental", replace_existing=True)
        self.scheduler.start()
        logging.info("incremental mode every %s hour(s)", self.interval_hours)
        while True:
            time.sleep(30)


if __name__ == "__main__":
    engine = Engine()
    if engine.fast_mode:
        engine.run_fast()
    engine.run_incremental_forever()
