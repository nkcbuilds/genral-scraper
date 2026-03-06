import hashlib
import json
import logging
import os
import re
import time
from collections import Counter
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

import duckdb
from apscheduler.schedulers.background import BackgroundScheduler

try:
    from google import genai
    from google.genai import types as genai_types
except Exception:
    genai = None
    genai_types = None


def now_utc() -> datetime:
    return datetime.now(timezone.utc)


def parse_bool(value: Any, default: bool = False) -> bool:
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    return str(value).strip().lower() in {"1", "true", "yes", "y", "on"}


def parse_int(name: str, default: int) -> int:
    raw = os.getenv(name)
    try:
        return int(raw) if raw is not None else default
    except ValueError:
        return default


def parse_float(name: str, default: float) -> float:
    raw = os.getenv(name)
    try:
        return float(raw) if raw is not None else default
    except ValueError:
        return default


def normalize_text(text: Any) -> str:
    return re.sub(r"\s+", " ", str(text or "").lower()).strip()


def tokenize(text: Any) -> List[str]:
    return [t for t in re.findall(r"[a-z0-9]+", normalize_text(text)) if len(t) > 2]


def slugify(text: str) -> str:
    slug = re.sub(r"[^a-z0-9]+", "-", (text or "").lower()).strip("-")
    return slug[:120] if slug else "idea"


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


STOPWORDS = {
    "a", "an", "and", "are", "as", "at", "be", "been", "being", "but", "by", "can", "cannot", "could",
    "did", "do", "does", "doing", "for", "from", "had", "has", "have", "having", "he", "her", "here", "hers",
    "him", "his", "how", "i", "if", "in", "into", "is", "it", "its", "itself", "just", "me", "more", "most",
    "my", "no", "not", "of", "on", "or", "our", "ours", "please", "she", "so", "some", "than", "that", "the",
    "their", "them", "there", "these", "they", "this", "those", "to", "too", "us", "very", "was", "we", "were",
    "what", "when", "where", "which", "who", "why", "will", "with", "would", "you", "your", "yours",
    "app", "apps", "software", "tool", "tools", "issue", "issues", "problem", "problems", "need", "better",
}
WILLINGNESS_CUES = ["pay", "pricing", "cost", "subscription", "expensive", "budget", "charge", "price"]
NEGATIVE_SIGNAL_WORDS = {
    "hate", "frustrated", "annoying", "broken", "bad", "worst", "bug", "issue",
    "problem", "pain", "difficult", "slow", "useless", "terrible", "awful", "fail", "missing",
}
GENERIC_CATEGORY_TOKENS = {"general", "unknown", "misc", "miscellaneous", "other", "n/a"}
LOCATION_TOKENS = {
    "africa", "asia", "australia", "canada", "china", "europe", "france", "germany", "india",
    "indonesia", "italy", "japan", "latin america", "mexico", "new zealand", "singapore", "spain",
    "uk", "united kingdom", "united states", "usa", "us", "uae", "brazil", "argentina", "chile",
    "colombia", "peru", "nigeria", "kenya", "egypt", "saudi arabia", "dubai", "delhi", "mumbai",
    "bangalore", "london", "paris", "berlin", "toronto", "vancouver", "sydney", "melbourne",
    "california", "new york", "texas", "washington", "florida", "european", "american", "indian",
    "canadian", "british",
}


class Enricher:
    def __init__(self) -> None:
        base = Path(__file__).resolve().parent
        load_env_file(base / ".env")

        self.fast_mode = parse_bool(os.getenv("FAST_BACKFILL_MODE", "true"), True)
        self.fast_days = parse_int("FAST_BACKFILL_DAYS", 5)
        self.target_records = parse_int("FAST_TARGET_RECORDS", 200000)
        self.interval_hours = parse_int("INCREMENTAL_INTERVAL_HOURS", 4)
        self.fast_sleep = parse_int("FAST_PROCESS_SLEEP_SECONDS", 1200)

        self.batch_size = max(50, parse_int("ENRICH_BATCH_SIZE", 300))
        self.max_batches = max(1, parse_int("ENRICH_MAX_BATCHES_PER_RUN", 10))
        self.min_cluster_size = max(1, parse_int("MIN_EVIDENCE_LINKS", 2))
        self.min_sources_for_publish = max(1, parse_int("MIN_SOURCES_FOR_PUBLISH", 1))
        self.min_confidence = parse_float("MIN_CANDIDATE_CONFIDENCE", 0.55)
        self.max_supporting_quotes = max(2, parse_int("MAX_SUPPORTING_QUOTES", 6))
        self.min_supporting_evidence_for_publish = max(1, parse_int("MIN_SUPPORTING_EVIDENCE_FOR_PUBLISH", 2))
        self.candidate_variants_per_cluster = max(1, parse_int("CANDIDATE_VARIANTS_PER_CLUSTER", 1))
        self.variant_min_evidence = max(2, parse_int("VARIANT_MIN_EVIDENCE", 4))
        self.enrich_allow_reprocess = parse_bool(os.getenv("ENRICH_ALLOW_REPROCESS", "false"), False)
        self.enrich_reprocess_cooldown_hours = max(1, parse_int("ENRICH_REPROCESS_COOLDOWN_HOURS", 24))

        self.ai_enrichment_enabled = parse_bool(os.getenv("AI_ENRICHMENT_ENABLED", "true"), True)
        self.ai_enrichment_temperature = parse_float("AI_ENRICHMENT_TEMPERATURE", 0.2)
        self.ai_enrichment_max_rows = max(4, parse_int("AI_ENRICHMENT_MAX_ROWS", 14))
        self.ai_enrichment_max_output_tokens = max(512, parse_int("AI_ENRICHMENT_MAX_OUTPUT_TOKENS", 1200))
        self.vertex_model = os.getenv("VERTEX_MODEL", "gemini-2.5-flash").strip() or "gemini-2.5-flash"
        self.vertex_model_candidates = [x.strip() for x in os.getenv("VERTEX_MODEL_CANDIDATES", "").split(",") if x.strip()]
        if self.vertex_model not in self.vertex_model_candidates:
            self.vertex_model_candidates.insert(0, self.vertex_model)
        self.vertex_location = os.getenv("VERTEX_LOCATION", "us-central1").strip() or "us-central1"
        self.project_id = os.getenv("PROJECT_ID", "").strip()
        self.gemini_api_key = os.getenv("GEMINI_API_KEY", "").strip()

        self.daily_budget_inr = parse_float("DAILY_BUDGET_INR", 500.0)
        self.bootstrap_publish_on_start = parse_bool(os.getenv("BOOTSTRAP_PUBLISH_ON_START", "true"), True)

        self.base = base
        self.db_path = Path(os.getenv("DB_PATH", str(self.base / "reviews.db")))
        self.public_db_path = Path(os.getenv("PUBLIC_DB_PATH", str(self.base / "public.db")))
        self.exports = self.base / "exports"
        self.exports.mkdir(parents=True, exist_ok=True)

        logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")

        self.conn: Optional[duckdb.DuckDBPyConnection] = None
        self.scheduler = BackgroundScheduler(timezone="UTC")
        self.ai_client = self._init_ai_client()
        self.ai_failure_count = 0
        self.ai_disabled_models: set[str] = set()

        self._connect_db(120, 2)
        self._setup_tables()
        self._close_db()

    def _connect_db(self, retries: int = 30, delay_seconds: int = 3) -> None:
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

    def _init_ai_client(self) -> Any:
        if not self.ai_enrichment_enabled:
            logging.info("AI enrichment disabled (AI_ENRICHMENT_ENABLED=false)")
            return None
        if genai is None:
            logging.warning("google-genai is unavailable. Falling back to deterministic enrichment.")
            return None
        try:
            if self.gemini_api_key:
                logging.info("AI enrichment enabled via GEMINI_API_KEY")
                return genai.Client(api_key=self.gemini_api_key)
            if self.project_id:
                logging.info("AI enrichment enabled via Vertex project=%s location=%s", self.project_id, self.vertex_location)
                return genai.Client(vertexai=True, project=self.project_id, location=self.vertex_location)
            logging.warning("AI enrichment requested but no PROJECT_ID or GEMINI_API_KEY found. Falling back to deterministic enrichment.")
            return None
        except Exception as exc:
            logging.warning("AI enrichment init failed: %s", exc)
            return None

    def _safe_json_load(self, raw: str) -> Optional[Dict[str, Any]]:
        text = str(raw or "").strip()
        if not text:
            return None
        candidates = [text]
        first = text.find("{")
        last = text.rfind("}")
        if first >= 0 and last > first:
            candidates.append(text[first : last + 1])
        for item in candidates:
            try:
                payload = json.loads(item)
            except Exception:
                continue
            if isinstance(payload, dict):
                return payload
        return None

    def _normalize_level(self, value: Any, default: str = "medium") -> str:
        txt = normalize_text(value)
        if "high" in txt:
            return "high"
        if "low" in txt:
            return "low"
        return default

    def _bounded_float(self, value: Any, default: float, lo: float, hi: float) -> float:
        try:
            return min(hi, max(lo, float(value)))
        except Exception:
            return default

    def _looks_like_location(self, value: Any) -> bool:
        txt = normalize_text(value)
        if not txt:
            return False
        if txt.startswith("r/"):
            txt = txt[2:]
        if txt in LOCATION_TOKENS:
            return True
        if txt.endswith((" city", " county", " state", " province", " region")):
            return True
        parts = [x for x in re.findall(r"[a-z]+", txt) if x]
        if not parts:
            return False
        if len(parts) <= 3 and any(x in LOCATION_TOKENS for x in parts):
            return True
        if re.fullmatch(r"[a-z]{2}", txt) and txt in {"us", "uk", "in", "ca", "au"}:
            return True
        return False

    def _clean_tag(self, value: Any) -> str:
        txt = re.sub(r"[/_]+", " ", str(value or "")).strip(" ,;")
        txt = re.sub(r"\s+", " ", txt).strip()
        if not txt:
            return ""
        if normalize_text(txt) in GENERIC_CATEGORY_TOKENS:
            return ""
        if self._looks_like_location(txt):
            return ""
        txt = txt[:40].strip()
        return txt.title() if txt == txt.lower() else txt

    def _sanitize_category_tags(self, ai_tags: List[str], domain: str, top_terms: List[str]) -> List[str]:
        merged: List[str] = [domain] + list(ai_tags or []) + [t.replace("_", " ") for t in top_terms[:3]]
        out: List[str] = []
        seen = set()
        for raw in merged:
            tag = self._clean_tag(raw)
            if not tag:
                continue
            key = normalize_text(tag)
            if key in seen:
                continue
            seen.add(key)
            out.append(tag)
            if len(out) >= 4:
                break
        if not out:
            out = ["Pain Intelligence"]
        return out

    def _default_domain(self, rows: List[Dict[str, Any]]) -> str:
        for row in rows[:6]:
            for key in ("category", "entity_name"):
                tag = self._clean_tag(row.get(key))
                if tag:
                    return tag
        return "Operations"

    def _select_supporting_rows(self, rows: List[Dict[str, Any]], limit: Optional[int] = None) -> List[Dict[str, Any]]:
        max_items = max(2, limit or self.max_supporting_quotes)
        scored: List[Any] = []
        for row in rows:
            text = str(row.get("comment_text") or "").strip()
            if not text:
                continue
            low = text.lower()
            neg_hits = sum(low.count(w) for w in NEGATIVE_SIGNAL_WORDS)
            cue_hits = sum(low.count(w) for w in WILLINGNESS_CUES)
            length_bonus = min(2.5, len(text) / 220.0)
            rating_bonus = 0.0
            try:
                rating = float(row.get("rating"))
                if rating <= 2.0:
                    rating_bonus = 2.0
                elif rating <= 3.0:
                    rating_bonus = 1.0
            except Exception:
                pass
            score = neg_hits * 1.4 + cue_hits * 1.1 + length_bonus + rating_bonus
            scored.append((score, row))

        if not scored:
            return rows[:max_items]

        scored.sort(key=lambda x: x[0], reverse=True)
        selected: List[Dict[str, Any]] = []
        seen_fp = set()
        seen_item = set()

        for _, row in scored:
            fp = str(row.get("fingerprint") or "").strip()
            item = str(row.get("source_item_id") or "").strip()
            if fp and fp in seen_fp:
                continue
            if item and item in seen_item:
                continue
            selected.append(row)
            if fp:
                seen_fp.add(fp)
            if item:
                seen_item.add(item)
            if len(selected) >= max_items:
                break

        if len(selected) < min(max_items, len(rows)):
            for _, row in scored:
                fp = str(row.get("fingerprint") or "").strip()
                if fp and fp in seen_fp:
                    continue
                selected.append(row)
                if fp:
                    seen_fp.add(fp)
                if len(selected) >= max_items:
                    break

        return selected[:max_items]

    def _supporting_payload(self, rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        payload = []
        for row in rows[: self.max_supporting_quotes]:
            payload.append(
                {
                    "quote": str(row.get("comment_text") or "")[:700],
                    "source": str(row.get("source") or ""),
                    "url": str(row.get("url") or ""),
                    "author": str(row.get("reviewer_name") or ""),
                    "posted_at": str(row.get("posted_at") or ""),
                    "match_score": 1.0,
                }
            )
        return payload

    def _build_cluster_key(self, row: Dict[str, Any]) -> str:
        domain = self._clean_tag(row.get("category")) or self._clean_tag(row.get("entity_name")) or "General"
        terms = [t for t in tokenize(row.get("comment_text")) if t not in STOPWORDS][:3]
        if terms:
            sig = "-".join(sorted(set(terms))[:3])
            return f"{normalize_text(domain)}::{sig}"
        return f"{normalize_text(domain)}::general"

    def _ai_enrich_cluster(
        self,
        key: str,
        rows: List[Dict[str, Any]],
        top_terms: List[str],
        sources: List[str],
        avg_rating: float,
        evidence_count: int,
    ) -> Optional[Dict[str, Any]]:
        if not self.ai_client:
            return None
        evidence = []
        for i, row in enumerate(rows[: self.ai_enrichment_max_rows]):
            evidence.append(
                {
                    "id": i,
                    "source": str(row.get("source") or ""),
                    "entity_name": str(row.get("entity_name") or ""),
                    "seed_category": str(row.get("category") or ""),
                    "rating": row.get("rating"),
                    "country": str(row.get("country") or ""),
                    "language": str(row.get("language") or ""),
                    "comment": str(row.get("comment_text") or "")[:420],
                    "url": str(row.get("url") or ""),
                }
            )
        if not evidence:
            return None

        prompt = (
            "You are enriching startup pain-point evidence.\n"
            "Infer missing fields from evidence and return ONLY valid JSON.\n"
            "Do not use location or geography as a category/tag.\n"
            "When at least 2 comments exist, supporting_comment_ids must include at least 2 ids.\n"
            "Allowed labels for impact_label/frequency_label/willingness_signal: low, medium, high.\n"
            "JSON keys required: pain_point, reasoning, suggested_solution, domain_category, category_tags, "
            "impact_label, frequency_label, willingness_signal, pricing_hint, confidence_score, name, description, location_hint, supporting_comment_ids.\n"
            "Keep claims grounded only in supplied evidence.\n\n"
            "INPUT:\n"
            + json.dumps(
                {
                    "cluster_key": key,
                    "evidence_count": evidence_count,
                    "sources": sources,
                    "avg_rating": round(avg_rating, 3),
                    "top_terms": top_terms[:6],
                    "evidence": evidence,
                },
                ensure_ascii=False,
            )
        )

        config: Any
        if genai_types is not None and hasattr(genai_types, "GenerateContentConfig"):
            config = genai_types.GenerateContentConfig(
                temperature=max(0.0, min(1.0, self.ai_enrichment_temperature)),
                max_output_tokens=self.ai_enrichment_max_output_tokens,
                response_mime_type="application/json",
            )
        else:
            config = {
                "temperature": max(0.0, min(1.0, self.ai_enrichment_temperature)),
                "max_output_tokens": self.ai_enrichment_max_output_tokens,
                "response_mime_type": "application/json",
            }

        last_exc: Optional[Exception] = None
        reachable_model_used = False
        for model in self.vertex_model_candidates:
            if model in self.ai_disabled_models:
                continue
            try:
                resp = self.ai_client.models.generate_content(model=model, contents=prompt, config=config)
                reachable_model_used = True
                payload = self._safe_json_load(getattr(resp, "text", ""))
                if payload:
                    self.ai_failure_count = 0
                    return payload
                logging.warning("AI enrichment produced non-JSON payload for cluster=%s model=%s", key, model)
            except Exception as exc:
                msg = str(exc)
                if "404" in msg or "NOT_FOUND" in msg:
                    self.ai_disabled_models.add(model)
                    logging.warning("AI model disabled due not found model=%s", model)
                    continue
                last_exc = exc
                logging.warning("AI enrichment call failed model=%s: %s", model, exc)

        if last_exc is not None:
            self.ai_failure_count += 1
            if self.ai_failure_count >= 3:
                logging.warning("AI enrichment disabled after repeated failures; using deterministic fallback.")
                self.ai_client = None
            else:
                logging.warning("AI enrichment fallback for cluster %s: %s", key, last_exc)
        elif not reachable_model_used:
            logging.warning("No reachable AI models available; using deterministic fallback.")
            self.ai_client = None
        return None

    def _setup_tables(self) -> None:
        self.conn.execute(
            """
            CREATE TABLE IF NOT EXISTS idea_candidates (
                candidate_id TEXT,
                cluster_id TEXT,
                pain_point TEXT,
                reasoning TEXT,
                evidence_count BIGINT,
                sources_present TEXT,
                willingness_signal TEXT,
                willingness_score DOUBLE,
                raw_opportunity_score DOUBLE,
                boosted_opportunity_score DOUBLE,
                impact_label TEXT,
                frequency_label TEXT,
                suggested_solution TEXT,
                pricing_hint TEXT,
                confidence_score DOUBLE,
                quality_score DOUBLE,
                quality_status TEXT,
                quality_reason TEXT,
                evidence_quotes TEXT,
                generated_at TIMESTAMP,
                batch_id TEXT
            )
            """
        )
        self.conn.execute("CREATE INDEX IF NOT EXISTS idx_idea_candidates_candidate_id ON idea_candidates(candidate_id)")

        self.conn.execute(
            """
            CREATE TABLE IF NOT EXISTS idea_evidence_map (
                candidate_id TEXT,
                review_fingerprint TEXT,
                source TEXT,
                source_item_id TEXT,
                entity_id TEXT,
                entity_name TEXT,
                category TEXT,
                reviewer_name TEXT,
                posted_at TEXT,
                url TEXT,
                quote_text TEXT,
                match_score DOUBLE,
                link_method TEXT,
                is_backfill BOOLEAN,
                linked_at TIMESTAMP,
                PRIMARY KEY(candidate_id, review_fingerprint)
            )
            """
        )

        self.conn.execute(
            """
            CREATE TABLE IF NOT EXISTS idea_publish_cards (
                candidate_id TEXT PRIMARY KEY,
                name TEXT,
                description TEXT,
                categories TEXT,
                difficulty TEXT,
                pain_point TEXT,
                reasoning TEXT,
                supporting_evidence TEXT,
                source_metadata TEXT,
                source TEXT,
                source_date TEXT,
                impact_label TEXT,
                frequency_label TEXT,
                opportunity_score DOUBLE,
                suggested_solution TEXT,
                pricing_hint TEXT,
                confidence_score DOUBLE,
                generated_at TIMESTAMP,
                batch_id TEXT
            )
            """
        )
        for col in [
            "name TEXT",
            "description TEXT",
            "categories TEXT",
            "difficulty TEXT",
            "source TEXT",
            "source_date TEXT",
        ]:
            try:
                self.conn.execute(f"ALTER TABLE idea_publish_cards ADD COLUMN {col}")
            except Exception:
                pass

        self.conn.execute(
            """
            CREATE TABLE IF NOT EXISTS idea_rejects (
                created_at TIMESTAMP,
                batch_id TEXT,
                cluster_id TEXT,
                pain_point TEXT,
                reason TEXT,
                quality_score DOUBLE,
                payload TEXT
            )
            """
        )

        self.conn.execute(
            """
            CREATE TABLE IF NOT EXISTS cost_ledger (
                ledger_date DATE PRIMARY KEY,
                total_inr DOUBLE,
                updated_at TIMESTAMP
            )
            """
        )
        try:
            self.conn.execute("ALTER TABLE reviews ADD COLUMN enriched_at TIMESTAMP")
        except Exception:
            pass

    def _today(self) -> date:
        return now_utc().date()

    def _today_spend(self) -> float:
        row = self.conn.execute("SELECT total_inr FROM cost_ledger WHERE ledger_date = ?", [self._today()]).fetchone()
        return float(row[0]) if row else 0.0

    def _budget_reached(self) -> bool:
        return self._today_spend() >= self.daily_budget_inr

    def _fetch_batch(self) -> List[Dict[str, Any]]:
        rows = self.conn.execute(
            """
            SELECT fingerprint, source, source_item_id, entity_id, entity_name, category, reviewer_name,
                   rating, comment_text, posted_at, url, country, language
            FROM reviews
            WHERE enriched_at IS NULL
              AND comment_text IS NOT NULL
              AND TRIM(comment_text) <> ''
            LIMIT ?
            """,
            [self.batch_size],
        ).fetchall()
        if self.enrich_allow_reprocess and len(rows) < self.batch_size:
            remaining = self.batch_size - len(rows)
            cooldown_cutoff = now_utc() - timedelta(hours=self.enrich_reprocess_cooldown_hours)
            seen_fp = {str(r[0]) for r in rows}
            extra = self.conn.execute(
                """
                SELECT fingerprint, source, source_item_id, entity_id, entity_name, category, reviewer_name,
                       rating, comment_text, posted_at, url, country, language
                FROM reviews
                WHERE enriched_at IS NOT NULL
                  AND enriched_at <= ?
                  AND comment_text IS NOT NULL
                  AND TRIM(comment_text) <> ''
                ORDER BY enriched_at ASC, scraped_at DESC
                LIMIT ?
                """,
                [cooldown_cutoff, max(1, remaining * 2)],
            ).fetchall()
            for r in extra:
                fp = str(r[0])
                if fp in seen_fp:
                    continue
                rows.append(r)
                seen_fp.add(fp)
                if len(rows) >= self.batch_size:
                    break
        cols = [
            "fingerprint", "source", "source_item_id", "entity_id", "entity_name", "category", "reviewer_name",
            "rating", "comment_text", "posted_at", "url", "country", "language",
        ]
        return [{cols[i]: row[i] for i in range(len(cols))} for row in rows]

    def _boost_score(self, source_count: int, score: float) -> float:
        boost = {1: 0, 2: 10, 3: 18, 4: 24, 5: 30, 6: 35}.get(max(1, min(6, source_count)), 0)
        return min(100.0, max(0.0, score + boost))

    def _upsert_candidate(self, rec: Dict[str, Any]) -> None:
        self.conn.execute("DELETE FROM idea_candidates WHERE candidate_id = ?", [rec["candidate_id"]])
        self.conn.execute(
            """
            INSERT INTO idea_candidates(
                candidate_id, cluster_id, pain_point, reasoning, evidence_count, sources_present,
                willingness_signal, willingness_score, raw_opportunity_score, boosted_opportunity_score,
                impact_label, frequency_label, suggested_solution, pricing_hint, confidence_score,
                quality_score, quality_status, quality_reason, evidence_quotes, generated_at, batch_id
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                rec["candidate_id"], rec["cluster_id"], rec["pain_point"], rec["reasoning"], rec["evidence_count"], rec["sources_present"],
                rec["willingness_signal"], rec["willingness_score"], rec["raw_opportunity_score"], rec["boosted_opportunity_score"],
                rec["impact_label"], rec["frequency_label"], rec["suggested_solution"], rec["pricing_hint"], rec["confidence_score"],
                rec["quality_score"], rec["quality_status"], rec["quality_reason"], rec["evidence_quotes"], rec["generated_at"], rec["batch_id"],
            ],
        )

    def _upsert_publish(self, rec: Dict[str, Any], supporting: List[Dict[str, Any]], source_meta: List[Dict[str, Any]]) -> None:
        self.conn.execute("DELETE FROM idea_publish_cards WHERE candidate_id = ?", [rec["candidate_id"]])
        self.conn.execute(
            """
            INSERT INTO idea_publish_cards(
                candidate_id, name, description, categories, difficulty,
                pain_point, reasoning, supporting_evidence, source_metadata, source, source_date,
                impact_label, frequency_label, opportunity_score, suggested_solution, pricing_hint,
                confidence_score, generated_at, batch_id
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                rec["candidate_id"], rec["name"], rec["description"], rec["categories"], rec["difficulty"],
                rec["pain_point"], rec["reasoning"],
                json.dumps(supporting, ensure_ascii=False), json.dumps(source_meta, ensure_ascii=False),
                rec["source"], rec["source_date"],
                rec["impact_label"], rec["frequency_label"], rec["boosted_opportunity_score"], rec["suggested_solution"], rec["pricing_hint"],
                rec["confidence_score"], rec["generated_at"], rec["batch_id"],
            ],
        )

    def _insert_links(self, candidate_id: str, rows: List[Dict[str, Any]]) -> None:
        for row in rows[:8]:
            fp = str(row.get("fingerprint") or "").strip()
            if not fp:
                continue
            self.conn.execute(
                """
                INSERT INTO idea_evidence_map(
                    candidate_id, review_fingerprint, source, source_item_id, entity_id, entity_name,
                    category, reviewer_name, posted_at, url, quote_text, match_score, link_method, is_backfill, linked_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT DO NOTHING
                """,
                [
                    candidate_id,
                    fp,
                    str(row.get("source") or ""),
                    str(row.get("source_item_id") or ""),
                    str(row.get("entity_id") or ""),
                    str(row.get("entity_name") or ""),
                    str(row.get("category") or ""),
                    str(row.get("reviewer_name") or ""),
                    str(row.get("posted_at") or ""),
                    str(row.get("url") or ""),
                    str(row.get("comment_text") or "")[:1200],
                    1.0,
                    "direct_cluster",
                    False,
                    now_utc(),
                ],
            )

    def _build_rec(self, key: str, rows: List[Dict[str, Any]], batch_id: str) -> Dict[str, Any]:
        comments = [str(r.get("comment_text") or "") for r in rows]
        terms = Counter()
        for c in comments:
            for t in tokenize(c):
                if t not in STOPWORDS:
                    terms[t] += 1
        top_terms = [x[0] for x in terms.most_common(6)] or ["workflow", "reliability"]

        domain = self._default_domain(rows)
        sources = sorted({str(r.get("source") or "") for r in rows if str(r.get("source") or "")})
        source_count = len(sources) if sources else 1

        ratings = []
        for r in rows:
            try:
                ratings.append(float(r.get("rating")))
            except Exception:
                pass
        avg_rating = sum(ratings) / max(1, len(ratings)) if ratings else 2.0

        full = " ".join(comments).lower()
        cue_hits = sum(full.count(c) for c in WILLINGNESS_CUES)
        heuristic_willingness_score = min(100.0, cue_hits * 10.0 + min(60, len(rows)) * 0.9 + source_count * 8.0)
        heuristic_willingness_label = "low" if heuristic_willingness_score < 35 else "medium" if heuristic_willingness_score < 70 else "high"

        base_score = min(100.0, 35.0 + min(45.0, len(rows) * 4.0) + min(15.0, source_count * 5.0) + heuristic_willingness_score * 0.1)
        boosted = self._boost_score(source_count, base_score)
        heuristic_confidence = min(0.95, 0.45 + min(0.35, len(rows) * 0.03) + min(0.15, source_count * 0.04))
        confidence = heuristic_confidence

        pain_point = f"{domain} users repeatedly report issues around {', '.join(top_terms[:3])}, causing delays, rework, and poor trust in existing tools."
        reasoning = f"Based on {len(rows)} negative complaints across {source_count} source(s) ({', '.join(sources)}). Recurring signals center on: {', '.join(top_terms[:5])}."
        solution = f"Build a focused {domain} operations assistant that detects and resolves {', '.join(top_terms[:2])} failures early, adds proactive alerts, and provides one-click remediation workflows."
        willingness_label = heuristic_willingness_label
        pricing_hint = "$49-$199/mo per team" if willingness_label == "high" else "$19-$79/mo per team" if willingness_label == "medium" else "$9-$29/mo starter"
        impact = "high" if avg_rating <= 1.6 or len(rows) >= 10 else "medium" if avg_rating <= 2.0 or len(rows) >= 5 else "low"
        frequency = "high" if len(rows) >= 10 else "medium" if len(rows) >= 5 else "low"
        inferred_location = ""
        ai_tags: List[str] = []

        support_pool = self._select_supporting_rows(rows, limit=max(self.max_supporting_quotes, self.ai_enrichment_max_rows))
        ai_payload = self._ai_enrich_cluster(
            key=key,
            rows=support_pool,
            top_terms=top_terms,
            sources=sources,
            avg_rating=avg_rating,
            evidence_count=len(rows),
        )
        support_rows = support_pool[: self.max_supporting_quotes]
        if ai_payload:
            ai_domain = self._clean_tag(ai_payload.get("domain_category"))
            if ai_domain:
                domain = ai_domain
            pain_point = str(ai_payload.get("pain_point") or pain_point).strip()[:420]
            reasoning = str(ai_payload.get("reasoning") or reasoning).strip()[:420]
            solution = str(ai_payload.get("suggested_solution") or solution).strip()[:420]
            impact = self._normalize_level(ai_payload.get("impact_label"), impact)
            frequency = self._normalize_level(ai_payload.get("frequency_label"), frequency)
            willingness_label = self._normalize_level(ai_payload.get("willingness_signal"), willingness_label)
            pricing_hint = str(ai_payload.get("pricing_hint") or pricing_hint).strip()[:80]
            inferred_location = re.sub(r"\s+", " ", str(ai_payload.get("location_hint") or "")).strip()[:90]
            ai_conf = self._bounded_float(ai_payload.get("confidence_score"), heuristic_confidence, 0.2, 0.98)
            confidence = min(0.98, max(0.35, heuristic_confidence * 0.6 + ai_conf * 0.4))
            raw_tags = ai_payload.get("category_tags")
            if isinstance(raw_tags, list):
                ai_tags = [str(x) for x in raw_tags]
            elif raw_tags:
                ai_tags = [x.strip() for x in str(raw_tags).split(",") if x.strip()]

            supporting_ids = ai_payload.get("supporting_comment_ids")
            if isinstance(supporting_ids, list):
                remapped: List[Dict[str, Any]] = []
                seen_fps = set()
                for rid in supporting_ids:
                    try:
                        idx = int(rid)
                    except Exception:
                        continue
                    if idx < 0 or idx >= len(support_pool):
                        continue
                    row = support_pool[idx]
                    fp = str(row.get("fingerprint") or "")
                    if fp and fp in seen_fps:
                        continue
                    remapped.append(row)
                    if fp:
                        seen_fps.add(fp)
                    if len(remapped) >= self.max_supporting_quotes:
                        break
                if remapped:
                    support_rows = remapped
                    if len(support_rows) < min(2, len(support_pool)):
                        for row in support_pool:
                            fp = str(row.get("fingerprint") or "")
                            if fp and fp in seen_fps:
                                continue
                            support_rows.append(row)
                            if fp:
                                seen_fps.add(fp)
                            if len(support_rows) >= min(self.max_supporting_quotes, len(support_pool)):
                                break

        title_terms = [t.title() for t in top_terms[:2]]
        base_name = f"{domain} {' '.join(title_terms)} Optimizer".strip()
        product_name = re.sub(r"\s+", " ", base_name)[:90]
        product_description = (
            f"A SaaS platform that helps {domain.lower()} teams reduce issues related to "
            f"{', '.join(top_terms[:3])} by automating detection, prioritization, and guided resolution workflows."
        )[:360]
        if ai_payload:
            ai_name = str(ai_payload.get("name") or "").strip()
            ai_description = str(ai_payload.get("description") or "").strip()
            if ai_name:
                product_name = re.sub(r"\s+", " ", ai_name)[:90]
            if ai_description:
                product_description = re.sub(r"\s+", " ", ai_description)[:360]
        category_tags = self._sanitize_category_tags(ai_tags, domain, top_terms)
        categories_csv = ", ".join(category_tags)

        primary_row = support_rows[0] if support_rows else rows[0]
        primary_source = str(primary_row.get("source") or "unknown")
        primary_date_raw = str(primary_row.get("posted_at") or "")
        primary_date = primary_date_raw[:10] if len(primary_date_raw) >= 10 else primary_date_raw

        cluster_id = hashlib.sha1(key.encode("utf-8")).hexdigest()[:16]
        candidate_id = hashlib.sha1((cluster_id + pain_point + batch_id).encode("utf-8")).hexdigest()[:24]

        willingness_floor = {"low": 30.0, "medium": 55.0, "high": 78.0}[willingness_label]
        willingness_score = max(heuristic_willingness_score, willingness_floor)
        base_score = min(100.0, 30.0 + min(45.0, len(rows) * 4.0) + min(18.0, source_count * 5.5) + willingness_score * 0.12 + confidence * 12.0)
        boosted = self._boost_score(source_count, base_score)
        difficulty = "Easy" if boosted < 60 else "Medium" if boosted < 80 else "Hard"

        source_gate = source_count >= self.min_sources_for_publish
        single_source_volume_gate = len(rows) >= max(6, self.min_cluster_size * 3)
        accepted = len(rows) >= self.min_cluster_size and confidence >= self.min_confidence and (source_gate or single_source_volume_gate)
        if accepted:
            quality_reason = "accepted"
        elif len(rows) < self.min_cluster_size:
            quality_reason = "insufficient_evidence"
        elif not source_gate and not single_source_volume_gate:
            quality_reason = "insufficient_sources"
        else:
            quality_reason = "low_confidence"
        quality_status = "accepted" if accepted else "rejected"

        evidence_quotes = self._supporting_payload(support_rows)

        return {
            "accepted": accepted,
            "candidate_id": candidate_id,
            "cluster_id": cluster_id,
            "pain_point": pain_point,
            "reasoning": reasoning,
            "evidence_count": len(rows),
            "sources_present": json.dumps(sources, ensure_ascii=False),
            "willingness_signal": willingness_label,
            "willingness_score": round(willingness_score, 2),
            "raw_opportunity_score": round(base_score, 2),
            "boosted_opportunity_score": round(boosted, 2),
            "impact_label": impact,
            "frequency_label": frequency,
            "suggested_solution": solution,
            "pricing_hint": pricing_hint,
            "name": product_name,
            "description": product_description,
            "categories": categories_csv,
            "difficulty": difficulty,
            "source": primary_source,
            "source_date": primary_date,
            "confidence_score": round(confidence, 2),
            "quality_score": round(confidence * 100, 2),
            "quality_status": quality_status,
            "quality_reason": quality_reason,
            "evidence_quotes": json.dumps(evidence_quotes, ensure_ascii=False),
            "generated_at": now_utc(),
            "batch_id": batch_id,
            "inferred_location": inferred_location,
            "top_terms": top_terms,
            "rows": rows,
            "support_rows": support_rows,
        }

    def _expand_variants(self, rec: Dict[str, Any]) -> List[Dict[str, Any]]:
        out = [rec]
        if not rec.get("accepted"):
            return out
        if self.candidate_variants_per_cluster <= 1:
            return out
        if int(rec.get("evidence_count") or 0) < self.variant_min_evidence:
            return out

        top_terms = [str(x).strip() for x in (rec.get("top_terms") or []) if str(x).strip()]
        if not top_terms:
            return out

        for idx in range(1, self.candidate_variants_per_cluster):
            term = top_terms[min(idx, len(top_terms) - 1)]
            v = dict(rec)
            v["pain_point"] = f"{str(rec.get('pain_point') or '').rstrip('.')} A recurring sub-problem is {term}."
            v["reasoning"] = f"{str(rec.get('reasoning') or '').rstrip('.')} Additional evidence points to {term} as a high-friction step."
            v["suggested_solution"] = f"{str(rec.get('suggested_solution') or '').rstrip('.')} Include an explicit workflow for {term}."
            v["name"] = f"{str(rec.get('name') or '').strip()} {term.title()}"[:90].strip()
            v["description"] = (
                f"{str(rec.get('description') or '').rstrip('.')} This variant prioritizes improvements around {term}."
            )[:360]
            v["candidate_id"] = hashlib.sha1(
                (str(rec.get("cluster_id") or "") + str(v["pain_point"]) + str(rec.get("batch_id") or "") + str(idx)).encode("utf-8")
            ).hexdigest()[:24]
            out.append(v)
        return out

    def _process_batch(self, rows: List[Dict[str, Any]]) -> bool:
        clusters: Dict[str, List[Dict[str, Any]]] = {}
        for row in rows:
            terms = [t for t in tokenize(row.get("comment_text")) if t not in STOPWORDS]
            if not terms:
                continue
            key = self._build_cluster_key(row)
            clusters.setdefault(key, []).append(row)

        merged_clusters: Dict[str, List[Dict[str, Any]]] = {}
        small_by_domain: Dict[str, List[Dict[str, Any]]] = {}
        for key, crows in clusters.items():
            domain = key.split("::", 1)[0]
            if len(crows) < self.min_cluster_size:
                small_by_domain.setdefault(domain, []).extend(crows)
            else:
                merged_clusters[key] = crows
        for domain, crows in small_by_domain.items():
            if len(crows) >= self.min_cluster_size:
                merged_clusters[f"{domain}::merged"] = crows
        clusters = merged_clusters if merged_clusters else clusters

        batch_id = hashlib.sha1((str(now_utc()) + str(len(rows))).encode("utf-8")).hexdigest()[:16]
        accepted = 0
        rejected = 0

        for key, crows in clusters.items():
            rec = self._build_rec(key, crows, batch_id)
            if not rec["accepted"]:
                rejected += 1
                self.conn.execute(
                    "INSERT INTO idea_rejects(created_at,batch_id,cluster_id,pain_point,reason,quality_score,payload) VALUES (?,?,?,?,?,?,?)",
                    [now_utc(), batch_id, rec["cluster_id"], rec["pain_point"], rec["quality_reason"], rec["quality_score"], rec["evidence_quotes"]],
                )
                continue

            for vrec in self._expand_variants(rec):
                self._upsert_candidate(vrec)
                self._insert_links(vrec["candidate_id"], vrec["support_rows"])
                source_meta = [
                    {
                        "source": str(r.get("source") or ""),
                        "entity_name": str(r.get("entity_name") or ""),
                        "seed_category": str(r.get("category") or ""),
                        "inferred_location": str(vrec.get("inferred_location") or ""),
                        "country": str(r.get("country") or ""),
                        "language": str(r.get("language") or ""),
                        "url": str(r.get("url") or ""),
                    }
                    for r in vrec["support_rows"][:8]
                ]
                self._upsert_publish(vrec, json.loads(vrec["evidence_quotes"]), source_meta)
                accepted += 1

        fps = [str(r.get("fingerprint")) for r in rows if str(r.get("fingerprint") or "")]
        if fps:
            placeholders = ", ".join(["?"] * len(fps))
            self.conn.execute(f"UPDATE reviews SET enriched_at = ? WHERE fingerprint IN ({placeholders})", [now_utc(), *fps])

        logging.info("batch_quality accepted=%s rejected=%s", accepted, rejected)
        return True

    def _bootstrap_publish_cards_from_existing(self, limit: int = 1000) -> int:
        rows = self.conn.execute(
            """
            SELECT ic.candidate_id, ic.pain_point, coalesce(ic.reasoning, ''), coalesce(ic.impact_label, 'medium'),
                   coalesce(ic.frequency_label, 'medium'), coalesce(ic.boosted_opportunity_score, 0),
                   coalesce(ic.suggested_solution, ''), coalesce(ic.pricing_hint, ''), coalesce(ic.confidence_score, 0),
                   coalesce(ic.batch_id, '')
            FROM idea_candidates ic
            WHERE coalesce(ic.quality_status, 'accepted') = 'accepted'
            ORDER BY ic.generated_at DESC
            LIMIT ?
            """,
            [limit],
        ).fetchall()
        created = 0
        for r in rows:
            ev = self.conn.execute("SELECT source,url,reviewer_name,posted_at,quote_text,match_score FROM idea_evidence_map WHERE candidate_id=? ORDER BY linked_at DESC LIMIT 8", [r[0]]).fetchall()
            if len(ev) < self.min_supporting_evidence_for_publish:
                continue
            supporting = [{"quote": str(x[4] or "")[:700], "source": str(x[0] or ""), "url": str(x[1] or ""), "author": str(x[2] or ""), "posted_at": str(x[3] or ""), "match_score": float(x[5] or 0)} for x in ev]
            source_meta = [{"source": str(x[0] or ""), "url": str(x[1] or ""), "author": str(x[2] or ""), "posted_at": str(x[3] or "")} for x in ev]
            rec = {
                "candidate_id": r[0], "pain_point": r[1], "reasoning": r[2], "impact_label": str(r[3]).lower(),
                "frequency_label": str(r[4]).lower(), "boosted_opportunity_score": float(r[5] or 0), "suggested_solution": r[6],
                "pricing_hint": r[7], "confidence_score": float(r[8] or 0), "generated_at": now_utc(), "batch_id": r[9],
                "name": (str(r[1] or "").split(" users ")[0].strip() + " Opportunity")[:90],
                "description": str(r[2] or r[6] or r[1] or "")[:360],
                "categories": ", ".join([x for x in [str(ev[0][0] if ev else ""), "Pain Intelligence"] if x])[:120],
                "difficulty": "Medium",
                "source": str(ev[0][0] if ev else "unknown"),
                "source_date": str(ev[0][3] if ev else "")[:10],
            }
            self._upsert_publish(rec, supporting, source_meta)
            created += 1
        return created

    def _cleanup_low_evidence_cards(self) -> int:
        rows = self.conn.execute("SELECT candidate_id, coalesce(supporting_evidence, '[]') FROM idea_publish_cards").fetchall()
        bad_ids: List[str] = []
        for cid, payload in rows:
            count = 0
            try:
                parsed = json.loads(payload) if isinstance(payload, str) else payload
                if isinstance(parsed, list):
                    count = len(parsed)
            except Exception:
                count = 0
            if count < self.min_supporting_evidence_for_publish:
                bad_ids.append(str(cid))
        if not bad_ids:
            return 0
        placeholders = ", ".join(["?"] * len(bad_ids))
        self.conn.execute(f"DELETE FROM idea_publish_cards WHERE candidate_id IN ({placeholders})", bad_ids)
        return len(bad_ids)

    def _export_daily(self) -> None:
        today_utc = now_utc().date()
        today_local = datetime.now().date()
        candidates = [today_utc, today_local]
        best_day = today_utc
        best_count = -1
        for dt in candidates:
            try:
                cnt = int(self.conn.execute("SELECT COUNT(*) FROM idea_candidates WHERE DATE(generated_at)=?", [dt]).fetchone()[0])
            except Exception:
                cnt = 0
            if cnt > best_count:
                best_count = cnt
                best_day = dt

        d = best_day.strftime("%Y%m%d")
        day = self.exports / best_day.strftime("%Y-%m-%d")
        day.mkdir(parents=True, exist_ok=True)

        ideas = self.conn.execute("SELECT * FROM idea_candidates WHERE DATE(generated_at)=?", [best_day]).fetchall()
        idea_cols = [x[0] for x in self.conn.execute("DESCRIBE idea_candidates").fetchall()]
        idea_items = [{idea_cols[i]: row[i] for i in range(len(idea_cols))} for row in ideas]
        with (day / f"ideas_{d}.json").open("w", encoding="utf-8") as f:
            json.dump(idea_items, f, ensure_ascii=False, indent=2, default=str)

        with (day / f"opportunity_clusters_{d}.json").open("w", encoding="utf-8") as f:
            json.dump(sorted(idea_items, key=lambda x: x.get("boosted_opportunity_score") or 0, reverse=True), f, ensure_ascii=False, indent=2, default=str)

        pseo = [{"candidate_id": x.get("candidate_id"), "slug": slugify(str(x.get("pain_point", ""))), "title": x.get("pain_point"), "score": x.get("boosted_opportunity_score"), "pricing_hint": x.get("pricing_hint"), "solution": x.get("suggested_solution")} for x in idea_items if x.get("quality_status") == "accepted"]
        with (day / f"pseo_seed_{d}.json").open("w", encoding="utf-8") as f:
            json.dump(pseo, f, ensure_ascii=False, indent=2, default=str)

        cards = self.conn.execute("SELECT * FROM idea_publish_cards WHERE DATE(generated_at)=? ORDER BY opportunity_score DESC, confidence_score DESC", [best_day]).fetchall()
        card_cols = [x[0] for x in self.conn.execute("DESCRIBE idea_publish_cards").fetchall()]
        card_items = [{card_cols[i]: row[i] for i in range(len(card_cols))} for row in cards]
        with (day / f"publish_cards_{d}.json").open("w", encoding="utf-8") as f:
            json.dump(card_items, f, ensure_ascii=False, indent=2, default=str)

    def _export_public_db(self) -> None:
        tmp_path = self.public_db_path.with_suffix(".tmp")
        if tmp_path.exists():
            try:
                tmp_path.unlink()
            except Exception:
                pass
        try:
            self.conn.execute(f"ATTACH '{tmp_path}' AS pub")
            self.conn.execute("CREATE TABLE pub.idea_publish_cards AS SELECT * FROM idea_publish_cards")
            self.conn.execute("CREATE TABLE pub.idea_candidates AS SELECT * FROM idea_candidates")
            self.conn.execute("CREATE TABLE pub.idea_evidence_map AS SELECT * FROM idea_evidence_map")
            self.conn.execute("CREATE INDEX IF NOT EXISTS idx_pub_candidate ON pub.idea_publish_cards(candidate_id)")
            self.conn.execute("CREATE INDEX IF NOT EXISTS idx_evidence_candidate ON pub.idea_evidence_map(candidate_id)")
            self.conn.execute("CREATE INDEX IF NOT EXISTS idx_candidates_score ON pub.idea_candidates(boosted_opportunity_score)")
        finally:
            try:
                self.conn.execute("DETACH pub")
            except Exception:
                pass
        if self.public_db_path.exists():
            try:
                self.public_db_path.unlink()
            except Exception:
                pass
        tmp_path.replace(self.public_db_path)

    def process_once(self) -> int:
        self._connect_db(60, 5)
        try:
            if self.bootstrap_publish_on_start:
                self._bootstrap_publish_cards_from_existing(3000)
                self._cleanup_low_evidence_cards()

            if self._budget_reached():
                logging.warning("Daily budget reached: %.2f INR", self._today_spend())
                return 0

            done = 0
            for _ in range(self.max_batches):
                batch = self._fetch_batch()
                if not batch:
                    break
                self._process_batch(batch)
                done += 1

            self._bootstrap_publish_cards_from_existing(500)
            self._cleanup_low_evidence_cards()
            if done > 0:
                self._export_daily()
            self._export_public_db()
            logging.info("process_once complete batches=%s", done)
            return done
        finally:
            self._close_db()

    def _records_count(self) -> int:
        self._connect_db(60, 2)
        try:
            row = self.conn.execute("SELECT COUNT(*) FROM reviews").fetchone()
            return int(row[0]) if row else 0
        finally:
            self._close_db()

    def run_fast(self) -> None:
        start = now_utc()
        logging.info("processor fast mode started (AI enrichment %s)", "enabled" if self.ai_client else "fallback-only")
        while True:
            self.process_once()
            if self._records_count() >= self.target_records:
                logging.info("processor fast mode finished: target reached")
                return
            if now_utc() - start >= timedelta(days=self.fast_days):
                logging.info("processor fast mode finished: max days reached")
                return
            time.sleep(max(30, self.fast_sleep))

    def _scheduled(self) -> None:
        try:
            self.process_once()
        except Exception:
            logging.exception("scheduled process run failed")

    def run_incremental_forever(self) -> None:
        self.scheduler.add_job(self._scheduled, trigger="interval", hours=max(1, self.interval_hours), next_run_time=now_utc(), max_instances=1, coalesce=True, id="enrich", replace_existing=True)
        self.scheduler.start()
        logging.info("processor incremental mode every %s hour(s)", self.interval_hours)
        while True:
            time.sleep(30)


if __name__ == "__main__":
    e = Enricher()
    if e.fast_mode:
        e.run_fast()
    e.run_incremental_forever()
