# StartupIdeaDB Project Data Dictionary

This document describes the runtime schema and file roles used by the scraper pipeline. It intentionally avoids including live row counts, snapshots, or exported records.

## Databases

### `runtime.db`
Primary operational DuckDB database used by the scraper and enrichment pipeline.

### `public.db`
Publish-oriented DuckDB snapshot containing the public-facing opportunity tables.

### `reviews.db`
Legacy database path retained for compatibility with older run modes.

## Runtime Tables

### `reviews`
Normalized source records gathered from enabled ingestion connectors.

Common columns:

| Column | Type | Meaning |
|---|---|---|
| `source` | `VARCHAR` | Source system such as `play_store`, `app_store`, `reddit`, `hacker_news`, or `upwork` |
| `source_item_id` | `VARCHAR` | Native item identifier from the source |
| `entity_id` | `VARCHAR` | App, subreddit, query, or product identifier used for grouping |
| `entity_name` | `VARCHAR` | Human-readable app, subreddit, or query label |
| `category` | `VARCHAR` | Seed or inferred category |
| `reviewer_name` | `VARCHAR` | Source author or reviewer name when available |
| `rating` | `DOUBLE` | Numeric rating when available |
| `comment_text` | `VARCHAR` | Normalized review, post, or job text |
| `posted_at` | `TIMESTAMP/TEXT` | Source event time |
| `url` | `VARCHAR` | Canonical source URL |
| `country` | `VARCHAR` | Country code if available |
| `language` | `VARCHAR` | Language code |
| `raw_json` | `JSON/TEXT` | Original payload snapshot for troubleshooting |
| `fingerprint` | `VARCHAR` | Deterministic dedupe key |
| `enriched_at` | `TIMESTAMP` | Time the record was processed into opportunity candidates |

### `seed_progress`
Tracks run status for app, subreddit, query, and marketplace seeds.

### `source_cursor`
Stores incremental cursors used to avoid re-fetching older records.

### `run_log`
Stores per-cycle run statistics, mode, and aggregate counts.

### `idea_candidates`
Intermediate and accepted opportunity records generated from clustered evidence.

Representative fields:

| Column | Meaning |
|---|---|
| `candidate_id` | Stable identifier for a generated idea |
| `cluster_id` | Evidence-cluster identifier |
| `pain_point` | Condensed pain statement |
| `reasoning` | Grounded explanation from source evidence |
| `suggested_solution` | Proposed product direction |
| `pricing_hint` | Pricing signal inferred from evidence |
| `confidence_score` | Confidence score used by quality gating |
| `boosted_opportunity_score` | Opportunity score with source-count boost |
| `quality_status` | Acceptance status |
| `generated_at` | Candidate generation time |

### `idea_evidence_map`
Maps each candidate to supporting quotes and source metadata.

### `idea_publish_cards`
Public-facing, site-ready records built from accepted candidates plus supporting evidence.

### `idea_rejects`
Rejected clusters retained for debugging and threshold tuning.

## Files And Directories

### `seeds/`
Bootstrap seed files used to discover apps, communities, marketplace products, and query terms.

### `exports/`
Generated JSON exports and daily cycle stats. These are runtime artifacts and should not be committed.

### `logs/`
Pipeline logs plus watcher status snapshots. These are runtime artifacts and should not be committed.

## Operational Notes

- `orchestrator.py` runs scrape and process phases in a single coordinated loop.
- `scraper.py` ingests raw evidence into `runtime.db`.
- `process.py` clusters evidence, generates candidates, and exports `public.db`.
- `pipeline_watch.py` writes lightweight health snapshots to `logs/pipeline_status.json`.
