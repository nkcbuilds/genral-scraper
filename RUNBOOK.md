# StartupIdeaDB Scraper Runbook

## 1) Bootstrap
```powershell
cd path\to\startupideadb-scraper
powershell -ExecutionPolicy Bypass -File .\bootstrap.ps1
```

## 2) Configure
- Edit `.env` and set at least:
  - `PROJECT_ID`
  - optional `REDDIT_CLIENT_ID` / `REDDIT_CLIENT_SECRET`
  - optional HTML source toggles:
    - `HACKERNEWS_ENABLED=true|false`
    - `PRODUCTHUNT_ENABLED=true|false`
  - for Upwork first-party ingestion:
    - `UPWORK_API_ENABLED=true`
    - `UPWORK_API_URL`, `UPWORK_API_TOKEN`
    - optional policy caps: `UPWORK_API_MAX_RPS`, `UPWORK_API_MAX_RPM`, `UPWORK_API_MAX_DAILY`
    - optional strict mode: `UPWORK_API_FIRST_PARTY_REQUIRED=true`

## 3) Start manually
```powershell
.\run_orchestrator.bat
.\run_watch.bat
```

## 3b) One-click start/stop/status
```powershell
cmd /c run_all.bat
cmd /c status.bat
cmd /c stop_all.bat
```

## 4) Persistence
```powershell
powershell -ExecutionPolicy Bypass -File .\setup_task_scheduler.ps1
```

If Windows blocks Scheduled Tasks (permission denied), the script automatically installs fallback launchers in your Startup folder:
- `StartupIdeaDB-Orchestrator.cmd`
- `StartupIdeaDB-Watcher.cmd`

## 5) Data locations
- DB: `reviews.db`
- Active runtime DB: `runtime.db`
- Public snapshot DB (site-ready): `public.db`
- Seeds: `seeds\`
  - `hackernews_queries.csv`
  - `producthunt_queries.csv`
- Exports: `exports\YYYY-MM-DD\`
- Health snapshots: `logs\pipeline_status.json` and `logs\pipeline_status.log`

## 6) Quick health checks
```powershell
.\.venv\Scripts\python -m py_compile scraper.py process.py orchestrator.py pipeline_watch.py
.\.venv\Scripts\python -c "import duckdb; print(duckdb.connect('runtime.db', read_only=True).execute('select count(*) from reviews').fetchone()[0])"
```
