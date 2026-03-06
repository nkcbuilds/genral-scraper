# StartupIdeaDB Scraper

Windows-first scraper and enrichment pipeline for collecting product pain-point evidence, clustering it into startup opportunities, and exporting public-ready idea cards.

## Included

- Source code for scraping, enrichment, orchestration, and watcher processes
- Seed files and seed-generation scripts
- Windows bootstrap and task-scheduler helpers
- Sample `.env.example` without live credentials

## Not Included

- Runtime databases
- Logs and exports
- Local virtual environments
- Real API tokens, AI keys, or other secrets

## Quick Start

```powershell
python -m venv .venv
.\.venv\Scripts\python.exe -m pip install -r requirements.txt
Copy-Item .env.example .env
```

Fill in `.env`, then run:

```powershell
cmd /c run_all.bat
```

For more operational detail, see `RUNBOOK.md`.
