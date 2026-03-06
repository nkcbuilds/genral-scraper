@echo off
setlocal
cd /d "%~dp0"
if not exist "logs" mkdir "logs"
".venv\Scripts\python.exe" scraper.py >> "logs\scraper.log" 2>&1
endlocal
