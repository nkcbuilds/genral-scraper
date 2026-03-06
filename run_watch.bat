@echo off
setlocal
cd /d "%~dp0"
if not exist "logs" mkdir "logs"
".venv\Scripts\python.exe" -u pipeline_watch.py >> "logs\watch.log" 2>&1
endlocal
