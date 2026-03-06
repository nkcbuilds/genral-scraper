@echo off
setlocal
cd /d "%~dp0"
powershell -NoProfile -Command ^
  "Get-CimInstance Win32_Process | Where-Object { $_.Name -eq 'python.exe' -and $_.CommandLine -match 'orchestrator.py|scraper.py|process.py|pipeline_watch.py' } | ForEach-Object { Stop-Process -Id $_.ProcessId -Force }"
echo Stopped orchestrator/scraper/process/watch processes.
endlocal
