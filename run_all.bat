@echo off
setlocal
cd /d "%~dp0"

call stop_all.bat
timeout /t 2 >nul

for /f %%I in ('powershell -NoProfile -Command "Get-CimInstance Win32_Process ^| Where-Object { $_.Name -eq 'python.exe' -and $_.CommandLine -match 'orchestrator.py' } ^| Measure-Object ^| Select-Object -ExpandProperty Count"') do set ORC_COUNT=%%I
if not defined ORC_COUNT set ORC_COUNT=0
if "%ORC_COUNT%"=="0" start "StartupIdeaDB Orchestrator" /min cmd /c "\"%CD%\run_orchestrator.bat\""
timeout /t 2 >nul
for /f %%I in ('powershell -NoProfile -Command "Get-CimInstance Win32_Process ^| Where-Object { $_.Name -eq 'python.exe' -and $_.CommandLine -match 'pipeline_watch.py' } ^| Measure-Object ^| Select-Object -ExpandProperty Count"') do set WATCH_COUNT=%%I
if not defined WATCH_COUNT set WATCH_COUNT=0
if "%WATCH_COUNT%"=="0" start "StartupIdeaDB Watch" /min cmd /c "\"%CD%\run_watch.bat\""

echo Started orchestrator + watcher.
echo Logs:
echo   logs\orchestrator.log
echo   logs\watch.log
echo   logs\pipeline_status.json
endlocal
