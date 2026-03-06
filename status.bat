@echo off
setlocal
cd /d "%~dp0"
echo Process status:
powershell -NoProfile -Command ^
  "$p = Get-CimInstance Win32_Process | Where-Object { $_.Name -eq 'python.exe' -and $_.CommandLine -match 'orchestrator.py|pipeline_watch.py|scraper.py|process.py' }; $parents = @($p | Select-Object -ExpandProperty ParentProcessId); $p | Where-Object { $parents -notcontains $_.ProcessId } | Select-Object ProcessId,CommandLine | Format-Table -AutoSize"
echo.
echo Latest pipeline snapshot:
if exist "logs\pipeline_status.json" (
  type "logs\pipeline_status.json"
) else (
  echo logs\pipeline_status.json not found yet.
)
endlocal
