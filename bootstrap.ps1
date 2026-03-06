param(
    [string]$ProjectId
)

$ErrorActionPreference = "Stop"
Set-Location -Path $PSScriptRoot

if (-not (Test-Path ".venv")) {
    python -m venv .venv
}

& ".\.venv\Scripts\python.exe" -m pip install --upgrade pip
& ".\.venv\Scripts\python.exe" -m pip install -r requirements.txt

if (-not (Test-Path ".env")) {
    Copy-Item ".env.example" ".env"
}

if ($ProjectId) {
    [Environment]::SetEnvironmentVariable("PROJECT_ID", $ProjectId, "User")
    Write-Host "Set PROJECT_ID in User environment."
}

Write-Host "Bootstrap complete."
Write-Host "Next:"
Write-Host "1) Edit .env and fill missing values."
Write-Host "2) Start manual runs:"
Write-Host "   .\\run_scraper.bat"
Write-Host "   .\\run_process.bat"
Write-Host "3) Setup scheduled tasks:"
Write-Host "   powershell -ExecutionPolicy Bypass -File .\\setup_task_scheduler.ps1"
