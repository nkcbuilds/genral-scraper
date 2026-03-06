param(
    [string]$TaskPrefix = "StartupIdeaDB",
    [string]$WorkDir = $PSScriptRoot
)

$ErrorActionPreference = "Stop"

$orchestratorTaskName = "$TaskPrefix-Orchestrator"
$watchTaskName = "$TaskPrefix-Watcher"
$legacyScraperTaskName = "$TaskPrefix-Scraper"
$legacyProcessorTaskName = "$TaskPrefix-Processor"

$orchestratorBat = Join-Path $WorkDir "run_orchestrator.bat"
$watchBat = Join-Path $WorkDir "run_watch.bat"

if (-not (Test-Path $orchestratorBat)) { throw "Missing run_orchestrator.bat at $orchestratorBat" }
if (-not (Test-Path $watchBat)) { throw "Missing run_watch.bat at $watchBat" }

$principal = New-ScheduledTaskPrincipal -UserId "$env:USERDOMAIN\$env:USERNAME" -LogonType S4U -RunLevel Highest
$settings = New-ScheduledTaskSettingsSet `
    -AllowStartIfOnBatteries `
    -DontStopIfGoingOnBatteries `
    -StartWhenAvailable `
    -MultipleInstances IgnoreNew `
    -RestartCount 10 `
    -RestartInterval (New-TimeSpan -Minutes 1) `
    -ExecutionTimeLimit (New-TimeSpan -Days 3650)

$startupTrigger = New-ScheduledTaskTrigger -AtStartup
$logonTrigger = New-ScheduledTaskTrigger -AtLogOn

$orchestratorAction = New-ScheduledTaskAction -Execute "cmd.exe" -Argument "/c `"$orchestratorBat`"" -WorkingDirectory $WorkDir
$watchAction = New-ScheduledTaskAction -Execute "cmd.exe" -Argument "/c `"$watchBat`"" -WorkingDirectory $WorkDir

try { Unregister-ScheduledTask -TaskName $orchestratorTaskName -Confirm:$false -ErrorAction Stop } catch {}
try { Unregister-ScheduledTask -TaskName $watchTaskName -Confirm:$false -ErrorAction Stop } catch {}
try { Unregister-ScheduledTask -TaskName $legacyScraperTaskName -Confirm:$false -ErrorAction Stop } catch {}
try { Unregister-ScheduledTask -TaskName $legacyProcessorTaskName -Confirm:$false -ErrorAction Stop } catch {}
$tasksCreated = $false

function Register-Tasks([System.Collections.IEnumerable]$triggers) {
    Register-ScheduledTask `
        -TaskName $orchestratorTaskName `
        -Action $orchestratorAction `
        -Trigger $triggers `
        -Principal $principal `
        -Settings $settings `
        -Description "StartupIdeaDB single-orchestrator 24/7 task"

    Register-ScheduledTask `
        -TaskName $watchTaskName `
        -Action $watchAction `
        -Trigger $triggers `
        -Principal $principal `
        -Settings $settings `
        -Description "StartupIdeaDB watcher 24/7 task"
}

function Install-StartupFolderFallback() {
    $startupDir = [Environment]::GetFolderPath("Startup")
    $legacyScraperFallback = Join-Path $startupDir "StartupIdeaDB-Scraper.cmd"
    $legacyProcessorFallback = Join-Path $startupDir "StartupIdeaDB-Processor.cmd"
    $orchestratorFallback = Join-Path $startupDir "StartupIdeaDB-Orchestrator.cmd"
    $watchFallback = Join-Path $startupDir "StartupIdeaDB-Watcher.cmd"

    if (Test-Path $legacyScraperFallback) { Remove-Item $legacyScraperFallback -Force }
    if (Test-Path $legacyProcessorFallback) { Remove-Item $legacyProcessorFallback -Force }

    @"
@echo off
start "StartupIdeaDB Orchestrator" /min cmd /c "$orchestratorBat"
"@ | Set-Content -Path $orchestratorFallback

    @"
@echo off
start "StartupIdeaDB Watcher" /min cmd /c "$watchBat"
"@ | Set-Content -Path $watchFallback

    Write-Warning "ScheduledTask registration failed. Startup-folder fallback installed:"
    Write-Host " - $orchestratorFallback"
    Write-Host " - $watchFallback"
}

try {
    Register-Tasks @($startupTrigger, $logonTrigger)
    $tasksCreated = $true
    Write-Host "Created tasks with Startup + Logon triggers."
}
catch {
    Write-Warning "Could not register startup trigger (likely admin rights). Falling back to Logon trigger only."
    try {
        Register-Tasks @($logonTrigger)
        $tasksCreated = $true
        Write-Host "Created tasks with Logon trigger only."
    }
    catch {
        Install-StartupFolderFallback
    }
}

if ($tasksCreated) {
    Write-Host "Created tasks:"
    Write-Host " - $orchestratorTaskName"
    Write-Host " - $watchTaskName"
    Write-Host "Run now (optional):"
    Write-Host "Start-ScheduledTask -TaskName `"$orchestratorTaskName`""
    Write-Host "Start-ScheduledTask -TaskName `"$watchTaskName`""
}
