import atexit
import ctypes
import json
import os
import subprocess
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Optional

import duckdb


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def load_env_file(env_path: Path) -> None:
    if not env_path.exists():
        return
    for line in env_path.read_text(encoding="utf-8").splitlines():
        raw = line.strip()
        if not raw or raw.startswith("#") or "=" not in raw:
            continue
        key, value = raw.split("=", 1)
        key = key.strip()
        value = value.strip().strip('"').strip("'")
        if key and key not in os.environ:
            os.environ[key] = value


def process_count(pattern: str) -> int:
    cmd = (
        "$p = Get-CimInstance Win32_Process | "
        f"Where-Object {{ $_.Name -eq 'python.exe' -and $_.CommandLine -match '{pattern}' }}; "
        "$parents = @($p | Select-Object -ExpandProperty ParentProcessId); "
        "($p | Where-Object { $parents -notcontains $_.ProcessId } | Measure-Object | Select-Object -ExpandProperty Count)"
    )
    result = subprocess.run(
        ["powershell", "-NoProfile", "-Command", cmd],
        capture_output=True,
        text=True,
        check=False,
    )
    text = (result.stdout or "").strip()
    try:
        return int(text)
    except ValueError:
        return 0


def tail_line(path: Path) -> str:
    if not path.exists():
        return ""
    content = path.read_text(encoding="utf-8", errors="replace").splitlines()
    if not content:
        return ""
    return content[-1][-400:]


def safe_db_metrics(db_path: Path) -> Dict[str, Optional[object]]:
    metrics: Dict[str, Optional[object]] = {
        "db_read_ok": False,
        "db_error": None,
        "reviews_count": None,
        "ideas_count": None,
        "run_log_count": None,
        "seed_running_count": None,
        "seed_error_count": None,
    }
    if not db_path.exists():
        metrics["db_error"] = f"DB file not found: {db_path}"
        return metrics

    try:
        con = duckdb.connect(str(db_path), read_only=True)
        try:
            metrics["reviews_count"] = int(con.execute("SELECT COUNT(*) FROM reviews").fetchone()[0])
        except Exception:
            pass
        try:
            metrics["ideas_count"] = int(con.execute("SELECT COUNT(*) FROM idea_candidates").fetchone()[0])
        except Exception:
            pass
        try:
            metrics["run_log_count"] = int(con.execute("SELECT COUNT(*) FROM run_log").fetchone()[0])
        except Exception:
            pass
        try:
            metrics["seed_running_count"] = int(
                con.execute("SELECT COUNT(*) FROM seed_progress WHERE status = 'running'").fetchone()[0]
            )
            metrics["seed_error_count"] = int(
                con.execute("SELECT COUNT(*) FROM seed_progress WHERE status = 'error'").fetchone()[0]
            )
        except Exception:
            pass
        con.close()
        metrics["db_read_ok"] = True
    except Exception as exc:
        metrics["db_error"] = str(exc)
    return metrics


def latest_cycle_metrics(exports_dir: Path) -> Dict[str, Optional[object]]:
    result: Dict[str, Optional[object]] = {
        "latest_cycle_mode": None,
        "latest_cycle_total_records": None,
        "latest_cycle_inserted": None,
        "latest_cycle_seen": None,
        "latest_cycle_errors": None,
        "latest_cycle_run_at": None,
    }
    if not exports_dir.exists():
        return result

    files = sorted(exports_dir.glob("*/cycle_stats.jsonl"), key=lambda p: p.stat().st_mtime, reverse=True)
    if not files:
        return result

    lines = files[0].read_text(encoding="utf-8", errors="replace").splitlines()
    if not lines:
        return result
    try:
        payload = json.loads(lines[-1])
    except Exception:
        return result

    stats = payload.get("stats", {}) if isinstance(payload, dict) else {}
    result["latest_cycle_mode"] = payload.get("mode")
    result["latest_cycle_total_records"] = payload.get("total_records")
    result["latest_cycle_inserted"] = stats.get("inserted")
    result["latest_cycle_seen"] = stats.get("seen")
    result["latest_cycle_errors"] = stats.get("errors")
    result["latest_cycle_run_at"] = payload.get("run_at")
    return result


def write_snapshot(logs_dir: Path, snapshot: Dict[str, object]) -> None:
    logs_dir.mkdir(parents=True, exist_ok=True)
    json_path = logs_dir / "pipeline_status.json"
    line_path = logs_dir / "pipeline_status.log"
    json_path.write_text(json.dumps(snapshot, ensure_ascii=False, indent=2), encoding="utf-8")
    with line_path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(snapshot, ensure_ascii=False) + "\n")


def load_runtime_metrics(path: Path) -> Dict[str, object]:
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text(encoding="utf-8", errors="replace"))
    except Exception:
        return {}


def _pid_running(pid: int) -> bool:
    if pid <= 0:
        return False
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    except OSError:
        return False
    return True


def acquire_single_instance_lock(lock_path: Path) -> int:
    kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
    create_mutex = kernel32.CreateMutexW
    create_mutex.argtypes = [ctypes.c_void_p, ctypes.c_int, ctypes.c_wchar_p]
    create_mutex.restype = ctypes.c_void_p
    mutex_handle = create_mutex(None, 0, "Global\\StartupIdeaDB_Watcher")
    if not mutex_handle:
        raise RuntimeError("Failed to acquire watcher mutex.")
    if ctypes.get_last_error() == 183:  # ERROR_ALREADY_EXISTS
        kernel32.CloseHandle(mutex_handle)
        raise RuntimeError("pipeline_watch already running.")

    if lock_path.exists():
        stale_pid = 0
        try:
            stale_pid = int(lock_path.read_text(encoding="utf-8", errors="replace").strip() or "0")
        except Exception:
            stale_pid = 0
        if stale_pid and _pid_running(stale_pid):
            raise RuntimeError(f"pipeline_watch already running (PID {stale_pid})")
        try:
            lock_path.unlink(missing_ok=True)
        except Exception:
            pass

    fd = os.open(str(lock_path), os.O_CREAT | os.O_EXCL | os.O_RDWR)
    os.write(fd, str(os.getpid()).encode("utf-8"))
    setattr(acquire_single_instance_lock, "_mutex_handle", mutex_handle)
    return fd


def release_single_instance_lock(lock_fd: int, lock_path: Path) -> None:
    try:
        os.close(lock_fd)
    except Exception:
        pass
    try:
        lock_path.unlink(missing_ok=True)
    except Exception:
        pass
    mutex_handle = getattr(acquire_single_instance_lock, "_mutex_handle", None)
    if mutex_handle:
        try:
            ctypes.WinDLL("kernel32", use_last_error=True).CloseHandle(mutex_handle)
        except Exception:
            pass
        setattr(acquire_single_instance_lock, "_mutex_handle", None)


def main() -> None:
    base_dir = Path(__file__).resolve().parent
    logs_dir = base_dir / "logs"
    load_env_file(base_dir / ".env")
    lock_path = base_dir / "watcher.lock"

    try:
        lock_fd = acquire_single_instance_lock(lock_path)
    except Exception as exc:
        print(f"pipeline_watch not started: {exc}")
        return
    atexit.register(release_single_instance_lock, lock_fd, lock_path)

    interval_seconds = int(os.getenv("WATCH_INTERVAL_SECONDS", "60"))
    db_path = Path(os.getenv("DB_PATH", str(base_dir / "runtime.db")))
    orchestrator_log = logs_dir / "orchestrator.log"
    scraper_log = logs_dir / "scraper.log"
    process_log = logs_dir / "process.log"
    cycle_exports = base_dir / "exports"
    runtime_metrics_path = logs_dir / "runtime_metrics.json"

    while True:
        orchestrator_count = process_count("orchestrator.py")
        scraper_count = process_count("scraper.py")
        processor_count = process_count("process.py")
        watcher_count = process_count("pipeline_watch.py")
        orchestrator_last_log = tail_line(orchestrator_log) if orchestrator_count >= 1 else ""
        scraper_last_log = tail_line(scraper_log) if scraper_count >= 1 else ""
        process_last_log = tail_line(process_log) if processor_count >= 1 else ""
        status = {
            "timestamp_utc": now_iso(),
            "orchestrator_process_count": orchestrator_count,
            "scraper_process_count": scraper_count,
            "processor_process_count": processor_count,
            "watcher_process_count": watcher_count,
            "orchestrator_running": orchestrator_count >= 1,
            "scraper_running": scraper_count >= 1,
            "processor_running": processor_count >= 1,
            "watcher_running": watcher_count >= 1,
            "orchestrator_last_log": orchestrator_last_log,
            "scraper_last_log": scraper_last_log,
            "process_last_log": process_last_log,
            "db_file": str(db_path),
            "db_file_exists": db_path.exists(),
            "db_file_size_bytes": db_path.stat().st_size if db_path.exists() else None,
        }
        status.update(safe_db_metrics(db_path))
        runtime_metrics = load_runtime_metrics(runtime_metrics_path)
        if not status.get("db_read_ok"):
            status.update(latest_cycle_metrics(cycle_exports))
            if runtime_metrics:
                status["runtime_metrics"] = runtime_metrics
                if status.get("orchestrator_last_log"):
                    status["orchestrator_last_log"] = f"{status['orchestrator_last_log']} | runtime_phase={runtime_metrics.get('phase','')}"
        elif runtime_metrics:
            status["runtime_metrics"] = runtime_metrics
        write_snapshot(logs_dir, status)
        time.sleep(max(15, interval_seconds))


if __name__ == "__main__":
    main()
