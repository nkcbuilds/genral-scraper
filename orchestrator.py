import atexit
import ctypes
import logging
import os
import random
import subprocess
import time
from datetime import timedelta
from pathlib import Path
from typing import Optional

from process import Enricher, now_utc
from scraper import Engine, load_env_file


class SingleOrchestrator:
    def __init__(self) -> None:
        self.base_dir = Path(__file__).resolve().parent
        load_env_file(self.base_dir / ".env")

        logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")

        self.instance_lock_path = self.base_dir / "orchestrator.lock"
        self._mutex_handle = None
        self._lock_fd: Optional[int] = None
        self._acquire_single_instance_lock()

        # Release stale locks from legacy workers when switching modes.
        self._stop_legacy_workers()

        self.engine = Engine()
        self.enricher = Enricher()
        self.fast_enrich_every_n_cycles = max(1, self._parse_int_env("FAST_ENRICH_EVERY_N_CYCLES", 1))
        self._fast_cycle_count = 0

    @staticmethod
    def _parse_int_env(name: str, default: int) -> int:
        raw = os.getenv(name)
        try:
            return int(raw) if raw is not None else default
        except ValueError:
            return default

    def _acquire_single_instance_lock(self) -> None:
        self._acquire_named_mutex()
        stale_pid = None
        if self.instance_lock_path.exists():
            try:
                stale_pid = int(self.instance_lock_path.read_text(encoding="utf-8").strip() or "0")
            except Exception:
                stale_pid = None
            if stale_pid and self._pid_running(stale_pid):
                raise RuntimeError(
                    f"Another orchestrator instance is already running (PID {stale_pid}). Not starting a new instance."
                )
            try:
                self.instance_lock_path.unlink(missing_ok=True)
            except Exception:
                pass

        try:
            fd = os.open(str(self.instance_lock_path), os.O_CREAT | os.O_EXCL | os.O_RDWR)
        except FileExistsError as exc:
            raise RuntimeError("Another orchestrator instance is already running. Not starting a new instance.") from exc

        self._lock_fd = fd
        os.write(fd, str(os.getpid()).encode("utf-8"))
        atexit.register(self._release_lock)

    def _acquire_named_mutex(self) -> None:
        kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
        create_mutex = kernel32.CreateMutexW
        create_mutex.argtypes = [ctypes.c_void_p, ctypes.c_int, ctypes.c_wchar_p]
        create_mutex.restype = ctypes.c_void_p
        handle = create_mutex(None, 0, "Global\\StartupIdeaDB_Orchestrator")
        if not handle:
            raise RuntimeError("Failed to acquire orchestrator mutex.")
        err = ctypes.get_last_error()
        if err == 183:  # ERROR_ALREADY_EXISTS
            kernel32.CloseHandle(handle)
            raise RuntimeError("Another orchestrator instance is already running. Not starting a new instance.")
        self._mutex_handle = handle

    def _pid_running(self, pid: int) -> bool:
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

    def _release_lock(self) -> None:
        if self._lock_fd is None:
            return
        try:
            os.close(self._lock_fd)
        except Exception:
            pass
        try:
            self.instance_lock_path.unlink(missing_ok=True)
        except Exception:
            pass
        if self._mutex_handle:
            try:
                ctypes.WinDLL("kernel32", use_last_error=True).CloseHandle(self._mutex_handle)
            except Exception:
                pass
            self._mutex_handle = None
        self._lock_fd = None

    def _stop_legacy_workers(self) -> None:
        cmd = (
            "Get-CimInstance Win32_Process | "
            "Where-Object { $_.Name -eq 'python.exe' -and $_.CommandLine -match 'scraper.py|process.py' } | "
            "ForEach-Object { Stop-Process -Id $_.ProcessId -Force }"
        )
        subprocess.run(["powershell", "-NoProfile", "-Command", cmd], capture_output=True, text=True, check=False)
        time.sleep(1)

    def _run_scrape_then_process(self, fast_mode: bool) -> None:
        try:
            self.engine.run_cycle(fast_mode)
        except Exception:
            logging.exception("Scraper cycle failed")

        should_process = True
        if fast_mode and self.fast_enrich_every_n_cycles > 1:
            self._fast_cycle_count += 1
            should_process = (self._fast_cycle_count % self.fast_enrich_every_n_cycles) == 0
            if not should_process:
                logging.info(
                    "Skipping enrichment this fast cycle (%s/%s) to maximize scrape throughput",
                    self._fast_cycle_count % self.fast_enrich_every_n_cycles,
                    self.fast_enrich_every_n_cycles,
                )
        if not should_process:
            return

        try:
            self.enricher.process_once()
        except Exception:
            logging.exception("Processor batch failed")

    def run(self) -> None:
        if self.engine.fast_mode:
            self._run_fast_phase()

        self._run_incremental_forever()

    def _run_fast_phase(self) -> None:
        started = now_utc()
        logging.info("single-orchestrator fast phase started")

        while True:
            self._run_scrape_then_process(fast_mode=True)

            reason = self.engine._fast_stop(started)
            if reason:
                logging.info("single-orchestrator fast phase finished: %s", reason)
                break

            delay = random.randint(
                max(1, self.engine.sleep_min),
                max(max(1, self.engine.sleep_min), self.engine.sleep_max),
            )
            logging.info("single-orchestrator sleeping %s sec before next fast cycle", delay)
            time.sleep(delay)

    def _run_incremental_forever(self) -> None:
        interval_hours = max(1, self.engine.interval_hours)
        next_run = now_utc()
        logging.info("single-orchestrator incremental phase started (every %s hour[s])", interval_hours)

        while True:
            now = now_utc()
            if now >= next_run:
                self._run_scrape_then_process(fast_mode=False)
                next_run = now + timedelta(hours=interval_hours)

            sleep_seconds = min(30, max(5, int((next_run - now_utc()).total_seconds())))
            time.sleep(sleep_seconds)


if __name__ == "__main__":
    orchestrator = SingleOrchestrator()
    orchestrator.run()
