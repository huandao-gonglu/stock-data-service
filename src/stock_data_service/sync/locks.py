from __future__ import annotations

import os
import threading
import time
from contextlib import contextmanager
from pathlib import Path

_LOCKS: dict[tuple[str, str, int, int], threading.Lock] = {}
_LOCKS_GUARD = threading.Lock()


@contextmanager
def partition_lock(timeframe: str, symbol: str, year: int, month: int):
    key = (timeframe, symbol, year, month)
    with _LOCKS_GUARD:
        lock = _LOCKS.setdefault(key, threading.Lock())
    lock.acquire()
    try:
        yield
    finally:
        lock.release()


class ProcessLock:
    def __init__(self, path: str | Path, timeout_seconds: float = 1.0):
        self.path = Path(path)
        self.timeout_seconds = timeout_seconds
        self._fd: int | None = None

    def __enter__(self) -> "ProcessLock":
        self.path.parent.mkdir(parents=True, exist_ok=True)
        deadline = time.monotonic() + self.timeout_seconds
        while True:
            try:
                self._fd = os.open(self.path, os.O_CREAT | os.O_EXCL | os.O_WRONLY)
                os.write(self._fd, str(os.getpid()).encode("ascii"))
                return self
            except FileExistsError:
                if self._clear_stale_lock():
                    continue
                if time.monotonic() >= deadline:
                    raise RuntimeError(f"sync lock is already held: {self.path}")
                time.sleep(0.05)

    def __exit__(self, exc_type, exc, tb) -> None:
        if self._fd is not None:
            os.close(self._fd)
            self._fd = None
        try:
            self.path.unlink()
        except FileNotFoundError:
            pass

    def _clear_stale_lock(self) -> bool:
        try:
            text = self.path.read_text(encoding="ascii").strip()
        except FileNotFoundError:
            return True
        except OSError:
            return self._clear_invalid_lock()
        if not text:
            return self._clear_invalid_lock()
        try:
            pid = int(text)
        except ValueError:
            return self._clear_invalid_lock()
        if _process_exists(pid):
            return False
        try:
            self.path.unlink()
        except FileNotFoundError:
            return True
        except OSError:
            return False
        return True

    def _clear_invalid_lock(self) -> bool:
        try:
            age = time.time() - self.path.stat().st_mtime
        except FileNotFoundError:
            return True
        except OSError:
            return False
        if age < max(self.timeout_seconds, 1.0):
            return False
        try:
            self.path.unlink()
        except FileNotFoundError:
            return True
        except OSError:
            return False
        return True


def _process_exists(pid: int) -> bool:
    if pid <= 0:
        return False
    if os.name == "nt":
        import ctypes

        kernel32 = ctypes.windll.kernel32
        handle = kernel32.OpenProcess(0x1000, False, pid)
        if not handle:
            return False
        exit_code = ctypes.c_ulong()
        try:
            if not kernel32.GetExitCodeProcess(handle, ctypes.byref(exit_code)):
                return False
            return exit_code.value == 259
        finally:
            kernel32.CloseHandle(handle)
    try:
        os.kill(pid, 0)
    except OSError:
        return False
    return True
