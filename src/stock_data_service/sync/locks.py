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
