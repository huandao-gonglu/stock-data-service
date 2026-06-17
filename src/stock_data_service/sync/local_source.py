from __future__ import annotations

import datetime as dt
import hashlib
import re
from dataclasses import dataclass
from pathlib import Path

from stock_data_service.market.timeframe import Timeframe


@dataclass(frozen=True)
class LocalRawFile:
    remote_path: str
    local_path: Path
    size: int
    content_hash: str
    trade_date: dt.date
    timeframe: Timeframe


class LocalRawScanner:
    def __init__(self, raw_root: str | Path):
        self.raw_root = Path(raw_root)

    def scan(
        self,
        *,
        timeframe: Timeframe,
        start: dt.date,
        end: dt.date,
    ) -> list[LocalRawFile]:
        if not self.raw_root.exists():
            return []
        suffix = _suffix(timeframe)
        results: list[LocalRawFile] = []
        for path in sorted(self.raw_root.rglob(f"*{suffix}")):
            if not path.is_file():
                continue
            trade_date = _date_from_filename(path.name)
            if trade_date is None or not (start <= trade_date <= end):
                continue
            stat = path.stat()
            results.append(
                LocalRawFile(
                    remote_path="/" + path.relative_to(self.raw_root).as_posix(),
                    local_path=path,
                    size=stat.st_size,
                    content_hash=_sha256(path),
                    trade_date=trade_date,
                    timeframe=timeframe,
                )
            )
        return results


def _date_from_filename(name: str) -> dt.date | None:
    match = re.search(r"(20\d{6}|19\d{6})", name)
    if not match:
        return None
    try:
        return dt.datetime.strptime(match.group(1), "%Y%m%d").date()
    except ValueError:
        return None


def _suffix(timeframe: Timeframe) -> str:
    return {
        Timeframe.M1: "_1min.zip",
        Timeframe.M5: "_5min.zip",
        Timeframe.M15: "_15min.zip",
        Timeframe.M30: "_30min.zip",
        Timeframe.H1: "_60min.zip",
        Timeframe.D1: "_1min.zip",
    }[timeframe]


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()
