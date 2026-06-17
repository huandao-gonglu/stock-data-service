from __future__ import annotations

import datetime as dt
import json
from dataclasses import asdict, dataclass
from pathlib import Path

from stock_data_service.config import Settings
from stock_data_service.market.symbol_normalizer import normalize_symbol
from stock_data_service.market.timeframe import Timeframe


@dataclass(frozen=True)
class AdminSyncSettings:
    source_id: str
    timeframe: str
    start: str
    end: str
    symbols: list[str]


class AdminSettingsStore:
    def __init__(self, settings: Settings):
        self.path = settings.metadata_db.parent / "admin_settings.json"

    def load(self) -> AdminSyncSettings:
        if not self.path.exists():
            return default_admin_sync_settings()
        payload = json.loads(self.path.read_text(encoding="utf-8"))
        return validate_admin_sync_settings(payload)

    def save(self, payload: dict) -> AdminSyncSettings:
        sync_settings = validate_admin_sync_settings(payload)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.path.write_text(json.dumps(asdict(sync_settings), ensure_ascii=False, indent=2), encoding="utf-8")
        return sync_settings


def default_admin_sync_settings() -> AdminSyncSettings:
    today = dt.date.today()
    return AdminSyncSettings(
        source_id="baidu-main",
        timeframe=Timeframe.M1.value,
        start=today.isoformat(),
        end=today.isoformat(),
        symbols=["sh600000"],
    )


def validate_admin_sync_settings(payload: dict) -> AdminSyncSettings:
    source_id = str(payload.get("source_id") or "baidu-main").strip()
    if not source_id:
        raise ValueError("source_id is required")
    timeframe = Timeframe.parse(str(payload.get("timeframe") or Timeframe.M1.value)).value
    start = _date_string(payload.get("start"), "start")
    end = _date_string(payload.get("end"), "end")
    if dt.date.fromisoformat(start) > dt.date.fromisoformat(end):
        raise ValueError("start must be before or equal to end")
    symbols = payload.get("symbols") or []
    if isinstance(symbols, str):
        symbols = [item.strip() for item in symbols.replace("\n", ",").split(",") if item.strip()]
    normalized = [normalize_symbol(str(symbol)) for symbol in symbols]
    if not normalized:
        raise ValueError("at least one symbol is required")
    return AdminSyncSettings(
        source_id=source_id,
        timeframe=timeframe,
        start=start,
        end=end,
        symbols=normalized,
    )


def _date_string(value: object, field_name: str) -> str:
    if value is None:
        raise ValueError(f"{field_name} is required")
    text = str(value).strip()
    dt.date.fromisoformat(text)
    return text
