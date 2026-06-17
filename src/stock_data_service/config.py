from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path


def _load_dotenv_defaults(path: Path = Path(".env")) -> None:
    if not path.exists():
        return
    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip().strip('"').strip("'")
        if key and key not in os.environ:
            os.environ[key] = value


def _bool_env(name: str, default: bool = False) -> bool:
    value = os.getenv(name)
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "on"}


@dataclass(frozen=True)
class Settings:
    data_root: Path = Path("./data")
    meta_db: Path | None = None
    log_dir: Path | None = None
    log_level: str = "INFO"
    server_mode: bool = False
    data_api_key: str | None = None
    admin_api_key: str | None = None
    baidu_app_key: str | None = None
    baidu_app_secret: str | None = None
    baidu_token_file: Path = Path("./baidu_token.json")
    baidu_cache_dir: Path = Path("./data/raw/baidu")

    @property
    def parquet_root(self) -> Path:
        return self.data_root / "parquet"

    @property
    def metadata_db(self) -> Path:
        return self.meta_db or self.data_root / "meta" / "sync_metadata.duckdb"

    @property
    def logs_dir(self) -> Path:
        return self.log_dir or self.data_root / "logs"

    @classmethod
    def from_env(cls) -> "Settings":
        _load_dotenv_defaults()
        data_root = Path(os.getenv("STOCK_DATA_DATA_ROOT", "./data"))
        meta_db_env = os.getenv("STOCK_DATA_META_DB")
        log_dir_env = os.getenv("STOCK_DATA_LOG_DIR")
        return cls(
            data_root=data_root,
            meta_db=Path(meta_db_env) if meta_db_env else None,
            log_dir=Path(log_dir_env) if log_dir_env else None,
            log_level=os.getenv("STOCK_DATA_LOG_LEVEL", "INFO"),
            server_mode=_bool_env("STOCK_DATA_SERVER_MODE", False),
            data_api_key=os.getenv("DATA_API_KEY") or None,
            admin_api_key=os.getenv("ADMIN_API_KEY") or None,
            baidu_app_key=os.getenv("BAIDU_APP_KEY") or None,
            baidu_app_secret=os.getenv("BAIDU_APP_SECRET") or None,
            baidu_token_file=Path(os.getenv("BAIDU_TOKEN_FILE", "./baidu_token.json")),
            baidu_cache_dir=Path(os.getenv("BAIDU_CACHE_DIR", "./data/raw/baidu")),
        )


def ensure_runtime_dirs(settings: Settings) -> None:
    settings.parquet_root.mkdir(parents=True, exist_ok=True)
    settings.metadata_db.parent.mkdir(parents=True, exist_ok=True)
    settings.baidu_cache_dir.mkdir(parents=True, exist_ok=True)
    settings.logs_dir.mkdir(parents=True, exist_ok=True)
