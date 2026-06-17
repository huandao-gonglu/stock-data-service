from __future__ import annotations

import argparse
import datetime as dt
import logging
from dataclasses import replace
from pathlib import Path

from stock_data_service.auth.token_manager import TokenManager
from stock_data_service.baidu.pan_client import BaiduPanClient
from stock_data_service.config import Settings, ensure_runtime_dirs
from stock_data_service.logging_config import configure_logging
from stock_data_service.market.timeframe import Timeframe
from stock_data_service.storage.sync_metadata import SyncMetadata
from stock_data_service.sync.downloader import BaiduDownloader
from stock_data_service.sync.job_runner import BaiduSyncJobRunner, LocalSyncJobRunner

logger = logging.getLogger(__name__)


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(prog="stock-data")
    subcommands = parser.add_subparsers(dest="command", required=True)

    ingest = subcommands.add_parser("ingest-local")
    ingest.add_argument("--raw-root", required=True)
    ingest.add_argument("--data-root", default="./data")
    ingest.add_argument("--meta-db")
    ingest.add_argument("--timeframe", required=True)
    ingest.add_argument("--start", required=True)
    ingest.add_argument("--end", required=True)
    ingest.add_argument("--symbol", action="append", required=True)

    download = subcommands.add_parser("download-baidu")
    download.add_argument("--data-root", default="./data")
    download.add_argument("--cache-dir")
    download.add_argument("--timeframe", required=True)
    download.add_argument("--start", required=True)
    download.add_argument("--end", required=True)

    sync_baidu = subcommands.add_parser("sync-baidu")
    sync_baidu.add_argument("--data-root", default="./data")
    sync_baidu.add_argument("--meta-db")
    sync_baidu.add_argument("--cache-dir")
    sync_baidu.add_argument("--source-id", default="baidu-main")
    sync_baidu.add_argument("--timeframe", required=True)
    sync_baidu.add_argument("--start", required=True)
    sync_baidu.add_argument("--end", required=True)
    sync_baidu.add_argument("--symbol", action="append", required=True)

    args = parser.parse_args(argv)
    if args.command == "ingest-local":
        _ingest_local(args)
    elif args.command == "download-baidu":
        _download_baidu(args)
    elif args.command == "sync-baidu":
        _sync_baidu(args)


def _ingest_local(args: argparse.Namespace) -> None:
    settings = _settings_for_command(args.data_root, args.meta_db)
    ensure_runtime_dirs(settings)
    configure_logging(settings)
    logger.info(
        "cli ingest-local start raw_root=%s data_root=%s timeframe=%s start=%s end=%s symbols=%s",
        args.raw_root,
        settings.data_root,
        args.timeframe,
        args.start,
        args.end,
        args.symbol,
    )
    metadata = SyncMetadata(settings.metadata_db)
    runner = LocalSyncJobRunner(
        raw_root=args.raw_root,
        parquet_root=settings.parquet_root,
        metadata=metadata,
    )
    result = runner.run(
        timeframe=Timeframe.parse(args.timeframe),
        start=dt.date.fromisoformat(args.start),
        end=dt.date.fromisoformat(args.end),
        symbols=args.symbol,
    )
    print(
        f"{result.job_id}: scanned={result.scanned_count} "
        f"ingested={result.ingested_count} failed={result.failed_count}"
    )
    logger.info(
        "cli ingest-local finished job_id=%s scanned=%s ingested=%s failed=%s",
        result.job_id,
        result.scanned_count,
        result.ingested_count,
        result.failed_count,
    )


def _download_baidu(args: argparse.Namespace) -> None:
    settings = _settings_for_command(args.data_root)
    ensure_runtime_dirs(settings)
    configure_logging(settings)
    cache_dir = Path(args.cache_dir) if args.cache_dir else settings.data_root / "raw" / "baidu"
    logger.info(
        "cli download-baidu start cache_dir=%s timeframe=%s start=%s end=%s",
        cache_dir,
        args.timeframe,
        args.start,
        args.end,
    )
    client = _baidu_client(cache_dir)
    downloader = BaiduDownloader(client, cache_dir)
    result = downloader.download_for_range(
        timeframe=Timeframe.parse(args.timeframe),
        start=dt.date.fromisoformat(args.start),
        end=dt.date.fromisoformat(args.end),
    )
    ok = sum(1 for item in result if item.is_downloaded)
    failed = sum(1 for item in result if not item.is_downloaded)
    print(f"downloaded={ok} failed={failed}")
    logger.info("cli download-baidu finished downloaded=%s failed=%s", ok, failed)


def _sync_baidu(args: argparse.Namespace) -> None:
    settings = _settings_for_command(args.data_root, args.meta_db)
    ensure_runtime_dirs(settings)
    configure_logging(settings)
    cache_dir = Path(args.cache_dir) if args.cache_dir else settings.data_root / "raw" / "baidu"
    logger.info(
        "cli sync-baidu start data_root=%s cache_dir=%s source_id=%s timeframe=%s start=%s end=%s symbols=%s",
        settings.data_root,
        cache_dir,
        args.source_id,
        args.timeframe,
        args.start,
        args.end,
        args.symbol,
    )
    metadata = SyncMetadata(settings.metadata_db)
    runner = BaiduSyncJobRunner(
        client=_baidu_client(cache_dir),
        cache_dir=cache_dir,
        parquet_root=settings.parquet_root,
        metadata=metadata,
        source_id=args.source_id,
    )
    result = runner.run(
        timeframe=Timeframe.parse(args.timeframe),
        start=dt.date.fromisoformat(args.start),
        end=dt.date.fromisoformat(args.end),
        symbols=args.symbol,
    )
    print(
        f"{result.job_id}: scanned={result.scanned_count} "
        f"downloaded={result.downloaded_count} ingested={result.ingested_count} "
        f"failed={result.failed_count}"
    )
    logger.info(
        "cli sync-baidu finished job_id=%s scanned=%s downloaded=%s ingested=%s failed=%s",
        result.job_id,
        result.scanned_count,
        result.downloaded_count,
        result.ingested_count,
        result.failed_count,
    )


def _baidu_client(cache_dir: Path) -> BaiduPanClient:
    settings = Settings.from_env()
    token_manager = TokenManager(
        token_file=settings.baidu_token_file,
        app_key=settings.baidu_app_key,
        app_secret=settings.baidu_app_secret,
    )
    return BaiduPanClient(token_manager=token_manager, enable_cache=True, cache_dir=cache_dir)


def _settings_for_command(data_root: str, meta_db: str | None = None) -> Settings:
    base = Settings.from_env()
    return replace(
        base,
        data_root=Path(data_root),
        meta_db=Path(meta_db) if meta_db else None,
    )


if __name__ == "__main__":
    main()
