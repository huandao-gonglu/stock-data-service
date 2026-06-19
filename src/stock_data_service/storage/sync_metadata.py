from __future__ import annotations

import datetime as dt
import threading
import time
import uuid
from pathlib import Path
from typing import Any

import duckdb
import pandas as pd

_DB_LOCKS: dict[Path, threading.RLock] = {}
_DB_LOCKS_GUARD = threading.Lock()


class SyncMetadata:
    def __init__(self, db_path: str | Path):
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)

    def connect(self) -> duckdb.DuckDBPyConnection:
        lock = _db_lock(self.db_path)
        lock.acquire()
        deadline = time.monotonic() + 5.0
        while True:
            try:
                return _LockedConnection(duckdb.connect(str(self.db_path)), lock)
            except duckdb.IOException:
                if time.monotonic() >= deadline:
                    lock.release()
                    raise
                time.sleep(0.1)
            except Exception:
                lock.release()
                raise

    def initialize(self) -> None:
        with self.connect() as con:
            for statement in _DDL:
                con.execute(statement)

    def upsert_source(
        self,
        *,
        source_id: str,
        source_type: str,
        name: str,
        root_path: str,
        share_url: str | None = None,
        share_password: str | None = None,
        enabled: bool = True,
    ) -> None:
        now = _now()
        with self.connect() as con:
            existing = con.execute(
                "SELECT created_at FROM upstream_sources WHERE id = ?",
                [source_id],
            ).fetchone()
            created_at = existing[0] if existing else now
            con.execute("DELETE FROM upstream_sources WHERE id = ?", [source_id])
            con.execute(
                """
                INSERT INTO upstream_sources
                (id, type, name, root_path, share_url, share_password, enabled, created_at, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                [source_id, source_type, name, root_path, share_url, share_password, enabled, created_at, now],
            )

    def upsert_remote_file(
        self,
        *,
        source_id: str,
        remote_path: str,
        size: int | None = None,
        md5: str | None = None,
        server_mtime: dt.datetime | None = None,
        content_hash: str | None = None,
        local_raw_path: str | None = None,
    ) -> str:
        now = _now()
        with self.connect() as con:
            existing = con.execute(
                """
                SELECT size, md5, server_mtime, content_hash, local_raw_path
                FROM remote_files
                WHERE source_id = ? AND remote_path = ?
                """,
                [source_id, remote_path],
            ).fetchone()
            status = "discovered"
            if existing and tuple(existing) == (size, md5, server_mtime, content_hash, local_raw_path):
                status = "skipped"
            con.execute(
                "DELETE FROM remote_files WHERE source_id = ? AND remote_path = ?",
                [source_id, remote_path],
            )
            con.execute(
                """
                INSERT INTO remote_files
                (source_id, remote_path, size, md5, server_mtime, content_hash,
                 local_raw_path, status, discovered_at, downloaded_at, error_message)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, NULL, NULL)
                """,
                [source_id, remote_path, size, md5, server_mtime, content_hash, local_raw_path, status, now],
            )
        return status

    def mark_remote_downloaded(self, source_id: str, remote_path: str, local_raw_path: str) -> None:
        with self.connect() as con:
            con.execute(
                """
                UPDATE remote_files
                SET status = 'downloaded', local_raw_path = ?, downloaded_at = ?, error_message = NULL
                WHERE source_id = ? AND remote_path = ?
                """,
                [local_raw_path, _now(), source_id, remote_path],
            )

    def mark_remote_failed(self, source_id: str, remote_path: str, error_message: str) -> None:
        with self.connect() as con:
            con.execute(
                """
                UPDATE remote_files
                SET status = 'failed', error_message = ?
                WHERE source_id = ? AND remote_path = ?
                """,
                [error_message, source_id, remote_path],
            )

    def get_remote_file(self, source_id: str, remote_path: str) -> dict[str, Any] | None:
        row = self.fetchone(
            """
            SELECT source_id, remote_path, size, md5, server_mtime, content_hash,
                   local_raw_path, status, discovered_at, downloaded_at, error_message
            FROM remote_files
            WHERE source_id = ? AND remote_path = ?
            """,
            [source_id, remote_path],
        )
        if row is None:
            return None
        return {
            "source_id": row[0],
            "remote_path": row[1],
            "size": row[2],
            "md5": row[3],
            "server_mtime": row[4],
            "content_hash": row[5],
            "local_raw_path": row[6],
            "status": row[7],
            "discovered_at": row[8],
            "downloaded_at": row[9],
            "error_message": row[10],
        }

    def start_file_ingest(
        self,
        *,
        source_id: str,
        remote_path: str,
        timeframe: str,
        symbol: str,
        content_hash: str | None,
    ) -> None:
        with self.connect() as con:
            con.execute(
                """
                DELETE FROM file_ingests
                WHERE source_id = ? AND remote_path = ? AND timeframe = ? AND symbol = ?
                """,
                [source_id, remote_path, timeframe, symbol],
            )
            con.execute(
                """
                INSERT INTO file_ingests
                (source_id, remote_path, timeframe, symbol, start_ts, end_ts, row_count,
                 expected_row_count, content_hash, parquet_path, status, ingested_at,
                 committed_at, error_message)
                VALUES (?, ?, ?, ?, NULL, NULL, NULL, NULL, ?, NULL, 'ingesting', ?, NULL, NULL)
                """,
                [source_id, remote_path, timeframe, symbol, content_hash, _now()],
            )

    def commit_file_ingest(
        self,
        *,
        source_id: str,
        remote_path: str,
        timeframe: str,
        symbol: str,
        start_ts: dt.datetime | None,
        end_ts: dt.datetime | None,
        row_count: int,
        expected_row_count: int | None,
        content_hash: str | None,
        parquet_path: str | None,
    ) -> None:
        now = _now()
        with self.connect() as con:
            con.execute(
                """
                DELETE FROM file_ingests
                WHERE source_id = ? AND remote_path = ? AND timeframe = ? AND symbol = ?
                """,
                [source_id, remote_path, timeframe, symbol],
            )
            con.execute(
                """
                INSERT INTO file_ingests
                (source_id, remote_path, timeframe, symbol, start_ts, end_ts, row_count,
                 expected_row_count, content_hash, parquet_path, status, ingested_at,
                 committed_at, error_message)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'committed', ?, ?, NULL)
                """,
                [
                    source_id,
                    remote_path,
                    timeframe,
                    symbol,
                    start_ts,
                    end_ts,
                    row_count,
                    expected_row_count,
                    content_hash,
                    parquet_path,
                    now,
                    now,
                ],
            )

    def mark_file_ingest_status(
        self,
        *,
        source_id: str,
        remote_path: str,
        timeframe: str,
        symbol: str,
        status: str,
        error_message: str | None = None,
        content_hash: str | None = None,
    ) -> None:
        now = _now()
        with self.connect() as con:
            con.execute(
                """
                DELETE FROM file_ingests
                WHERE source_id = ? AND remote_path = ? AND timeframe = ? AND symbol = ?
                """,
                [source_id, remote_path, timeframe, symbol],
            )
            con.execute(
                """
                INSERT INTO file_ingests
                (source_id, remote_path, timeframe, symbol, start_ts, end_ts, row_count,
                 expected_row_count, content_hash, parquet_path, status, ingested_at,
                 committed_at, error_message)
                VALUES (?, ?, ?, ?, NULL, NULL, 0, NULL, ?, NULL, ?, ?, NULL, ?)
                """,
                [source_id, remote_path, timeframe, symbol, content_hash, status, now, error_message],
            )

    def mark_file_ingest_status_many(
        self,
        *,
        source_id: str,
        remote_path: str,
        timeframe: str,
        symbols: list[str],
        status: str,
        error_message: str | None = None,
        content_hash: str | None = None,
    ) -> int:
        unique_symbols = list(dict.fromkeys(symbols))
        if not unique_symbols:
            return 0
        now = _now()
        rows = pd.DataFrame(
            {
                "source_id": source_id,
                "remote_path": remote_path,
                "timeframe": timeframe,
                "symbol": unique_symbols,
                "content_hash": content_hash,
                "status": status,
                "ingested_at": now,
                "error_message": error_message,
            }
        )
        with self.connect() as con:
            con.register("bulk_file_ingest_status", rows)
            con.execute(
                """
                DELETE FROM file_ingests
                WHERE source_id = ? AND remote_path = ? AND timeframe = ?
                  AND symbol IN (SELECT symbol FROM bulk_file_ingest_status)
                """,
                [source_id, remote_path, timeframe],
            )
            con.execute(
                """
                INSERT INTO file_ingests
                (source_id, remote_path, timeframe, symbol, start_ts, end_ts, row_count,
                 expected_row_count, content_hash, parquet_path, status, ingested_at,
                 committed_at, error_message)
                SELECT source_id, remote_path, timeframe, symbol, NULL, NULL, 0, NULL,
                       content_hash, NULL, status, ingested_at, NULL, error_message
                FROM bulk_file_ingest_status
                """
            )
        return len(unique_symbols)

    def get_archive_symbol_members(
        self,
        *,
        source_id: str,
        remote_path: str,
        content_hash: str | None,
    ) -> dict[str, str] | None:
        with self.connect() as con:
            index_row = con.execute(
                """
                SELECT content_hash
                FROM archive_indexes
                WHERE source_id = ? AND remote_path = ?
                """,
                [source_id, remote_path],
            ).fetchone()
            if index_row is None or index_row[0] != content_hash:
                return None
            rows = con.execute(
                """
                SELECT symbol, member_name
                FROM archive_symbol_members
                WHERE source_id = ? AND remote_path = ?
                """,
                [source_id, remote_path],
            ).fetchall()
        return {row[0]: row[1] for row in rows}

    def upsert_archive_symbol_members(
        self,
        *,
        source_id: str,
        remote_path: str,
        content_hash: str | None,
        members: dict[str, str],
    ) -> int:
        now = _now()
        sorted_members = sorted(members.items())
        rows = pd.DataFrame(
            {
                "source_id": source_id,
                "remote_path": remote_path,
                "symbol": [symbol for symbol, _ in sorted_members],
                "member_name": [member_name for _, member_name in sorted_members],
                "content_hash": content_hash,
                "indexed_at": now,
            }
        )
        with self.connect() as con:
            con.execute(
                "DELETE FROM archive_symbol_members WHERE source_id = ? AND remote_path = ?",
                [source_id, remote_path],
            )
            con.execute(
                "DELETE FROM archive_indexes WHERE source_id = ? AND remote_path = ?",
                [source_id, remote_path],
            )
            con.execute(
                """
                INSERT INTO archive_indexes
                (source_id, remote_path, content_hash, symbol_count, indexed_at)
                VALUES (?, ?, ?, ?, ?)
                """,
                [source_id, remote_path, content_hash, len(sorted_members), now],
            )
            if sorted_members:
                con.register("bulk_archive_symbol_members", rows)
                con.execute(
                    """
                    INSERT INTO archive_symbol_members
                    (source_id, remote_path, symbol, member_name, content_hash, indexed_at)
                    SELECT source_id, remote_path, symbol, member_name, content_hash, indexed_at
                    FROM bulk_archive_symbol_members
                    """
                )
        return len(sorted_members)

    def update_coverage_daily(
        self,
        *,
        symbol: str,
        timeframe: str,
        trade_date: dt.date,
        start_ts: dt.datetime | None,
        end_ts: dt.datetime | None,
        row_count: int,
        expected_row_count: int,
        is_complete: bool,
        quality_flag: str,
    ) -> None:
        with self.connect() as con:
            con.execute(
                """
                DELETE FROM coverage_daily
                WHERE symbol = ? AND timeframe = ? AND trade_date = ?
                """,
                [symbol, timeframe, trade_date],
            )
            con.execute(
                """
                INSERT INTO coverage_daily
                (symbol, timeframe, trade_date, start_ts, end_ts, row_count,
                 expected_row_count, is_complete, quality_flag, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    symbol,
                    timeframe,
                    trade_date,
                    start_ts,
                    end_ts,
                    row_count,
                    expected_row_count,
                    is_complete,
                    quality_flag,
                    _now(),
                ],
            )

    def update_coverage_daily_many(self, rows: list[dict[str, Any]]) -> int:
        if not rows:
            return 0
        now = _now()
        frame = pd.DataFrame(rows)
        frame["updated_at"] = now
        with self.connect() as con:
            con.register("bulk_coverage_daily", frame)
            con.execute(
                """
                DELETE FROM coverage_daily
                USING bulk_coverage_daily
                WHERE coverage_daily.symbol = bulk_coverage_daily.symbol
                  AND coverage_daily.timeframe = bulk_coverage_daily.timeframe
                  AND coverage_daily.trade_date = bulk_coverage_daily.trade_date
                """
            )
            con.execute(
                """
                INSERT INTO coverage_daily
                (symbol, timeframe, trade_date, start_ts, end_ts, row_count,
                 expected_row_count, is_complete, quality_flag, updated_at)
                SELECT symbol, timeframe, trade_date, start_ts, end_ts, row_count,
                       expected_row_count, is_complete, quality_flag, updated_at
                FROM bulk_coverage_daily
                """
            )
        return len(rows)

    def record_ingest_metadata_many(
        self,
        *,
        symbol_records: list[dict[str, Any]] | None = None,
        coverage_rows: list[dict[str, Any]] | None = None,
        file_ingest_rows: list[dict[str, Any]] | None = None,
    ) -> None:
        symbol_records = symbol_records or []
        coverage_rows = coverage_rows or []
        file_ingest_rows = file_ingest_rows or []
        if not symbol_records and not coverage_rows and not file_ingest_rows:
            return

        now = _now()
        with self.connect() as con:
            con.execute("BEGIN TRANSACTION")
            try:
                if symbol_records:
                    symbols_frame = _frame_with_columns(
                        symbol_records,
                        ["symbol", "code", "name", "exchange", "listed_at", "delisted_at", "status", "source"],
                    )
                    symbols_frame = symbols_frame.drop_duplicates(subset=["symbol"], keep="last")
                    symbols_frame["updated_at"] = now
                    con.register("bulk_ingest_symbols", symbols_frame)
                    con.execute(
                        """
                        DELETE FROM symbols
                        USING bulk_ingest_symbols
                        WHERE symbols.symbol = bulk_ingest_symbols.symbol
                        """
                    )
                    con.execute(
                        """
                        INSERT INTO symbols
                        (symbol, code, name, exchange, listed_at, delisted_at, status, source, updated_at)
                        SELECT symbol, code, name, exchange, listed_at, delisted_at, status, source, updated_at
                        FROM bulk_ingest_symbols
                        """
                    )

                if coverage_rows:
                    coverage_frame = _frame_with_columns(
                        coverage_rows,
                        [
                            "symbol",
                            "timeframe",
                            "trade_date",
                            "start_ts",
                            "end_ts",
                            "row_count",
                            "expected_row_count",
                            "is_complete",
                            "quality_flag",
                        ],
                    )
                    coverage_frame["updated_at"] = now
                    con.register("bulk_ingest_coverage_daily", coverage_frame)
                    con.execute(
                        """
                        DELETE FROM coverage_daily
                        USING bulk_ingest_coverage_daily
                        WHERE coverage_daily.symbol = bulk_ingest_coverage_daily.symbol
                          AND coverage_daily.timeframe = bulk_ingest_coverage_daily.timeframe
                          AND coverage_daily.trade_date = bulk_ingest_coverage_daily.trade_date
                        """
                    )
                    con.execute(
                        """
                        INSERT INTO coverage_daily
                        (symbol, timeframe, trade_date, start_ts, end_ts, row_count,
                         expected_row_count, is_complete, quality_flag, updated_at)
                        SELECT symbol, timeframe, trade_date, start_ts, end_ts, row_count,
                               expected_row_count, is_complete, quality_flag, updated_at
                        FROM bulk_ingest_coverage_daily
                        """
                    )

                if file_ingest_rows:
                    ingest_frame = _frame_with_columns(
                        file_ingest_rows,
                        [
                            "source_id",
                            "remote_path",
                            "timeframe",
                            "symbol",
                            "start_ts",
                            "end_ts",
                            "row_count",
                            "expected_row_count",
                            "content_hash",
                            "parquet_path",
                            "status",
                            "error_message",
                        ],
                    )
                    ingest_frame["ingested_at"] = now
                    ingest_frame["committed_at"] = ingest_frame["status"].map(
                        lambda status: now if status == "committed" else None
                    )
                    con.register("bulk_ingest_file_ingests", ingest_frame)
                    con.execute(
                        """
                        DELETE FROM file_ingests
                        USING bulk_ingest_file_ingests
                        WHERE file_ingests.source_id = bulk_ingest_file_ingests.source_id
                          AND file_ingests.remote_path = bulk_ingest_file_ingests.remote_path
                          AND file_ingests.timeframe = bulk_ingest_file_ingests.timeframe
                          AND file_ingests.symbol = bulk_ingest_file_ingests.symbol
                        """
                    )
                    con.execute(
                        """
                        INSERT INTO file_ingests
                        (source_id, remote_path, timeframe, symbol, start_ts, end_ts, row_count,
                         expected_row_count, content_hash, parquet_path, status, ingested_at,
                         committed_at, error_message)
                        SELECT source_id, remote_path, timeframe, symbol, start_ts, end_ts, row_count,
                               expected_row_count, content_hash, parquet_path, status, ingested_at,
                               committed_at, error_message
                        FROM bulk_ingest_file_ingests
                        """
                    )
                con.execute("COMMIT")
            except Exception:
                con.execute("ROLLBACK")
                raise

    def dates_requiring_sync(
        self,
        *,
        symbols: list[str],
        timeframe: str,
        trade_dates: list[dt.date],
    ) -> list[dt.date]:
        unique_symbols = sorted(set(symbols))
        unique_dates = sorted(set(trade_dates))
        if not unique_symbols or not unique_dates:
            return []

        with self.connect() as con:
            rows = con.execute(
                """
                SELECT symbol, trade_date, is_complete, quality_flag
                FROM coverage_daily
                WHERE symbol IN (SELECT * FROM UNNEST(?))
                  AND timeframe = ?
                  AND trade_date >= ?
                  AND trade_date <= ?
                """,
                [unique_symbols, timeframe, unique_dates[0], unique_dates[-1]],
            ).fetchall()

        complete = {
            (row[0], row[1])
            for row in rows
            if bool(row[2]) and row[3] == "ok"
        }
        return [
            trade_date
            for trade_date in unique_dates
            if any((symbol, trade_date) not in complete for symbol in unique_symbols)
        ]

    def committed_ingest_paths(
        self,
        *,
        source_id: str,
        timeframe: str,
        symbols: list[str],
        remote_paths: list[str],
    ) -> set[str]:
        unique_symbols = sorted(set(symbols))
        unique_paths = sorted(set(remote_paths))
        if not unique_symbols or not unique_paths:
            return set()

        with self.connect() as con:
            rows = con.execute(
                """
                SELECT remote_path, COUNT(DISTINCT symbol) AS symbol_count
                FROM file_ingests
                WHERE source_id = ?
                  AND timeframe = ?
                  AND status = 'committed'
                  AND symbol IN (SELECT * FROM UNNEST(?))
                  AND remote_path IN (SELECT * FROM UNNEST(?))
                GROUP BY remote_path
                """,
                [source_id, timeframe, unique_symbols, unique_paths],
            ).fetchall()
        required_symbol_count = len(unique_symbols)
        return {row[0] for row in rows if int(row[1]) >= required_symbol_count}

    def upsert_symbol(
        self,
        *,
        symbol: str,
        code: str,
        name: str | None = None,
        exchange: str | None = None,
        listed_at: dt.date | None = None,
        delisted_at: dt.date | None = None,
        status: str | None = None,
        source: str | None = None,
    ) -> None:
        with self.connect() as con:
            con.execute("DELETE FROM symbols WHERE symbol = ?", [symbol])
            con.execute(
                """
                INSERT INTO symbols
                (symbol, code, name, exchange, listed_at, delisted_at, status, source, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                [symbol, code, name, exchange, listed_at, delisted_at, status, source, _now()],
            )

    def upsert_symbols(self, listings: list[Any]) -> int:
        rows = [
            (
                listing.symbol,
                listing.code,
                listing.name,
                listing.exchange,
                listing.listed_at,
                listing.delisted_at,
                listing.status,
                listing.source,
                _now(),
            )
            for listing in listings
        ]
        if not rows:
            return 0
        symbols = [row[0] for row in rows]
        with self.connect() as con:
            con.execute("DELETE FROM symbols WHERE symbol IN (SELECT * FROM UNNEST(?))", [symbols])
            con.executemany(
                """
                INSERT INTO symbols
                (symbol, code, name, exchange, listed_at, delisted_at, status, source, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                rows,
            )
        return len(rows)

    def create_sync_job(self, source_id: str) -> str:
        job_id = f"sync-{uuid.uuid4()}"
        with self.connect() as con:
            con.execute(
                """
                INSERT INTO sync_jobs
                (id, source_id, status, started_at, finished_at, scanned_count,
                 downloaded_count, ingested_count, failed_count, error_message)
                VALUES (?, ?, 'running', ?, NULL, 0, 0, 0, 0, NULL)
                """,
                [job_id, source_id, _now()],
            )
        return job_id

    def complete_sync_job(
        self,
        job_id: str,
        *,
        status: str = "completed",
        scanned_count: int = 0,
        downloaded_count: int = 0,
        ingested_count: int = 0,
        failed_count: int = 0,
        error_message: str | None = None,
    ) -> None:
        with self.connect() as con:
            con.execute(
                """
                UPDATE sync_jobs
                SET status = ?, finished_at = ?, scanned_count = ?, downloaded_count = ?,
                    ingested_count = ?, failed_count = ?, error_message = ?
                WHERE id = ?
                """,
                [
                    status,
                    _now(),
                    scanned_count,
                    downloaded_count,
                    ingested_count,
                    failed_count,
                    error_message,
                    job_id,
                ],
            )

    def mark_unfinished_sync_jobs_stopped(
        self,
        *,
        error_message: str = "stale running job recovered after process restart",
    ) -> int:
        with self.connect() as con:
            count = con.execute(
                """
                SELECT COUNT(*)
                FROM sync_jobs
                WHERE status IN ('queued', 'running', 'stopping') AND finished_at IS NULL
                """
            ).fetchone()[0]
            if int(count) == 0:
                return 0
            con.execute(
                """
                UPDATE sync_jobs
                SET status = 'stopped', finished_at = ?, error_message = COALESCE(error_message, ?)
                WHERE status IN ('queued', 'running', 'stopping') AND finished_at IS NULL
                """,
                [_now(), error_message],
            )
            return int(count)

    def fetchone(self, sql: str, params: list[Any] | None = None) -> tuple[Any, ...] | None:
        with self.connect() as con:
            return con.execute(sql, params or []).fetchone()

    def fetchall(self, sql: str, params: list[Any] | None = None) -> list[tuple[Any, ...]]:
        with self.connect() as con:
            return con.execute(sql, params or []).fetchall()


def _now() -> dt.datetime:
    return dt.datetime.now(dt.timezone.utc).replace(tzinfo=None)


def _frame_with_columns(rows: list[dict[str, Any]], columns: list[str]) -> pd.DataFrame:
    frame = pd.DataFrame(rows)
    for column in columns:
        if column not in frame.columns:
            frame[column] = None
    return frame[columns]


def _db_lock(path: Path) -> threading.RLock:
    key = path.resolve()
    with _DB_LOCKS_GUARD:
        return _DB_LOCKS.setdefault(key, threading.RLock())


class _LockedConnection:
    def __init__(self, connection: duckdb.DuckDBPyConnection, lock: threading.RLock):
        self._connection = connection
        self._lock = lock
        self._released = False

    def __enter__(self):
        self._connection.__enter__()
        return self._connection

    def __exit__(self, exc_type, exc, tb):
        try:
            return self._connection.__exit__(exc_type, exc, tb)
        finally:
            self._release()

    def close(self) -> None:
        try:
            self._connection.close()
        finally:
            self._release()

    def __getattr__(self, name: str):
        return getattr(self._connection, name)

    def _release(self) -> None:
        if not self._released:
            self._released = True
            self._lock.release()


_DDL = [
    """
    CREATE TABLE IF NOT EXISTS upstream_sources (
        id VARCHAR PRIMARY KEY,
        type VARCHAR NOT NULL,
        name VARCHAR NOT NULL,
        root_path VARCHAR NOT NULL,
        share_url VARCHAR,
        share_password VARCHAR,
        enabled BOOLEAN NOT NULL DEFAULT true,
        created_at TIMESTAMP NOT NULL,
        updated_at TIMESTAMP NOT NULL
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS remote_files (
        source_id VARCHAR NOT NULL,
        remote_path VARCHAR NOT NULL,
        size BIGINT,
        md5 VARCHAR,
        server_mtime TIMESTAMP,
        content_hash VARCHAR,
        local_raw_path VARCHAR,
        status VARCHAR NOT NULL,
        discovered_at TIMESTAMP NOT NULL,
        downloaded_at TIMESTAMP,
        error_message VARCHAR,
        PRIMARY KEY (source_id, remote_path)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS file_ingests (
        source_id VARCHAR NOT NULL,
        remote_path VARCHAR NOT NULL,
        timeframe VARCHAR NOT NULL,
        symbol VARCHAR NOT NULL,
        start_ts TIMESTAMP,
        end_ts TIMESTAMP,
        row_count BIGINT,
        expected_row_count BIGINT,
        content_hash VARCHAR,
        parquet_path VARCHAR,
        status VARCHAR NOT NULL,
        ingested_at TIMESTAMP,
        committed_at TIMESTAMP,
        error_message VARCHAR,
        PRIMARY KEY (source_id, remote_path, timeframe, symbol)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS archive_indexes (
        source_id VARCHAR NOT NULL,
        remote_path VARCHAR NOT NULL,
        content_hash VARCHAR,
        symbol_count BIGINT NOT NULL,
        indexed_at TIMESTAMP NOT NULL,
        PRIMARY KEY (source_id, remote_path)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS archive_symbol_members (
        source_id VARCHAR NOT NULL,
        remote_path VARCHAR NOT NULL,
        symbol VARCHAR NOT NULL,
        member_name VARCHAR NOT NULL,
        content_hash VARCHAR,
        indexed_at TIMESTAMP NOT NULL,
        PRIMARY KEY (source_id, remote_path, symbol)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS coverage_daily (
        symbol VARCHAR NOT NULL,
        timeframe VARCHAR NOT NULL,
        trade_date DATE NOT NULL,
        start_ts TIMESTAMP,
        end_ts TIMESTAMP,
        row_count BIGINT NOT NULL,
        expected_row_count BIGINT NOT NULL,
        is_complete BOOLEAN NOT NULL,
        quality_flag VARCHAR NOT NULL,
        updated_at TIMESTAMP NOT NULL,
        PRIMARY KEY (symbol, timeframe, trade_date)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS sync_jobs (
        id VARCHAR PRIMARY KEY,
        source_id VARCHAR NOT NULL,
        status VARCHAR NOT NULL,
        started_at TIMESTAMP NOT NULL,
        finished_at TIMESTAMP,
        scanned_count BIGINT DEFAULT 0,
        downloaded_count BIGINT DEFAULT 0,
        ingested_count BIGINT DEFAULT 0,
        failed_count BIGINT DEFAULT 0,
        error_message VARCHAR
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS symbols (
        symbol VARCHAR PRIMARY KEY,
        code VARCHAR NOT NULL,
        name VARCHAR,
        exchange VARCHAR,
        listed_at DATE,
        delisted_at DATE,
        status VARCHAR,
        source VARCHAR,
        updated_at TIMESTAMP NOT NULL
    )
    """,
    "ALTER TABLE symbols ADD COLUMN IF NOT EXISTS status VARCHAR",
    "ALTER TABLE symbols ADD COLUMN IF NOT EXISTS source VARCHAR",
]
