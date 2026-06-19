import datetime as dt

from stock_data_service.market.security_master import SecurityListing
from stock_data_service.storage.sync_metadata import SyncMetadata


def test_creates_metadata_tables_idempotently(tmp_path):
    meta = SyncMetadata(tmp_path / "meta.duckdb")
    meta.initialize()
    meta.initialize()
    assert meta.fetchone("SELECT count(*) FROM information_schema.tables WHERE table_name='remote_files'")[0] == 1


def test_remote_files_and_ingests_are_independent(metadata):
    assert (
        metadata.upsert_remote_file(
            source_id="s", remote_path="/x.zip", size=1, content_hash="a", local_raw_path="/tmp/x.zip"
        )
        == "discovered"
    )
    assert (
        metadata.upsert_remote_file(
            source_id="s", remote_path="/x.zip", size=1, content_hash="a", local_raw_path="/tmp/x.zip"
        )
        == "skipped"
    )
    metadata.mark_remote_downloaded("s", "/x.zip", "/tmp/x.zip")
    metadata.commit_file_ingest(
        source_id="s",
        remote_path="/x.zip",
        timeframe="1m",
        symbol="sh600000",
        start_ts=dt.datetime(2024, 12, 20, 9, 30),
        end_ts=dt.datetime(2024, 12, 20, 9, 31),
        row_count=1,
        expected_row_count=240,
        content_hash="a",
        parquet_path="/p",
    )
    metadata.mark_file_ingest_status(
        source_id="s",
        remote_path="/x.zip",
        timeframe="1m",
        symbol="sz000001",
        status="symbol_missing",
        content_hash="a",
    )
    rows = metadata.fetchall("SELECT symbol, status FROM file_ingests ORDER BY symbol")
    assert rows == [("sh600000", "committed"), ("sz000001", "symbol_missing")]


def test_remote_file_changed_metadata_is_discovered_again(metadata):
    assert metadata.upsert_remote_file(source_id="s", remote_path="/x.zip", size=1, md5="a") == "discovered"
    assert metadata.upsert_remote_file(source_id="s", remote_path="/x.zip", size=1, md5="a") == "skipped"
    assert metadata.upsert_remote_file(source_id="s", remote_path="/x.zip", size=2, md5="b") == "discovered"
    assert metadata.fetchone("SELECT size, md5, status FROM remote_files WHERE remote_path = '/x.zip'") == (
        2,
        "b",
        "discovered",
    )


def test_records_parse_failure_statuses_distinctly(metadata):
    for status in ["symbol_missing", "parse_failed", "corrupted_zip"]:
        metadata.mark_file_ingest_status(
            source_id="s",
            remote_path=f"/{status}.zip",
            timeframe="1m",
            symbol="sh600000",
            status=status,
            error_message=status,
            content_hash=status,
        )
    assert metadata.fetchall("SELECT status FROM file_ingests ORDER BY status") == [
        ("corrupted_zip",),
        ("parse_failed",),
        ("symbol_missing",),
    ]


def test_marks_file_ingest_statuses_in_batch(metadata):
    metadata.commit_file_ingest(
        source_id="s",
        remote_path="/x.zip",
        timeframe="1m",
        symbol="sh600000",
        start_ts=dt.datetime(2024, 12, 20, 9, 30),
        end_ts=dt.datetime(2024, 12, 20, 9, 31),
        row_count=1,
        expected_row_count=240,
        content_hash="a",
        parquet_path="/p",
    )

    count = metadata.mark_file_ingest_status_many(
        source_id="s",
        remote_path="/x.zip",
        timeframe="1m",
        symbols=["sz000001", "sh600004", "sz000001"],
        status="symbol_missing",
        error_message="symbol not found in archive",
        content_hash="a",
    )

    assert count == 2
    assert metadata.fetchall("SELECT symbol, status, error_message FROM file_ingests ORDER BY symbol") == [
        ("sh600000", "committed", None),
        ("sh600004", "symbol_missing", "symbol not found in archive"),
        ("sz000001", "symbol_missing", "symbol not found in archive"),
    ]


def test_archive_symbol_members_cache_is_content_hash_scoped(metadata):
    metadata.upsert_archive_symbol_members(
        source_id="s",
        remote_path="/x.zip",
        content_hash="hash-a",
        members={"sh600000": "nested/sh600000.csv", "sz000001": "sz000001.csv"},
    )

    assert metadata.get_archive_symbol_members(source_id="s", remote_path="/x.zip", content_hash="hash-a") == {
        "sh600000": "nested/sh600000.csv",
        "sz000001": "sz000001.csv",
    }
    assert metadata.get_archive_symbol_members(source_id="s", remote_path="/x.zip", content_hash="hash-b") is None


def test_upserts_symbol_listing_metadata(metadata):
    count = metadata.upsert_symbols(
        [
            SecurityListing(
                symbol="sh600000",
                code="600000",
                name="浦发银行",
                exchange="sh",
                listed_at=dt.date(1999, 11, 10),
                delisted_at=None,
                status="listed",
                source="baostock",
            )
        ]
    )

    assert count == 1
    assert metadata.fetchone(
        "SELECT symbol, code, name, exchange, listed_at, delisted_at, status, source FROM symbols"
    ) == ("sh600000", "600000", "浦发银行", "sh", dt.date(1999, 11, 10), None, "listed", "baostock")


def test_batch_ingest_symbols_do_not_overwrite_authoritative_listing_metadata(metadata):
    metadata.upsert_symbols(
        [
            SecurityListing(
                symbol="sh600000",
                code="600000",
                name="Official Name",
                exchange="sh",
                listed_at=dt.date(1999, 11, 10),
                delisted_at=None,
                status="listed",
                source="baostock",
            )
        ]
    )

    metadata.record_ingest_metadata_many(
        symbol_records=[
            {
                "symbol": "sh600000",
                "code": "600000",
                "name": "Archive Name",
                "exchange": "sh",
                "source": "ingest",
            },
            {
                "symbol": "sz000001",
                "code": "000001",
                "name": "Archive Only",
                "exchange": "sz",
                "source": "ingest",
            },
        ]
    )

    assert metadata.fetchone(
        "SELECT name, listed_at, status, source FROM symbols WHERE symbol = 'sh600000'"
    ) == ("Official Name", dt.date(1999, 11, 10), "listed", "baostock")
    assert metadata.fetchone(
        "SELECT name, listed_at, status, source FROM symbols WHERE symbol = 'sz000001'"
    ) == ("Archive Only", None, None, "ingest")


def test_single_ingest_symbol_does_not_overwrite_authoritative_listing_metadata(metadata):
    metadata.upsert_symbol(
        symbol="sh600000",
        code="600000",
        name="Official Name",
        exchange="sh",
        listed_at=dt.date(1999, 11, 10),
        status="listed",
        source="baostock",
    )

    metadata.upsert_symbol(symbol="sh600000", code="600000", name="Archive Name", exchange="sh")
    metadata.upsert_symbol(symbol="sz000001", code="000001", name="Archive Only", exchange="sz")

    assert metadata.fetchone(
        "SELECT name, listed_at, status, source FROM symbols WHERE symbol = 'sh600000'"
    ) == ("Official Name", dt.date(1999, 11, 10), "listed", "baostock")
    assert metadata.fetchone(
        "SELECT name, listed_at, status, source FROM symbols WHERE symbol = 'sz000001'"
    ) == ("Archive Only", None, None, "ingest")


def test_coverage_and_sync_job_counters(metadata):
    metadata.update_coverage_daily(
        symbol="sh600000",
        timeframe="1m",
        trade_date=dt.date(2024, 12, 20),
        start_ts=dt.datetime(2024, 12, 20, 9, 30),
        end_ts=dt.datetime(2024, 12, 20, 9, 31),
        row_count=1,
        expected_row_count=240,
        is_complete=False,
        quality_flag="partial",
    )
    job = metadata.create_sync_job("s")
    metadata.complete_sync_job(job, scanned_count=2, downloaded_count=2, ingested_count=1, failed_count=1)
    assert metadata.fetchone("SELECT row_count, quality_flag FROM coverage_daily")[0:2] == (1, "partial")
    assert metadata.fetchone("SELECT scanned_count, ingested_count FROM sync_jobs WHERE id = ?", [job]) == (2, 1)


def test_updates_coverage_daily_in_batch(metadata):
    metadata.update_coverage_daily_many(
        [
            {
                "symbol": "sh600000",
                "timeframe": "1m",
                "trade_date": dt.date(2024, 12, 20),
                "start_ts": dt.datetime(2024, 12, 20, 9, 30),
                "end_ts": dt.datetime(2024, 12, 20, 15, 0),
                "row_count": 240,
                "expected_row_count": 240,
                "is_complete": True,
                "quality_flag": "ok",
            },
            {
                "symbol": "sz000001",
                "timeframe": "1m",
                "trade_date": dt.date(2024, 12, 20),
                "start_ts": dt.datetime(2024, 12, 20, 9, 30),
                "end_ts": dt.datetime(2024, 12, 20, 14, 0),
                "row_count": 180,
                "expected_row_count": 240,
                "is_complete": False,
                "quality_flag": "partial",
            },
        ]
    )
    metadata.update_coverage_daily_many(
        [
            {
                "symbol": "sh600000",
                "timeframe": "1m",
                "trade_date": dt.date(2024, 12, 20),
                "start_ts": dt.datetime(2024, 12, 20, 9, 30),
                "end_ts": dt.datetime(2024, 12, 20, 11, 0),
                "row_count": 90,
                "expected_row_count": 240,
                "is_complete": False,
                "quality_flag": "partial",
            }
        ]
    )

    assert metadata.fetchall("SELECT symbol, row_count, quality_flag FROM coverage_daily ORDER BY symbol") == [
        ("sh600000", 90, "partial"),
        ("sz000001", 180, "partial"),
    ]


def test_records_ingest_metadata_in_one_batch(metadata):
    metadata.record_ingest_metadata_many(
        symbol_records=[
            {
                "symbol": "sh600000",
                "code": "600000",
                "name": "浦发银行",
                "exchange": "sh",
                "source": "ingest",
            }
        ],
        coverage_rows=[
            {
                "symbol": "sh600000",
                "timeframe": "1m",
                "trade_date": dt.date(2024, 12, 20),
                "start_ts": dt.datetime(2024, 12, 20, 9, 30),
                "end_ts": dt.datetime(2024, 12, 20, 9, 32),
                "row_count": 2,
                "expected_row_count": 240,
                "is_complete": False,
                "quality_flag": "partial",
            }
        ],
        file_ingest_rows=[
            {
                "source_id": "s",
                "remote_path": "/x.zip",
                "timeframe": "1m",
                "symbol": "sh600000",
                "start_ts": dt.datetime(2024, 12, 20, 9, 30),
                "end_ts": dt.datetime(2024, 12, 20, 9, 32),
                "row_count": 2,
                "expected_row_count": 240,
                "content_hash": "hash",
                "parquet_path": "/p",
                "status": "committed",
                "error_message": None,
            },
            {
                "source_id": "s",
                "remote_path": "/x.zip",
                "timeframe": "1m",
                "symbol": "sz000001",
                "row_count": 0,
                "content_hash": "hash",
                "status": "parse_failed",
                "error_message": "bad csv",
            },
        ],
    )

    assert metadata.fetchone("SELECT symbol, code, name, source FROM symbols WHERE symbol='sh600000'") == (
        "sh600000",
        "600000",
        "浦发银行",
        "ingest",
    )
    assert metadata.fetchone("SELECT row_count, quality_flag FROM coverage_daily WHERE symbol='sh600000'") == (
        2,
        "partial",
    )
    assert metadata.fetchall("SELECT symbol, status, row_count, error_message FROM file_ingests ORDER BY symbol") == [
        ("sh600000", "committed", 2, None),
        ("sz000001", "parse_failed", 0, "bad csv"),
    ]


def test_marks_unfinished_sync_jobs_stopped(metadata):
    running_job = metadata.create_sync_job("s")
    completed_job = metadata.create_sync_job("s")
    metadata.complete_sync_job(completed_job, status="completed")

    assert metadata.mark_unfinished_sync_jobs_stopped(error_message="recovered") == 1

    assert metadata.fetchone("SELECT status, error_message FROM sync_jobs WHERE id = ?", [running_job]) == (
        "stopped",
        "recovered",
    )
    assert metadata.fetchone("SELECT status, error_message FROM sync_jobs WHERE id = ?", [completed_job]) == (
        "completed",
        None,
    )
    assert metadata.mark_unfinished_sync_jobs_stopped(error_message="again") == 0


def test_dates_requiring_sync_skips_only_dates_complete_for_all_symbols(metadata):
    dates = [dt.date(2024, 12, 20), dt.date(2024, 12, 23), dt.date(2024, 12, 24)]
    for symbol in ["sh600000", "sz000001"]:
        metadata.update_coverage_daily(
            symbol=symbol,
            timeframe="1m",
            trade_date=dt.date(2024, 12, 20),
            start_ts=dt.datetime(2024, 12, 20, 9, 30),
            end_ts=dt.datetime(2024, 12, 20, 15, 0),
            row_count=240,
            expected_row_count=240,
            is_complete=True,
            quality_flag="ok",
        )
    metadata.update_coverage_daily(
        symbol="sh600000",
        timeframe="1m",
        trade_date=dt.date(2024, 12, 23),
        start_ts=dt.datetime(2024, 12, 23, 9, 30),
        end_ts=dt.datetime(2024, 12, 23, 10, 0),
        row_count=30,
        expected_row_count=240,
        is_complete=False,
        quality_flag="partial",
    )

    assert metadata.dates_requiring_sync(symbols=["sh600000", "sz000001"], timeframe="1m", trade_dates=dates) == [
        dt.date(2024, 12, 23),
        dt.date(2024, 12, 24),
    ]
