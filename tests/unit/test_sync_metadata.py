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
