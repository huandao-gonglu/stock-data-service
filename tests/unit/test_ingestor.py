import datetime as dt

import pytest

from conftest import make_zip, sample_rows
from stock_data_service.market.timeframe import Timeframe
from stock_data_service.storage.parquet_writer import ParquetBarWriter
from stock_data_service.storage.sync_metadata import SyncMetadata
from stock_data_service.sync.ingestor import Ingestor
from stock_data_service.sync.local_source import LocalRawFile


def test_archive_cancel_before_metadata_commit_does_not_record_file_ingest(tmp_path):
    zip_path = tmp_path / "20241220_1min.zip"
    zip_path.write_bytes(make_zip(sample_rows()))
    metadata = SyncMetadata(tmp_path / "meta.duckdb")
    metadata.initialize()
    raw_file = LocalRawFile(
        remote_path="/x/20241220_1min.zip",
        local_path=zip_path,
        size=zip_path.stat().st_size,
        content_hash="hash",
        trade_date=dt.date(2024, 12, 20),
        timeframe=Timeframe.M1,
    )
    cancelled = False

    def cancel_check():
        if cancelled:
            raise RuntimeError("cancelled")

    def progress_callback(symbol, outcome):
        nonlocal cancelled
        if symbol == "sh600000" and outcome.status == "committed":
            cancelled = True

    with pytest.raises(RuntimeError, match="cancelled"):
        Ingestor(
            writer=ParquetBarWriter(tmp_path / "parquet"),
            metadata=metadata,
            archive_workers=1,
        ).ingest_archive_result(
            raw_file,
            symbols=["sh600000"],
            start=dt.datetime(2024, 12, 20),
            end=dt.datetime(2024, 12, 21),
            cancel_check=cancel_check,
            progress_callback=progress_callback,
        )

    assert metadata.fetchall("SELECT symbol, status FROM file_ingests") == []
