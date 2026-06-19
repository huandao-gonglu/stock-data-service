from stock_data_service.sync.locks import ProcessLock


def test_process_lock_clears_stale_pid_file(tmp_path):
    lock_path = tmp_path / "sync.lock"
    lock_path.write_text("999999999", encoding="ascii")

    with ProcessLock(lock_path, timeout_seconds=0.1):
        assert lock_path.exists()

    assert not lock_path.exists()
