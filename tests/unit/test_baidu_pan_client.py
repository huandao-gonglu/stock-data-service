import threading
from pathlib import Path

import pytest
import requests

from stock_data_service.baidu.pan_client import BaiduApiError, BaiduFileMeta, BaiduPanClient


class Token:
    def get_access_token(self, auto_refresh=True):
        return "token"


class Response:
    def __init__(self, payload=None, content=b"", chunks=None, headers=None, status_code=200, raise_http=False):
        self.payload = payload or {}
        self.content = content
        self.chunks = chunks or [content]
        self.headers = headers or {}
        self.status_code = status_code
        self.raise_http = raise_http

    def raise_for_status(self):
        if self.raise_http:
            raise requests.HTTPError(response=self)
        return None

    def json(self):
        return self.payload

    def iter_content(self, chunk_size=8192):
        yield from self.chunks


class Session:
    def __init__(self):
        self.requests = []
        self.gets = []

    def request(self, method, url, **kwargs):
        self.requests.append((method, url, kwargs))
        if kwargs["params"].get("method") == "list":
            return Response({"errno": 0, "list": [{"server_filename": "x.zip", "fs_id": 1}]})
        if kwargs["params"].get("method") == "filemetas":
            return Response({"errno": 0, "list": [{"fs_id": 1, "dlink": "https://download"}]})
        return Response({"errno": 0})

    def get(self, url, **kwargs):
        self.gets.append((url, kwargs))
        return Response(content=b"zip-bytes", headers={"Content-Length": "9"})


def test_adds_access_token_and_raises_baidu_error():
    session = Session()
    client = BaiduPanClient(Token(), session=session)
    client.list_files("/")
    assert session.requests[0][2]["params"]["access_token"] == "token"

    class ErrorSession(Session):
        def request(self, method, url, **kwargs):
            return Response({"errno": 1, "errmsg": "bad"})

    with pytest.raises(BaiduApiError):
        BaiduPanClient(Token(), session=ErrorSession()).list_files("/")


def test_download_by_path_uses_index_and_user_agent(tmp_path):
    session = Session()
    client = BaiduPanClient(Token(), enable_cache=True, cache_dir=tmp_path / "cache", session=session)
    saved = client.download_by_path("/dir/x.zip", tmp_path / "x.zip")
    assert Path(saved).read_bytes() == b"zip-bytes"
    assert session.gets[0][1]["headers"]["User-Agent"] == "pan.baidu.com"
    assert (tmp_path / "cache/dir/x.zip").read_bytes() == b"zip-bytes"


def test_download_by_path_can_bypass_existing_local_cache(tmp_path):
    cache_file = tmp_path / "cache/dir/x.zip"
    cache_file.parent.mkdir(parents=True)
    cache_file.write_bytes(b"cached")
    session = Session()
    client = BaiduPanClient(Token(), enable_cache=True, cache_dir=tmp_path / "cache", session=session)

    saved = client.download_by_path("/dir/x.zip", cache_file, use_local_cache=False)

    assert Path(saved).read_bytes() == b"zip-bytes"
    assert cache_file.read_bytes() == b"zip-bytes"
    assert [call[2]["params"].get("method") for call in session.requests] == ["list", "filemetas"]
    assert session.gets


def test_download_to_file_reports_progress(tmp_path):
    class ChunkSession(Session):
        def get(self, url, **kwargs):
            self.gets.append((url, kwargs))
            return Response(chunks=[b"zip", b"-bytes"], headers={"Content-Length": "9"})

    progress = []
    client = BaiduPanClient(Token(), session=ChunkSession())
    saved = client.download_to_file("https://download", tmp_path / "x.zip", progress_callback=progress.append)

    assert Path(saved).read_bytes() == b"zip-bytes"
    assert progress[-1]["bytes_downloaded"] == 9
    assert progress[-1]["total_bytes"] == 9
    assert progress[-1]["speed_bytes_per_sec"] > 0


def test_download_to_file_resumes_partial_file(tmp_path):
    class ResumeSession(Session):
        def get(self, url, **kwargs):
            self.gets.append((url, kwargs))
            return Response(
                chunks=[b"-bytes"],
                headers={"Content-Length": "6", "Content-Range": "bytes 3-8/9"},
                status_code=206,
            )

    dest = tmp_path / "x.zip"
    (tmp_path / "x.zip.part").write_bytes(b"zip")
    progress = []
    client = BaiduPanClient(Token(), session=ResumeSession())

    saved = client.download_to_file("https://download", dest, progress_callback=progress.append)

    assert Path(saved).read_bytes() == b"zip-bytes"
    assert not (tmp_path / "x.zip.part").exists()
    assert client.session.gets[0][1]["headers"]["Range"] == "bytes=3-"
    assert progress[-1]["bytes_downloaded"] == 9
    assert progress[-1]["total_bytes"] == 9


def test_download_to_file_restarts_when_range_is_ignored(tmp_path):
    class NoResumeSession(Session):
        def get(self, url, **kwargs):
            self.gets.append((url, kwargs))
            return Response(chunks=[b"fresh"], headers={"Content-Length": "5"}, status_code=200)

    dest = tmp_path / "x.zip"
    (tmp_path / "x.zip.part").write_bytes(b"stale-part")
    client = BaiduPanClient(Token(), session=NoResumeSession())

    saved = client.download_to_file("https://download", dest)

    assert Path(saved).read_bytes() == b"fresh"
    assert client.session.gets[0][1]["headers"]["Range"] == "bytes=10-"


def test_download_to_file_restarts_when_resume_range_is_invalid(tmp_path):
    class InvalidRangeSession(Session):
        def get(self, url, **kwargs):
            self.gets.append((url, kwargs))
            if len(self.gets) == 1:
                return Response(status_code=416, raise_http=True)
            return Response(chunks=[b"fresh"], headers={"Content-Length": "5"}, status_code=200)

    dest = tmp_path / "x.zip"
    (tmp_path / "x.zip.part").write_bytes(b"stale-part")
    client = BaiduPanClient(Token(), session=InvalidRangeSession())

    saved = client.download_to_file("https://download", dest)

    assert Path(saved).read_bytes() == b"fresh"
    assert client.session.gets[0][1]["headers"]["Range"] == "bytes=10-"
    assert "Range" not in client.session.gets[1][1]["headers"]


def test_download_to_file_can_be_cancelled_mid_stream(tmp_path):
    class ChunkSession(Session):
        def get(self, url, **kwargs):
            self.gets.append((url, kwargs))
            return Response(chunks=[b"zip", b"-bytes"], headers={"Content-Length": "9"})

    cancel_event = threading.Event()
    client = BaiduPanClient(Token(), session=ChunkSession())

    def cancel_after_first_chunk(progress):
        cancel_event.set()

    with pytest.raises(RuntimeError, match="download cancelled"):
        client.download_to_file(
            "https://download",
            tmp_path / "x.zip",
            progress_callback=cancel_after_first_chunk,
            cancel_event=cancel_event,
        )

    assert not (tmp_path / "x.zip").exists()
    assert (tmp_path / "x.zip.part").read_bytes() == b"zip"


def test_batch_download_groups_directory_and_handles_cache(tmp_path):
    client = BaiduPanClient(Token(), enable_cache=True, cache_dir=tmp_path / "cache", session=Session())
    (tmp_path / "cache/dir").mkdir(parents=True)
    (tmp_path / "cache/dir/cached.zip").write_bytes(b"cached")
    result = client.download_batch_content(["/dir/cached.zip", "/dir/x.zip"])
    assert result["/dir/cached.zip"] == b"cached"
    assert result["/dir/x.zip"] == b"zip-bytes"


def test_build_directory_index_pages_and_keeps_file_meta():
    class PagingSession(Session):
        def request(self, method, url, **kwargs):
            self.requests.append((method, url, kwargs))
            params = kwargs["params"]
            if params.get("method") == "list":
                start = params["start"]
                if start == 0:
                    return Response(
                        {
                            "errno": 0,
                            "list": [
                                {"server_filename": f"{idx}.zip", "fs_id": idx + 1, "size": idx, "md5": f"md5-{idx}"}
                                for idx in range(1000)
                            ],
                        }
                    )
                return Response({"errno": 0, "list": [{"server_filename": "last.zip", "fs_id": 1001, "size": 7}]})
            return super().request(method, url, **kwargs)

    client = BaiduPanClient(Token(), session=PagingSession())
    index = client.build_directory_index("/dir")
    assert index["0.zip"] == 1
    assert index["last.zip"] == 1001
    meta = client.get_file_meta_by_path("/dir/last.zip")
    assert meta.size == 7
    assert [call[2]["params"]["start"] for call in client.session.requests if call[2]["params"].get("method") == "list"] == [
        0,
        1000,
    ]


def test_file_meta_lookup_is_scoped_to_parent_directory():
    class ScopedSession(Session):
        def request(self, method, url, **kwargs):
            self.requests.append((method, url, kwargs))
            params = kwargs["params"]
            if params.get("method") == "list":
                dir_path = params["dir"]
                fs_id = 1 if dir_path == "/dir-a" else 2
                return Response(
                    {
                        "errno": 0,
                        "list": [
                            {
                                "server_filename": "same.zip",
                                "fs_id": fs_id,
                                "size": fs_id,
                                "md5": f"md5-{fs_id}",
                            }
                        ],
                    }
                )
            return super().request(method, url, **kwargs)

    client = BaiduPanClient(Token(), session=ScopedSession())

    assert client.get_file_meta_by_path("/dir-a/same.zip").fs_id == 1
    assert client.get_file_meta_by_path("dir-a\\same.zip").fs_id == 1
    assert client.get_file_meta_by_path("//dir-a//same.zip/").fs_id == 1
    assert client.get_file_meta_by_path("/dir-b/same.zip").fs_id == 2


def test_file_meta_lookup_rejects_cached_path_mismatch():
    client = BaiduPanClient(Token(), session=Session())
    client._meta_cache["/dir"] = {
        "x.zip": BaiduFileMeta(remote_path="/other/x.zip", fs_id=99),
    }

    assert client.get_file_meta_by_path("/dir/x.zip") is None


def test_missing_directory_builds_empty_index():
    class MissingDirSession(Session):
        def request(self, method, url, **kwargs):
            self.requests.append((method, url, kwargs))
            return Response({"errno": -9})

    client = BaiduPanClient(Token(), session=MissingDirSession())
    assert client.build_directory_index("/missing") == {}
    assert client.get_file_meta_by_path("/missing/x.zip") is None


def test_download_content_by_path_uses_local_cache_without_api_request(tmp_path):
    cache_file = tmp_path / "cache/dir/x.zip"
    cache_file.parent.mkdir(parents=True)
    cache_file.write_bytes(b"cached")
    session = Session()
    client = BaiduPanClient(Token(), enable_cache=True, cache_dir=tmp_path / "cache", session=session)
    assert client.download_content_by_path("/dir/x.zip") == b"cached"
    assert session.requests == []
    assert session.gets == []


def test_batch_download_keeps_successful_results_when_one_download_fails(tmp_path):
    class PartialSession(Session):
        def request(self, method, url, **kwargs):
            self.requests.append((method, url, kwargs))
            params = kwargs["params"]
            if params.get("method") == "list":
                return Response(
                    {
                        "errno": 0,
                        "list": [
                            {"server_filename": "ok.zip", "fs_id": 1},
                            {"server_filename": "bad.zip", "fs_id": 2},
                        ],
                    }
                )
            if params.get("method") == "filemetas":
                return Response(
                    {
                        "errno": 0,
                        "list": [
                            {"fs_id": 1, "dlink": "https://download/ok"},
                            {"fs_id": 2, "dlink": "https://download/bad"},
                        ],
                    }
                )
            return Response({"errno": 0})

        def get(self, url, **kwargs):
            self.gets.append((url, kwargs))
            if url.startswith("https://download/bad"):
                raise RuntimeError("boom")
            return Response(content=b"ok")

    client = BaiduPanClient(Token(), enable_cache=True, cache_dir=tmp_path / "cache", session=PartialSession())
    result = client.download_batch_content(["/dir/ok.zip", "/dir/bad.zip"])
    assert result == {"/dir/ok.zip": b"ok"}
    assert (tmp_path / "cache/dir/ok.zip").read_bytes() == b"ok"
    assert not (tmp_path / "cache/dir/bad.zip").exists()
