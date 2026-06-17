from pathlib import Path

import pytest

from stock_data_service.baidu.pan_client import BaiduApiError, BaiduPanClient


class Token:
    def get_access_token(self, auto_refresh=True):
        return "token"


class Response:
    def __init__(self, payload=None, content=b"", chunks=None, headers=None):
        self.payload = payload or {}
        self.content = content
        self.chunks = chunks or [content]
        self.headers = headers or {}

    def raise_for_status(self):
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
