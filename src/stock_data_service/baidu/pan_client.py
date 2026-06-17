from __future__ import annotations

import json
import logging
import time
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable

import requests

from stock_data_service.auth.token_manager import TokenManager

logger = logging.getLogger(__name__)
DownloadProgressCallback = Callable[[dict[str, Any]], None]


class BaiduApiError(RuntimeError):
    pass


@dataclass(frozen=True)
class BaiduFileMeta:
    remote_path: str
    fs_id: int
    size: int | None = None
    md5: str | None = None
    server_mtime: int | None = None


class BaiduPanClient:
    API_BASE_URL = "https://pan.baidu.com/rest/2.0/xpan"

    def __init__(
        self,
        token_manager: TokenManager,
        *,
        enable_cache: bool = False,
        cache_dir: str | Path = "./data/raw/baidu",
        session: requests.Session | None = None,
    ):
        self.token_manager = token_manager
        self.enable_cache = enable_cache
        self.cache_dir = Path(cache_dir)
        self.session = session or requests.Session()
        self._path_cache: dict[str, dict[str, int]] = {}
        self._meta_cache: dict[str, dict[str, BaiduFileMeta]] = {}

    def _make_request(
        self,
        method: str,
        endpoint: str,
        *,
        params: dict[str, Any] | None = None,
        data: dict[str, Any] | None = None,
        **kwargs: Any,
    ) -> dict[str, Any]:
        access_token = self.token_manager.get_access_token(auto_refresh=True)
        if not access_token:
            raise BaiduApiError("unable to get a valid Baidu access token")
        params = dict(params or {})
        params["access_token"] = access_token
        kwargs.setdefault("timeout", 30)
        response = self.session.request(
            method,
            f"{self.API_BASE_URL}{endpoint}",
            params=params,
            data=data,
            **kwargs,
        )
        response.raise_for_status()
        payload = response.json()
        if payload.get("errno", 0) != 0:
            logger.warning(
                "baidu api error endpoint=%s errno=%s errmsg=%s",
                endpoint,
                payload.get("errno"),
                payload.get("errmsg"),
            )
            raise BaiduApiError(payload.get("errmsg") or f"Baidu API errno={payload.get('errno')}")
        return payload

    def list_files(
        self,
        dir_path: str = "/",
        *,
        order: str = "name",
        desc: int = 0,
        start: int = 0,
        limit: int = 100,
        web: int = 1,
        folder: int = 0,
    ) -> dict[str, Any]:
        return self._make_request(
            "GET",
            "/file",
            params={
                "method": "list",
                "dir": dir_path,
                "order": order,
                "desc": desc,
                "start": start,
                "limit": limit,
                "web": web,
                "folder": folder,
            },
        )

    def build_directory_index(self, dir_path: str) -> dict[str, int]:
        logger.info("building baidu directory index dir_path=%s", dir_path)
        index = self._path_cache.setdefault(dir_path, {})
        meta_index = self._meta_cache.setdefault(dir_path, {})
        start = 0
        limit = 1000
        while True:
            payload = self.list_files(dir_path, start=start, limit=limit)
            files = payload.get("list", [])
            for item in files:
                name = item.get("server_filename")
                fs_id = item.get("fs_id")
                if name and fs_id:
                    index[name] = fs_id
                    meta_index[name] = BaiduFileMeta(
                        remote_path=f"{dir_path.rstrip('/')}/{name}" if dir_path != "/" else f"/{name}",
                        fs_id=int(fs_id),
                        size=item.get("size"),
                        md5=item.get("md5"),
                        server_mtime=item.get("server_mtime"),
                    )
            if len(files) < limit:
                break
            start += limit
        logger.info("baidu directory index built dir_path=%s file_count=%s", dir_path, len(index))
        return index

    def get_file_meta_by_path(self, path: str) -> BaiduFileMeta | None:
        path = path.replace("\\", "/")
        parent = _parent(path)
        name = Path(path).name
        if parent not in self._meta_cache:
            self.build_directory_index(parent)
        return self._meta_cache.get(parent, {}).get(name)

    def get_file_meta(self, fs_ids: list[int], *, dlink: int = 0) -> dict[str, Any]:
        return self._make_request(
            "GET",
            "/multimedia",
            params={"method": "filemetas", "fsids": json.dumps(fs_ids), "dlink": dlink},
        )

    def get_download_link(self, fs_id: int) -> str | None:
        payload = self.get_file_meta([fs_id], dlink=1)
        items = payload.get("list", [])
        if not items:
            return None
        return items[0].get("dlink")

    def download_to_file(
        self,
        dlink: str,
        dest_path: str | Path,
        *,
        progress_callback: DownloadProgressCallback | None = None,
    ) -> str:
        access_token = self.token_manager.get_access_token(auto_refresh=True)
        if not access_token:
            raise BaiduApiError("unable to get a valid Baidu access token")
        response = self.session.get(
            f"{dlink}&access_token={access_token}",
            stream=True,
            timeout=60,
            headers={"User-Agent": "pan.baidu.com"},
        )
        response.raise_for_status()
        dest = Path(dest_path)
        dest.parent.mkdir(parents=True, exist_ok=True)
        downloaded = 0
        started = time.monotonic()
        total = _content_length(response)
        with dest.open("wb") as handle:
            for chunk in response.iter_content(chunk_size=1024 * 1024):
                if chunk:
                    handle.write(chunk)
                    downloaded += len(chunk)
                    _report_download_progress(progress_callback, downloaded, total, started)
        return str(dest)

    def download_content(self, dlink: str) -> bytes:
        access_token = self.token_manager.get_access_token(auto_refresh=True)
        if not access_token:
            raise BaiduApiError("unable to get a valid Baidu access token")
        response = self.session.get(
            f"{dlink}&access_token={access_token}",
            timeout=60,
            headers={"User-Agent": "pan.baidu.com"},
        )
        response.raise_for_status()
        return response.content

    def download_by_path(
        self,
        path: str,
        dest_path: str | Path,
        *,
        progress_callback: DownloadProgressCallback | None = None,
    ) -> str | None:
        path = path.replace("\\", "/")
        cached = self._local_cache_path(path)
        if self.enable_cache and cached.exists():
            logger.info("baidu file cache hit remote_path=%s cache_path=%s", path, cached)
            dest = Path(dest_path)
            if dest.resolve() != cached.resolve():
                dest.parent.mkdir(parents=True, exist_ok=True)
                dest.write_bytes(cached.read_bytes())
            return str(dest)

        fs_id = self._fs_id_for_path(path)
        if fs_id is None:
            logger.info("baidu file not found remote_path=%s", path)
            return None
        dlink = self.get_download_link(fs_id)
        if not dlink:
            logger.warning("baidu download link missing remote_path=%s fs_id=%s", path, fs_id)
            return None
        saved = self.download_to_file(dlink, dest_path, progress_callback=progress_callback)
        if self.enable_cache:
            cache_path = self._local_cache_path(path)
            cache_path.parent.mkdir(parents=True, exist_ok=True)
            if Path(saved).resolve() != cache_path.resolve():
                cache_path.write_bytes(Path(saved).read_bytes())
        return saved

    def download_content_by_path(self, path: str) -> bytes | None:
        path = path.replace("\\", "/")
        cache_path = self._local_cache_path(path)
        if self.enable_cache and cache_path.exists():
            logger.info("baidu content cache hit remote_path=%s cache_path=%s", path, cache_path)
            return cache_path.read_bytes()
        fs_id = self._fs_id_for_path(path)
        if fs_id is None:
            logger.info("baidu content file not found remote_path=%s", path)
            return None
        dlink = self.get_download_link(fs_id)
        if not dlink:
            return None
        content = self.download_content(dlink)
        if self.enable_cache:
            cache_path.parent.mkdir(parents=True, exist_ok=True)
            cache_path.write_bytes(content)
        return content

    def download_batch_content(self, paths: list[str], max_workers: int = 5) -> dict[str, bytes]:
        self.token_manager.get_access_token(auto_refresh=True)
        results: dict[str, bytes] = {}
        paths_to_download: list[str] = []
        for path in paths:
            cache_path = self._local_cache_path(path)
            if self.enable_cache and cache_path.exists():
                results[path] = cache_path.read_bytes()
            else:
                paths_to_download.append(path)
        if not paths_to_download:
            return results

        by_parent: dict[str, list[str]] = defaultdict(list)
        for path in paths_to_download:
            by_parent[_parent(path)].append(path)
        path_to_fsid: dict[str, int] = {}
        for parent, grouped_paths in by_parent.items():
            if parent not in self._path_cache:
                self.build_directory_index(parent)
            index = self._path_cache[parent]
            for path in grouped_paths:
                fs_id = index.get(Path(path).name)
                if fs_id:
                    path_to_fsid[path] = fs_id

        fsid_to_dlink: dict[int, str] = {}
        fs_ids = list(dict.fromkeys(path_to_fsid.values()))
        for idx in range(0, len(fs_ids), 100):
            payload = self.get_file_meta(fs_ids[idx : idx + 100], dlink=1)
            for item in payload.get("list", []):
                if item.get("fs_id") and item.get("dlink"):
                    fsid_to_dlink[item["fs_id"]] = item["dlink"]

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {
                executor.submit(self.download_content, fsid_to_dlink[fs_id]): path
                for path, fs_id in path_to_fsid.items()
                if fs_id in fsid_to_dlink
            }
            for future in as_completed(futures):
                path = futures[future]
                try:
                    content = future.result()
                except Exception:
                    continue
                results[path] = content
                if self.enable_cache:
                    cache_path = self._local_cache_path(path)
                    cache_path.parent.mkdir(parents=True, exist_ok=True)
                    cache_path.write_bytes(content)
        return results

    def _fs_id_for_path(self, path: str) -> int | None:
        parent = _parent(path)
        name = Path(path).name
        if parent not in self._path_cache:
            self.build_directory_index(parent)
        return self._path_cache[parent].get(name)

    def _local_cache_path(self, path: str) -> Path:
        return self.cache_dir / path.lstrip("/")


def _parent(path: str) -> str:
    parent = str(Path(path.replace("\\", "/")).parent).replace("\\", "/")
    return "/" if parent == "." else parent


def _content_length(response: requests.Response) -> int | None:
    headers = getattr(response, "headers", {}) or {}
    value = headers.get("Content-Length") or headers.get("content-length")
    if not value:
        return None
    try:
        return int(value)
    except ValueError:
        return None


def _report_download_progress(
    progress_callback: DownloadProgressCallback | None,
    downloaded: int,
    total: int | None,
    started: float,
) -> None:
    if progress_callback is None:
        return
    elapsed = max(time.monotonic() - started, 0.001)
    progress_callback(
        {
            "bytes_downloaded": downloaded,
            "total_bytes": total,
            "speed_bytes_per_sec": downloaded / elapsed,
        }
    )
