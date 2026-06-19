from __future__ import annotations

import json
import logging
import threading
import time
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from pathlib import Path, PurePosixPath
from typing import Any, Callable

import requests

from stock_data_service.auth.token_manager import TokenManager

logger = logging.getLogger(__name__)
DownloadProgressCallback = Callable[[dict[str, Any]], None]


class BaiduApiError(RuntimeError):
    def __init__(self, message: str, *, errno: int | None = None):
        super().__init__(message)
        self.errno = errno


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
            errno = payload.get("errno")
            raise BaiduApiError(payload.get("errmsg") or f"Baidu API errno={errno}", errno=errno)
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
        dir_path = _normalize_remote_path(dir_path)
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
        dir_path = _normalize_remote_path(dir_path)
        logger.info("building baidu directory index dir_path=%s", dir_path)
        index = self._path_cache.setdefault(dir_path, {})
        meta_index = self._meta_cache.setdefault(dir_path, {})
        start = 0
        limit = 1000
        while True:
            try:
                payload = self.list_files(dir_path, start=start, limit=limit)
            except BaiduApiError as exc:
                if exc.errno == -9:
                    logger.info("baidu directory missing dir_path=%s", dir_path)
                    break
                raise
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
        path = _normalize_remote_path(path)
        parent = _parent(path)
        name = _remote_name(path)
        if parent not in self._meta_cache:
            self.build_directory_index(parent)
        meta = self._meta_cache.get(parent, {}).get(name)
        if meta and _normalize_remote_path(meta.remote_path) != path:
            logger.warning(
                "baidu metadata path mismatch requested_path=%s meta_path=%s fs_id=%s",
                path,
                meta.remote_path,
                meta.fs_id,
            )
            return None
        return meta

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
        cancel_event: threading.Event | None = None,
    ) -> str:
        access_token = self.token_manager.get_access_token(auto_refresh=True)
        if not access_token:
            raise BaiduApiError("unable to get a valid Baidu access token")
        _raise_if_download_cancelled(cancel_event)
        dest = Path(dest_path)
        dest.parent.mkdir(parents=True, exist_ok=True)
        part = _partial_path(dest)
        resume_from = part.stat().st_size if part.exists() else 0
        headers = {"User-Agent": "pan.baidu.com"}
        if resume_from > 0:
            headers["Range"] = f"bytes={resume_from}-"
        response = self.session.get(
            f"{dlink}&access_token={access_token}",
            stream=True,
            timeout=60,
            headers=headers,
        )
        try:
            response.raise_for_status()
        except requests.HTTPError:
            if resume_from <= 0 or getattr(response, "status_code", None) != 416:
                raise
            logger.info("baidu resume range invalid, restarting download dest_path=%s", dest)
            resume_from = 0
            response = self.session.get(
                f"{dlink}&access_token={access_token}",
                stream=True,
                timeout=60,
                headers={"User-Agent": "pan.baidu.com"},
            )
            response.raise_for_status()
        status_code = getattr(response, "status_code", 200)
        append = resume_from > 0 and status_code == 206
        downloaded = resume_from if append else 0
        started = time.monotonic()
        total = _download_total(response, resume_from if append else 0)
        if resume_from > 0 and not append:
            logger.info("baidu resume not supported, restarting download dest_path=%s", dest)
        with part.open("ab" if append else "wb") as handle:
            for chunk in response.iter_content(chunk_size=1024 * 1024):
                _raise_if_download_cancelled(cancel_event)
                if chunk:
                    handle.write(chunk)
                    downloaded += len(chunk)
                    _report_download_progress(progress_callback, downloaded, total, started)
                _raise_if_download_cancelled(cancel_event)
        part.replace(dest)
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
        cancel_event: threading.Event | None = None,
        use_local_cache: bool = True,
    ) -> str | None:
        path = _normalize_remote_path(path)
        cached = self._local_cache_path(path)
        if use_local_cache and self.enable_cache and cached.exists():
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
        saved = self.download_to_file(dlink, dest_path, progress_callback=progress_callback, cancel_event=cancel_event)
        if self.enable_cache:
            cache_path = self._local_cache_path(path)
            cache_path.parent.mkdir(parents=True, exist_ok=True)
            if Path(saved).resolve() != cache_path.resolve():
                cache_path.write_bytes(Path(saved).read_bytes())
        return saved

    def download_content_by_path(self, path: str) -> bytes | None:
        path = _normalize_remote_path(path)
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
        paths = [_normalize_remote_path(path) for path in paths]
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
                fs_id = index.get(_remote_name(path))
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
        path = _normalize_remote_path(path)
        parent = _parent(path)
        name = _remote_name(path)
        if parent not in self._path_cache:
            self.build_directory_index(parent)
        return self._path_cache[parent].get(name)

    def _local_cache_path(self, path: str) -> Path:
        path = _normalize_remote_path(path)
        return self.cache_dir / path.lstrip("/")


def _normalize_remote_path(path: str) -> str:
    text = str(path or "/").strip().replace("\\", "/")
    while "//" in text:
        text = text.replace("//", "/")
    if not text.startswith("/"):
        text = f"/{text}"
    if len(text) > 1:
        text = text.rstrip("/")
    return text or "/"


def _parent(path: str) -> str:
    parent = str(PurePosixPath(_normalize_remote_path(path)).parent)
    return "/" if parent == "." else parent


def _remote_name(path: str) -> str:
    return PurePosixPath(_normalize_remote_path(path)).name


def _content_length(response: requests.Response) -> int | None:
    headers = getattr(response, "headers", {}) or {}
    value = headers.get("Content-Length") or headers.get("content-length")
    if not value:
        return None
    try:
        return int(value)
    except ValueError:
        return None


def _content_range_total(response: requests.Response) -> int | None:
    headers = getattr(response, "headers", {}) or {}
    value = headers.get("Content-Range") or headers.get("content-range")
    if not value or "/" not in value:
        return None
    total = value.rsplit("/", 1)[-1].strip()
    if total == "*":
        return None
    try:
        return int(total)
    except ValueError:
        return None


def _download_total(response: requests.Response, resumed_bytes: int) -> int | None:
    content_range_total = _content_range_total(response)
    if content_range_total is not None:
        return content_range_total
    content_length = _content_length(response)
    if content_length is None:
        return None
    return resumed_bytes + content_length


def _partial_path(dest: Path) -> Path:
    return dest.with_name(f"{dest.name}.part")


def _raise_if_download_cancelled(cancel_event: threading.Event | None) -> None:
    if cancel_event and cancel_event.is_set():
        raise RuntimeError("download cancelled")


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
