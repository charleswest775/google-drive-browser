"""
Download queue manager with concurrency control.
"""

import os
import re
import threading
import uuid
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from enum import Enum


class DownloadStatus(str, Enum):
    QUEUED = "queued"
    DOWNLOADING = "downloading"
    COMPLETED = "completed"
    FAILED = "failed"


@dataclass
class DownloadItem:
    id: str
    file_id: str
    file_name: str
    mime_type: str
    dest_path: str
    status: DownloadStatus = DownloadStatus.QUEUED
    progress: float = 0.0
    error: str = ""
    size: int = 0


class DownloadManager:
    MAX_CONCURRENT = 4

    def __init__(self, drive_service):
        self.drive_service = drive_service
        self._downloads: dict = {}
        self._lock = threading.Lock()
        self._executor = ThreadPoolExecutor(max_workers=self.MAX_CONCURRENT)
        self._batch_id = None
        self._batch_total = 0
        self._batch_completed = 0
        self._batch_failed = 0

    def queue_downloads(self, files, dest_dir):
        batch_id = str(uuid.uuid4())[:8]
        with self._lock:
            self._batch_id = batch_id
            self._batch_total = len(files)
            self._batch_completed = 0
            self._batch_failed = 0
        for f in files:
            safe_name = self._sanitize_filename(f["name"])
            dest_path = os.path.join(dest_dir, safe_name)
            dest_path = self._deduplicate_path(dest_path)
            item = DownloadItem(
                id=str(uuid.uuid4())[:8], file_id=f["id"],
                file_name=f["name"],
                mime_type=f.get("mime_type", f.get("mimeType", "")),
                dest_path=dest_path, size=int(f.get("size", 0)),
            )
            with self._lock:
                self._downloads[item.id] = item
            self._executor.submit(self._download_one, item)
        return batch_id

    def get_progress(self):
        with self._lock:
            items = [{"id": i.id, "file_name": i.file_name, "status": i.status.value, "progress": i.progress, "error": i.error} for i in self._downloads.values()]
            return {"batch_id": self._batch_id, "total": self._batch_total, "completed": self._batch_completed, "failed": self._batch_failed, "items": items}

    def clear(self):
        with self._lock:
            self._downloads = {k: v for k, v in self._downloads.items() if v.status in (DownloadStatus.QUEUED, DownloadStatus.DOWNLOADING)}

    def _download_one(self, item):
        with self._lock:
            item.status = DownloadStatus.DOWNLOADING
        try:
            def on_progress(progress):
                with self._lock:
                    item.progress = progress
            actual_path = self.drive_service.download_file(
                file_id=item.file_id, mime_type=item.mime_type,
                dest_path=item.dest_path, on_progress=on_progress,
            )
            with self._lock:
                item.status = DownloadStatus.COMPLETED
                item.progress = 1.0
                item.dest_path = actual_path
                self._batch_completed += 1
        except Exception as e:
            with self._lock:
                item.status = DownloadStatus.FAILED
                item.error = str(e)
                self._batch_failed += 1

    @staticmethod
    def _sanitize_filename(name):
        name = re.sub(r'[<>:"/\\|?*]', "_", name)
        name = name.strip(". ")
        return name or "unnamed"

    @staticmethod
    def _deduplicate_path(path):
        if not os.path.exists(path):
            return path
        base, ext = os.path.splitext(path)
        counter = 1
        while os.path.exists(f"{base} ({counter}){ext}"):
            counter += 1
        return f"{base} ({counter}){ext}"
