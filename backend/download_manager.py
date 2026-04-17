"""
Download queue manager with concurrency control.
"""

import json
import os
import re
import threading
import time
import uuid
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from enum import Enum


FAILURE_LOG_NAME = ".download_failures.log"
MANIFEST_NAME = ".download_manifest.jsonl"


class DownloadStatus(str, Enum):
    QUEUED = "queued"
    DOWNLOADING = "downloading"
    COMPLETED = "completed"
    SKIPPED = "skipped"
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
    skip_reason: str = ""


class DownloadManager:
    MAX_CONCURRENT = 32

    def __init__(self, drive_service):
        self.drive_service = drive_service
        self._downloads: dict = {}
        self._failures: list = []
        self._lock = threading.Lock()
        self._executor = ThreadPoolExecutor(max_workers=self.MAX_CONCURRENT)
        self._batch_id = None
        self._batch_total = 0
        self._batch_completed = 0
        self._batch_failed = 0
        self._batch_skipped = 0
        self._batch_dest_dir = None
        self._batch_item_ids: list = []
        self._batch_bytes_total = 0
        self._batch_started_at = None
        self._manifest_lock = threading.Lock()

    def queue_downloads(self, files, dest_dir):
        batch_id = str(uuid.uuid4())[:8]
        with self._lock:
            in_flight = any(
                i.status in (DownloadStatus.QUEUED, DownloadStatus.DOWNLOADING)
                for i in self._downloads.values()
            )
            self._batch_id = batch_id
            self._batch_total = len(files)
            self._batch_completed = 0
            self._batch_failed = 0
            self._batch_skipped = 0
            self._batch_dest_dir = dest_dir
            self._batch_item_ids = []
            self._batch_bytes_total = sum(int(f.get("size", 0) or 0) for f in files)
            self._batch_started_at = time.time()
        if not in_flight:
            self._cleanup_stale_parts(dest_dir)

        manifest = self._load_manifest(dest_dir)
        claimed = {}

        for f in files:
            mime_type = f.get("mime_type", f.get("mimeType", ""))
            safe_name = self._sanitize_filename(f["name"])
            base_path = os.path.join(dest_dir, safe_name)
            base_path = self.drive_service.resolve_export_path(mime_type, base_path)
            dest_path, confirmed_skip = self._resolve_dest_path(
                base_path, f["id"], claimed, manifest
            )
            claimed[dest_path.lower()] = f["id"]
            item = DownloadItem(
                id=str(uuid.uuid4())[:8], file_id=f["id"],
                file_name=f["name"], mime_type=mime_type,
                dest_path=dest_path, size=int(f.get("size", 0)),
            )
            if confirmed_skip:
                item.status = DownloadStatus.SKIPPED
                item.progress = 1.0
                item.skip_reason = "already downloaded (manifest match)"
                with self._lock:
                    self._downloads[item.id] = item
                    self._batch_item_ids.append(item.id)
                    self._batch_completed += 1
                    self._batch_skipped += 1
                continue
            with self._lock:
                self._downloads[item.id] = item
                self._batch_item_ids.append(item.id)
            self._executor.submit(self._download_one, item, dest_dir)
        return batch_id

    def _resolve_dest_path(self, base_path, file_id, claimed, manifest):
        """Find a dest path that doesn't collide with another file in this batch
        or a different file already on disk. Returns (path, confirmed_skip).

        confirmed_skip=True means: file exists on disk AND manifest records it
        as having been written for this same file_id, so we can safely skip.
        """
        path = base_path
        while True:
            key = path.lower()
            claim_owner = claimed.get(key)
            if claim_owner is not None and claim_owner != file_id:
                path = self._next_candidate(path)
                continue
            exists = os.path.exists(path)
            manifest_owner = manifest.get(key)
            if exists and manifest_owner == file_id:
                return path, True
            if exists and manifest_owner != file_id:
                path = self._next_candidate(path)
                continue
            return path, False

    @staticmethod
    def _next_candidate(path):
        base, ext = os.path.splitext(path)
        m = re.match(r"^(.*) \((\d+)\)$", base)
        if m:
            return f"{m.group(1)} ({int(m.group(2)) + 1}){ext}"
        return f"{base} (1){ext}"

    def check_existing(self, files, dest_dir):
        manifest = self._load_manifest(dest_dir)
        groups = {}
        for f in files:
            mime_type = f.get("mime_type", f.get("mimeType", ""))
            safe_name = self._sanitize_filename(f["name"])
            base_path = os.path.join(dest_dir, safe_name)
            base_path = self.drive_service.resolve_export_path(mime_type, base_path)
            key = base_path.lower()
            g = groups.setdefault(key, {"base_path": base_path, "files": []})
            g["files"].append(f)

        existing_ids = []
        for g in groups.values():
            base_path = g["base_path"]
            group_files = g["files"]
            disk_paths = self._enumerate_variants(base_path)
            if not disk_paths:
                continue
            claimed = set()
            claimed_path_keys = set()
            for dp in disk_paths:
                owner = manifest.get(dp.lower())
                if not owner:
                    continue
                for f in group_files:
                    if f["id"] == owner and f["id"] not in claimed:
                        claimed.add(f["id"])
                        claimed_path_keys.add(dp.lower())
                        break
            remaining = len(disk_paths) - len(claimed_path_keys)
            for f in group_files:
                if remaining <= 0:
                    break
                if f["id"] in claimed:
                    continue
                claimed.add(f["id"])
                remaining -= 1
            existing_ids.extend(claimed)

        log_path = os.path.join(dest_dir, FAILURE_LOG_NAME)
        return {
            "existing_ids": existing_ids,
            "failure_log_path": log_path if os.path.exists(log_path) else None,
            "manifest_path": os.path.join(dest_dir, MANIFEST_NAME),
        }

    @staticmethod
    def _enumerate_variants(base_path):
        paths = []
        if os.path.exists(base_path):
            paths.append(base_path)
        base, ext = os.path.splitext(base_path)
        n = 1
        while True:
            variant = f"{base} ({n}){ext}"
            if os.path.exists(variant):
                paths.append(variant)
                n += 1
            else:
                break
        return paths

    def get_progress(self):
        with self._lock:
            bytes_downloaded = 0
            downloading_count = 0
            queued_count = 0
            skipped_items = []
            for item_id in self._batch_item_ids:
                item = self._downloads.get(item_id)
                if item is None:
                    continue
                if item.status in (DownloadStatus.DOWNLOADING, DownloadStatus.COMPLETED):
                    bytes_downloaded += int(item.size * item.progress)
                if item.status == DownloadStatus.DOWNLOADING:
                    downloading_count += 1
                elif item.status == DownloadStatus.QUEUED:
                    queued_count += 1
                elif item.status == DownloadStatus.SKIPPED:
                    skipped_items.append({
                        "file_name": item.file_name,
                        "dest_path": item.dest_path,
                        "skip_reason": item.skip_reason,
                    })
            elapsed = (time.time() - self._batch_started_at) if self._batch_started_at else 0
            return {
                "batch_id": self._batch_id,
                "total": self._batch_total,
                "completed": self._batch_completed,
                "failed": self._batch_failed,
                "skipped": self._batch_skipped,
                "downloading": downloading_count,
                "queued": queued_count,
                "bytes_downloaded": bytes_downloaded,
                "bytes_total": self._batch_bytes_total,
                "elapsed_seconds": elapsed,
                "skipped_items": skipped_items,
                "failures": list(self._failures),
                "failure_log_path": (
                    os.path.join(self._batch_dest_dir, FAILURE_LOG_NAME)
                    if self._batch_dest_dir else None
                ),
                "manifest_path": (
                    os.path.join(self._batch_dest_dir, MANIFEST_NAME)
                    if self._batch_dest_dir else None
                ),
            }

    def get_failures(self):
        with self._lock:
            return {
                "failures": list(self._failures),
                "failure_log_path": (
                    os.path.join(self._batch_dest_dir, FAILURE_LOG_NAME)
                    if self._batch_dest_dir else None
                ),
            }

    def clear_failures(self):
        with self._lock:
            self._failures = []

    def clear(self):
        with self._lock:
            self._downloads = {k: v for k, v in self._downloads.items() if v.status in (DownloadStatus.QUEUED, DownloadStatus.DOWNLOADING)}

    def _download_one(self, item, dest_dir):
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
            self._append_manifest(dest_dir, item.file_id, actual_path)
        except Exception as e:
            err = str(e) or e.__class__.__name__
            print(f"[download] FAILED {item.file_name!r} (mime={item.mime_type}): {err}", flush=True)
            failure_record = {
                "timestamp": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
                "file_id": item.file_id,
                "file_name": item.file_name,
                "mime_type": item.mime_type,
                "dest_path": item.dest_path,
                "error": err,
                "error_type": e.__class__.__name__,
            }
            with self._lock:
                item.status = DownloadStatus.FAILED
                item.error = err
                self._batch_failed += 1
                self._failures.append(failure_record)
            self._append_failure_log(dest_dir, failure_record)

    def _load_manifest(self, dest_dir):
        """Load manifest as a dict: lowercased_path -> file_id."""
        manifest_path = os.path.join(dest_dir, MANIFEST_NAME)
        result = {}
        if not os.path.exists(manifest_path):
            return result
        try:
            with open(manifest_path, "r", encoding="utf-8") as f:
                for line in f:
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        rec = json.loads(line)
                        p = rec.get("dest_path", "")
                        fid = rec.get("file_id", "")
                        if p and fid:
                            result[p.lower()] = fid
                    except json.JSONDecodeError:
                        continue
        except OSError as e:
            print(f"[download] could not read manifest: {e}", flush=True)
        return result

    def _append_manifest(self, dest_dir, file_id, dest_path):
        rec = {
            "timestamp": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
            "file_id": file_id,
            "dest_path": dest_path,
        }
        with self._manifest_lock:
            try:
                os.makedirs(dest_dir, exist_ok=True)
                with open(os.path.join(dest_dir, MANIFEST_NAME), "a", encoding="utf-8") as f:
                    f.write(json.dumps(rec, ensure_ascii=False) + "\n")
            except OSError as e:
                print(f"[download] could not write manifest: {e}", flush=True)

    @staticmethod
    def _append_failure_log(dest_dir, record):
        try:
            os.makedirs(dest_dir, exist_ok=True)
            with open(os.path.join(dest_dir, FAILURE_LOG_NAME), "a", encoding="utf-8") as f:
                f.write(json.dumps(record, ensure_ascii=False) + "\n")
        except OSError as e:
            print(f"[download] could not write failure log: {e}", flush=True)

    @staticmethod
    def _cleanup_stale_parts(dest_dir):
        if not os.path.isdir(dest_dir):
            return
        removed = 0
        for name in os.listdir(dest_dir):
            if name.endswith(".part"):
                try:
                    os.remove(os.path.join(dest_dir, name))
                    removed += 1
                except OSError:
                    pass
        if removed:
            print(f"[download] cleaned up {removed} stale .part file(s) in {dest_dir}", flush=True)

    @staticmethod
    def _sanitize_filename(name):
        name = re.sub(r'[<>:"/\\|?*]', "_", name)
        name = name.strip(". ")
        return name or "unnamed"
