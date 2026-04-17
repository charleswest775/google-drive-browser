"""
Async deletion manager with concurrency and failure tracking.
"""

import threading
import uuid
from concurrent.futures import ThreadPoolExecutor


class DeleteManager:
    MAX_CONCURRENT = 8

    def __init__(self, drive_service, cache):
        self.drive_service = drive_service
        self.cache = cache
        self._executor = ThreadPoolExecutor(max_workers=self.MAX_CONCURRENT)
        self._lock = threading.Lock()
        self._batch_id = None
        self._total = 0
        self._completed = 0
        self._failed = 0
        self._failures: list = []
        self._running = False

    def queue_deletes(self, files):
        batch_id = str(uuid.uuid4())[:8]
        with self._lock:
            self._batch_id = batch_id
            self._total = len(files)
            self._completed = 0
            self._failed = 0
            self._failures = []
            self._running = True

        for f in files:
            self._executor.submit(self._delete_one, f)
        threading.Thread(target=self._mark_done_when_drained, daemon=True).start()
        return batch_id

    def _mark_done_when_drained(self):
        while True:
            with self._lock:
                total = self._total
                done = self._completed + self._failed
            if done >= total:
                with self._lock:
                    self._running = False
                return
            threading.Event().wait(0.2)

    def _delete_one(self, f):
        file_id = f.get("id")
        file_name = f.get("name") or file_id or "?"
        try:
            self.drive_service.delete_file(file_id)
            self.cache.delete_files([file_id])
            with self._lock:
                self._completed += 1
        except Exception as e:
            err = str(e) or e.__class__.__name__
            print(f"[delete] FAILED {file_name!r}: {err}", flush=True)
            with self._lock:
                self._failed += 1
                self._failures.append({
                    "file_id": file_id,
                    "file_name": file_name,
                    "error": err,
                    "error_type": e.__class__.__name__,
                })

    def get_progress(self):
        with self._lock:
            return {
                "batch_id": self._batch_id,
                "total": self._total,
                "completed": self._completed,
                "failed": self._failed,
                "running": self._running,
                "failures": list(self._failures),
            }

    def clear(self):
        with self._lock:
            if self._running:
                return False
            self._batch_id = None
            self._total = 0
            self._completed = 0
            self._failed = 0
            self._failures = []
            return True
