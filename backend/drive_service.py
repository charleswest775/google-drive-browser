"""
Google Drive API v3 wrapper with pagination, export mapping, and backoff.
"""

import io
import os
import threading
import time
import random

from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
from googleapiclient.errors import HttpError

FILE_FIELDS = (
    "id, name, mimeType, size, modifiedTime, createdTime, parents, "
    "owners, shared, trashed, webViewLink, iconLink"
)

EXPORT_MAP = {
    "application/vnd.google-apps.document": {"mime": "application/vnd.openxmlformats-officedocument.wordprocessingml.document", "ext": ".docx"},
    "application/vnd.google-apps.spreadsheet": {"mime": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", "ext": ".xlsx"},
    "application/vnd.google-apps.presentation": {"mime": "application/vnd.openxmlformats-officedocument.presentationml.presentation", "ext": ".pptx"},
    "application/vnd.google-apps.drawing": {"mime": "image/png", "ext": ".png"},
    "application/vnd.google-apps.form": {"mime": "application/pdf", "ext": ".pdf"},
    "application/vnd.google-apps.site": {"mime": "application/pdf", "ext": ".pdf"},
    "application/vnd.google-apps.jam": {"mime": "application/pdf", "ext": ".pdf"},
    "application/vnd.google-apps.script": {"mime": "application/vnd.google-apps.script+json", "ext": ".json"},
}

PAGE_SIZE = 1000


class DriveService:
    def __init__(self, credentials):
        self._credentials = credentials
        self._tls = threading.local()

    @property
    def service(self):
        svc = getattr(self._tls, "service", None)
        if svc is None:
            svc = build("drive", "v3", credentials=self._credentials, cache_discovery=False)
            self._tls.service = svc
        return svc

    def list_all_files(self, on_page=None):
        page_token = None
        page_num = 0
        while True:
            try:
                resp = self._execute_with_backoff(
                    self.service.files().list(
                        pageSize=PAGE_SIZE,
                        fields=f"nextPageToken, files({FILE_FIELDS})",
                        pageToken=page_token,
                        includeItemsFromAllDrives=True,
                        supportsAllDrives=True,
                        q="trashed = false",
                    )
                )
            except HttpError as e:
                if e.resp.status in (429, 500, 503):
                    time.sleep(5)
                    continue
                raise
            files = resp.get("files", [])
            normalized = [self._normalize_file(f) for f in files]
            page_num += 1
            if on_page:
                on_page(normalized, page_num)
            yield normalized
            page_token = resp.get("nextPageToken")
            if not page_token:
                break

    @staticmethod
    def resolve_export_path(mime_type, dest_path):
        if mime_type in EXPORT_MAP:
            ext = EXPORT_MAP[mime_type]["ext"]
        elif mime_type.startswith("application/vnd.google-apps."):
            ext = ".pdf"
        else:
            return dest_path
        base, cur_ext = os.path.splitext(dest_path)
        return base + ext if cur_ext != ext else dest_path

    def download_file(self, file_id, mime_type, dest_path, on_progress=None):
        dest_path = self.resolve_export_path(mime_type, dest_path)
        if mime_type in EXPORT_MAP:
            request = self.service.files().export_media(fileId=file_id, mimeType=EXPORT_MAP[mime_type]["mime"])
        elif mime_type.startswith("application/vnd.google-apps."):
            request = self.service.files().export_media(fileId=file_id, mimeType="application/pdf")
        else:
            request = self.service.files().get_media(fileId=file_id)
        os.makedirs(os.path.dirname(dest_path) or ".", exist_ok=True)
        tmp_path = dest_path + ".part"
        fh = io.FileIO(tmp_path, "wb")
        try:
            downloader = MediaIoBaseDownload(fh, request, chunksize=10 * 1024 * 1024)
            done = False
            while not done:
                status, done = downloader.next_chunk()
                if on_progress and status:
                    on_progress(status.progress())
        except Exception:
            fh.close()
            if os.path.exists(tmp_path):
                try: os.remove(tmp_path)
                except OSError: pass
            raise
        fh.close()
        try:
            os.replace(tmp_path, dest_path)
        except FileNotFoundError:
            if os.path.exists(dest_path):
                return dest_path
            raise
        return dest_path

    def delete_file(self, file_id):
        self._execute_with_backoff(
            self.service.files().delete(fileId=file_id, supportsAllDrives=True)
        )

    def get_about(self):
        return self._execute_with_backoff(
            self.service.about().get(fields="user, storageQuota")
        )

    def _normalize_file(self, f):
        parents = f.get("parents", [])
        owners = f.get("owners", [])
        return {
            "id": f["id"], "name": f["name"],
            "mimeType": f.get("mimeType", ""),
            "size": int(f.get("size", 0)),
            "modifiedTime": f.get("modifiedTime", ""),
            "createdTime": f.get("createdTime", ""),
            "parent_id": parents[0] if parents else "",
            "owner": owners[0].get("emailAddress", "") if owners else "",
            "shared": f.get("shared", False),
            "trashed": f.get("trashed", False),
            "webViewLink": f.get("webViewLink", ""),
            "iconLink": f.get("iconLink", ""),
        }

    def _execute_with_backoff(self, request, max_retries=5):
        for attempt in range(max_retries):
            try:
                return request.execute()
            except HttpError as e:
                if e.resp.status in (429, 500, 503) and attempt < max_retries - 1:
                    wait = (2 ** attempt) + random.random()
                    time.sleep(wait)
                    continue
                raise
