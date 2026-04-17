"""
Google Drive API v3 wrapper with pagination, export mapping, and backoff.
"""

import io
import os
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
        self.service = build("drive", "v3", credentials=credentials)

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

    def download_file(self, file_id, mime_type, dest_path, on_progress=None):
        if mime_type in EXPORT_MAP:
            export_info = EXPORT_MAP[mime_type]
            request = self.service.files().export_media(fileId=file_id, mimeType=export_info["mime"])
            base, ext = os.path.splitext(dest_path)
            if ext != export_info["ext"]:
                dest_path = base + export_info["ext"]
        elif mime_type.startswith("application/vnd.google-apps."):
            request = self.service.files().export_media(fileId=file_id, mimeType="application/pdf")
            base, ext = os.path.splitext(dest_path)
            if ext != ".pdf":
                dest_path = base + ".pdf"
        else:
            request = self.service.files().get_media(fileId=file_id)
        os.makedirs(os.path.dirname(dest_path) or ".", exist_ok=True)
        fh = io.FileIO(dest_path, "wb")
        downloader = MediaIoBaseDownload(fh, request, chunksize=10 * 1024 * 1024)
        done = False
        while not done:
            try:
                status, done = downloader.next_chunk()
                if on_progress and status:
                    on_progress(status.progress())
            except HttpError as e:
                fh.close()
                if os.path.exists(dest_path):
                    os.remove(dest_path)
                raise
        fh.close()
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
