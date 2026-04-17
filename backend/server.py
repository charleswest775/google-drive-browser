"""
FastAPI backend for Google Drive Browser.
"""

import os
import time

from fastapi import FastAPI, HTTPException, Query, BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
import uvicorn

from auth import AuthManager
from drive_service import DriveService
from file_cache import FileCache
from download_manager import DownloadManager

APP_DIR = os.path.dirname(os.path.abspath(__file__))
ROOT_DIR = os.path.dirname(APP_DIR)
DB_PATH = os.path.join(APP_DIR, "file_cache.db")

app = FastAPI(title="Google Drive Browser API")
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])

auth_manager = AuthManager(ROOT_DIR)
cache = FileCache(DB_PATH)
drive_service = None
download_manager = None

indexing_state = {"is_indexing": False, "progress": 0, "total_files": 0, "pages_fetched": 0, "error": None}


class DeleteRequest(BaseModel):
    file_ids: list[str]

class DownloadRequest(BaseModel):
    files: list[dict]
    dest_dir: str


@app.get("/health")
def health():
    return {"status": "ok"}


@app.get("/auth/status")
def auth_status():
    creds = auth_manager.get_credentials()
    authenticated = creds is not None and creds.valid
    user_info = None
    if authenticated:
        global drive_service, download_manager
        if drive_service is None:
            drive_service = DriveService(creds)
            download_manager = DownloadManager(drive_service)
        try:
            about = drive_service.get_about()
            user_info = {
                "email": about.get("user", {}).get("emailAddress", ""),
                "name": about.get("user", {}).get("displayName", ""),
                "photo": about.get("user", {}).get("photoLink", ""),
                "storage_used": about.get("storageQuota", {}).get("usage", 0),
                "storage_limit": about.get("storageQuota", {}).get("limit", 0),
            }
        except Exception:
            pass
    return {"authenticated": authenticated, "user": user_info}


@app.post("/auth/login")
def auth_login():
    try:
        success = auth_manager.complete_auth_flow()
        if success:
            creds = auth_manager.get_credentials()
            global drive_service, download_manager
            drive_service = DriveService(creds)
            download_manager = DownloadManager(drive_service)
            return {"success": True}
        return {"success": False, "error": "Auth flow cancelled or timed out"}
    except FileNotFoundError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/auth/logout")
def auth_logout():
    global drive_service, download_manager
    auth_manager.logout()
    drive_service = None
    download_manager = None
    cache.clear()
    return {"success": True}


@app.post("/index/start")
def start_indexing(background_tasks: BackgroundTasks):
    if indexing_state["is_indexing"]:
        return {"status": "already_indexing", **indexing_state}
    if drive_service is None:
        raise HTTPException(status_code=401, detail="Not authenticated")
    background_tasks.add_task(_run_indexing)
    return {"status": "started"}


@app.get("/index/status")
def index_status():
    stats = cache.get_stats()
    return {**indexing_state, "cache_stats": stats}


@app.post("/index/clear")
def clear_index():
    if indexing_state["is_indexing"]:
        raise HTTPException(status_code=409, detail="Indexing in progress")
    cache.clear()
    return {"success": True}


def _run_indexing():
    global indexing_state
    indexing_state = {"is_indexing": True, "progress": 0, "total_files": 0, "pages_fetched": 0, "error": None}
    try:
        cache.clear()
        def on_page(files, page_num):
            indexing_state["pages_fetched"] = page_num
            indexing_state["total_files"] += len(files)
        for page_files in drive_service.list_all_files(on_page=on_page):
            cache.upsert_files(page_files)
        cache.mark_orphans()
        cache.set_sync_state("last_indexed", str(time.time()))
        indexing_state["is_indexing"] = False
        indexing_state["progress"] = 100
    except Exception as e:
        indexing_state["is_indexing"] = False
        indexing_state["error"] = str(e)


@app.get("/files/children")
def get_children(parent_id: str = "root", offset: int = Query(0, ge=0), limit: int = Query(200, ge=1, le=50000), sort_by: str = Query("name"), sort_dir: str = Query("ASC"), search: str = Query("")):
    return cache.get_children(parent_id, offset, limit, sort_by, sort_dir, search)


@app.get("/files/orphans")
def get_orphans(offset: int = Query(0, ge=0), limit: int = Query(200, ge=1, le=50000), sort_by: str = Query("name"), sort_dir: str = Query("ASC"), search: str = Query("")):
    return cache.get_orphans(offset, limit, sort_by, sort_dir, search)


@app.get("/files/search")
def search_files(q: str = Query(..., min_length=1), offset: int = Query(0, ge=0), limit: int = Query(200, ge=1, le=50000), sort_by: str = Query("name"), sort_dir: str = Query("ASC")):
    return cache.search_files(q, offset, limit, sort_by, sort_dir)


@app.get("/files/{file_id}")
def get_file(file_id: str):
    f = cache.get_file(file_id)
    if not f:
        raise HTTPException(status_code=404, detail="File not found in cache")
    return f


@app.get("/files/{file_id}/path")
def get_file_path(file_id: str):
    return cache.get_path(file_id)


@app.get("/stats")
def get_stats():
    return cache.get_stats()


@app.post("/download")
def start_download(req: DownloadRequest):
    if download_manager is None:
        raise HTTPException(status_code=401, detail="Not authenticated")
    if not os.path.isdir(req.dest_dir):
        raise HTTPException(status_code=400, detail=f"Directory does not exist: {req.dest_dir}")
    batch_id = download_manager.queue_downloads(req.files, req.dest_dir)
    return {"batch_id": batch_id}


@app.get("/download/progress")
def download_progress():
    if download_manager is None:
        return {"total": 0, "completed": 0, "failed": 0, "items": []}
    return download_manager.get_progress()


@app.post("/download/clear")
def clear_downloads():
    if download_manager:
        download_manager.clear()
    return {"success": True}


@app.post("/delete")
def delete_files(req: DeleteRequest):
    if drive_service is None:
        raise HTTPException(status_code=401, detail="Not authenticated")
    results = []
    for fid in req.file_ids:
        try:
            drive_service.delete_file(fid)
            cache.delete_files([fid])
            results.append({"id": fid, "success": True})
        except Exception as e:
            results.append({"id": fid, "success": False, "error": str(e)})
    return {"results": results}


if __name__ == "__main__":
    port = int(os.environ.get("BACKEND_PORT", 5000))
    uvicorn.run(app, host="127.0.0.1", port=port, log_level="info")
