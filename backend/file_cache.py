"""
SQLite-based file metadata cache for fast querying of 50k+ files.
"""

import sqlite3
import os
import threading
import time
from typing import Any


class FileCache:
    def __init__(self, db_path: str):
        self.db_path = db_path
        self._local = threading.local()
        self._init_db()

    @property
    def _conn(self) -> sqlite3.Connection:
        if not hasattr(self._local, "conn") or self._local.conn is None:
            self._local.conn = sqlite3.connect(self.db_path, check_same_thread=False)
            self._local.conn.row_factory = sqlite3.Row
            self._local.conn.execute("PRAGMA journal_mode=WAL")
            self._local.conn.execute("PRAGMA synchronous=NORMAL")
        return self._local.conn

    def _init_db(self):
        conn = sqlite3.connect(self.db_path)
        conn.executescript("""
            CREATE TABLE IF NOT EXISTS files (
                id TEXT PRIMARY KEY,
                name TEXT NOT NULL,
                mime_type TEXT,
                size INTEGER DEFAULT 0,
                modified_time TEXT,
                created_time TEXT,
                parent_id TEXT,
                owner TEXT,
                is_folder INTEGER DEFAULT 0,
                is_orphan INTEGER DEFAULT 0,
                shared INTEGER DEFAULT 0,
                trashed INTEGER DEFAULT 0,
                web_view_link TEXT,
                icon_link TEXT
            );
            CREATE INDEX IF NOT EXISTS idx_parent ON files(parent_id);
            CREATE INDEX IF NOT EXISTS idx_name ON files(name COLLATE NOCASE);
            CREATE INDEX IF NOT EXISTS idx_mime ON files(mime_type);
            CREATE INDEX IF NOT EXISTS idx_orphan ON files(is_orphan);
            CREATE INDEX IF NOT EXISTS idx_folder ON files(is_folder);
            CREATE INDEX IF NOT EXISTS idx_modified ON files(modified_time);
            CREATE INDEX IF NOT EXISTS idx_size ON files(size);

            CREATE TABLE IF NOT EXISTS sync_state (
                key TEXT PRIMARY KEY,
                value TEXT
            );
        """)
        conn.commit()
        conn.close()

    def clear(self):
        self._conn.execute("DELETE FROM files")
        self._conn.execute("DELETE FROM sync_state")
        self._conn.commit()

    def upsert_files(self, files: list):
        if not files:
            return
        self._conn.executemany(
            """INSERT OR REPLACE INTO files
               (id, name, mime_type, size, modified_time, created_time,
                parent_id, owner, is_folder, is_orphan, shared, trashed,
                web_view_link, icon_link)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            [
                (
                    f["id"], f["name"], f.get("mimeType", ""),
                    int(f.get("size", 0)), f.get("modifiedTime", ""),
                    f.get("createdTime", ""), f.get("parent_id", ""),
                    f.get("owner", ""),
                    1 if f.get("mimeType") == "application/vnd.google-apps.folder" else 0,
                    1 if f.get("is_orphan", False) else 0,
                    1 if f.get("shared", False) else 0,
                    1 if f.get("trashed", False) else 0,
                    f.get("webViewLink", ""), f.get("iconLink", ""),
                )
                for f in files
            ],
        )
        self._conn.commit()

    def get_children(self, parent_id, offset=0, limit=200, sort_by="name", sort_dir="ASC", search=""):
        allowed_sorts = {"name": "name COLLATE NOCASE", "size": "size", "modified_time": "modified_time", "mime_type": "mime_type"}
        order = allowed_sorts.get(sort_by, "name COLLATE NOCASE")
        direction = "DESC" if sort_dir.upper() == "DESC" else "ASC"
        order_clause = f"is_folder DESC, {order} {direction}"
        where = "parent_id = ? AND trashed = 0"
        params: list = [parent_id]
        if search:
            where += " AND name LIKE ?"
            params.append(f"%{search}%")
        count_row = self._conn.execute(f"SELECT COUNT(*) as cnt FROM files WHERE {where}", params).fetchone()
        total = count_row["cnt"] if count_row else 0
        params.extend([limit, offset])
        rows = self._conn.execute(f"SELECT * FROM files WHERE {where} ORDER BY {order_clause} LIMIT ? OFFSET ?", params).fetchall()
        return {"files": [dict(r) for r in rows], "total": total, "offset": offset, "limit": limit}

    def get_orphans(self, offset=0, limit=200, sort_by="name", sort_dir="ASC", search=""):
        allowed_sorts = {"name": "name COLLATE NOCASE", "size": "size", "modified_time": "modified_time", "mime_type": "mime_type"}
        order = allowed_sorts.get(sort_by, "name COLLATE NOCASE")
        direction = "DESC" if sort_dir.upper() == "DESC" else "ASC"
        where = "is_orphan = 1 AND trashed = 0"
        params: list = []
        if search:
            where += " AND name LIKE ?"
            params.append(f"%{search}%")
        count_row = self._conn.execute(f"SELECT COUNT(*) as cnt FROM files WHERE {where}", params).fetchone()
        total = count_row["cnt"] if count_row else 0
        params.extend([limit, offset])
        rows = self._conn.execute(f"SELECT * FROM files WHERE {where} ORDER BY {order} {direction} LIMIT ? OFFSET ?", params).fetchall()
        return {"files": [dict(r) for r in rows], "total": total, "offset": offset, "limit": limit}

    def search_files(self, query, offset=0, limit=200, sort_by="name", sort_dir="ASC"):
        allowed_sorts = {"name": "name COLLATE NOCASE", "size": "size", "modified_time": "modified_time", "mime_type": "mime_type"}
        order = allowed_sorts.get(sort_by, "name COLLATE NOCASE")
        direction = "DESC" if sort_dir.upper() == "DESC" else "ASC"
        where = "name LIKE ? AND trashed = 0"
        params: list = [f"%{query}%"]
        count_row = self._conn.execute(f"SELECT COUNT(*) as cnt FROM files WHERE {where}", params).fetchone()
        total = count_row["cnt"] if count_row else 0
        params.extend([limit, offset])
        rows = self._conn.execute(f"SELECT * FROM files WHERE {where} ORDER BY is_folder DESC, {order} {direction} LIMIT ? OFFSET ?", params).fetchall()
        return {"files": [dict(r) for r in rows], "total": total, "offset": offset, "limit": limit}

    def get_file(self, file_id):
        row = self._conn.execute("SELECT * FROM files WHERE id = ?", (file_id,)).fetchone()
        return dict(row) if row else None

    def get_path(self, file_id):
        path = []
        current_id = file_id
        visited = set()
        while current_id and current_id not in visited:
            visited.add(current_id)
            row = self._conn.execute("SELECT id, name, parent_id FROM files WHERE id = ?", (current_id,)).fetchone()
            if not row:
                break
            path.insert(0, {"id": row["id"], "name": row["name"]})
            current_id = row["parent_id"]
        return path

    def delete_files(self, file_ids):
        if not file_ids:
            return
        placeholders = ",".join("?" for _ in file_ids)
        self._conn.execute(f"DELETE FROM files WHERE id IN ({placeholders})", file_ids)
        self._conn.commit()

    def mark_orphans(self):
        self._conn.execute("UPDATE files SET is_orphan = 0")
        self._conn.execute("""
            UPDATE files SET is_orphan = 1
            WHERE trashed = 0 AND parent_id != '' AND parent_id != 'root'
            AND parent_id NOT IN (SELECT id FROM files WHERE is_folder = 1)
        """)
        self._conn.commit()

    def get_stats(self):
        total = self._conn.execute("SELECT COUNT(*) as cnt FROM files WHERE trashed = 0").fetchone()
        folders = self._conn.execute("SELECT COUNT(*) as cnt FROM files WHERE is_folder = 1 AND trashed = 0").fetchone()
        orphans = self._conn.execute("SELECT COUNT(*) as cnt FROM files WHERE is_orphan = 1 AND trashed = 0").fetchone()
        return {
            "total_files": total["cnt"] if total else 0,
            "total_folders": folders["cnt"] if folders else 0,
            "total_orphans": orphans["cnt"] if orphans else 0,
        }

    def set_sync_state(self, key, value):
        self._conn.execute("INSERT OR REPLACE INTO sync_state (key, value) VALUES (?, ?)", (key, value))
        self._conn.commit()

    def get_sync_state(self, key):
        row = self._conn.execute("SELECT value FROM sync_state WHERE key = ?", (key,)).fetchone()
        return row["value"] if row else None
