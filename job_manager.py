"""
job_manager.py - SQLite-backed Mobile Job Manager for StreamVault

Provides persistent job tracking and state management for mobile client downloads.
Implements in-memory real-time progress caching with throttled SQLite persistence.
"""

import os
import sqlite3
import threading
import time
from contextlib import contextmanager
from typing import Any, Dict, List, Optional

VALID_STATUSES = {
    "queued",
    "extracting",
    "downloading",
    "processing",
    "ready",
    "delivering",
    "completed",
    "failed",
    "cancelled",
}

TERMINAL_STATUSES = {"completed", "failed", "cancelled"}

def get_default_db_path() -> str:
    env_path = os.getenv("STREAMVAULT_DB_PATH")
    if env_path:
        return env_path
    if os.path.exists("/app") and os.path.isdir("/app"):
        return "/app/data/streamvault.db"
    return os.path.join(os.path.dirname(os.path.abspath(__file__)), "data", "streamvault.db")


PROGRESS_WRITE_INTERVAL = 1.0  # seconds between SQLite writes for live progress


class JobManager:
    def __init__(self, db_path: Optional[str] = None):
        self.db_path = db_path or get_default_db_path()
        # Thread lock for in-memory live progress and throttling state
        self._lock = threading.Lock()
        # In-memory progress cache: job_id -> dict of live attributes
        self._live_progress: Dict[str, Dict[str, Any]] = {}

    @contextmanager
    def _get_connection(self):
        """Yields an SQLite connection configured with busy timeout and Row factory."""
        db_dir = os.path.dirname(os.path.abspath(self.db_path))
        if db_dir and not os.path.exists(db_dir):
            os.makedirs(db_dir, exist_ok=True)

        conn = sqlite3.connect(self.db_path, timeout=5.0)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA busy_timeout = 5000;")
        try:
            yield conn
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

    def init_db(self) -> None:
        """Initializes database schema, WAL mode, and indices."""
        db_dir = os.path.dirname(os.path.abspath(self.db_path))
        if db_dir and not os.path.exists(db_dir):
            os.makedirs(db_dir, exist_ok=True)

        with self._get_connection() as conn:
            conn.execute("PRAGMA journal_mode = WAL;")
            conn.execute("PRAGMA busy_timeout = 5000;")
            conn.execute("PRAGMA synchronous = NORMAL;")
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS mobile_jobs (
                    job_id TEXT PRIMARY KEY,
                    url TEXT NOT NULL,
                    status TEXT NOT NULL,
                    progress REAL DEFAULT 0.0,
                    downloaded_bytes INTEGER DEFAULT 0,
                    total_bytes INTEGER DEFAULT 0,
                    speed TEXT,
                    eta TEXT,
                    title TEXT,
                    file_id TEXT,
                    error_message TEXT,
                    created_at REAL NOT NULL,
                    updated_at REAL NOT NULL,
                    completed_at REAL
                );
                """
            )
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_mobile_jobs_status ON mobile_jobs(status);"
            )
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_mobile_jobs_created_at ON mobile_jobs(created_at DESC);"
            )

    def create_job(
        self,
        job_id: str,
        url: str,
        title: Optional[str] = None,
        status: str = "queued",
    ) -> Dict[str, Any]:
        """Creates a new job in the database and initializes live tracking."""
        if not job_id or not isinstance(job_id, str):
            raise ValueError("job_id must be a non-empty string")
        if not url or not isinstance(url, str):
            raise ValueError("url must be a non-empty string")
        if status not in VALID_STATUSES:
            raise ValueError(f"Invalid initial status '{status}'. Must be one of {sorted(VALID_STATUSES)}")

        now = time.time()
        completed_at = now if status in TERMINAL_STATUSES else None

        with self._get_connection() as conn:
            conn.execute(
                """
                INSERT INTO mobile_jobs (
                    job_id, url, status, progress, downloaded_bytes, total_bytes,
                    speed, eta, title, file_id, error_message, created_at, updated_at, completed_at
                ) VALUES (?, ?, ?, 0.0, 0, 0, NULL, NULL, ?, NULL, NULL, ?, ?, ?);
                """,
                (job_id, url, status, title, now, now, completed_at),
            )

        with self._lock:
            self._live_progress[job_id] = {
                "progress": 0.0,
                "downloaded_bytes": 0,
                "total_bytes": 0,
                "speed": None,
                "eta": None,
                "updated_at": now,
                "last_write": now,
            }

        job = self.get_job(job_id)
        if job is None:
            raise RuntimeError(f"Failed to retrieve newly created job '{job_id}'")
        return job

    def get_job(self, job_id: str) -> Optional[Dict[str, Any]]:
        """Retrieves a job by ID, merging live in-memory progress if active."""
        with self._get_connection() as conn:
            cursor = conn.execute(
                "SELECT * FROM mobile_jobs WHERE job_id = ?;",
                (job_id,),
            )
            row = cursor.fetchone()

        if not row:
            return None

        job = dict(row)

        with self._lock:
            live = self._live_progress.get(job_id)
            if live:
                job["progress"] = live.get("progress", job["progress"])
                job["downloaded_bytes"] = live.get("downloaded_bytes", job["downloaded_bytes"])
                job["total_bytes"] = live.get("total_bytes", job["total_bytes"])
                job["speed"] = live.get("speed", job["speed"])
                job["eta"] = live.get("eta", job["eta"])
                job["updated_at"] = max(job["updated_at"], live.get("updated_at", 0.0))

        return job

    def list_jobs(
        self,
        limit: int = 50,
        status: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        """Lists jobs ordered by created_at DESC, with live progress merged."""
        with self._get_connection() as conn:
            if status:
                cursor = conn.execute(
                    """
                    SELECT * FROM mobile_jobs
                    WHERE status = ?
                    ORDER BY created_at DESC
                    LIMIT ?;
                    """,
                    (status, limit),
                )
            else:
                cursor = conn.execute(
                    """
                    SELECT * FROM mobile_jobs
                    ORDER BY created_at DESC
                    LIMIT ?;
                    """,
                    (limit,),
                )
            rows = cursor.fetchall()

        jobs = [dict(r) for r in rows]

        with self._lock:
            for job in jobs:
                jid = job["job_id"]
                live = self._live_progress.get(jid)
                if live:
                    job["progress"] = live.get("progress", job["progress"])
                    job["downloaded_bytes"] = live.get("downloaded_bytes", job["downloaded_bytes"])
                    job["total_bytes"] = live.get("total_bytes", job["total_bytes"])
                    job["speed"] = live.get("speed", job["speed"])
                    job["eta"] = live.get("eta", job["eta"])
                    job["updated_at"] = max(job["updated_at"], live.get("updated_at", 0.0))

        return jobs

    def update_job(
        self,
        job_id: str,
        status: Optional[str] = None,
        title: Optional[str] = None,
        file_id: Optional[str] = None,
        error_message: Optional[str] = None,
        progress: Optional[float] = None,
        downloaded_bytes: Optional[int] = None,
        total_bytes: Optional[int] = None,
        speed: Optional[str] = None,
        eta: Optional[str] = None,
    ) -> Optional[Dict[str, Any]]:
        """Updates job fields and persists state transitions immediately."""
        if status is not None and status not in VALID_STATUSES:
            raise ValueError(f"Invalid status '{status}'. Must be one of {sorted(VALID_STATUSES)}")

        now = time.time()
        is_terminal = status in TERMINAL_STATUSES if status else False

        # Incorporate pending in-memory progress values if caller did not provide overrides
        with self._lock:
            live = self._live_progress.get(job_id)
            if live:
                if progress is None:
                    progress = live.get("progress")
                if downloaded_bytes is None:
                    downloaded_bytes = live.get("downloaded_bytes")
                if total_bytes is None:
                    total_bytes = live.get("total_bytes")
                if speed is None:
                    speed = live.get("speed")
                if eta is None:
                    eta = live.get("eta")

        updates: List[str] = ["updated_at = ?"]
        params: List[Any] = [now]

        if status is not None:
            updates.append("status = ?")
            params.append(status)
            if is_terminal:
                updates.append("completed_at = COALESCE(completed_at, ?)")
                params.append(now)

        if title is not None:
            updates.append("title = ?")
            params.append(title)
        if file_id is not None:
            updates.append("file_id = ?")
            params.append(file_id)
        if error_message is not None:
            updates.append("error_message = ?")
            params.append(error_message)
        if progress is not None:
            updates.append("progress = ?")
            params.append(progress)
        if downloaded_bytes is not None:
            updates.append("downloaded_bytes = ?")
            params.append(downloaded_bytes)
        if total_bytes is not None:
            updates.append("total_bytes = ?")
            params.append(total_bytes)
        if speed is not None:
            updates.append("speed = ?")
            params.append(speed)
        if eta is not None:
            updates.append("eta = ?")
            params.append(eta)

        params.append(job_id)

        sql = f"UPDATE mobile_jobs SET {', '.join(updates)} WHERE job_id = ?;"

        with self._get_connection() as conn:
            cursor = conn.execute(sql, tuple(params))
            if cursor.rowcount == 0:
                return None

        with self._lock:
            if is_terminal:
                self._live_progress.pop(job_id, None)
            elif job_id in self._live_progress:
                live_entry = self._live_progress[job_id]
                if progress is not None:
                    live_entry["progress"] = progress
                if downloaded_bytes is not None:
                    live_entry["downloaded_bytes"] = downloaded_bytes
                if total_bytes is not None:
                    live_entry["total_bytes"] = total_bytes
                if speed is not None:
                    live_entry["speed"] = speed
                if eta is not None:
                    live_entry["eta"] = eta
                live_entry["updated_at"] = now
                live_entry["last_write"] = now

        return self.get_job(job_id)

    def update_progress(
        self,
        job_id: str,
        progress: float,
        downloaded_bytes: int = 0,
        total_bytes: int = 0,
        speed: Optional[str] = None,
        eta: Optional[str] = None,
        status: Optional[str] = None,
    ) -> bool:
        """
        Updates live progress in memory.
        Throttles SQLite writes to approximately once per second.
        Persists immediately if a status transition occurs.
        Returns True if SQLite was updated, False if cached in memory.
        """
        now = time.time()
        should_write_db = False

        with self._lock:
            live = self._live_progress.setdefault(job_id, {
                "progress": 0.0,
                "downloaded_bytes": 0,
                "total_bytes": 0,
                "speed": None,
                "eta": None,
                "updated_at": 0.0,
                "last_write": 0.0,
            })

            live["progress"] = progress
            live["downloaded_bytes"] = downloaded_bytes
            live["total_bytes"] = total_bytes
            live["speed"] = speed
            live["eta"] = eta
            live["updated_at"] = now

            if status is not None:
                # State transition specified -> persist immediately
                should_write_db = True
                live["last_write"] = now
            elif (now - live.get("last_write", 0.0)) >= PROGRESS_WRITE_INTERVAL:
                should_write_db = True
                live["last_write"] = now

        if should_write_db:
            updates = [
                "progress = ?",
                "downloaded_bytes = ?",
                "total_bytes = ?",
                "speed = ?",
                "eta = ?",
                "updated_at = ?",
            ]
            params = [progress, downloaded_bytes, total_bytes, speed, eta, now]

            if status is not None:
                if status not in VALID_STATUSES:
                    raise ValueError(f"Invalid status '{status}'. Must be one of {sorted(VALID_STATUSES)}")
                updates.append("status = ?")
                params.append(status)
                if status in TERMINAL_STATUSES:
                    updates.append("completed_at = COALESCE(completed_at, ?)")
                    params.append(now)

            params.append(job_id)
            sql = f"UPDATE mobile_jobs SET {', '.join(updates)} WHERE job_id = ?;"

            with self._get_connection() as conn:
                conn.execute(sql, tuple(params))

            return True

        return False

    def delete_expired_jobs(self, max_age_seconds: float = 86400) -> int:
        """
        Deletes terminal jobs (completed, failed, cancelled) or abandoned jobs
        older than max_age_seconds.
        """
        cutoff = time.time() - max_age_seconds
        with self._get_connection() as conn:
            cursor = conn.execute(
                """
                DELETE FROM mobile_jobs
                WHERE (status IN ('completed', 'failed', 'cancelled') AND COALESCE(completed_at, updated_at) < ?)
                   OR (updated_at < ?);
                """,
                (cutoff, cutoff),
            )
            deleted_count = cursor.rowcount

        # Clean in-memory tracking for removed jobs
        with self._lock:
            if self._live_progress:
                with self._get_connection() as conn:
                    cursor = conn.execute("SELECT job_id FROM mobile_jobs;")
                    existing_ids = {row[0] for row in cursor.fetchall()}
                stale_ids = [jid for jid in self._live_progress if jid not in existing_ids]
                for jid in stale_ids:
                    self._live_progress.pop(jid, None)

        return deleted_count

    def get_active_job_count(self) -> int:
        """Returns the number of jobs currently queued, extracting, downloading, or processing."""
        with self._get_connection() as conn:
            cursor = conn.execute(
                """
                SELECT COUNT(*) FROM mobile_jobs
                WHERE status IN ('queued', 'extracting', 'downloading', 'processing');
                """
            )
            return cursor.fetchone()[0]

    def get_queued_jobs(self, limit: int = 1) -> List[Dict[str, Any]]:
        """Returns queued jobs in FIFO order (oldest created_at first)."""
        with self._get_connection() as conn:
            cursor = conn.execute(
                """
                SELECT * FROM mobile_jobs
                WHERE status = 'queued'
                ORDER BY created_at ASC
                LIMIT ?;
                """,
                (limit,),
            )
            return [dict(r) for r in cursor.fetchall()]

    def clear_all_jobs(self) -> int:
        """
        Clean-slate model: deletes all records from mobile_jobs and resets
        all in-memory live progress tracking during server restart.
        """
        with self._get_connection() as conn:
            cursor = conn.execute("DELETE FROM mobile_jobs;")
            deleted = cursor.rowcount

        with self._lock:
            self._live_progress.clear()

        return deleted

    def claim_next_queued_job(self) -> Optional[Dict[str, Any]]:
        """
        Atomically claims the oldest 'queued' job in FIFO order by transitioning its
        status from 'queued' to 'extracting'.
        Returns the claimed job dict, or None if no queued job exists.
        """
        now = time.time()
        with self._get_connection() as conn:
            cur = conn.execute(
                """
                SELECT job_id FROM mobile_jobs
                WHERE status = 'queued'
                ORDER BY created_at ASC
                LIMIT 1;
                """
            )
            row = cur.fetchone()
            if not row:
                return None

            job_id = row["job_id"]
            update_cur = conn.execute(
                """
                UPDATE mobile_jobs
                SET status = 'extracting', updated_at = ?
                WHERE job_id = ? AND status = 'queued';
                """,
                (now, job_id),
            )
            if update_cur.rowcount == 0:
                return None

        return self.get_job(job_id)

    def unclaim_job(self, job_id: str) -> None:
        """
        Reverts a claimed job from 'extracting' back to 'queued' if task creation fails.
        """
        now = time.time()
        with self._get_connection() as conn:
            conn.execute(
                """
                UPDATE mobile_jobs
                SET status = 'queued', updated_at = ?
                WHERE job_id = ? AND status = 'extracting';
                """,
                (now, job_id),
            )


# Module-level singleton instance & API delegates
_default_manager: Optional[JobManager] = None
_manager_lock = threading.Lock()


def get_job_manager(db_path: Optional[str] = None) -> JobManager:
    """Returns the singleton JobManager instance, optionally configuring db_path."""
    global _default_manager
    target_path = db_path or get_default_db_path()
    with _manager_lock:
        if _default_manager is None or _default_manager.db_path != target_path:
            _default_manager = JobManager(db_path=target_path)
        return _default_manager


def init_db(db_path: Optional[str] = None) -> None:
    """Initializes the database schema and WAL settings on the default manager."""
    get_job_manager(db_path).init_db()


def create_job(
    job_id: str,
    url: str,
    title: Optional[str] = None,
    status: str = "queued",
) -> Dict[str, Any]:
    return get_job_manager().create_job(job_id, url, title=title, status=status)


def get_job(job_id: str) -> Optional[Dict[str, Any]]:
    return get_job_manager().get_job(job_id)


def list_jobs(limit: int = 50, status: Optional[str] = None) -> List[Dict[str, Any]]:
    return get_job_manager().list_jobs(limit=limit, status=status)


def update_job(
    job_id: str,
    status: Optional[str] = None,
    title: Optional[str] = None,
    file_id: Optional[str] = None,
    error_message: Optional[str] = None,
    progress: Optional[float] = None,
    downloaded_bytes: Optional[int] = None,
    total_bytes: Optional[int] = None,
    speed: Optional[str] = None,
    eta: Optional[str] = None,
) -> Optional[Dict[str, Any]]:
    return get_job_manager().update_job(
        job_id=job_id,
        status=status,
        title=title,
        file_id=file_id,
        error_message=error_message,
        progress=progress,
        downloaded_bytes=downloaded_bytes,
        total_bytes=total_bytes,
        speed=speed,
        eta=eta,
    )


def update_progress(
    job_id: str,
    progress: float,
    downloaded_bytes: int = 0,
    total_bytes: int = 0,
    speed: Optional[str] = None,
    eta: Optional[str] = None,
    status: Optional[str] = None,
) -> bool:
    return get_job_manager().update_progress(
        job_id=job_id,
        progress=progress,
        downloaded_bytes=downloaded_bytes,
        total_bytes=total_bytes,
        speed=speed,
        eta=eta,
        status=status,
    )


def delete_expired_jobs(max_age_seconds: float = 86400) -> int:
    return get_job_manager().delete_expired_jobs(max_age_seconds=max_age_seconds)


def get_active_job_count() -> int:
    return get_job_manager().get_active_job_count()


def get_queued_jobs(limit: int = 1) -> List[Dict[str, Any]]:
    return get_job_manager().get_queued_jobs(limit=limit)


def clear_all_jobs() -> int:
    return get_job_manager().clear_all_jobs()


def claim_next_queued_job() -> Optional[Dict[str, Any]]:
    return get_job_manager().claim_next_queued_job()


def unclaim_job(job_id: str) -> None:
    get_job_manager().unclaim_job(job_id)
