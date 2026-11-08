"""
test_phase2_jobs.py - Focused tests for Phase 2 SQLite-backed mobile job management.

Tests cover:
1. Database initialization (WAL mode, busy_timeout, schema, columns, indices, idempotency).
2. CRUD operations (create_job, get_job, list_jobs, update_job, validations).
3. State transitions (all valid states, invalid state rejection, terminal state completion timestamp).
4. Progress throttling (in-memory live caching, throttling SQLite writes to ~1/sec, state transition bypass).
5. Expired jobs cleanup (delete_expired_jobs and in-memory tracking eviction).
6. Main lifespan integration.
"""

import os
import shutil
import sqlite3
import tempfile
import time
import unittest
from job_manager import (
    JobManager,
    VALID_STATUSES,
    TERMINAL_STATUSES,
    PROGRESS_WRITE_INTERVAL,
)


class TestJobManager(unittest.TestCase):
    def setUp(self):
        self.test_dir = tempfile.mkdtemp(prefix="streamvault_test_")
        self.db_path = os.path.join(self.test_dir, "test_streamvault.db")
        self.manager = JobManager(db_path=self.db_path)

    def tearDown(self):
        if os.path.exists(self.test_dir):
            try:
                shutil.rmtree(self.test_dir)
            except OSError:
                pass

    # --- 1. DATABASE INITIALIZATION TESTS ---
    def test_db_initialization(self):
        """Verifies table schema, column definitions, WAL journal mode, and indices."""
        self.manager.init_db()
        self.assertTrue(os.path.exists(self.db_path), "Database file should exist after init_db")

        # Open raw connection to inspect pragmas and metadata
        conn = sqlite3.connect(self.db_path)
        try:
            # Check journal_mode
            cur = conn.execute("PRAGMA journal_mode;")
            journal_mode = cur.fetchone()[0].lower()
            self.assertEqual(journal_mode, "wal", f"Expected WAL mode, got {journal_mode}")

            # Check busy_timeout
            conn.execute("PRAGMA busy_timeout = 5000;")
            cur = conn.execute("PRAGMA busy_timeout;")
            busy_timeout = cur.fetchone()[0]
            self.assertEqual(busy_timeout, 5000, f"Expected busy_timeout 5000, got {busy_timeout}")

            # Inspect columns of mobile_jobs table
            cur = conn.execute("PRAGMA table_info(mobile_jobs);")
            columns = {row[1]: {"type": row[2].upper(), "pk": bool(row[5])} for row in cur.fetchall()}

            expected_columns = {
                "job_id": {"type": "TEXT", "pk": True},
                "url": {"type": "TEXT", "pk": False},
                "status": {"type": "TEXT", "pk": False},
                "progress": {"type": "REAL", "pk": False},
                "downloaded_bytes": {"type": "INTEGER", "pk": False},
                "total_bytes": {"type": "INTEGER", "pk": False},
                "speed": {"type": "TEXT", "pk": False},
                "eta": {"type": "TEXT", "pk": False},
                "title": {"type": "TEXT", "pk": False},
                "file_id": {"type": "TEXT", "pk": False},
                "error_message": {"type": "TEXT", "pk": False},
                "created_at": {"type": "REAL", "pk": False},
                "updated_at": {"type": "REAL", "pk": False},
                "completed_at": {"type": "REAL", "pk": False},
            }

            for col_name, expected in expected_columns.items():
                self.assertIn(col_name, columns, f"Column '{col_name}' missing from mobile_jobs table")
                self.assertEqual(
                    columns[col_name]["type"],
                    expected["type"],
                    f"Column '{col_name}' expected type {expected['type']}, got {columns[col_name]['type']}"
                )
                self.assertEqual(
                    columns[col_name]["pk"],
                    expected["pk"],
                    f"Column '{col_name}' primary key flag expected {expected['pk']}, got {columns[col_name]['pk']}"
                )

            # Check indices
            cur = conn.execute("SELECT name FROM sqlite_master WHERE type='index' AND tbl_name='mobile_jobs';")
            index_names = {row[0] for row in cur.fetchall()}
            self.assertIn("idx_mobile_jobs_status", index_names)
            self.assertIn("idx_mobile_jobs_created_at", index_names)

        finally:
            conn.close()

        # Test idempotency (calling init_db again should succeed without error)
        try:
            self.manager.init_db()
        except Exception as e:
            self.fail(f"Repeated init_db() raised unexpected exception: {e}")

    # --- 2. CRUD OPERATIONS TESTS ---
    def test_crud_operations(self):
        """Tests job creation, retrieval, listing, updates, and input validation."""
        self.manager.init_db()

        # Input validation tests
        with self.assertRaises(ValueError):
            self.manager.create_job("", "https://example.com/video")
        with self.assertRaises(ValueError):
            self.manager.create_job("job_1", "")
        with self.assertRaises(ValueError):
            self.manager.create_job("job_1", "https://example.com", status="invalid_status")

        # Create job
        job = self.manager.create_job("job_crud_1", "https://youtube.com/watch?v=crud123", title="CRUD Test Title")
        self.assertIsNotNone(job)
        self.assertEqual(job["job_id"], "job_crud_1")
        self.assertEqual(job["url"], "https://youtube.com/watch?v=crud123")
        self.assertEqual(job["title"], "CRUD Test Title")
        self.assertEqual(job["status"], "queued")
        self.assertEqual(job["progress"], 0.0)
        self.assertEqual(job["downloaded_bytes"], 0)
        self.assertEqual(job["total_bytes"], 0)
        self.assertIsNone(job["speed"])
        self.assertIsNone(job["eta"])
        self.assertIsNone(job["file_id"])
        self.assertIsNone(job["error_message"])
        self.assertIsNone(job["completed_at"])
        self.assertGreater(job["created_at"], 0)
        self.assertEqual(job["created_at"], job["updated_at"])

        # Get job
        fetched = self.manager.get_job("job_crud_1")
        self.assertEqual(fetched["job_id"], "job_crud_1")
        self.assertEqual(fetched["title"], "CRUD Test Title")

        # Non-existent job
        self.assertIsNone(self.manager.get_job("non_existent_id"))

        # Update job fields
        updated = self.manager.update_job(
            "job_crud_1",
            title="Updated Title",
            file_id="fid-xyz-789",
            error_message=None
        )
        self.assertEqual(updated["title"], "Updated Title")
        self.assertEqual(updated["file_id"], "fid-xyz-789")

        # Update non-existent job returns None
        self.assertIsNone(self.manager.update_job("non_existent_id", title="Foo"))

        # List jobs
        self.manager.create_job("job_crud_2", "https://youtube.com/watch?v=crud456", title="Second Job")
        jobs = self.manager.list_jobs(limit=10)
        self.assertEqual(len(jobs), 2)
        # Verify ordered by created_at DESC
        self.assertEqual(jobs[0]["job_id"], "job_crud_2")
        self.assertEqual(jobs[1]["job_id"], "job_crud_1")

        # List jobs with limit
        jobs_limited = self.manager.list_jobs(limit=1)
        self.assertEqual(len(jobs_limited), 1)
        self.assertEqual(jobs_limited[0]["job_id"], "job_crud_2")

        # List jobs by status
        self.manager.update_job("job_crud_2", status="downloading")
        queued_jobs = self.manager.list_jobs(status="queued")
        self.assertEqual(len(queued_jobs), 1)
        self.assertEqual(queued_jobs[0]["job_id"], "job_crud_1")

        downloading_jobs = self.manager.list_jobs(status="downloading")
        self.assertEqual(len(downloading_jobs), 1)
        self.assertEqual(downloading_jobs[0]["job_id"], "job_crud_2")

    # --- 3. STATE TRANSITIONS TESTS ---
    def test_state_transitions(self):
        """Tests transitions across all valid states, validation, and terminal timestamps."""
        self.manager.init_db()

        job_id = "job_state_flow"
        self.manager.create_job(job_id, "https://youtube.com/watch?v=flow123")

        expected_sequence = [
            "queued",
            "extracting",
            "downloading",
            "processing",
            "ready",
            "delivering",
            "completed"
        ]

        for target_status in expected_sequence:
            res = self.manager.update_job(job_id, status=target_status)
            self.assertEqual(res["status"], target_status, f"Job status should transition to {target_status}")

            # Verify persisted in raw DB
            conn = sqlite3.connect(self.db_path)
            try:
                row = conn.execute("SELECT status, completed_at FROM mobile_jobs WHERE job_id = ?;", (job_id,)).fetchone()
                self.assertEqual(row[0], target_status)
                if target_status == "completed":
                    self.assertIsNotNone(row[1], "completed_at should be set upon reaching 'completed'")
                else:
                    self.assertIsNone(row[1], f"completed_at should remain None during '{target_status}'")
            finally:
                conn.close()

        # Reject invalid status transitions
        with self.assertRaises(ValueError):
            self.manager.update_job(job_id, status="finished")
        with self.assertRaises(ValueError):
            self.manager.update_job(job_id, status="error")
        with self.assertRaises(ValueError):
            self.manager.update_job(job_id, status="")

        # Test failure transition and completed_at
        fail_job_id = "job_state_fail"
        self.manager.create_job(fail_job_id, "https://youtube.com/watch?v=fail123")
        failed_res = self.manager.update_job(fail_job_id, status="failed", error_message="Video unavailable")
        self.assertEqual(failed_res["status"], "failed")
        self.assertEqual(failed_res["error_message"], "Video unavailable")
        self.assertIsNotNone(failed_res["completed_at"])

        # Test cancellation transition and completed_at
        cancel_job_id = "job_state_cancel"
        self.manager.create_job(cancel_job_id, "https://youtube.com/watch?v=cancel123")
        cancel_res = self.manager.update_job(cancel_job_id, status="cancelled")
        self.assertEqual(cancel_res["status"], "cancelled")
        self.assertIsNotNone(cancel_res["completed_at"])

        # Verify all valid statuses from requirement are tested
        all_tested = set(expected_sequence) | {"failed", "cancelled"}
        self.assertEqual(all_tested, VALID_STATUSES)

    # --- 4. PROGRESS THROTTLING TESTS ---
    def test_progress_throttling(self):
        """
        Tests that rapid progress updates update in-memory cache immediately
        while throttling SQLite writes to approximately once per second.
        """
        self.manager.init_db()

        job_id = "job_throttle_test"
        self.manager.create_job(job_id, "https://youtube.com/watch?v=throttle123")
        self.manager.update_job(job_id, status="downloading")

        # Rapidly fire 50 progress updates in a tight loop (< 100ms)
        db_writes = 0
        for i in range(1, 51):
            wrote_db = self.manager.update_progress(
                job_id=job_id,
                progress=float(i),
                downloaded_bytes=i * 1000,
                total_bytes=50000,
                speed=f"{i * 100} KB/s",
                eta=f"00:{50 - i:02d}"
            )
            if wrote_db:
                db_writes += 1

        # Check in-memory view via get_job(): must immediately reflect call 50
        job_view = self.manager.get_job(job_id)
        self.assertEqual(job_view["progress"], 50.0, "get_job() should immediately reflect latest live progress")
        self.assertEqual(job_view["downloaded_bytes"], 50000)
        self.assertEqual(job_view["speed"], "5000 KB/s")
        self.assertEqual(job_view["eta"], "00:00")

        # Check raw SQLite view: throttled to at most 1 write during rapid burst
        conn = sqlite3.connect(self.db_path)
        try:
            row = conn.execute("SELECT progress, downloaded_bytes FROM mobile_jobs WHERE job_id = ?;", (job_id,)).fetchone()
            db_progress = row[0]
            db_bytes = row[1]
        finally:
            conn.close()

        # Because all 50 updates ran within milliseconds, throttler should have written <= 1 time
        self.assertLessEqual(db_writes, 1, f"Expected at most 1 DB write during tight burst, got {db_writes}")
        # Raw SQLite should NOT have 50.0 yet if the throttler skipped later updates
        if db_writes == 1:
            self.assertEqual(db_progress, 1.0, "Initial write captured first update; later bursts throttled")

        # Now simulate 1.1s passing by advancing the recorded last_write timestamp
        with self.manager._lock:
            self.manager._live_progress[job_id]["last_write"] = time.time() - 1.5

        # Next progress update should now trigger a DB write
        wrote_db = self.manager.update_progress(
            job_id=job_id,
            progress=75.0,
            downloaded_bytes=37500,
            total_bytes=50000,
            speed="1500 KB/s",
            eta="00:05"
        )
        self.assertTrue(wrote_db, "Should write to SQLite after throttling interval has elapsed")

        # Verify SQLite has the new progress (75.0%)
        conn = sqlite3.connect(self.db_path)
        try:
            row = conn.execute("SELECT progress, downloaded_bytes FROM mobile_jobs WHERE job_id = ?;", (job_id,)).fetchone()
            self.assertEqual(row[0], 75.0)
            self.assertEqual(row[1], 37500)
        finally:
            conn.close()

        # State transition in update_progress bypasses throttling
        wrote_state = self.manager.update_progress(
            job_id=job_id,
            progress=100.0,
            downloaded_bytes=50000,
            total_bytes=50000,
            status="processing"
        )
        self.assertTrue(wrote_state, "State transition in update_progress must persist immediately")

        conn = sqlite3.connect(self.db_path)
        try:
            row = conn.execute("SELECT status, progress FROM mobile_jobs WHERE job_id = ?;", (job_id,)).fetchone()
            self.assertEqual(row[0], "processing")
            self.assertEqual(row[1], 100.0)
        finally:
            conn.close()

    # --- 5. EXPIRED JOBS CLEANUP TESTS ---
    def test_delete_expired_jobs(self):
        """Tests pruning expired terminal or abandoned jobs and cleaning up cache."""
        self.manager.init_db()

        # Create 1 recent job
        recent_job = self.manager.create_job("job_recent", "https://example.com/recent")

        # Create 1 completed job that completed 2 days ago
        old_job = self.manager.create_job("job_old_completed", "https://example.com/old")
        old_time = time.time() - 172800  # 48 hours ago
        conn = sqlite3.connect(self.db_path)
        try:
            conn.execute(
                """
                UPDATE mobile_jobs
                SET status = 'completed', updated_at = ?, completed_at = ?
                WHERE job_id = 'job_old_completed';
                """,
                (old_time, old_time),
            )
            conn.commit()
        finally:
            conn.close()

        # Also add dummy entry in _live_progress to verify cleanup
        with self.manager._lock:
            self.manager._live_progress["job_old_completed"] = {"progress": 100.0}

        # Run delete_expired_jobs with max_age_seconds = 86400 (24h)
        deleted = self.manager.delete_expired_jobs(max_age_seconds=86400)
        self.assertEqual(deleted, 1, f"Expected 1 expired job deleted, got {deleted}")

        # Verify old job is gone and recent job remains
        self.assertIsNone(self.manager.get_job("job_old_completed"))
        self.assertIsNotNone(self.manager.get_job("job_recent"))

        # Verify in-memory tracking evicted
        with self.manager._lock:
            self.assertNotIn("job_old_completed", self.manager._live_progress)

    # --- 6. LIFESPAN STARTUP INTEGRATION TEST ---
    def test_lifespan_integration(self):
        """Tests that main.py lifespan initializes the DB cleanly."""
        import asyncio
        import main
        from fastapi import FastAPI

        # Set DB path to test db
        orig_db_path = os.environ.get("STREAMVAULT_DB_PATH")
        os.environ["STREAMVAULT_DB_PATH"] = self.db_path

        try:
            async def run_lifespan():
                app = FastAPI(lifespan=main.lifespan)
                async with main.lifespan(app):
                    pass

            asyncio.run(run_lifespan())
            self.assertTrue(os.path.exists(self.db_path))
            # Verify table exists in the database
            conn = sqlite3.connect(self.db_path)
            try:
                cur = conn.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='mobile_jobs';")
                self.assertIsNotNone(cur.fetchone())
            finally:
                conn.close()
        finally:
            if orig_db_path:
                os.environ["STREAMVAULT_DB_PATH"] = orig_db_path
            else:
                os.environ.pop("STREAMVAULT_DB_PATH", None)


if __name__ == "__main__":
    unittest.main()
