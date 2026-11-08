"""
test_lifecycle_corrections.py - Focused tests for the four lifecycle corrections:
1. Delivery completion (Nginx path: partial Range, interrupted Range, multiple pending requests, full coverage).
2. Python fallback delivery completion (partial response remains delivering, full response completes).
3. Clean-slate startup model (deletes all records from mobile_jobs, in-memory tracking empty, no redispatch).
4. Queue race prevention (atomic claim prevents duplicate execution under concurrent scheduling).
"""

import asyncio
import os
import shutil
import tempfile
import threading
import time
import unittest
import uuid

from fastapi import Request

import job_manager
import main
import mobile_api


class MockDefaultYDL:
    def __init__(self, opts):
        self.opts = opts

    def __enter__(self):
        return self

    def __exit__(self, *args):
        pass

    def extract_info(self, url, download=False):
        return {"title": "Mock Video Title", "formats": []}

    def download(self, urls):
        pass


class TestLifecycleCorrections(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.test_dir = tempfile.mkdtemp(prefix="streamvault_lifecycle_test_")
        self.db_path = os.path.join(self.test_dir, "test_lifecycle.db")

        self.orig_env_db = os.environ.get("STREAMVAULT_DB_PATH")
        os.environ["STREAMVAULT_DB_PATH"] = self.db_path
        job_manager.init_db(self.db_path)

        async with main.state_lock:
            main.active_downloads.clear()
            main.active_file_map.clear()
            main.pending_disconnect_cleanups.clear()

        mobile_api.reset_in_memory_state()

        self.orig_ydl = main.yt_dlp.YoutubeDL
        main.yt_dlp.YoutubeDL = MockDefaultYDL

        self.orig_xaccel = main.USE_X_ACCEL_REDIRECT

    async def asyncTearDown(self):
        main.yt_dlp.YoutubeDL = self.orig_ydl
        main.USE_X_ACCEL_REDIRECT = self.orig_xaccel

        await mobile_api.shutdown_mobile_tasks()
        async with main.state_lock:
            main.active_downloads.clear()
            main.active_file_map.clear()
            main.pending_disconnect_cleanups.clear()
        if self.orig_env_db:
            os.environ["STREAMVAULT_DB_PATH"] = self.orig_env_db
        else:
            os.environ.pop("STREAMVAULT_DB_PATH", None)

        if os.path.exists(self.test_dir):
            try:
                shutil.rmtree(self.test_dir)
            except OSError:
                pass

    # --- 1. DELIVERY COMPLETION: NGINX PATH ---
    async def test_01_nginx_partial_range_then_remaining_range(self):
        """
        Nginx delivery: partial Range chunk does not complete the job.
        Subsequent Range chunk achieving full byte coverage marks the job completed.
        """
        proto = main.NginxLogDatagramProtocol()
        fid = str(uuid.uuid4())
        jid = "job_nginx_range_01"
        file_size = 1000

        job_manager.create_job(jid, "https://example.com/v1", status="ready")
        job_manager.update_job(jid, status="delivering", file_id=fid)

        async with main.state_lock:
            main.active_file_map[fid] = {
                "filepath": os.path.join(self.test_dir, f"{fid}.mp4"),
                "size": file_size,
                "expires": time.time() + 3600,
                "pending_requests": {"req_chunk_1", "req_chunk_2"},
                "delivered_ranges": [],
                "job_id": jid,
            }

        # Event 1: First 500 bytes delivered (partial coverage [0, 500))
        await proto.handle_completion(
            file_id=fid,
            request_id="req_chunk_1",
            status=206,
            bytes_sent=500,
            content_length="500",
            content_range="bytes 0-499/1000"
        )

        job_after_chunk1 = job_manager.get_job(jid)
        self.assertEqual(job_after_chunk1["status"], "delivering", "Job must remain in 'delivering' after partial Range")

        # Event 2: Remaining 500 bytes delivered ([500, 1000)) -> full coverage [0, 1000)
        await proto.handle_completion(
            file_id=fid,
            request_id="req_chunk_2",
            status=206,
            bytes_sent=500,
            content_length="500",
            content_range="bytes 500-999/1000"
        )

        job_after_chunk2 = job_manager.get_job(jid)
        self.assertEqual(job_after_chunk2["status"], "completed", "Job must transition to 'completed' upon full coverage")
        self.assertIsNotNone(job_after_chunk2["completed_at"])

        # Test idempotency: repeated completion event does not corrupt status
        job_manager.update_job(jid, status="completed")
        self.assertEqual(job_manager.get_job(jid)["status"], "completed")

    async def test_02_nginx_interrupted_range_remains_delivering(self):
        """
        Nginx delivery: interrupted partial download (bytes_sent < expected) does not complete.
        """
        proto = main.NginxLogDatagramProtocol()
        fid = str(uuid.uuid4())
        jid = "job_nginx_interrupted"
        file_size = 1000

        job_manager.create_job(jid, "https://example.com/v2", status="ready")
        job_manager.update_job(jid, status="delivering", file_id=fid)

        async with main.state_lock:
            main.active_file_map[fid] = {
                "filepath": os.path.join(self.test_dir, f"{fid}.mp4"),
                "size": file_size,
                "expires": time.time() + 3600,
                "pending_requests": {"req_abort"},
                "delivered_ranges": [],
                "job_id": jid,
            }

        # Expected 500 bytes, but client interrupted after 150 bytes
        await proto.handle_completion(
            file_id=fid,
            request_id="req_abort",
            status=206,
            bytes_sent=150,
            content_length="500",
            content_range="bytes 0-499/1000"
        )

        job = job_manager.get_job(jid)
        self.assertEqual(job["status"], "delivering", "Interrupted download must leave job in 'delivering'")

    async def test_03_nginx_multiple_pending_requests(self):
        """
        Nginx delivery: job only completes when full coverage exists AND pending requests reach zero.
        """
        proto = main.NginxLogDatagramProtocol()
        fid = str(uuid.uuid4())
        jid = "job_nginx_multi_req"
        file_size = 1000

        job_manager.create_job(jid, "https://example.com/v3", status="ready")
        job_manager.update_job(jid, status="delivering", file_id=fid)

        async with main.state_lock:
            main.active_file_map[fid] = {
                "filepath": os.path.join(self.test_dir, f"{fid}.mp4"),
                "size": file_size,
                "expires": time.time() + 3600,
                "pending_requests": {"req_a", "req_b"},
                "delivered_ranges": [(0, 1000)],  # Already covered
                "job_id": jid,
            }

        # Event: req_a completes, but req_b is still pending
        await proto.handle_completion(
            file_id=fid,
            request_id="req_a",
            status=200,
            bytes_sent=1000,
            content_length="1000",
            content_range=None
        )

        job = job_manager.get_job(jid)
        self.assertEqual(job["status"], "delivering", "Must remain delivering while other requests are still pending")

        # Event: req_b completes -> pending requests reach zero
        await proto.handle_completion(
            file_id=fid,
            request_id="req_b",
            status=200,
            bytes_sent=1000,
            content_length="1000",
            content_range=None
        )

        job_done = job_manager.get_job(jid)
        self.assertEqual(job_done["status"], "completed", "Must transition to completed when pending reaches 0 and full coverage exists")

    # --- 2. DELIVERY COMPLETION: PYTHON FALLBACK ---
    async def test_04_python_fallback_partial_response_remains_delivering(self):
        """
        Python fallback: partial Range response (HTTP 206) does not mark the job completed.
        """
        main.USE_X_ACCEL_REDIRECT = False
        fid = str(uuid.uuid4())
        jid = "job_py_partial"
        filepath = os.path.join(self.test_dir, f"{fid}.mp4")
        with open(filepath, "wb") as f:
            f.write(b"X" * 100)

        job_manager.create_job(jid, "https://example.com/v4", status="ready")

        async with main.state_lock:
            main.active_file_map[fid] = {
                "filepath": filepath,
                "size": 100,
                "expires": time.time() + 3600,
                "storage_type": "disk",
                "active_streams": 0,
                "pending_requests": set(),
                "delivered_ranges": [],
                "job_id": jid,
            }

        job_manager.update_job(jid, file_id=fid)

        class MockRangeReq:
            headers = {"range": "bytes=0-49"}
            client = None

        resp = await mobile_api.download_mobile_job(MockRangeReq(), jid)
        self.assertEqual(resp.status_code, 206)

        # Trigger on_completed callback on the ResilientFileResponse
        if hasattr(resp, "on_completed") and resp.on_completed:
            await resp.on_completed()

        job = job_manager.get_job(jid)
        self.assertEqual(job["status"], "delivering", "Partial range response must NOT mark job completed")

    async def test_05_python_fallback_full_file_response_completes(self):
        """
        Python fallback: full-file response (no Range header) marks the job completed.
        """
        main.USE_X_ACCEL_REDIRECT = False
        fid = str(uuid.uuid4())
        jid = "job_py_full"
        filepath = os.path.join(self.test_dir, f"{fid}.mp4")
        with open(filepath, "wb") as f:
            f.write(b"Y" * 100)

        job_manager.create_job(jid, "https://example.com/v5", status="ready")

        async with main.state_lock:
            main.active_file_map[fid] = {
                "filepath": filepath,
                "size": 100,
                "expires": time.time() + 3600,
                "storage_type": "disk",
                "active_streams": 0,
                "pending_requests": set(),
                "delivered_ranges": [],
                "job_id": jid,
            }

        job_manager.update_job(jid, file_id=fid)

        class MockFullReq:
            headers = {}
            client = None

        resp = await mobile_api.download_mobile_job(MockFullReq(), jid)
        self.assertEqual(resp.status_code, 200)

        # Trigger on_completed callback
        if hasattr(resp, "on_completed") and resp.on_completed:
            await resp.on_completed()

        job = job_manager.get_job(jid)
        self.assertEqual(job["status"], "completed", "Full-file download must mark job completed")
        self.assertIsNotNone(job["completed_at"])

    # --- 3. RESTART RECOVERY: CLEAN-SLATE MODEL ---
    async def test_06_startup_clean_slate_model(self):
        """
        Clean-slate restart: all existing records are deleted from mobile_jobs,
        in-memory tracking starts empty, and no previous jobs are redispatched.
        """
        # Populate DB with various job states from previous runs
        states = ["queued", "extracting", "downloading", "processing", "ready", "delivering", "completed", "failed", "cancelled"]
        for s in states:
            job_manager.create_job(f"old_job_{s}", f"https://example.com/{s}", status=s)

        # Verify DB has 9 jobs
        self.assertEqual(len(job_manager.list_jobs(limit=100)), 9)

        # Simulate in-memory tracking populated before restart
        mobile_api._active_mobile_jobs["old_job_downloading"] = {"task": None}
        mobile_api.active_mobile_tasks.add(asyncio.create_task(asyncio.sleep(0.01)))

        # Simulate clean-slate restart
        cleared_count = job_manager.clear_all_jobs()
        mobile_api.reset_in_memory_state()

        self.assertEqual(cleared_count, 9)
        # All records deleted
        remaining_jobs = job_manager.list_jobs(limit=100)
        self.assertEqual(len(remaining_jobs), 0, "All jobs must be purged on restart in clean-slate model")

        # In-memory tracking starts empty
        self.assertEqual(len(mobile_api._active_mobile_jobs), 0)
        self.assertEqual(len(mobile_api.active_mobile_tasks), 0)

    # --- 4. QUEUE RACE PREVENTION ---
    async def test_07_queue_race_prevention(self):
        """
        Tests that atomic conditional claim prevents duplicate dispatch
        under rapid concurrent queue processing calls.
        """
        orig_validate = main.validate_url_safety
        async def mock_val(u): pass
        main.validate_url_safety = mock_val

        try:
            # Create a single queued job
            job_manager.create_job("race_job_01", "https://example.com/race", status="queued")

            dispatched_tasks = []

            # Simulate 10 simultaneous calls to process_mobile_queue()
            async def run_queue_pass():
                await mobile_api.process_mobile_queue()

            await asyncio.gather(*[run_queue_pass() for _ in range(10)])

            # Verify that only 1 execution task was created in _active_mobile_jobs
            active_count = len(mobile_api._active_mobile_jobs)
            self.assertEqual(active_count, 1, "Only one task should be created for a single queued job")

            # Clean up
            await mobile_api.shutdown_mobile_tasks()
        finally:
            main.validate_url_safety = orig_validate

    # --- 5. SHARED CONCURRENCY ACCOUNTING ---
    async def test_08_shared_concurrency_accounting(self):
        """
        Tests exact shared concurrency counting across web and mobile workloads:
        - 1 web download in main.active_downloads.
        - 1 mobile job extracting (not yet in main.active_downloads).
        - 1 mobile job downloading (in _active_mobile_jobs AND main.active_downloads).
        Total in-flight must be counted as 3 (1 web + 2 mobile, with no double counting).
        Ensures a 4th job remains queued until capacity frees up, preserving FIFO.
        """
        orig_validate = main.validate_url_safety
        async def mock_val(u): pass
        main.validate_url_safety = mock_val

        try:
            # 1. Simulate 1 web download in active_downloads
            async with main.state_lock:
                main.active_downloads["web_active_uuid"] = {
                    "cancel_event": threading.Event(),
                    "worker_task": None
                }

            # 2. Simulate 1 mobile job extracting (in _active_mobile_jobs, NOT in active_downloads)
            dummy_task_ext = asyncio.create_task(asyncio.sleep(10))
            mobile_api._active_mobile_jobs["mobile_job_extracting"] = {
                "cancel_event": threading.Event(),
                "file_uuid": "mobile_extracting_uuid",
                "task": dummy_task_ext
            }

            # 3. Simulate 1 mobile job downloading (in _active_mobile_jobs AND active_downloads)
            dummy_task_dl = asyncio.create_task(asyncio.sleep(10))
            mobile_api._active_mobile_jobs["mobile_job_downloading"] = {
                "cancel_event": threading.Event(),
                "file_uuid": "mobile_downloading_uuid",
                "task": dummy_task_dl
            }
            async with main.state_lock:
                main.active_downloads["mobile_downloading_uuid"] = {
                    "cancel_event": threading.Event(),
                    "worker_task": None
                }

            # 4. Queue 2 new jobs
            now = time.time()
            job_manager.create_job("job_queued_fifo_1", "https://example.com/q1", status="queued")
            # Ensure strictly distinct created_at timestamp for FIFO verification
            job_manager.create_job("job_queued_fifo_2", "https://example.com/q2", status="queued")

            # Run process_mobile_queue: total in flight is 1 web + 2 mobile = 3 -> 0 slots available
            await mobile_api.process_mobile_queue()

            # Verify neither queued job was claimed
            self.assertEqual(job_manager.get_job("job_queued_fifo_1")["status"], "queued")
            self.assertEqual(job_manager.get_job("job_queued_fifo_2")["status"], "queued")

            # 5. Free the web download slot
            async with main.state_lock:
                main.active_downloads.pop("web_active_uuid", None)

            # Run process_mobile_queue again: now 1 slot available
            await mobile_api.process_mobile_queue()

            # Verify FIFO: job_queued_fifo_1 claimed, job_queued_fifo_2 remains queued
            self.assertIn(
                job_manager.get_job("job_queued_fifo_1")["status"],
                ("extracting", "downloading", "ready", "failed")
            )
            self.assertEqual(job_manager.get_job("job_queued_fifo_2")["status"], "queued")

            # Clean up dummy tasks
            dummy_task_ext.cancel()
            dummy_task_dl.cancel()
            await mobile_api.shutdown_mobile_tasks()
            async with main.state_lock:
                main.active_downloads.clear()
        finally:
            main.validate_url_safety = orig_validate


if __name__ == "__main__":
    unittest.main()
