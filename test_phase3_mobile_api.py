"""
test_phase3_mobile_api.py - Focused tests for Phase 3 Mobile API.

Tests cover:
1. Submission & URL validation (POST /api/mobile/submit, SSRF/scheme checks, 201 response).
2. Job status & listing (GET /api/mobile/status/{job_id}, GET /api/mobile/jobs, 404 handling).
3. Queueing & Concurrency (shared limit with web, queue capacity limit of 10 jobs).
4. Cancellation (POST /api/mobile/cancel/{job_id}, cancel_event signaling, queued vs running).
5. Failed job handling (graceful failure, error_message persistence, slot reclamation).
6. Download authorization by job ID (GET /api/mobile/download/{job_id}, status gates, X-Accel & Range).
"""

import asyncio
import os
import shutil
import tempfile
import threading
import time
import unittest
import uuid

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


class TestMobileAPIFocused(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.test_dir = tempfile.mkdtemp(prefix="streamvault_mobile_test_")
        self.db_path = os.path.join(self.test_dir, "test_mobile.db")

        # Point job_manager to test database
        self.orig_env_db = os.environ.get("STREAMVAULT_DB_PATH")
        os.environ["STREAMVAULT_DB_PATH"] = self.db_path
        job_manager.init_db(self.db_path)

        # Clear state dictionaries
        async with main.state_lock:
            main.active_downloads.clear()
            main.active_file_map.clear()
            main.pending_disconnect_cleanups.clear()

        mobile_api.active_mobile_tasks.clear()
        mobile_api._active_mobile_jobs.clear()

        self.orig_ydl = main.yt_dlp.YoutubeDL
        main.yt_dlp.YoutubeDL = MockDefaultYDL

        self.orig_xaccel = main.USE_X_ACCEL_REDIRECT

    async def asyncTearDown(self):
        main.yt_dlp.YoutubeDL = self.orig_ydl
        main.USE_X_ACCEL_REDIRECT = self.orig_xaccel

        await mobile_api.shutdown_mobile_tasks()
        if self.orig_env_db:
            os.environ["STREAMVAULT_DB_PATH"] = self.orig_env_db
        else:
            os.environ.pop("STREAMVAULT_DB_PATH", None)

        if os.path.exists(self.test_dir):
            try:
                shutil.rmtree(self.test_dir)
            except OSError:
                pass

    # --- 1. SUBMISSION & URL VALIDATION ---
    async def test_01_submission_and_url_validation(self):
        """Tests URL validation, SSRF blocking, job creation, and 201 response."""
        orig_validate = main.validate_url_safety

        async def mock_val(u):
            if not u.startswith("https://"):
                raise ValueError("URL must begin with https://")
            if "127.0.0.1" in u or "localhost" in u:
                raise ValueError("URL resolves to a restricted or non-global network range")

        main.validate_url_safety = mock_val

        try:
            req = mobile_api.SubmitRequest(url="https://youtube.com/watch?v=valid_test_01")
            resp = await mobile_api.submit_job(req)
            self.assertIn("job_id", resp)
            self.assertEqual(resp["status"], "queued")
            self.assertEqual(resp["url"], "https://youtube.com/watch?v=valid_test_01")

            # Check SQLite
            job_in_db = job_manager.get_job(resp["job_id"])
            self.assertIsNotNone(job_in_db)
            self.assertEqual(job_in_db["status"], "queued")

            # 2. Invalid URL scheme
            with self.assertRaises(mobile_api.HTTPException) as ctx:
                await mobile_api.submit_job(mobile_api.SubmitRequest(url="ftp://youtube.com/video"))
            self.assertEqual(ctx.exception.status_code, 400)

            # 3. SSRF / Local IP blocking
            with self.assertRaises(mobile_api.HTTPException) as ctx:
                await mobile_api.submit_job(mobile_api.SubmitRequest(url="https://127.0.0.1/video"))
            self.assertEqual(ctx.exception.status_code, 400)
            self.assertIn("restricted", ctx.exception.detail)

            # 4. Empty URL
            with self.assertRaises(mobile_api.HTTPException) as ctx:
                await mobile_api.submit_job(mobile_api.SubmitRequest(url="   "))
            self.assertEqual(ctx.exception.status_code, 400)
        finally:
            main.validate_url_safety = orig_validate

    # --- 2. STATUS & JOBS LISTING ---
    async def test_02_status_and_jobs_listing(self):
        """Tests GET /api/mobile/status/{job_id} and GET /api/mobile/jobs."""
        # Create 3 jobs
        job_manager.create_job("job_list_1", "https://youtube.com/watch?v=1", title="Title 1")
        time.sleep(0.01)
        job_manager.create_job("job_list_2", "https://youtube.com/watch?v=2", title="Title 2")
        time.sleep(0.01)
        job_manager.create_job("job_list_3", "https://youtube.com/watch?v=3", title="Title 3")

        # Test status of existing job
        status_res = await mobile_api.get_job_status("job_list_2")
        self.assertEqual(status_res["job_id"], "job_list_2")
        self.assertEqual(status_res["title"], "Title 2")
        self.assertEqual(status_res["status"], "queued")

        # Test status of non-existent job -> 404
        with self.assertRaises(mobile_api.HTTPException) as ctx:
            await mobile_api.get_job_status("non_existent_uuid")
        self.assertEqual(ctx.exception.status_code, 404)

        # Test listing jobs
        all_jobs = await mobile_api.list_mobile_jobs()
        self.assertGreaterEqual(len(all_jobs), 3)
        # Should be ordered created_at DESC (job_list_3, job_list_2, job_list_1)
        job_ids = [j["job_id"] for j in all_jobs[:3]]
        self.assertEqual(job_ids, ["job_list_3", "job_list_2", "job_list_1"])

    # --- 3. QUEUEING & CONCURRENCY LIMITS ---
    async def test_03_queueing_and_concurrency(self):
        """Tests queue capacity (max 10) and shared concurrency limit (max 3)."""
        orig_validate = main.validate_url_safety

        async def mock_val(u): pass

        main.validate_url_safety = mock_val

        try:
            # 1. Capacity limit test: fill up to 10 active/queued jobs
            for i in range(10):
                job_manager.create_job(f"cap_job_{i}", f"https://example.com/v_{i}", status="queued")

            self.assertEqual(job_manager.get_active_job_count(), 10)

            # Submitting 11th job must be rejected with 429
            with self.assertRaises(mobile_api.HTTPException) as ctx:
                await mobile_api.submit_job(mobile_api.SubmitRequest(url="https://example.com/eleventh"))
            self.assertEqual(ctx.exception.status_code, 429)
            self.assertIn("limit reached", ctx.exception.detail)

            # Prune capacity jobs
            for i in range(10):
                job_manager.update_job(f"cap_job_{i}", status="cancelled")

            # 2. Shared concurrency limit test (3 concurrent downloads)
            async with main.state_lock:
                # Simulate 3 active web downloads occupying all slots
                main.active_downloads["web_1"] = {"cancel_event": threading.Event(), "worker_task": None}
                main.active_downloads["web_2"] = {"cancel_event": threading.Event(), "worker_task": None}
                main.active_downloads["web_3"] = {"cancel_event": threading.Event(), "worker_task": None}

            # Submit mobile job while capacity slots are full
            resp = await mobile_api.submit_job(mobile_api.SubmitRequest(url="https://example.com/queued_test"))
            qid = resp["job_id"]
            self.assertEqual(resp["status"], "queued")

            # Verify it remains queued because no slots are free
            await asyncio.sleep(0.05)
            q_job = job_manager.get_job(qid)
            self.assertEqual(q_job["status"], "queued")

            # Now free 1 slot and schedule mobile queue
            async with main.state_lock:
                main.active_downloads.pop("web_1", None)

            mobile_api.schedule_mobile_queue()
            await asyncio.sleep(0.05)

            # Queued job should have been picked up
            updated_q = job_manager.get_job(qid)
            self.assertIn(updated_q["status"], ("extracting", "downloading", "failed", "ready"))

            # Clean up dummy active downloads
            async with main.state_lock:
                main.active_downloads.clear()

        finally:
            main.validate_url_safety = orig_validate

    # --- 4. CANCELLATION ---
    async def test_04_cancellation(self):
        """Tests POST /api/mobile/cancel/{job_id} on queued and active jobs."""
        # 1. Cancel a queued job
        job_manager.create_job("job_to_cancel_queued", "https://example.com/cancel1", status="queued")
        res = await mobile_api.cancel_mobile_job("job_to_cancel_queued")
        self.assertEqual(res["status"], "cancelled")

        job_db = job_manager.get_job("job_to_cancel_queued")
        self.assertEqual(job_db["status"], "cancelled")
        self.assertIsNotNone(job_db["completed_at"])

        # 2. Cancel an active job with cancel_event
        cancel_ev = threading.Event()
        dummy_task = asyncio.create_task(asyncio.sleep(10))
        job_manager.create_job("job_to_cancel_active", "https://example.com/cancel2", status="downloading")
        mobile_api._active_mobile_jobs["job_to_cancel_active"] = {
            "cancel_event": cancel_ev,
            "file_uuid": "dummy_uuid",
            "task": dummy_task
        }

        self.assertFalse(cancel_ev.is_set())
        res_active = await mobile_api.cancel_mobile_job("job_to_cancel_active")
        self.assertEqual(res_active["status"], "cancelled")
        self.assertTrue(cancel_ev.is_set(), "cancel_event must be signaled upon job cancellation")

        # Yield to let event loop propagate cancellation to dummy_task
        await asyncio.sleep(0)
        self.assertTrue(dummy_task.cancelled())

        # 3. Cancel non-existent job -> 404
        with self.assertRaises(mobile_api.HTTPException) as ctx:
            await mobile_api.cancel_mobile_job("non_existent_job_xyz")
        self.assertEqual(ctx.exception.status_code, 404)

        # 4. Cancel already cancelled job -> returns safely
        safe_res = await mobile_api.cancel_mobile_job("job_to_cancel_queued")
        self.assertEqual(safe_res["status"], "cancelled")
        self.assertIn("already", safe_res["message"])

    # --- 5. FAILED JOBS HANDLING ---
    async def test_05_failed_job_handling(self):
        """Tests failure transitions when metadata extraction fails."""
        fid = "job_failed_test"
        job_manager.create_job(fid, "https://example.com/fail_video")

        class MockYDLError:
            def __init__(self, opts): pass
            def __enter__(self): return self
            def __exit__(self, *args): pass
            def extract_info(self, url, download=False):
                raise RuntimeError("Video unavailable in your country")

        main.yt_dlp.YoutubeDL = MockYDLError
        try:
            await mobile_api._execute_mobile_job(fid, "https://example.com/fail_video")

            job_db = job_manager.get_job(fid)
            self.assertEqual(job_db["status"], "failed")
            self.assertIn("Failed to retrieve video metadata", job_db["error_message"])
            self.assertIsNotNone(job_db["completed_at"])

            # Verify active job was popped from tracking
            self.assertNotIn(fid, mobile_api._active_mobile_jobs)
        finally:
            main.yt_dlp.YoutubeDL = MockDefaultYDL

    # --- 6. DOWNLOAD AUTHORIZATION & DELIVERY BY JOB ID ---
    async def test_06_download_by_job_id(self):
        """Tests GET /api/mobile/download/{job_id} authorization, status checks, and delivery."""
        class MockRequest:
            def __init__(self, headers=None):
                self.headers = headers or {}
                self.client = None

        req = MockRequest()

        # 1. Non-existent job -> 404
        with self.assertRaises(mobile_api.HTTPException) as ctx:
            await mobile_api.download_mobile_job(req, "non_existent_job")
        self.assertEqual(ctx.exception.status_code, 404)

        # 2. Job in 'queued' or 'downloading' status -> 409 Conflict
        job_manager.create_job("job_download_queued", "https://example.com/dl1", status="queued")
        with self.assertRaises(mobile_api.HTTPException) as ctx:
            await mobile_api.download_mobile_job(req, "job_download_queued")
        self.assertEqual(ctx.exception.status_code, 409)

        # 3. Job in 'failed' status -> 400 Bad Request
        job_manager.create_job("job_download_failed", "https://example.com/dl2", status="failed")
        with self.assertRaises(mobile_api.HTTPException) as ctx:
            await mobile_api.download_mobile_job(req, "job_download_failed")
        self.assertEqual(ctx.exception.status_code, 400)

        # 4. Job in 'ready' status with registered file in active_file_map
        test_file_id = str(uuid.uuid4())
        test_file_path = os.path.join(self.test_dir, f"{test_file_id}.mp4")
        with open(test_file_path, "wb") as f:
            f.write(b"ABCDEFGHIJ" * 10)  # 100 bytes

        async with main.state_lock:
            main.active_file_map[test_file_id] = {
                "filepath": test_file_path,
                "expires": time.time() + 3600,
                "storage_type": "disk",
                "size": 100,
                "active_streams": 0,
                "pending_requests": set(),
                "delivered_ranges": []
            }

        job_manager.create_job("job_download_ready", "https://example.com/dl3", status="ready", title="Test Movie")
        job_manager.update_job("job_download_ready", file_id=test_file_id)

        # Mode A: Full download request via Python ResilientFileResponse
        main.USE_X_ACCEL_REDIRECT = False
        resp = await mobile_api.download_mobile_job(req, "job_download_ready")
        self.assertIsNotNone(resp)
        self.assertEqual(resp.status_code, 200)
        self.assertEqual(resp.headers["accept-ranges"], "bytes")

        # Verify job transitioned to 'delivering'
        job_delivering = job_manager.get_job("job_download_ready")
        self.assertEqual(job_delivering["status"], "delivering")

        # Mode B: Range request via Python ResilientFileResponse
        req_range = MockRequest(headers={"range": "bytes=0-49"})
        resp_range = await mobile_api.download_mobile_job(req_range, "job_download_ready")
        self.assertIsNotNone(resp_range)
        self.assertEqual(resp_range.status_code, 206)
        self.assertEqual(resp_range.headers["accept-ranges"], "bytes")

        # Mode C: X-Accel-Redirect delivery
        main.USE_X_ACCEL_REDIRECT = True
        resp_xaccel = await mobile_api.download_mobile_job(req, "job_download_ready")
        self.assertEqual(resp_xaccel.status_code, 200)
        self.assertIn("x-accel-redirect", resp_xaccel.headers)
        self.assertTrue(resp_xaccel.headers["x-accel-redirect"].startswith("/internal_downloads/"))
        self.assertIn("sv_fid=", resp_xaccel.headers["x-accel-redirect"])
        self.assertIn("sv_rid=", resp_xaccel.headers["x-accel-redirect"])

        # Verify request registered in active_file_map pending_requests
        async with main.state_lock:
            pending = main.active_file_map[test_file_id]["pending_requests"]
            self.assertGreaterEqual(len(pending), 1)

        # Clean up test file
        async with main.state_lock:
            main.active_file_map.pop(test_file_id, None)
            timer = main.pending_disconnect_cleanups.pop(test_file_id, None)
            if timer:
                timer.cancel()
        if os.path.exists(test_file_path):
            os.remove(test_file_path)


if __name__ == "__main__":
    unittest.main()
