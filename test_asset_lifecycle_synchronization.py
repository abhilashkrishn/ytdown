"""
test_asset_lifecycle_synchronization.py - Tests for mobile job and asset lifecycle synchronization.

Validation requirements:
1. Asset expires while job is ready.
2. Status endpoint does not report ready for a missing asset.
3. Download endpoint rejects an expired asset without entering delivering.
4. Active delivery is not invalidated by maintenance.
5. Completed/failed/cancelled jobs are not overwritten.
6. Active jobs are not deleted by the 24-hour cleanup condition.
7. A valid ready asset can still be downloaded normally.
"""

import asyncio
import os
import shutil
import tempfile
import time
import unittest
import uuid

from fastapi import HTTPException

import job_manager
import main
import mobile_api


class MockRequest:
    def __init__(self, headers=None):
        self.headers = headers or {}
        self.client = None


class TestAssetLifecycleSynchronization(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.test_dir = tempfile.mkdtemp(prefix="sv_lifecycle_test_")
        self.db_path = os.path.join(self.test_dir, "test_lifecycle.db")
        job_manager._default_manager = job_manager.JobManager(db_path=self.db_path)
        job_manager.init_db()

        self.orig_temp_dir = main.TEMP_DOWNLOAD_DIR
        self.orig_tmpfs_dir = main.TMPFS_DOWNLOAD_DIR
        main.TEMP_DOWNLOAD_DIR = self.test_dir
        main.TMPFS_DOWNLOAD_DIR = self.test_dir

        async with main.state_lock:
            main.active_file_map.clear()
            main.active_sessions.clear()
            main.format_cache.clear()
            main.pending_disconnect_cleanups.clear()

        mobile_api.reset_in_memory_state()

    async def asyncTearDown(self):
        main.TEMP_DOWNLOAD_DIR = self.orig_temp_dir
        main.TMPFS_DOWNLOAD_DIR = self.orig_tmpfs_dir
        if os.path.exists(self.test_dir):
            shutil.rmtree(self.test_dir, ignore_errors=True)

    # 1. Asset expires while job is ready
    async def test_01_asset_expires_while_job_is_ready(self):
        jid = "job_test_01"
        fid = str(uuid.uuid4())
        fpath = os.path.join(self.test_dir, f"{fid}.mp4")
        with open(fpath, "wb") as f:
            f.write(b"video_bytes_01")

        job_manager.create_job(jid, "https://example.com/v1", status="ready")
        job_manager.update_job(jid, file_id=fid)

        # Asset registered in active_file_map with past expiry
        async with main.state_lock:
            main.active_file_map[fid] = {
                "filepath": fpath,
                "size": len(b"video_bytes_01"),
                "expires": time.time() - 10.0,  # Expired
                "storage_type": "disk",
                "active_streams": 0,
                "pending_requests": set(),
                "job_id": jid,
            }

        # Run maintenance tasks
        await main.run_maintenance_tasks()

        # Verify asset was popped from active_file_map and cleaned up from disk
        async with main.state_lock:
            self.assertNotIn(fid, main.active_file_map)
        self.assertFalse(os.path.exists(fpath))

        # Verify job transitioned from ready to asset_expired
        job = job_manager.get_job(jid)
        self.assertEqual(job["status"], "asset_expired")
        self.assertIn("expired", job["error_message"].lower())
        self.assertIsNotNone(job["completed_at"])

    # 2. Status endpoint does not report ready for a missing asset
    async def test_02_status_endpoint_does_not_report_ready_for_missing_asset(self):
        jid = "job_test_02"
        fid = str(uuid.uuid4())

        # Job is marked ready in DB, but asset does not exist in active_file_map or on disk
        job_manager.create_job(jid, "https://example.com/v2", status="ready")
        job_manager.update_job(jid, file_id=fid)

        status_resp = await mobile_api.get_job_status(jid)
        self.assertEqual(status_resp["status"], "asset_expired")
        self.assertIn("expired", status_resp["error_message"].lower())

        # Verify DB is updated to asset_expired
        job = job_manager.get_job(jid)
        self.assertEqual(job["status"], "asset_expired")

    # 3. Download endpoint rejects an expired asset without entering delivering
    async def test_03_download_endpoint_rejects_expired_asset_without_delivering(self):
        jid = "job_test_03"
        fid = str(uuid.uuid4())

        job_manager.create_job(jid, "https://example.com/v3", status="ready")
        job_manager.update_job(jid, file_id=fid)

        # file_id not in active_file_map -> must raise HTTP 404
        req = MockRequest()
        with self.assertRaises(HTTPException) as ctx:
            await mobile_api.download_mobile_job(req, jid)

        self.assertEqual(ctx.exception.status_code, 404)

        # Crucial check: job must NOT have transitioned to 'delivering'
        job = job_manager.get_job(jid)
        self.assertNotEqual(job["status"], "delivering")
        self.assertEqual(job["status"], "asset_expired")

    # 4. Active delivery is not invalidated by maintenance
    async def test_04_active_delivery_not_invalidated_by_maintenance(self):
        jid = "job_test_04"
        fid = str(uuid.uuid4())
        fpath = os.path.join(self.test_dir, f"{fid}.mp4")
        with open(fpath, "wb") as f:
            f.write(b"video_bytes_04")

        job_manager.create_job(jid, "https://example.com/v4", status="delivering")
        job_manager.update_job(jid, file_id=fid)

        # Asset has passed expiry timestamp, BUT active_streams > 0
        async with main.state_lock:
            main.active_file_map[fid] = {
                "filepath": fpath,
                "size": len(b"video_bytes_04"),
                "expires": time.time() - 10.0,
                "storage_type": "disk",
                "active_streams": 1,
                "pending_requests": set(),
                "job_id": jid,
            }

        await main.run_maintenance_tasks()

        # Active asset must NOT be evicted or deleted while active_streams > 0
        async with main.state_lock:
            self.assertIn(fid, main.active_file_map)
        self.assertTrue(os.path.exists(fpath))

        # Job must remain in 'delivering'
        job = job_manager.get_job(jid)
        self.assertEqual(job["status"], "delivering")

    # 5. Completed/failed/cancelled jobs are not overwritten
    async def test_05_terminal_jobs_not_overwritten(self):
        fid = str(uuid.uuid4())

        # Test completed job
        jid_c = "job_term_completed"
        job_manager.create_job(jid_c, "https://example.com/c", status="completed")
        job_manager.update_job(jid_c, file_id=fid)
        res_c = job_manager.expire_job_asset(job_id=jid_c, file_id=fid)
        self.assertFalse(res_c)
        self.assertEqual(job_manager.get_job(jid_c)["status"], "completed")

        # Test failed job
        jid_f = "job_term_failed"
        job_manager.create_job(jid_f, "https://example.com/f", status="failed")
        job_manager.update_job(jid_f, file_id=fid)
        res_f = job_manager.expire_job_asset(job_id=jid_f, file_id=fid)
        self.assertFalse(res_f)
        self.assertEqual(job_manager.get_job(jid_f)["status"], "failed")

        # Test cancelled job
        jid_x = "job_term_cancelled"
        job_manager.create_job(jid_x, "https://example.com/x", status="cancelled")
        job_manager.update_job(jid_x, file_id=fid)
        res_x = job_manager.expire_job_asset(job_id=jid_x, file_id=fid)
        self.assertFalse(res_x)
        self.assertEqual(job_manager.get_job(jid_x)["status"], "cancelled")

    # 6. Active jobs are not deleted by the 24-hour cleanup condition
    async def test_06_active_jobs_not_deleted_by_cleanup(self):
        jid_active = "job_active_downloading"
        job_manager.create_job(jid_active, "https://example.com/active", status="downloading")

        # Artificially age the job past 24 hours (86400 seconds)
        old_time = time.time() - 90000
        with job_manager.get_job_manager()._get_connection() as conn:
            conn.execute(
                "UPDATE mobile_jobs SET updated_at = ? WHERE job_id = ?;",
                (old_time, jid_active),
            )

        deleted = job_manager.delete_expired_jobs(max_age_seconds=86400)
        # The active job was transitioned to failed, but NOT deleted
        self.assertEqual(deleted, 0)
        job = job_manager.get_job(jid_active)
        self.assertIsNotNone(job)
        self.assertEqual(job["status"], "failed")
        self.assertIn("abandoned", job["error_message"].lower())

    # 7. A valid ready asset can still be downloaded normally
    async def test_07_valid_ready_asset_downloads_normally(self):
        jid = "job_valid_download"
        fid = str(uuid.uuid4())
        fpath = os.path.join(self.test_dir, f"{fid}.mp4")
        with open(fpath, "wb") as f:
            f.write(b"Z" * 200)

        job_manager.create_job(jid, "https://example.com/dl_normal", status="ready", title="Valid Download")
        job_manager.update_job(jid, file_id=fid)

        async with main.state_lock:
            main.active_file_map[fid] = {
                "filepath": fpath,
                "size": 200,
                "expires": time.time() + 3600,
                "storage_type": "disk",
                "active_streams": 0,
                "pending_requests": set(),
                "job_id": jid,
            }

        main.USE_X_ACCEL_REDIRECT = False
        req = MockRequest()
        resp = await mobile_api.download_mobile_job(req, jid)
        self.assertEqual(resp.status_code, 200)

        # Job transitioned to delivering
        job = job_manager.get_job(jid)
        self.assertEqual(job["status"], "delivering")

    # 8. Oversized Unicode titles produce bounded Content-Disposition headers (< 1KB)
    async def test_08_oversized_title_bounded_content_disposition(self):
        jid = "job_huge_title"
        fid = str(uuid.uuid4())
        fpath = os.path.join(self.test_dir, f"{fid}.mp4")
        with open(fpath, "wb") as f:
            f.write(b"H" * 100)

        # 500-character Malayalam title that would expand to ~4.5 KB without truncation
        huge_malayalam_title = "കണ്ണൂര്‍ എഡിഎമ്മിന്റെ മരണത്തിൽ " * 15
        job_manager.create_job(jid, "https://example.com/huge", status="ready", title=huge_malayalam_title)
        job_manager.update_job(jid, file_id=fid)

        async with main.state_lock:
            main.active_file_map[fid] = {
                "filepath": fpath,
                "size": 100,
                "expires": time.time() + 3600,
                "storage_type": "disk",
                "active_streams": 0,
                "pending_requests": set(),
                "job_id": jid,
            }

        # Test Nginx Mode 1
        main.USE_X_ACCEL_REDIRECT = True
        req = MockRequest()
        resp_nx = await mobile_api.download_mobile_job(req, jid)
        cd_nx = resp_nx.headers.get("content-disposition", "")
        # Header must be bounded (well under 1.5 KB to prevent Nginx 502)
        self.assertLess(len(cd_nx), 1500)
        self.assertIn(".mp4", cd_nx)
        self.assertIn("filename*=UTF-8''", cd_nx)

        # Verify database record title remains untruncated
        job = job_manager.get_job(jid)
        self.assertEqual(job["title"], huge_malayalam_title)


if __name__ == "__main__":
    unittest.main()
