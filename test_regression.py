"""
test_regression.py - Existing regression tests for interval merger, protocol completion, and task references.
"""

import os
import tempfile
import asyncio
import time
import unittest
import main
from fastapi import Request


class TestRegressionSuite(unittest.IsolatedAsyncioTestCase):
    async def test_regression_suite(self):
        # 1. Interval merger
        ranges = []
        ranges = main.add_delivered_range(ranges, 0, 100)
        ranges = main.add_delivered_range(ranges, 100, 200)
        self.assertEqual(ranges, [(0, 200)])
        self.assertTrue(main.is_fully_covered(ranges, 200))

        # 2. Protocol completion and task reference test
        proto = main.NginxLogDatagramProtocol()
        fid = "test_fid_reg"
        main.active_file_map[fid] = {
            "filepath": "/tmp/dummy",
            "size": 500,
            "expires": time.time() + 3600,
            "pending_requests": {"r1"},
            "delivered_ranges": []
        }
        await proto.handle_completion(fid, "r1", 200, 500, "500", None)
        self.assertEqual(len(proto.active_tasks), 0)
        self.assertNotIn("r1", main.active_file_map[fid]["pending_requests"])
        self.assertIn(fid, main.pending_disconnect_cleanups)
        main.pending_disconnect_cleanups.pop(fid, None).cancel()
        main.active_file_map.pop(fid, None)

    async def test_deliver_file_response_filename_normalization(self):
        """Verify extension normalization, deduplication, and Content-Disposition headers."""
        import uuid
        with tempfile.TemporaryDirectory() as tmpdir:
            fid = str(uuid.uuid4())
            test_file = os.path.join(tmpdir, f"{fid}.unknown_video")
            with open(test_file, "wb") as f:
                f.write(b"dummy video data")

            async with main.state_lock:
                main.active_file_map[fid] = {
                    "filepath": test_file,
                    "size": len(b"dummy video data"),
                    "expires": time.time() + 3600,
                    "storage_type": "disk",
                    "active_streams": 0,
                    "pending_requests": set(),
                    "delivered_ranges": []
                }

            class MockReq:
                headers = {}
                client = None

            # Test Python mode (Mode 2)
            main.USE_X_ACCEL_REDIRECT = False
            resp_py = await main.deliver_file_response(MockReq(), fid, "My Facebook Video.mp4...")
            self.assertEqual(resp_py.media_type, "video/mp4")
            cd = resp_py.headers.get("content-disposition", "")
            self.assertIn('filename="My Facebook Video.mp4"', cd)
            self.assertIn("filename*=UTF-8''My%20Facebook%20Video.mp4", cd)

            # Test Nginx mode (Mode 1)
            main.USE_X_ACCEL_REDIRECT = True
            resp_nx = await main.deliver_file_response(MockReq(), fid, "Malayalam വിഡിയോ")
            self.assertEqual(resp_nx.headers.get("content-type"), "video/mp4")
            cd_nx = resp_nx.headers.get("content-disposition", "")
            self.assertIn('filename="Malayalam.mp4"', cd_nx)
            self.assertIn("filename*=UTF-8''Malayalam%20%E0%B4%B5%E0%B4%BF%E0%B4%A1%E0%B4%BF%E0%B4%AF%E0%B5%8B.mp4", cd_nx)

            # Cleanup
            async with main.state_lock:
                main.active_file_map.pop(fid, None)


if __name__ == "__main__":
    unittest.main()
