"""
test_regression.py - Existing regression tests for interval merger, protocol completion, and task references.
"""

import asyncio
import time
import unittest
import main


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


if __name__ == "__main__":
    unittest.main()
