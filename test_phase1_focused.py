"""
test_phase1_focused.py - Focused tests for Phase 1 decoupling and execution.
"""

import asyncio
import json
import os
import threading
import time
import unittest
import main


class TestPhase1Focused(unittest.IsolatedAsyncioTestCase):
    async def test_01_independent_background_execution(self):
        fid = "test_bg_job_001"
        cancel_ev = threading.Event()
        progress_updates = []

        def on_prog(payload):
            progress_updates.append(payload)

        class MockYDLSuccess:
            def __init__(self, opts):
                self.opts = opts

            def __enter__(self):
                return self

            def __exit__(self, *args):
                pass

            def download(self, urls):
                hook = self.opts["progress_hooks"][0]
                hook({"status": "downloading", "total_bytes": 50 * 1024 * 1024, "downloaded_bytes": 25 * 1024 * 1024, "speed": 1024 * 1024, "eta": 25})
                hook({"status": "downloading", "total_bytes": 50 * 1024 * 1024, "downloaded_bytes": 50 * 1024 * 1024, "speed": 2 * 1024 * 1024, "eta": 0})
                out_file = os.path.join(main.TEMP_DOWNLOAD_DIR, f"{fid}.mp4")
                with open(out_file, "wb") as f:
                    f.write(b"0" * (50 * 1024))

        orig_ydl = main.yt_dlp.YoutubeDL
        main.yt_dlp.YoutubeDL = MockYDLSuccess
        try:
            res = await main.execute_download_core(
                file_uuid=fid,
                url="https://youtube.com/watch?v=dummy123",
                format_id="best",
                cancel_event=cancel_ev,
                progress_callback=on_prog
            )
            self.assertEqual(res["status"], "completed")
            self.assertEqual(res["file_id"], fid)
            self.assertEqual(len(progress_updates), 2)
            self.assertEqual(progress_updates[0]["percent"], 50.0)
            self.assertEqual(progress_updates[1]["percent"], 100.0)

            self.assertIn(fid, main.active_file_map)
            self.assertEqual(main.active_file_map[fid]["storage_type"], "tmpfs")
            self.assertTrue(os.path.exists(main.active_file_map[fid]["filepath"]))
            self.assertNotIn(fid, main.active_downloads)

            main.cleanup_file(main.active_file_map[fid]["filepath"])
            main.active_file_map.pop(fid, None)
        finally:
            main.yt_dlp.YoutubeDL = orig_ydl

    async def test_02_sse_adapter_compatibility(self):
        fid2 = "test_sse_job_002"

        class MockYDLSSE:
            def __init__(self, opts):
                self.opts = opts

            def __enter__(self):
                return self

            def __exit__(self, *args):
                pass

            def download(self, urls):
                hook = self.opts["progress_hooks"][0]
                hook({"status": "downloading", "total_bytes": 1000, "downloaded_bytes": 500, "speed": 100, "eta": 5})
                tmpl = self.opts["outtmpl"]
                actual_path = tmpl.replace(".%(ext)s", ".mp4")
                with open(actual_path, "wb") as f:
                    f.write(b"video_bytes")

        orig_ydl = main.yt_dlp.YoutubeDL
        main.yt_dlp.YoutubeDL = MockYDLSSE
        try:
            class MockRequest:
                async def is_disconnected(self):
                    return False

            url = "https://youtube.com/watch?v=dummy456"
            main.format_cache[url] = {"formats": {"best"}, "expires": time.time() + 3600}

            resp = await main.download_progress(MockRequest(), url, "best")
            body_parts = []
            async for chunk in resp.body_iterator:
                body_parts.append(chunk)

            full_stream = "".join(body_parts)
            lines = [line for line in full_stream.split("\n\n") if line.strip()]
            self.assertGreaterEqual(len(lines), 2)
            ev1 = json.loads(lines[0].replace("data: ", ""))
            self.assertEqual(ev1["status"], "processing")
            self.assertEqual(ev1["percent"], 50.0)
            ev_final = json.loads(lines[-1].replace("data: ", ""))
            self.assertEqual(ev_final["status"], "completed")
            self.assertIn("file_id", ev_final)

            if ev_final["file_id"] in main.active_file_map:
                main.cleanup_file(main.active_file_map[ev_final["file_id"]]["filepath"])
                main.active_file_map.pop(ev_final["file_id"], None)
        finally:
            main.yt_dlp.YoutubeDL = orig_ydl

    async def test_03_browser_disconnect_cancellation(self):
        class MockYDLSlow:
            def __init__(self, opts):
                self.opts = opts

            def __enter__(self):
                return self

            def __exit__(self, *args):
                pass

            def download(self, urls):
                hook = self.opts["progress_hooks"][0]
                hook({"status": "downloading", "total_bytes": 1000, "downloaded_bytes": 100, "speed": 10, "eta": 90})
                for _ in range(50):
                    time.sleep(0.1)
                    hook({"status": "downloading", "total_bytes": 1000, "downloaded_bytes": 100, "speed": 10, "eta": 90})

        orig_ydl = main.yt_dlp.YoutubeDL
        main.yt_dlp.YoutubeDL = MockYDLSlow
        try:
            class MockDisconnectingRequest:
                def __init__(self):
                    self.calls = 0

                async def is_disconnected(self):
                    self.calls += 1
                    return self.calls >= 2

            url = "https://youtube.com/watch?v=dummy789"
            main.format_cache[url] = {"formats": {"best"}, "expires": time.time() + 3600}

            resp = await main.download_progress(MockDisconnectingRequest(), url, "best")
            async for chunk in resp.body_iterator:
                pass

            await asyncio.sleep(0.5)
            self.assertEqual(len(main.active_downloads), 0)
        finally:
            main.yt_dlp.YoutubeDL = orig_ydl


if __name__ == "__main__":
    unittest.main()
