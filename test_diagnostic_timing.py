"""
test_diagnostic_timing.py - Unit tests for StreamVault Diagnostic Timing corrections.

Verifies:
1. URL sanitization (strips auth tokens, cookies, tracking query params, preserving safe video ID).
2. CLIENT_DOWNLOAD_REQUEST recorded and logged only once across multiple requests.
3. Multi-stream download timing (media download end updates across streams and finalizes at postprocess boundary without overlap).
4. Stage duration vs cumulative elapsed time separation in READY and Full summaries.
"""

import unittest
import time
import io
import sys
from diagnostic_timing import DiagnosticTimingTracker, sanitize_url_for_logging, JobTimingRecord


class TestDiagnosticTimingCorrections(unittest.TestCase):
    def setUp(self):
        self.tracker = DiagnosticTimingTracker()

    def test_url_sanitization(self):
        # YouTube watch URL with sensitive tokens and extra query params
        yt_raw = "https://www.youtube.com/watch?v=dQw4w9WgXcQ&auth=secret_token&cookie=session123#timestamp"
        sanitized_yt = sanitize_url_for_logging(yt_raw)
        self.assertEqual(sanitized_yt, "https://www.youtube.com/watch?v=dQw4w9WgXcQ")

        # YouTube short link with tracking parameters
        short_raw = "https://youtu.be/dQw4w9WgXcQ?si=tracking_identifier_123"
        sanitized_short = sanitize_url_for_logging(short_raw)
        self.assertEqual(sanitized_short, "https://youtu.be/dQw4w9WgXcQ")

        # YouTube shorts
        shorts_raw = "https://www.youtube.com/shorts/dQw4w9WgXcQ?feature=share"
        sanitized_shorts = sanitize_url_for_logging(shorts_raw)
        self.assertEqual(sanitized_shorts, "https://www.youtube.com/shorts/dQw4w9WgXcQ")

        # Generic domain: strips sensitive queries and fragments
        generic_raw = "https://example.com/media/clip123?sig=abcdef&expires=999999#sec"
        sanitized_generic = sanitize_url_for_logging(generic_raw)
        self.assertEqual(sanitized_generic, "https://example.com/media/clip123")

        # Tracker record_submission_received uses sanitized URL
        rec = self.tracker.record_submission_received("test_job_url", yt_raw)
        self.assertEqual(rec.url, "https://www.youtube.com/watch?v=dQw4w9WgXcQ")

    def test_single_client_download_request(self):
        job_id = "test_single_req"
        self.tracker.record_submission_received(job_id)
        self.tracker.record_ready(job_id, "file_123", 1024, "Sample")

        captured_output = io.StringIO()
        old_stdout = sys.stdout
        try:
            sys.stdout = captured_output
            # First request (e.g. Range probe)
            self.tracker.record_download_request(job_id, "nginx")
            t_first = self.tracker.get(job_id).t_download_req
            self.assertIsNotNone(t_first)

            # Subsequent parallel Range requests
            time.sleep(0.01)
            self.tracker.record_download_request(job_id, "nginx")
            self.tracker.record_download_request(job_id, "nginx")
        finally:
            sys.stdout = old_stdout

        # Verify timestamp didn't change
        self.assertEqual(self.tracker.get(job_id).t_download_req, t_first)

        # Verify CLIENT_DOWNLOAD_REQUEST was logged exactly once
        logs = captured_output.getvalue().splitlines()
        client_req_logs = [line for line in logs if "CLIENT_DOWNLOAD_REQUEST" in line]
        self.assertEqual(len(client_req_logs), 1)

    def test_multi_stream_download_boundary(self):
        job_id = "test_multi_stream"
        captured_output = io.StringIO()
        old_stdout = sys.stdout
        try:
            sys.stdout = captured_output
            self.tracker.record_submission_received(job_id)
            self.tracker.record_worker_start(job_id)
            self.tracker.record_extraction_start(job_id)
            time.sleep(0.01)
            self.tracker.record_extraction_end(job_id)

            rec = self.tracker.get(job_id)
            self.assertIsNotNone(rec.t_media_download_start)

            # Stream 1 finished (video)
            time.sleep(0.02)
            self.tracker.record_media_download_end(job_id)
            t_end_1 = rec.t_media_download_end

            # Stream 2 finished (audio) - must update t_media_download_end
            time.sleep(0.02)
            self.tracker.record_media_download_end(job_id)
            t_end_2 = rec.t_media_download_end
            self.assertGreater(t_end_2, t_end_1)

            # Postprocess starts: finalizes media download end and starts postprocess
            time.sleep(0.01)
            self.tracker.record_postprocess_start(job_id)
            self.assertTrue(rec.media_download_finalized)
            self.assertLessEqual(rec.t_media_download_end, rec.t_postprocess_start)

            # Postprocess ends
            time.sleep(0.01)
            self.tracker.record_postprocess_end(job_id)
            self.assertGreater(rec.t_postprocess_end, rec.t_postprocess_start)

            # Ready
            self.tracker.record_ready(job_id, "file_multi", 2048, "Multi Title")
        finally:
            sys.stdout = old_stdout

        logs = captured_output.getvalue().splitlines()
        # Verify MEDIA_DOWNLOAD_END is logged exactly once
        media_end_logs = [line for line in logs if "MEDIA_DOWNLOAD_END" in line]
        self.assertEqual(len(media_end_logs), 1)

        # Verify POSTPROCESS_START is logged exactly once
        pp_start_logs = [line for line in logs if "POSTPROCESS_START" in line]
        self.assertEqual(len(pp_start_logs), 1)

    def test_single_stream_no_postprocess(self):
        job_id = "test_single_stream"
        captured_output = io.StringIO()
        old_stdout = sys.stdout
        try:
            sys.stdout = captured_output
            self.tracker.record_submission_received(job_id)
            self.tracker.record_worker_start(job_id)
            self.tracker.record_extraction_start(job_id)
            self.tracker.record_extraction_end(job_id)

            time.sleep(0.01)
            self.tracker.record_media_download_end(job_id)

            # Single stream download without postprocessor goes straight to ready
            self.tracker.record_ready(job_id, "file_single", 1024, "Single Title")
        finally:
            sys.stdout = old_stdout

        rec = self.tracker.get(job_id)
        self.assertTrue(rec.media_download_finalized)
        self.assertIsNone(rec.t_postprocess_start)

        logs = captured_output.getvalue().splitlines()
        media_end_logs = [line for line in logs if "MEDIA_DOWNLOAD_END" in line]
        self.assertEqual(len(media_end_logs), 1)

    def test_summary_no_double_counting(self):
        job_id = "test_summary_dc"
        captured_output = io.StringIO()
        old_stdout = sys.stdout
        try:
            sys.stdout = captured_output
            self.tracker.record_submission_received(job_id)
            self.tracker.record_queued(job_id)
            self.tracker.record_worker_start(job_id)
            self.tracker.record_extraction_start(job_id)
            self.tracker.record_extraction_end(job_id)
            self.tracker.record_media_download_end(job_id)
            self.tracker.record_postprocess_start(job_id)
            self.tracker.record_postprocess_end(job_id)
            self.tracker.record_ready(job_id, "f_dc", 5000, "Title DC")
            self.tracker.record_download_request(job_id)
            self.tracker.record_delivery_end(job_id, 5000)
        finally:
            sys.stdout = old_stdout

        output = captured_output.getvalue()
        # Ensure Job READY line displays incremental staging duration and cumulative elapsed duration
        self.assertIn("7. Job READY:", output)
        self.assertIn("[Assembly & Tiering]", output)
        self.assertIn("SERVER PREPARATION ELAPSED:", output)
        self.assertIn("TOTAL SERVER ELAPSED:", output)


if __name__ == "__main__":
    unittest.main()
